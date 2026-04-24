from __future__ import annotations

import datetime as dt
import logging
import random
import threading
import time
from typing import Literal, Optional

import pandas as pd
import requests

BoardType = Literal["concept", "industry"]


class EastMoneyBoardService:
    URL = "https://push2.eastmoney.com/api/qt/clist/get"
    BOARD_FS = {
        "concept": "m:90 t:3 f:!50",
        "industry": "m:90 t:2 f:!50",
    }
    BOARD_TYPE_LABEL = {
        "concept": "概念板块",
        "industry": "行业板块",
    }
    BOARD_COLUMNS = [
        "collected_at",
        "collected_at_iso",
        "board_type",
        "board_type_name",
        "board_code",
        "board_name",
        "latest_price",
        "pct_chg",
        "change_amount",
        "volume",
        "amount",
        "turnover",
        "total_mv",
        "up_count",
        "down_count",
        "breadth_ratio",
        "leader_name",
        "leader_code",
        "leader_market",
        "leader_ts_code",
        "leader_pct",
    ]
    MEMBER_COLUMNS = [
        "collected_at",
        "collected_at_iso",
        "board_type",
        "board_type_name",
        "board_code",
        "board_name",
        "stock_code",
        "symbol",
        "market",
        "ts_code",
        "stock_name",
        "latest_price",
        "pct_chg",
        "change_amount",
        "amplitude",
        "turnover",
        "volume_ratio",
        "volume",
        "amount",
        "open_price",
        "high_price",
        "low_price",
        "pre_close",
        "total_mv",
        "float_mv",
        "pe_dynamic",
        "pe_ttm",
        "pb",
        "main_net_inflow",
    ]
    _BOARD_FIELDS = (
        "f2,f3,f4,f5,f6,f8,"
        "f12,f14,f20,"
        "f104,f105,"
        "f128,f136,f140,f141"
    )
    _MEMBER_FIELDS = (
        "f2,f3,f4,f5,f6,f7,f8,f9,f10,"
        "f12,f13,f14,"
        "f15,f16,f17,f18,"
        "f20,f21,f23,f62,f115"
    )

    def __init__(
        self,
        *,
        min_request_interval: float = 0.8,
        timeout: int = 10,
        max_retries: int = 3,
        snapshot_cache_seconds: int = 5,
    ) -> None:
        self.min_request_interval = float(min_request_interval or 0.8)
        self.timeout = int(timeout or 10)
        self.max_retries = int(max_retries or 3)
        self.snapshot_cache_seconds = int(snapshot_cache_seconds or 5)
        self._last_request_time = 0.0
        self._request_lock = threading.Lock()
        self._snapshot_cache = pd.DataFrame()
        self._snapshot_cache_at = 0.0
        self._members_cache = pd.DataFrame()
        self._members_cache_at = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/132.0.0.0 Safari/537.36"
                ),
                "Referer": "https://quote.eastmoney.com/center/boardlist.html",
                "Accept": "application/json,text/plain,*/*",
            }
        )

    @staticmethod
    def normalize_stock_code(raw: object) -> str:
        text = str(raw or "").strip().upper()
        if not text:
            return ""
        if "." in text:
            code, market = text.split(".", 1)
            digits = "".join(ch for ch in code if ch.isdigit()).zfill(6)
            return f"{digits}.{market}"
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return ""
        digits = digits[-6:].zfill(6)
        if digits.startswith("6"):
            return f"{digits}.SH"
        if digits.startswith(("4", "8")):
            return f"{digits}.BJ"
        return f"{digits}.SZ"

    @staticmethod
    def normalize_board_code(raw: object) -> str:
        text = str(raw or "").strip().upper()
        if not text:
            return ""
        if text.startswith("BK"):
            digits = "".join(ch for ch in text[2:] if ch.isdigit())
            return f"BK{digits.zfill(4)}" if digits else text
        digits = "".join(ch for ch in text if ch.isdigit())
        return f"BK{digits.zfill(4)}" if digits else text

    @staticmethod
    def _safe_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def _build_stock_code(raw_code: object, raw_market: object) -> str:
        code = str(raw_code or "").strip()
        if not code or code == "-":
            return ""
        digits = "".join(ch for ch in code if ch.isdigit())
        if not digits:
            return ""
        digits = digits[-6:].zfill(6)
        try:
            market = int(raw_market)
        except Exception:
            market = None
        if market == 0:
            return f"{digits}.SZ"
        if market == 1:
            return f"{digits}.SH"
        if market == 2:
            return f"{digits}.BJ"
        return EastMoneyBoardService.normalize_stock_code(digits)

    @staticmethod
    def _normalize_board_type(value: object, fallback: BoardType = "concept") -> BoardType:
        text = str(value or "").strip().lower()
        if text in {"industry", "行业板块", "行业"}:
            return "industry"
        if text in {"concept", "概念板块", "概念"}:
            return "concept"
        return fallback

    def _rate_limit(self) -> None:
        with self._request_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self.min_request_interval:
                time.sleep(self.min_request_interval - elapsed)
            time.sleep(random.uniform(0.05, 0.2))
            self._last_request_time = time.time()

    def _request_clist(self, params: dict[str, str]) -> dict:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._rate_limit()
                response = self.session.get(self.URL, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                payload = data.get("data") if isinstance(data, dict) else None
                if not payload:
                    raise RuntimeError(f"EastMoney board response missing data: {data}")
                return data
            except Exception as exc:
                last_error = exc
                wait_seconds = min(3 * attempt, 15)
                logging.warning(
                    "EastMoney board request failed, retry %s/%s after %ss: %s",
                    attempt,
                    self.max_retries,
                    wait_seconds,
                    exc,
                )
                time.sleep(wait_seconds)
        raise RuntimeError(f"EastMoney board request failed: {last_error}")

    def _fetch_paged(
        self,
        *,
        fs: str,
        fields: str,
        page_size: int = 500,
        sort_field: str = "f3",
    ) -> pd.DataFrame:
        all_rows: list[dict] = []
        page = 1
        total = None
        while True:
            params = {
                "pn": str(page),
                "pz": str(page_size),
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "fid": sort_field,
                "fs": fs,
                "fields": fields,
                "_": str(int(time.time() * 1000)),
            }
            payload = (self._request_clist(params).get("data") or {})
            rows = payload.get("diff") or []
            total = payload.get("total", total)
            if not rows:
                break
            all_rows.extend(rows)
            if total is not None and len(all_rows) >= int(total):
                break
            if len(rows) < page_size:
                break
            page += 1
        return pd.DataFrame(all_rows)

    def fetch_boards(self, board_type: BoardType) -> pd.DataFrame:
        board_type = self._normalize_board_type(board_type, fallback="concept")
        raw_df = self._fetch_paged(
            fs=self.BOARD_FS[board_type],
            fields=self._BOARD_FIELDS,
            page_size=500,
            sort_field="f3",
        )
        if raw_df.empty:
            return pd.DataFrame(columns=self.BOARD_COLUMNS)

        collected_at = pd.Timestamp.now()
        df = pd.DataFrame(
            {
                "collected_at": collected_at,
                "collected_at_iso": collected_at.isoformat(timespec="seconds"),
                "board_type": board_type,
                "board_type_name": self.BOARD_TYPE_LABEL[board_type],
                "board_code": raw_df.get("f12", "").map(self.normalize_board_code),
                "board_name": raw_df.get("f14", "").astype(str).str.strip(),
                "latest_price": raw_df.get("f2"),
                "pct_chg": raw_df.get("f3"),
                "change_amount": raw_df.get("f4"),
                "volume": raw_df.get("f5"),
                "amount": raw_df.get("f6"),
                "turnover": raw_df.get("f8"),
                "total_mv": raw_df.get("f20"),
                "up_count": raw_df.get("f104"),
                "down_count": raw_df.get("f105"),
                "leader_name": raw_df.get("f128", "").astype(str).str.strip(),
                "leader_code": raw_df.get("f140", "").astype(str).str.strip(),
                "leader_market": raw_df.get("f141"),
                "leader_pct": raw_df.get("f136"),
            }
        )
        df = self._safe_numeric(
            df,
            [
                "latest_price",
                "pct_chg",
                "change_amount",
                "volume",
                "amount",
                "turnover",
                "total_mv",
                "up_count",
                "down_count",
                "leader_pct",
            ],
        )
        total_count = df["up_count"].fillna(0) + df["down_count"].fillna(0)
        df["breadth_ratio"] = (df["up_count"] / total_count.where(total_count != 0)).round(4)
        df["leader_ts_code"] = df.apply(
            lambda row: self._build_stock_code(row.get("leader_code"), row.get("leader_market")),
            axis=1,
        )
        df = df[self.BOARD_COLUMNS].sort_values(["pct_chg", "amount"], ascending=[False, False], na_position="last")
        return df.reset_index(drop=True)

    def fetch_all_boards_snapshot(self) -> pd.DataFrame:
        concept_df = self.fetch_boards("concept")
        industry_df = self.fetch_boards("industry")
        frames = [df for df in [concept_df, industry_df] if not df.empty]
        if not frames:
            return pd.DataFrame(columns=self.BOARD_COLUMNS)
        return pd.concat(frames, ignore_index=True)

    def get_board_snapshot(self, force: bool = False) -> pd.DataFrame:
        now = time.time()
        if (
            not force
            and not self._snapshot_cache.empty
            and (now - self._snapshot_cache_at) <= self.snapshot_cache_seconds
        ):
            return self._snapshot_cache.copy()
        df = self.fetch_all_boards_snapshot()
        self._snapshot_cache = df.copy()
        self._snapshot_cache_at = time.time()
        return df.copy()

    def fetch_board_members(
        self,
        board_code: object,
        board_name: object = "",
        board_type: object = "concept",
    ) -> pd.DataFrame:
        normalized_code = self.normalize_board_code(board_code)
        if not normalized_code:
            return pd.DataFrame(columns=self.MEMBER_COLUMNS)
        board_type_norm = self._normalize_board_type(board_type, fallback="concept")
        raw_df = self._fetch_paged(
            fs=f"b:{normalized_code}",
            fields=self._MEMBER_FIELDS,
            page_size=500,
            sort_field="f3",
        )
        if raw_df.empty:
            return pd.DataFrame(columns=self.MEMBER_COLUMNS)

        collected_at = pd.Timestamp.now()
        df = pd.DataFrame(
            {
                "collected_at": collected_at,
                "collected_at_iso": collected_at.isoformat(timespec="seconds"),
                "board_type": board_type_norm,
                "board_type_name": self.BOARD_TYPE_LABEL[board_type_norm],
                "board_code": normalized_code,
                "board_name": str(board_name or "").strip(),
                "stock_code": raw_df.get("f12", "").astype(str).str.strip().str.zfill(6),
                "symbol": raw_df.get("f12", "").astype(str).str.strip().str.zfill(6),
                "market": raw_df.get("f13"),
                "stock_name": raw_df.get("f14", "").astype(str).str.strip(),
                "latest_price": raw_df.get("f2"),
                "pct_chg": raw_df.get("f3"),
                "change_amount": raw_df.get("f4"),
                "amplitude": raw_df.get("f7"),
                "turnover": raw_df.get("f8"),
                "volume_ratio": raw_df.get("f10"),
                "volume": raw_df.get("f5"),
                "amount": raw_df.get("f6"),
                "open_price": raw_df.get("f17"),
                "high_price": raw_df.get("f15"),
                "low_price": raw_df.get("f16"),
                "pre_close": raw_df.get("f18"),
                "total_mv": raw_df.get("f20"),
                "float_mv": raw_df.get("f21"),
                "pe_dynamic": raw_df.get("f9"),
                "pe_ttm": raw_df.get("f115"),
                "pb": raw_df.get("f23"),
                "main_net_inflow": raw_df.get("f62"),
            }
        )
        df = self._safe_numeric(
            df,
            [
                "latest_price",
                "pct_chg",
                "change_amount",
                "amplitude",
                "turnover",
                "volume_ratio",
                "volume",
                "amount",
                "open_price",
                "high_price",
                "low_price",
                "pre_close",
                "total_mv",
                "float_mv",
                "pe_dynamic",
                "pe_ttm",
                "pb",
                "main_net_inflow",
            ],
        )
        df["ts_code"] = df.apply(
            lambda row: self._build_stock_code(row.get("stock_code"), row.get("market")),
            axis=1,
        )
        df = df[self.MEMBER_COLUMNS].sort_values(["pct_chg", "amount"], ascending=[False, False], na_position="last")
        return df.reset_index(drop=True)

    def fetch_members_for_boards(
        self,
        boards_df: pd.DataFrame,
        *,
        max_boards: Optional[int] = None,
    ) -> pd.DataFrame:
        if boards_df is None or boards_df.empty:
            return pd.DataFrame(columns=self.MEMBER_COLUMNS)
        work_df = boards_df.copy()
        if "board_code" not in work_df.columns:
            if "板块代码" in work_df.columns:
                work_df["board_code"] = work_df["板块代码"]
            else:
                return pd.DataFrame(columns=self.MEMBER_COLUMNS)
        if "board_name" not in work_df.columns:
            if "板块名称" in work_df.columns:
                work_df["board_name"] = work_df["板块名称"]
            else:
                work_df["board_name"] = ""
        if "board_type" not in work_df.columns:
            if "board_type_name" in work_df.columns:
                work_df["board_type"] = work_df["board_type_name"].map(
                    lambda x: self._normalize_board_type(x, fallback="concept")
                )
            elif "板块类型" in work_df.columns:
                work_df["board_type"] = work_df["板块类型"].map(
                    lambda x: self._normalize_board_type(x, fallback="concept")
                )
            else:
                work_df["board_type"] = "concept"
        work_df = work_df[["board_type", "board_code", "board_name"]].drop_duplicates()
        if max_boards is not None:
            work_df = work_df.head(int(max_boards))

        frames: list[pd.DataFrame] = []
        for row in work_df.itertuples(index=False):
            board_type = getattr(row, "board_type")
            board_code = getattr(row, "board_code")
            board_name = getattr(row, "board_name")
            try:
                member_df = self.fetch_board_members(
                    board_code=board_code,
                    board_name=board_name,
                    board_type=board_type,
                )
                if not member_df.empty:
                    frames.append(member_df)
            except Exception as exc:
                logging.warning("Fetch EastMoney board members failed for %s %s %s: %s", board_type, board_code, board_name, exc)
        if not frames:
            return pd.DataFrame(columns=self.MEMBER_COLUMNS)
        result = pd.concat(frames, ignore_index=True)
        self._members_cache = result.copy()
        self._members_cache_at = time.time()
        return result

    def to_board_table(self, boards_df: pd.DataFrame, board_type: BoardType) -> pd.DataFrame:
        board_type = self._normalize_board_type(board_type, fallback="concept")
        if boards_df is None or boards_df.empty:
            columns = [
                "board_type",
                "board_type_name",
                "board_code",
                "board_name",
                "板块代码",
                "板块名称",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "换手率",
                "总市值",
                "上涨家数",
                "下跌家数",
                "上涨占比",
                "领涨股票",
                "领涨股票代码",
                "领涨股票完整代码",
                "领涨股票-涨跌幅",
                "source",
                "synced_at",
            ]
            if board_type == "concept":
                columns.extend(["concept_code", "concept_name"])
            else:
                columns.extend(["industry_code", "industry_name"])
            return pd.DataFrame(columns=columns)

        df = boards_df.copy()
        synced_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = pd.DataFrame(
            {
                "board_type": board_type,
                "board_type_name": self.BOARD_TYPE_LABEL[board_type],
                "board_code": df["board_code"].map(self.normalize_board_code),
                "board_name": df["board_name"].astype(str).str.strip(),
                "板块代码": df["board_code"].map(self.normalize_board_code),
                "板块名称": df["board_name"].astype(str).str.strip(),
                "最新价": df["latest_price"],
                "涨跌幅": df["pct_chg"],
                "涨跌额": df["change_amount"],
                "成交量": df["volume"],
                "成交额": df["amount"],
                "换手率": df["turnover"],
                "总市值": df["total_mv"],
                "上涨家数": df["up_count"],
                "下跌家数": df["down_count"],
                "上涨占比": df["breadth_ratio"],
                "领涨股票": df["leader_name"].astype(str).str.strip(),
                "领涨股票代码": df["leader_code"].astype(str).str.strip(),
                "领涨股票完整代码": df["leader_ts_code"].astype(str).str.strip(),
                "领涨股票-涨跌幅": df["leader_pct"],
                "source": f"eastmoney_push2_{board_type}_board",
                "synced_at": synced_at,
            }
        )
        if board_type == "concept":
            out["concept_code"] = out["board_code"]
            out["concept_name"] = out["board_name"]
        else:
            out["industry_code"] = out["board_code"]
            out["industry_name"] = out["board_name"]
        return out.reset_index(drop=True)

    def to_member_table(self, members_df: pd.DataFrame, board_type: BoardType) -> pd.DataFrame:
        board_type = self._normalize_board_type(board_type, fallback="concept")
        if members_df is None or members_df.empty:
            if board_type == "concept":
                return pd.DataFrame(
                    columns=[
                        "concept_code",
                        "concept_name",
                        "板块代码",
                        "板块名称",
                        "代码",
                        "symbol",
                        "ts_code",
                        "名称",
                        "stock_name",
                        "source",
                        "synced_at",
                    ]
                )
            return pd.DataFrame(
                columns=[
                    "industry_code",
                    "industry_name",
                    "板块代码",
                    "板块名称",
                    "代码",
                    "symbol",
                    "ts_code",
                    "名称",
                    "stock_name",
                    "source",
                    "synced_at",
                ]
            )

        df = members_df.copy()
        synced_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = pd.DataFrame(
            {
                "板块代码": df["board_code"].map(self.normalize_board_code),
                "板块名称": df["board_name"].astype(str).str.strip(),
                "代码": df["stock_code"].astype(str).str.strip().str.zfill(6),
                "symbol": df["symbol"].astype(str).str.strip().str.zfill(6),
                "ts_code": df["ts_code"].astype(str).str.strip(),
                "名称": df["stock_name"].astype(str).str.strip(),
                "stock_name": df["stock_name"].astype(str).str.strip(),
                "最新价": df["latest_price"],
                "涨跌幅": df["pct_chg"],
                "涨跌额": df["change_amount"],
                "振幅": df["amplitude"],
                "换手率": df["turnover"],
                "量比": df["volume_ratio"],
                "成交量": df["volume"],
                "成交额": df["amount"],
                "开盘价": df["open_price"],
                "最高价": df["high_price"],
                "最低价": df["low_price"],
                "昨收": df["pre_close"],
                "总市值": df["total_mv"],
                "流通市值": df["float_mv"],
                "市盈率-动态": df["pe_dynamic"],
                "市盈率-TTM": df["pe_ttm"],
                "市净率": df["pb"],
                "主力净流入": df["main_net_inflow"],
                "source": f"eastmoney_push2_{board_type}_member",
                "synced_at": synced_at,
            }
        )
        if board_type == "concept":
            out["concept_code"] = out["板块代码"]
            out["concept_name"] = out["板块名称"]
        else:
            out["industry_code"] = out["板块代码"]
            out["industry_name"] = out["板块名称"]
        out = out.drop_duplicates(subset=["ts_code", "板块代码"], keep="last")
        return out.reset_index(drop=True)


_SERVICE: Optional[EastMoneyBoardService] = None
_SERVICE_LOCK = threading.Lock()


def get_eastmoney_board_service() -> EastMoneyBoardService:
    global _SERVICE
    with _SERVICE_LOCK:
        if _SERVICE is None:
            _SERVICE = EastMoneyBoardService()
        return _SERVICE


__all__ = [
    "BoardType",
    "EastMoneyBoardService",
    "get_eastmoney_board_service",
]
