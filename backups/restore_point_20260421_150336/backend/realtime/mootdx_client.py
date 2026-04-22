"""
Mootdx realtime market client wrapper.

Features:
- stock tick quotes
- index tick quotes (dedicated API to avoid stock/index code collision)
- minute bars
- transactions
- safe fallback (empty or mock)
"""
from __future__ import annotations

from typing import Optional
import datetime as _dt
import random
import time

from loguru import logger

try:
    from config import REALTIME_ALLOW_MOCK_FALLBACK as _ALLOW_MOCK_FALLBACK
except Exception:
    _ALLOW_MOCK_FALLBACK = False

try:
    from mootdx.quotes import Quotes  # type: ignore
    _MOOTDX_AVAILABLE = True
except ImportError:
    _MOOTDX_AVAILABLE = False
    if _ALLOW_MOCK_FALLBACK:
        logger.warning("mootdx not installed, using mock fallback")
    else:
        logger.warning("mootdx not installed, mock fallback disabled")


def _split_ts_code(ts_code: str) -> tuple[int, str]:
    """Convert 600519.SH to (1, 600519), 000001.SZ to (0, 000001)."""
    code, suffix = ts_code.split(".")
    market = 1 if suffix.upper() == "SH" else 0
    return market, code


def _parse_dt_to_ts(dt_val) -> int:
    if dt_val is None:
        return int(time.time())
    s = str(dt_val).strip()
    if not s:
        return int(time.time())
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return int(_dt.datetime.strptime(s, fmt).timestamp())
        except Exception:
            continue
    return int(time.time())


class MootdxClient:
    _instance: Optional["MootdxClient"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._client = None
        return cls._instance

    def _ensure_client(self):
        if not _MOOTDX_AVAILABLE:
            return None
        if self._client is None:
            try:
                self._client = Quotes.factory(market="std")
                logger.info("mootdx client initialized")
            except Exception as e:
                logger.error(f"mootdx init failed: {e}")
                self._client = None
        return self._client

    @staticmethod
    def _empty_tick(ts_code: str) -> dict:
        return {
            "ts_code": ts_code,
            "name": "",
            "price": 0.0,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "pre_close": 0.0,
            "volume": 0,
            "amount": 0.0,
            "pct_chg": 0.0,
            "timestamp": int(time.time()),
            "bids": [],
            "asks": [],
            "is_mock": False,
            "data_unavailable": True,
        }

    def _fallback_tick(self, ts_code: str) -> dict:
        return self._mock_tick(ts_code) if _ALLOW_MOCK_FALLBACK else self._empty_tick(ts_code)

    def _fallback_minute_bars(self, ts_code: str, n: int) -> list[dict]:
        return self._mock_minute_bars(ts_code, n) if _ALLOW_MOCK_FALLBACK else []

    def _fallback_transactions(self, ts_code: str, n: int) -> list[dict]:
        return self._mock_transactions(ts_code, n) if _ALLOW_MOCK_FALLBACK else []

    @staticmethod
    def _row_to_tick(ts_code: str, row: dict) -> dict:
        pre_close = float(row.get("last_close", 0) or 0)
        price = float(row.get("price", 0) or 0)
        ts = _parse_dt_to_ts(row.get("datetime"))
        return {
            "ts_code": ts_code,
            "name": row.get("name", "") or "",
            "price": price,
            "open": float(row.get("open", 0) or 0),
            "high": float(row.get("high", 0) or 0),
            "low": float(row.get("low", 0) or 0),
            "pre_close": pre_close,
            "volume": int(row.get("vol", 0) or 0),
            "amount": float(row.get("amount", 0) or 0),
            "pct_chg": ((price - pre_close) / pre_close * 100) if pre_close > 0 else 0.0,
            "timestamp": ts,
            "bids": [(float(row.get(f"bid{i}", 0) or 0), int(row.get(f"bid_vol{i}", 0) or 0)) for i in range(1, 6)],
            "asks": [(float(row.get(f"ask{i}", 0) or 0), int(row.get(f"ask_vol{i}", 0) or 0)) for i in range(1, 6)],
        }

    def get_ticks(self, ts_codes: list[str]) -> dict[str, dict]:
        if not ts_codes:
            return {}
        client = self._ensure_client()
        if client is None:
            return {tc: self._fallback_tick(tc) for tc in ts_codes}

        out: dict[str, dict] = {}
        market_groups: dict[int, list[str]] = {}
        for ts in ts_codes:
            market, code = _split_ts_code(ts)
            market_groups.setdefault(market, []).append(code)

        for market, codes in market_groups.items():
            try:
                df = client.quotes(symbol=codes, market=market)
                if df is None or len(df) == 0:
                    continue
                for _, row in df.iterrows():
                    rd = row.to_dict()
                    code = str(rd.get("code", rd.get("symbol", "")) or "").zfill(6)
                    if not code:
                        continue
                    suffix = "SH" if market == 1 else "SZ"
                    ts = f"{code}.{suffix}"
                    out[ts] = self._row_to_tick(ts, rd)
            except Exception as e:
                logger.debug(f"get_ticks batch failed market={market}: {e}")

        for ts in ts_codes:
            if ts in out:
                continue
            try:
                market, code = _split_ts_code(ts)
                df = client.quotes(symbol=code, market=market)
                if df is None or len(df) == 0:
                    out[ts] = self._fallback_tick(ts)
                    continue
                out[ts] = self._row_to_tick(ts, df.iloc[0].to_dict())
            except Exception:
                out[ts] = self._fallback_tick(ts)
        return out

    def get_tick(self, ts_code: str) -> dict:
        try:
            return self.get_ticks([ts_code]).get(ts_code, self._fallback_tick(ts_code))
        except Exception as e:
            logger.error(f"get_tick({ts_code}) failed: {e}")
            return self._fallback_tick(ts_code)

    def get_index_tick(self, ts_code: str) -> dict:
        """Fetch index quote via index API (correct for 000001/000300 etc.)."""
        client = self._ensure_client()
        if client is None:
            return self._fallback_tick(ts_code)
        try:
            market, code = _split_ts_code(ts_code)
            df_1m = client.index(symbol=code, market=market, frequency=8, offset=2)
            df_1d = client.index(symbol=code, market=market, frequency=9, offset=2)
            if (df_1m is None or len(df_1m) == 0) and (df_1d is None or len(df_1d) == 0):
                return self._fallback_tick(ts_code)

            row_1m = df_1m.iloc[-1].to_dict() if (df_1m is not None and len(df_1m) > 0) else {}
            row_1d_last = df_1d.iloc[-1].to_dict() if (df_1d is not None and len(df_1d) > 0) else {}
            row_1d_prev = df_1d.iloc[-2].to_dict() if (df_1d is not None and len(df_1d) > 1) else {}

            price = float(row_1m.get("close", row_1d_last.get("close", 0)) or 0)
            pre_close = float(row_1d_prev.get("close", row_1d_last.get("open", 0)) or 0)
            pct = ((price - pre_close) / pre_close * 100) if pre_close > 0 else 0.0
            ts = _parse_dt_to_ts(row_1m.get("datetime", row_1d_last.get("datetime")))

            return {
                "ts_code": ts_code,
                "name": "",
                "price": price,
                "open": float(row_1m.get("open", row_1d_last.get("open", 0)) or 0),
                "high": float(row_1m.get("high", row_1d_last.get("high", 0)) or 0),
                "low": float(row_1m.get("low", row_1d_last.get("low", 0)) or 0),
                "pre_close": pre_close,
                "volume": int(row_1m.get("volume", row_1d_last.get("volume", 0)) or 0),
                "amount": float(row_1m.get("amount", row_1d_last.get("amount", 0)) or 0),
                "pct_chg": pct,
                "timestamp": ts,
                "bids": [],
                "asks": [],
                "is_mock": False,
            }
        except Exception as e:
            logger.error(f"get_index_tick({ts_code}) failed: {e}")
            return self._fallback_tick(ts_code)

    def get_minute_bars(self, ts_code: str, n: int = 240) -> list[dict]:
        client = self._ensure_client()
        if client is None:
            return self._fallback_minute_bars(ts_code, n)
        try:
            market, code = _split_ts_code(ts_code)
            df = client.bars(symbol=code, market=market, frequency=8, offset=n)
            if df is None or len(df) == 0:
                return self._fallback_minute_bars(ts_code, n)
            records = df.to_dict(orient="records")
            return [
                {
                    "time": str(r.get("datetime", "")),
                    "open": float(r.get("open", 0) or 0),
                    "high": float(r.get("high", 0) or 0),
                    "low": float(r.get("low", 0) or 0),
                    "close": float(r.get("close", 0) or 0),
                    "volume": int(r.get("vol", 0) or 0),
                    "amount": float(r.get("amount", 0) or 0),
                }
                for r in records
            ]
        except Exception as e:
            logger.error(f"get_minute_bars({ts_code}) failed: {e}")
            return self._fallback_minute_bars(ts_code, n)

    def get_transactions(self, ts_code: str, n: int = 50) -> list[dict]:
        client = self._ensure_client()
        if client is None:
            return self._fallback_transactions(ts_code, n)
        try:
            market, code = _split_ts_code(ts_code)
            df = client.transaction(symbol=code, market=market, offset=n)
            if df is None or len(df) == 0:
                return self._fallback_transactions(ts_code, n)
            records = df.to_dict(orient="records")
            return [
                {
                    "time": str(r.get("time", "")),
                    "price": float(r.get("price", 0) or 0),
                    "volume": int(r.get("vol", 0) or 0),
                    "direction": int(r.get("buyorsell", 0) or 0),
                }
                for r in records
            ]
        except Exception as e:
            logger.error(f"get_transactions({ts_code}) failed: {e}")
            return self._fallback_transactions(ts_code, n)

    @staticmethod
    def _mock_tick(ts_code: str) -> dict:
        base = 10 + random.uniform(-2, 2)
        return {
            "ts_code": ts_code,
            "name": "MOCK",
            "price": round(base, 2),
            "open": round(base * 0.99, 2),
            "high": round(base * 1.02, 2),
            "low": round(base * 0.97, 2),
            "pre_close": round(base * 0.98, 2),
            "volume": random.randint(10000, 1000000),
            "amount": random.uniform(1e6, 1e8),
            "pct_chg": round(random.uniform(-3, 3), 2),
            "timestamp": int(time.time()),
            "bids": [(round(base - i * 0.01, 2), random.randint(100, 5000)) for i in range(1, 6)],
            "asks": [(round(base + i * 0.01, 2), random.randint(100, 5000)) for i in range(1, 6)],
            "is_mock": True,
        }

    @staticmethod
    def _mock_minute_bars(ts_code: str, n: int) -> list[dict]:
        base = 10.0
        bars = []
        for i in range(n):
            base += random.uniform(-0.05, 0.05)
            bars.append(
                {
                    "time": f"09:{30 + i // 60:02d}:{i % 60:02d}",
                    "open": round(base, 2),
                    "high": round(base + 0.02, 2),
                    "low": round(base - 0.02, 2),
                    "close": round(base + random.uniform(-0.02, 0.02), 2),
                    "volume": random.randint(1000, 50000),
                    "amount": random.uniform(1e4, 1e6),
                }
            )
        return bars

    @staticmethod
    def _mock_transactions(ts_code: str, n: int) -> list[dict]:
        base = 10.0
        return [
            {
                "time": f"14:{30 + i // 60:02d}:{i % 60:02d}",
                "price": round(base + random.uniform(-0.05, 0.05), 2),
                "volume": random.randint(100, 5000),
                "direction": random.choice([0, 1]),
            }
            for i in range(n)
        ]


client = MootdxClient()

