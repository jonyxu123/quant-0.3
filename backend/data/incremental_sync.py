"""日常增量数据同步"""

from scripts.akshare_proxy_patch_free import install_patch
install_patch(proxy_api_scheme="socks5h", proxy_api_url="http://bapi.51daili.com/getapi2?linePoolIndex=0,1&packid=2&time=1&qty=1&port=2&format=txt&dt=4&ct=1&dtc=1&usertype=17&uid=42083&accessName=sword721&accessPassword=CE2BFE18E746F92ECDB5479063290EAE&skey=autoaddwhiteip",
    log_file="akshare_proxy_patch.log",
    log_console=False,
    hook_domains=[
    "fund.eastmoney.com",
    "push2.eastmoney.com",
    "push2his.eastmoney.com",
    "emweb.securities.eastmoney.com",
    ],)
import tushare as ts
import pandas as pd
import time
import queue
import threading
from datetime import datetime, timedelta
from loguru import logger
from config import DB_PATH, TUSHARE_TOKEN
from backend.realtime.eastmoney_board_service import get_eastmoney_board_service
from backend.data.duckdb_manager import open_duckdb_conn


def _normalize_code6_local(raw: object) -> str:
    s = str(raw or "").strip().upper()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 6:
        digits = digits.zfill(6)
    elif len(digits) > 6:
        digits = digits[-6:]
    return digits


def _guess_ts_code_from_code6_local(code6: object) -> str:
    code6 = _normalize_code6_local(code6)
    if not code6:
        return ""
    if code6.startswith(("4", "8")):
        return f"{code6}.BJ"
    if code6.startswith(("5", "6", "9")):
        return f"{code6}.SH"
    return f"{code6}.SZ"


def _normalize_em_board_code_local(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("BK"):
        digits = "".join(ch for ch in s[2:] if ch.isdigit())
        return f"BK{digits.zfill(4)}" if digits else s
    digits = "".join(ch for ch in s if ch.isdigit())
    return f"BK{digits.zfill(4)}" if digits else s



class IncrementalSync:
    """日常增量数据同步（断点续传，自动检测最新日期）"""

    def __init__(self, db_path=DB_PATH, token=TUSHARE_TOKEN):
        self.pro = ts.pro_api(token)
        self.db_path = db_path
        self.conn = open_duckdb_conn(db_path, retries=5, base_sleep_sec=0.08)
        # stk_factor_pro can exceed one page on a single trade_date.
        self.stk_factor_page_size = 5000
        self.stk_factor_repair_threshold = 0.90
        # Incremental sync dedicated writer thread (separate from realtime writer).
        self._write_queue: "queue.Queue" = queue.Queue()
        self._writer_stop = False
        self._writer_thread = threading.Thread(
            target=self._write_loop, daemon=True, name="incremental-sync-writer"
        )
        self._writer_thread.start()

    def _write_loop(self):
        wconn = open_duckdb_conn(self.db_path, retries=5, base_sleep_sec=0.08)
        while True:
            task = self._write_queue.get()
            if task is None:
                self._write_queue.task_done()
                break
            event = task.get("event")
            try:
                kind = task.get("kind")
                if kind == "append_df":
                    table = task["table"]
                    df = task["df"]
                    tmp_name = "_tmp_inc_writer"
                    wconn.register(tmp_name, df)
                    try:
                        wconn.execute("BEGIN")
                        try:
                            wconn.execute(f'INSERT INTO "{table}" SELECT * FROM {tmp_name}')
                        except Exception:
                            wconn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM {tmp_name}')
                        wconn.execute("COMMIT")
                    except Exception:
                        try:
                            wconn.execute("ROLLBACK")
                        except Exception:
                            pass
                        raise
                    finally:
                        try:
                            wconn.unregister(tmp_name)
                        except Exception:
                            pass
                elif kind == "delete_date":
                    table = task["table"]
                    date_col = task.get("date_col", "trade_date")
                    trade_date = task["trade_date"]
                    wconn.execute(f'DELETE FROM "{table}" WHERE "{date_col}" = ?', [trade_date])
                elif kind == "replace_table":
                    table = task["table"]
                    df = task["df"]
                    tmp_name = "_tmp_inc_writer_replace"
                    wconn.register(tmp_name, df)
                    try:
                        wconn.execute("BEGIN")
                        wconn.execute(f'DROP TABLE IF EXISTS "{table}"')
                        wconn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM {tmp_name}')
                        wconn.execute("COMMIT")
                    except Exception:
                        try:
                            wconn.execute("ROLLBACK")
                        except Exception:
                            pass
                        raise
                    finally:
                        try:
                            wconn.unregister(tmp_name)
                        except Exception:
                            pass
            except Exception as e:
                task["error"] = e
            finally:
                if event is not None:
                    event.set()
                self._write_queue.task_done()
        try:
            wconn.close()
        except Exception:
            pass

    def _submit_write(self, task: dict, wait: bool = True):
        if self._writer_stop:
            raise RuntimeError("incremental writer already stopped")
        event = threading.Event()
        task["event"] = event
        task["error"] = None
        self._write_queue.put(task)
        if wait:
            event.wait()
            if task["error"] is not None:
                raise task["error"]

    def close(self):
        if self._writer_stop:
            return
        self._writer_stop = True
        try:
            self._write_queue.put(None)
            self._writer_thread.join(timeout=5)
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  工具方法
    # ------------------------------------------------------------------ #

    def get_last_date(self, table, date_col='trade_date'):
        """获取指定表的最新日期，表不存在时返回 None"""
        try:
            result = pd.read_sql(f'SELECT MAX("{date_col}") as d FROM "{table}"', self.conn)
            val = result.iloc[0, 0]
            return str(val) if val is not None else None
        except Exception:
            return None

    def _get_pending_dates(self, table, date_col='trade_date'):
        """返回该表从最新日期次日到今天的待同步交易日列表"""
        today = datetime.now().strftime('%Y%m%d')
        last = self.get_last_date(table, date_col)
        if last is None:
            logger.warning(f"{table} 表不存在或为空，跳过增量同步")
            return []
        next_date = (datetime.strptime(last, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        if next_date > today:
            return []
        trade_cal = pd.read_sql(
            f"SELECT cal_date FROM trade_cal WHERE is_open=1"
            f" AND cal_date >= '{next_date}' AND cal_date <= '{today}'",
            self.conn
        )
        return trade_cal['cal_date'].tolist()

    def _safe_append(self, df, table):
        """安全追加写入（用原生 DuckDB 避免事务问题）"""
        if df is None or df.empty:
            return
        try:
            self._submit_write({"kind": "append_df", "table": table, "df": df}, wait=True)
        except Exception as e:
            logger.error(f"写入 {table} 失败: {e}")

    def _call(self, func, sleep=0.12, **kwargs):
        """调用 API，捕获异常返回空 DataFrame"""
        try:
            df = func(**kwargs)
            time.sleep(sleep)
            return df
        except Exception as e:
            logger.warning(f"{func.__name__ if hasattr(func, '__name__') else func} 失败: {e}")
            time.sleep(sleep)
            return pd.DataFrame()

    def _call_retry(self, func, sleep=0.12, retries=3, **kwargs):
        """Strict API call with retries. Returns None when all retries fail."""
        last_err = None
        for attempt in range(retries):
            try:
                df = func(**kwargs)
                time.sleep(sleep)
                return df
            except Exception as e:
                last_err = e
                logger.warning(
                    f"{func.__name__ if hasattr(func, '__name__') else func} 失败，重试 {attempt + 1}/{retries}: {e}"
                )
                time.sleep(max(sleep, 0.2 * (attempt + 1)))
        logger.error(f"{func.__name__ if hasattr(func, '__name__') else func} 最终失败: {last_err}")
        return None

    def _replace_trade_date_rows(self, table, trade_date, date_col='trade_date'):
        """Delete one trade_date partition before re-insert."""
        try:
            self._submit_write(
                {
                    "kind": "delete_date",
                    "table": table,
                    "trade_date": trade_date,
                    "date_col": date_col,
                },
                wait=True,
            )
        except Exception:
            pass

    def _count_distinct_ts(self, table, trade_date, date_col='trade_date'):
        try:
            row = self.conn.execute(
                f'SELECT COUNT(DISTINCT ts_code) FROM \"{table}\" WHERE \"{date_col}\" = ?',
                [trade_date]
            ).fetchone()
            return int(row[0] or 0) if row else 0
        except Exception:
            return 0

    def _fetch_stk_factor_pro_paged(self, trade_date):
        """Fetch stk_factor_pro by trade_date with explicit pagination."""
        parts = []
        offset = 0
        page = 0
        while True:
            page += 1
            df = self._call_retry(
                self.pro.stk_factor_pro,
                sleep=2.0,
                retries=3,
                trade_date=trade_date,
                limit=self.stk_factor_page_size,
                offset=offset,
            )
            if df is None:
                raise RuntimeError(f"stk_factor_pro page failed trade_date={trade_date} offset={offset}")
            if df.empty:
                break
            parts.append(df)
            rows = len(df)
            if rows < self.stk_factor_page_size:
                break
            offset += self.stk_factor_page_size
            if page >= 20:
                logger.warning(f"stk_factor_pro 分页达到上限 page={page}, trade_date={trade_date}")
                break
        if not parts:
            return pd.DataFrame()
        df_all = pd.concat(parts, ignore_index=True)
        if 'ts_code' in df_all.columns and 'trade_date' in df_all.columns:
            df_all = df_all.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
        return df_all

    def _sync_stk_factor_pro_for_date(self, trade_date, replace=False):
        """Sync one trade_date for stk_factor_pro with pagination."""
        try:
            df = self._fetch_stk_factor_pro_paged(trade_date)
            if df is None or df.empty:
                logger.warning(f"  stk_factor_pro: {trade_date} 无数据")
                return False, 0
            if replace:
                self._replace_trade_date_rows('stk_factor_pro', trade_date, date_col='trade_date')
            self._safe_append(df, 'stk_factor_pro')
            logger.info(f"  stk_factor_pro: {len(df)}条{'(replace)' if replace else ''}")
            return True, len(df)
        except Exception as e:
            logger.error(f"  stk_factor_pro 同步失败 {trade_date}: {e}")
            return False, 0

    def _repair_latest_stk_factor_pro(self):
        """
        Repair latest trade_date when stk_factor_pro coverage is significantly lower
        than daily coverage (common symptom of missing pagination).
        """
        latest_daily = self.get_last_date('daily', 'trade_date')
        if not latest_daily:
            return
        daily_cnt = self._count_distinct_ts('daily', latest_daily, 'trade_date')
        stk_cnt = self._count_distinct_ts('stk_factor_pro', latest_daily, 'trade_date')
        if daily_cnt <= 0:
            return
        ratio = stk_cnt / daily_cnt
        logger.info(
            f"stk_factor_pro 覆盖检查 {latest_daily}: daily={daily_cnt}, stk_factor_pro={stk_cnt}, ratio={ratio:.2%}"
        )
        if ratio >= self.stk_factor_repair_threshold:
            return
        logger.warning(
            f"检测到 stk_factor_pro 覆盖不足（{ratio:.2%} < {self.stk_factor_repair_threshold:.0%}），"
            f"开始修复 trade_date={latest_daily}"
        )
        ok, _ = self._sync_stk_factor_pro_for_date(latest_daily, replace=True)
        if ok:
            new_cnt = self._count_distinct_ts('stk_factor_pro', latest_daily, 'trade_date')
            new_ratio = new_cnt / daily_cnt if daily_cnt > 0 else 0.0
            logger.info(
                f"stk_factor_pro 修复后 {latest_daily}: daily={daily_cnt}, stk_factor_pro={new_cnt}, ratio={new_ratio:.2%}"
            )

    # ------------------------------------------------------------------ #
    #  每日类接口（按 trade_date 逐日同步）
    # ------------------------------------------------------------------ #

    def _sync_one_date(self, trade_date):
        """同步单个交易日的所有每日类数据"""
        logger.info(f"增量同步交易日: {trade_date}")
        ok = True

        # 行情类
        for api, table, slp in [
            (self.pro.daily,        'daily',        0.12),
            (self.pro.daily_basic,  'daily_basic',  0.12),
            (self.pro.weekly,       'weekly',        0.12),
            (self.pro.monthly,      'monthly',       0.12),
            (self.pro.moneyflow,    'moneyflow',     0.12),
        ]:
            df = self._call(api, sleep=slp, trade_date=trade_date)
            if df is not None and not df.empty:
                self._safe_append(df, table)
                logger.info(f"  {table}: {len(df)}条")

        # 技术面因子（分页拉取，避免单页截断）
        s_ok, _ = self._sync_stk_factor_pro_for_date(trade_date, replace=False)
        ok = ok and s_ok

        # 筹码分布
        df = self._call(self.pro.cyq_perf, sleep=0.12, trade_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'cyq_perf')
            logger.info(f"  cyq_perf: {len(df)}条")

        # 龙虎榜
        df = self._call(self.pro.top_list, sleep=0.15, trade_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'top_list')
            logger.info(f"  top_list: {len(df)}条")

        df = self._call(self.pro.top_inst, sleep=0.15, trade_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'top_inst')
            logger.info(f"  top_inst: {len(df)}条")

        # 两融汇总 / 明细
        df = self._call(self.pro.margin, sleep=0.15, trade_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'margin')
            logger.info(f"  margin: {len(df)}条")

        df = self._call(self.pro.margin_detail, sleep=0.15, trade_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'margin_detail')
            logger.info(f"  margin_detail: {len(df)}条")

        # 沪深港通十大成交股（3种市场类型）
        for mtype in ('1', '2', '3'):
            df = self._call(self.pro.hsgt_top10, sleep=0.15,
                            trade_date=trade_date, market_type=mtype)
            if df is not None and not df.empty:
                self._safe_append(df, 'hsgt_top10')
        logger.info(f"  hsgt_top10: 完成")

        # 大宗交易
        df = self._call(self.pro.block_trade, sleep=0.15, trade_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'block_trade')
            logger.info(f"  block_trade: {len(df)}条")

        # 限售股解禁（按公告日）
        df = self._call(self.pro.share_float, sleep=0.15, ann_date=trade_date)
        if df is not None and not df.empty:
            self._safe_append(df, 'share_float')
            logger.info(f"  share_float: {len(df)}条")

        # 指数日线
        for idx in ('000001.SH', '399001.SZ', '000300.SH', '000905.SH', '000016.SH'):
            df = self._call(self.pro.index_daily, sleep=0.12,
                            ts_code=idx, trade_date=trade_date)
            if df is not None and not df.empty:
                self._safe_append(df, 'index_daily')
        logger.info(f"  index_daily: 完成")

        # 申万行业指数日线（增量）- 使用 sw_daily 专用接口
        try:
            df = self._call(self.pro.sw_daily, sleep=0.12, trade_date=trade_date)
            if df is not None and not df.empty:
                self._safe_append(df, 'shenwan_daily')
                logger.info(f"  shenwan_daily: {len(df)}条")
        except Exception as e:
            logger.debug(f"  shenwan_daily 跳过: {e}")

        return ok

    # ------------------------------------------------------------------ #
    #  季度类接口（每季度末后触发）
    # ------------------------------------------------------------------ #

    def _get_latest_period(self):
        """获取最近已结束的季报期（如 20250930）"""
        today = datetime.now()
        quarters = [
            datetime(today.year, 3, 31),
            datetime(today.year, 6, 30),
            datetime(today.year, 9, 30),
            datetime(today.year, 12, 31),
            datetime(today.year - 1, 12, 31),
        ]
        past = [q for q in quarters if q < today]
        return past[-1].strftime('%Y%m%d')

    def _get_stock_codes(self):
        """获取全部 A 股代码列表"""
        try:
            result = pd.read_sql('SELECT ts_code FROM stock_basic', self.conn)
            return result['ts_code'].tolist()
        except Exception:
            return []

    def _period_already_synced(self, table, period, date_col='end_date'):
        """检查该 period 是否已在表中存在数据"""
        try:
            result = pd.read_sql(
                f'SELECT COUNT(*) as cnt FROM "{table}" WHERE "{date_col}"=\'{period}\'',
                self.conn
            )
            return result.iloc[0, 0] > 0
        except Exception:
            return False

    def _sync_financial_daily(self):
        """按 ann_date 增量同步财务报表数据（每日触发，有新公告就拉取）
        
        财务数据按公告日(ann_date)滚动披露，1季报在4月中旬至月底陆续发布，
        因此改为每日按 ann_date 增量拉取，不再依赖固定季报期判断。
        """
        today = datetime.now().strftime('%Y%m%d')

        # 财务表：(api函数, 表名, ann_date列名)
        financial_tables = [
            (self.pro.income,        'income',        'ann_date'),
            (self.pro.balancesheet,  'balancesheet',  'ann_date'),
            (self.pro.cashflow,      'cashflow',      'ann_date'),
            (self.pro.fina_indicator,'fina_indicator','ann_date'),
        ]

        # 获取有新公告的股票列表（先查哪些股票在 start~today 期间有公告）
        # Tushare 财务接口必须传 ts_code，因此按股票逐批拉取
        stock_codes = self._get_stock_codes()
        if not stock_codes:
            logger.warning("  获取股票列表失败，跳过财务增量同步")
            return

        for api, table, date_col in financial_tables:
            try:
                last = self.get_last_date(table, date_col)
                if last is None:
                    logger.warning(f"  {table} 表不存在，跳过增量同步（请先全量初始化）")
                    continue

                start = (datetime.strptime(last, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                if start > today:
                    logger.info(f"  {table}: 已是最新（{last}），跳过")
                    continue

                logger.info(f"  {table}: 同步 {start} ~ {today} 新公告（共 {len(stock_codes)} 只股票）...")
                all_data = []
                # 每批50只股票，减少API调用次数
                batch_size = 50
                for i in range(0, len(stock_codes), batch_size):
                    batch = stock_codes[i:i + batch_size]
                    ts_code_str = ','.join(batch)
                    df = self._call(api, sleep=0.4,
                                    ts_code=ts_code_str,
                                    start_date=start, end_date=today)
                    if df is not None and not df.empty:
                        all_data.append(df)

                if all_data:
                    df_all = pd.concat(all_data, ignore_index=True)
                    if 'ts_code' in df_all.columns and 'end_date' in df_all.columns:
                        df_all = df_all.drop_duplicates(
                            subset=['ts_code', 'end_date'], keep='last'
                        )
                    self._safe_append(df_all, table)
                    logger.info(f"  {table}: 新增 {len(df_all)} 条")
                else:
                    logger.info(f"  {table}: 无新公告数据")

            except Exception as e:
                logger.error(f"  {table} 增量同步失败: {e}")

        # 前十大股东 / 流通股东（按 ann_date 增量）
        for api, table in [
            (self.pro.top10_holders,      'top10_holders'),
            (self.pro.top10_floatholders, 'top10_floatholders'),
        ]:
            try:
                last = self.get_last_date(table, 'ann_date')
                if last is None:
                    continue
                start = (datetime.strptime(last, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                if start > today:
                    logger.info(f"  {table}: 已是最新，跳过")
                    continue
                all_data, offset = [], 0
                while True:
                    df = self._call(api, sleep=0.15,
                                    start_date=start, end_date=today,
                                    limit=5000, offset=offset)
                    if df is None or df.empty:
                        break
                    all_data.append(df)
                    if len(df) < 5000:
                        break
                    offset += 5000
                if all_data:
                    df_all = pd.concat(all_data, ignore_index=True)
                    self._safe_append(df_all, table)
                    logger.info(f"  {table}: 新增 {len(df_all)} 条")
            except Exception as e:
                logger.error(f"  {table} 增量同步失败: {e}")

        # 基金持仓（按最新 end_date 对应的 period 增量）
        try:
            period = self._get_latest_period()
            last_period = self.get_last_date('fund_portfolio', 'end_date')
            if last_period is None or last_period < period:
                all_data, offset = [], 0
                while True:
                    df = self._call(self.pro.fund_portfolio, sleep=0.3,
                                    period=period, limit=5000, offset=offset)
                    if df is None or df.empty:
                        break
                    all_data.append(df)
                    if len(df) < 5000:
                        break
                    offset += 5000
                if all_data:
                    df_all = pd.concat(all_data, ignore_index=True)
                    self._safe_append(df_all, 'fund_portfolio')
                    logger.info(f"  fund_portfolio ({period}): {len(df_all)}条")
            else:
                logger.info(f"  fund_portfolio: 已是最新（{last_period}），跳过")
        except Exception as e:
            logger.error(f"  fund_portfolio 增量同步失败: {e}")

    # ------------------------------------------------------------------ #
    #  不定期类接口（按日期区间补）
    # ------------------------------------------------------------------ #

    def _sync_irregular(self):
        """同步不定期更新的数据（股东增减持、回购）"""
        today = datetime.now().strftime('%Y%m%d')

        # 股东增减持
        last = self.get_last_date('stk_holdertrade', 'ann_date')
        if last:
            start = (datetime.strptime(last, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            if start <= today:
                df = self._call(self.pro.stk_holdertrade, sleep=0.3,
                                start_date=start, end_date=today)
                if df is not None and not df.empty:
                    self._safe_append(df, 'stk_holdertrade')
                    logger.info(f"  stk_holdertrade: {len(df)}条")

        # 股票回购（全量替换，数据量小）
        df = self._call(self.pro.repurchase, sleep=0.3,
                        start_date='20160101', end_date=today)
        if df is not None and not df.empty:
            try:
                self._submit_write({"kind": "replace_table", "table": "repurchase", "df": df}, wait=True)
                logger.info(f"  repurchase: {len(df)}条")
            except Exception as e:
                logger.error(f"repurchase 写入失败: {e}")

        # 申万行业成分股（每月更新，取前7天有变化的数据）
        try:
            last_sw = self.get_last_date('shenwan_member', 'in_date')
            sw_start = '20230101' if not last_sw else (datetime.strptime(last_sw, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            if sw_start <= today:
                # 获取申万行业列表
                sw_codes = pd.read_sql(
                    "SELECT DISTINCT index_code FROM shenwan_industry",
                    self.conn
                )['index_code'].tolist()
                total_new = 0
                for sw_code in sw_codes[:10]:  # 增量时只检查前10个主要行业，避免太慢
                    df = self._call(self.pro.index_member, sleep=0.15, index_code=sw_code)
                    if df is not None and not df.empty:
                        # 只保留 in_date >= sw_start 的新记录
                        if 'in_date' in df.columns:
                            df = df[df['in_date'] >= sw_start]
                            if not df.empty:
                                self._safe_append(df, 'shenwan_member')
                                total_new += len(df)
                if total_new > 0:
                    logger.info(f"  shenwan_member: 新增 {total_new}条")
        except Exception as e:
            logger.debug(f"  shenwan_member 增量跳过: {e}")

    # ------------------------------------------------------------------ #
    #  主入口
    # ------------------------------------------------------------------ #

    def sync_em_industry_board(self):
        """同步东方财富行业板块目录到 em_industry_board。"""
        logger.info("开始同步东方财富行业板块列表...")
        try:
            service = get_eastmoney_board_service()
            board_df = service.fetch_boards("industry")
            df = service.to_board_table(board_df, "industry")
            if df is None or df.empty:
                logger.warning("东方财富行业板块列表为空")
                return False
            self._submit_write({"kind": "replace_table", "table": "em_industry_board", "df": df}, wait=True)
            logger.info(f"东方财富行业板块列表同步完成: {len(df)} 条")
            return True
        except Exception as e:
            logger.error(f"同步东方财富行业板块列表失败: {e}")
            return False
    def sync_em_industry_member(self):
        """同步东方财富行业板块成分股到 em_industry_member。"""
        logger.info("开始同步东方财富行业板块成分股...")
        try:
            board_df = pd.read_sql("SELECT * FROM em_industry_board", self.conn)
            if board_df.empty:
                logger.info("行业板块目录为空，先刷新板块目录...")
                if not self.sync_em_industry_board():
                    return False
                board_df = pd.read_sql("SELECT * FROM em_industry_board", self.conn)
            if board_df.empty:
                logger.warning("行业板块目录仍为空，跳过成分股同步")
                return False
        except Exception as e:
            logger.error(f"读取行业板块目录失败: {e}")
            return False

        name_col = next((c for c in ["industry_name", "板块名称", "board_name"] if c in board_df.columns), None)
        code_col = next((c for c in ["industry_code", "板块代码", "board_code"] if c in board_df.columns), None)
        if not name_col or not code_col:
            logger.error(f"行业板块目录缺少必要字段: {list(board_df.columns)}")
            return False

        service = get_eastmoney_board_service()
        all_data = []
        total = len(board_df)
        for idx, row in board_df.iterrows():
            industry_name = str(row.get(name_col) or "").strip()
            industry_code = service.normalize_board_code(row.get(code_col))
            if not industry_name or not industry_code:
                continue
            try:
                df = service.fetch_board_members(
                    board_code=industry_code,
                    board_name=industry_name,
                    board_type="industry",
                )
                if df is not None and not df.empty:
                    all_data.append(service.to_member_table(df, "industry"))
                logger.info(f"  [{idx + 1}/{total}] {industry_name}: {0 if df is None else len(df)} 条")
            except Exception as e:
                logger.error(f"同步行业板块 {industry_name} 成分股失败: {e}")

        if not all_data:
            logger.warning("东方财富行业板块成分股为空")
            return False

        df_all = pd.concat(all_data, ignore_index=True)
        self._submit_write({"kind": "replace_table", "table": "em_industry_member", "df": df_all}, wait=True)
        logger.info(f"东方财富行业板块成分股同步完成: {len(df_all)} 条")
        return True
    def run_daily_sync(self):
        """执行增量同步：每日类 + 季度类 + 不定期类"""
        logger.info("=" * 60)
        logger.info("开始增量同步（断点续传）")
        logger.info("=" * 60)

        today = datetime.now().strftime('%Y%m%d')

        # ── 每日类：以 daily 表为基准确定待补日期 ──
        pending_dates = self._get_pending_dates('daily')
        if not pending_dates:
            logger.info(f"每日数据已是最新（截至 {today}），跳过每日同步")
        else:
            logger.info(f"每日类需补 {len(pending_dates)} 天: {pending_dates[0]} ~ {pending_dates[-1]}")
            for trade_date in pending_dates:
                self._sync_one_date(trade_date)
                time.sleep(0.3)

        # 即使 daily 没有新日期，也检查并修复 latest-day 的 stk_factor_pro 不完整问题。
        self._repair_latest_stk_factor_pro()

        # ── 财务类（每日按 ann_date 增量，覆盖1季报滚动披露场景）──
        logger.info("开始财务数据增量同步（按公告日）...")
        self._sync_financial_daily()

        # ── 不定期类 ──
        logger.info("开始不定期类增量同步...")
        self._sync_irregular()

        # Eastmoney industry board taxonomy: runtime industry ecology uses this first
        logger.info("Start syncing Eastmoney industry taxonomy...")
        self.sync_em_industry_board()
        self.sync_em_industry_member()

        logger.info("增量同步全部完成")
