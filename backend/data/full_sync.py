"""首次全量数据同步 - 优化版（多线程并发、速率限制、分页处理、断点续传）"""

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

import queue
import tushare as ts
import pandas as pd
import akshare as ak
import duckdb
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from config import DB_PATH, TUSHARE_TOKEN
from backend.realtime.eastmoney_board_service import get_eastmoney_board_service

_MIN_EXPECTED_CONCEPT_BOARD_COUNT = 150


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


def _build_concept_board_table_from_akshare(
    concept_name_df: pd.DataFrame,
    service,
) -> pd.DataFrame:
    if concept_name_df is None or concept_name_df.empty:
        return service.to_board_table(pd.DataFrame(), "concept")

    df = concept_name_df.copy()
    synced_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    up_count = pd.to_numeric(df.get("上涨家数"), errors="coerce")
    down_count = pd.to_numeric(df.get("下跌家数"), errors="coerce")
    total_count = up_count.fillna(0) + down_count.fillna(0)
    breadth_ratio = (up_count / total_count.where(total_count != 0)).round(4)

    out = pd.DataFrame(
        {
            "board_type": "concept",
            "board_type_name": service.BOARD_TYPE_LABEL["concept"],
            "board_code": df.get("板块代码", "").map(service.normalize_board_code),
            "board_name": df.get("板块名称", "").astype(str).str.strip(),
            "板块代码": df.get("板块代码", "").map(service.normalize_board_code),
            "板块名称": df.get("板块名称", "").astype(str).str.strip(),
            "最新价": pd.to_numeric(df.get("最新价"), errors="coerce"),
            "涨跌幅": pd.to_numeric(df.get("涨跌幅"), errors="coerce"),
            "涨跌额": pd.to_numeric(df.get("涨跌额"), errors="coerce"),
            "成交量": pd.Series([None] * len(df), dtype="float64"),
            "成交额": pd.Series([None] * len(df), dtype="float64"),
            "换手率": pd.to_numeric(df.get("换手率"), errors="coerce"),
            "总市值": pd.to_numeric(df.get("总市值"), errors="coerce"),
            "上涨家数": up_count,
            "下跌家数": down_count,
            "上涨占比": breadth_ratio,
            "领涨股票": df.get("领涨股票", "").astype(str).str.strip(),
            "领涨股票代码": "",
            "领涨股票完整代码": "",
            "领涨股票-涨跌幅": pd.to_numeric(df.get("领涨股票-涨跌幅"), errors="coerce"),
            "source": "akshare_eastmoney_concept_board",
            "synced_at": synced_at,
            "concept_code": df.get("板块代码", "").map(service.normalize_board_code),
            "concept_name": df.get("板块名称", "").astype(str).str.strip(),
        }
    )
    return out.reset_index(drop=True)


def _load_complete_concept_board_catalog(service) -> tuple[pd.DataFrame, str]:
    errors: list[str] = []
    try:
        board_df = service.fetch_boards("concept")
        concept_df = service.to_board_table(board_df, "concept")
        if concept_df is not None and len(concept_df) >= _MIN_EXPECTED_CONCEPT_BOARD_COUNT:
            return concept_df, "eastmoney_push2"
        errors.append(f"eastmoney_push2_count={0 if concept_df is None else len(concept_df)}")
    except Exception as e:
        errors.append(f"eastmoney_push2_error={e}")

    concept_name_df = ak.stock_board_concept_name_em()
    concept_df = _build_concept_board_table_from_akshare(concept_name_df, service)
    if concept_df is None or concept_df.empty:
        error_text = "; ".join(errors) if errors else "concept board catalog empty"
        raise RuntimeError(error_text)
    if errors:
        logger.warning(
            f"EastMoney push2 concept catalog incomplete, fallback to AKShare full concept catalog: {', '.join(errors)}"
        )
    return concept_df, "akshare_eastmoney_fallback"


class RateLimiter:
    """线程安全的速率限制器（令牌桶算法）"""

    def __init__(self, max_requests, time_window=60):
        """
        Args:
            max_requests: 时间窗口内最大请求数
            time_window: 时间窗口（秒）
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.tokens = max_requests
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        """获取一个令牌，如果没有可用令牌则阻塞等待"""
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                # 补充令牌
                self.tokens = min(self.max_requests,
                                self.tokens + elapsed * self.max_requests / self.time_window)
                self.last_update = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                # 计算需要等待的时间，锁外 sleep
                wait_time = (1 - self.tokens) * self.time_window / self.max_requests
            time.sleep(wait_time)


class ProgressTracker:
    """进度跟踪器"""

    def __init__(self, total, desc="同步进度"):
        self.total = total
        self.current = 0
        self.success = 0
        self.failed = 0
        self.desc = desc
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._print_header()

    def _print_header(self):
        """打印进度表头"""
        logger.info("=" * 80)
        logger.info(f"{self.desc}: 总计 {self.total} 项")
        logger.info("=" * 80)

    def update(self, success=True, item_name=""):
        """更新进度"""
        with self.lock:
            self.current += 1
            if success:
                self.success += 1
            else:
                self.failed += 1

            # 计算进度百分比
            percent = (self.current / self.total) * 100 if self.total > 0 else 0

            # 计算预计剩余时间
            elapsed = time.time() - self.start_time
            avg_time = elapsed / self.current if self.current > 0 else 0
            remaining = avg_time * (self.total - self.current)

            # 每5%或每5项打印一次进度（实时性更强）
            if (self.current % max(1, self.total // 20) == 0 or
                self.current % 1 == 0 or
                self.current == self.total):
                status = "✓" if success else "✗"
                logger.info(f"[{self.current}/{self.total}] {percent:5.1f}% | "
                          f"成功:{self.success} 失败:{self.failed} | "
                          f"预计剩余:{remaining/60:.1f}分钟 | {status} {item_name}")

    def print_summary(self):
        """打印完成摘要"""
        elapsed = time.time() - self.start_time
        logger.info("=" * 80)
        logger.info(f"{self.desc} 完成!")
        logger.info(f"  总计: {self.total} | 成功: {self.success} | 失败: {self.failed}")
        logger.info(f"  耗时: {elapsed/60:.1f}分钟 | 平均: {elapsed/self.current:.2f}秒/项")
        logger.info("=" * 80)


class FullDataSync:
    """首次全量数据同步 - 优化版"""

    # 接口速率限制配置（次/分钟）
    RATE_LIMITS = {
        'default': 500,        # 基础接口 500次/分钟
        'stk_factor_pro': 30,  # 技术面因子 30次/分钟
        'fund_portfolio': 200, # 基金持仓 200次/分钟
        'forecast': 350,       # 业绩预告 400次/分钟，留余量
        'dividend': 350,       # 分红送股 400次/分钟，留余量
        'hsgt_top10': 160,     # 沪深港通十大成交股 每日3次调用，500/3留余量
        'hk_hold': 450,        # 沪深港通持股明细 500次/分钟，留余量
        'top_list': 400,       # 龙虎榜每日明细 500次/分钟，留余量
        'top_inst': 400,       # 龙虎榜机构明细 500次/分钟，留余量
    }

    def __init__(self, db_path=DB_PATH, token=TUSHARE_TOKEN, max_workers=4):
        self.pro = ts.pro_api(token)
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.batch_size = 5000
        self.max_workers = max_workers

        # 写入队列：所有线程把 (df, table_name) 放入队列，由单一写入线程消费
        self._write_queue = queue.Queue()
        self._write_thread = threading.Thread(target=self._write_worker, daemon=True)
        self._write_thread.start()

        # 数据库写入锁（非队列模式兼容用）
        self.db_lock = threading.Lock()

        # 初始化速率限制器
        self.rate_limiters = {}
        for key, limit in self.RATE_LIMITS.items():
            self.rate_limiters[key] = RateLimiter(limit)

    def _write_worker(self):
        """单一写入线程，消费写入队列，使用 DuckDB 原生写入避免事务冲突"""
        while True:
            item = self._write_queue.get()
            if item is None:  # 终止信号
                self._write_queue.task_done()
                break
            df, table_name, if_exists = item
            try:
                self.conn.register('_tmp_write_df', df)
                if if_exists == 'replace':
                    self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                # 表不存在时创建，存在时追加
                try:
                    self.conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM _tmp_write_df')
                except Exception:
                    self.conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM _tmp_write_df')
                self.conn.unregister('_tmp_write_df')
            except Exception as e:
                logger.error(f"写入 {table_name} 失败: {e}")
                try:
                    self.conn.unregister('_tmp_write_df')
                except Exception:
                    pass
            finally:
                self._write_queue.task_done()

    def _write_df(self, df, table_name, if_exists='append'):
        """将 DataFrame 写入队列（线程安全），if_exists: 'append' 或 'replace'"""
        self._write_queue.put((df, table_name, if_exists))
        self._write_queue.join()  # 等待写入完成再返回

    def close(self):
        """关闭写入线程和 DuckDB 连接。"""
        try:
            self._write_queue.put(None)
            self._write_thread.join(timeout=5)
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

    def _get_rate_limiter(self, interface_name):
        """获取对应接口的速率限制器"""
        for key in self.RATE_LIMITS:
            if key in interface_name.lower():
                return self.rate_limiters[key]
        return self.rate_limiters['default']

    # 不可重试的错误关键字（直接抛出，不消耗重试次数）
    _NO_RETRY_ERRORS = ('请指定正确的接口名', '接口名称不存在', '参数校验失败', '无权限')

    # 速率限制错误关键字（等待60s再重试）
    _RATE_LIMIT_ERRORS = ('每分钟最多访问', 'rate', '流控', '访问频率')

    def _sync_with_retry(self, func, *args, max_retries=3, **kwargs):
        """带重试机制的同步调用"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                err_msg = str(e)
                if any(kw in err_msg for kw in self._NO_RETRY_ERRORS):
                    raise e  # 不可重试错误直接抛出
                is_rate_limit = any(kw in err_msg for kw in self._RATE_LIMIT_ERRORS)
                if attempt == max_retries - 1 and not is_rate_limit:
                    raise e
                if is_rate_limit:
                    wait_time = 60  # 限流错误等待60秒后继续重试
                else:
                    wait_time = 2 ** attempt  # 其他错误指数退避
                logger.warning(f"  重试 {attempt+1}/{max_retries}, 等待 {wait_time}s...")
                time.sleep(wait_time)
        return None
        
    def create_tables(self):
        """DuckDB v8.0：不预先定义表结构，由to_sql自动创建以保存所有字段"""
        logger.info("DuckDB 模式下不预先创建表，将由 pandas.to_sql 自动创建并保存全部字段")
    
    def sync_stock_basic(self):
        """同步股票基本信息（一次性）"""
        logger.info("开始同步股票基本信息...")
        df = self.pro.stock_basic(exchange='', list_status='L')
        self._write_df(df, 'stock_basic', if_exists='replace')
        logger.info(f"股票基本信息同步完成: {len(df)}只")
        return df
    
    def sync_trade_cal(self):
        """同步交易日历（一次性）"""
        logger.info("开始同步交易日历...")
        df = self.pro.trade_cal(exchange='SSE', start_date='20200101', end_date='20261231')
        self._write_df(df, 'trade_cal', if_exists='replace')
        logger.info(f"交易日历同步完成: {len(df)}天")
    
    def _sync_single_date(self, trade_date, table_name, api_func, rate_limiter_name='default'):
        """同步单个日期的数据（用于多线程）"""
        rate_limiter = self._get_rate_limiter(rate_limiter_name)

        try:
            # 速率限制
            rate_limiter.acquire()

            # 执行API调用（带重试）
            df = self._sync_with_retry(api_func, trade_date=trade_date)

            if df is not None and not df.empty:
                self._write_df(df, table_name)
                return True, len(df), trade_date
            return True, 0, trade_date
        except Exception as e:
            logger.error(f"获取 {trade_date} 失败: {e}")
            return False, 0, trade_date

    def _get_synced_dates(self, table_name):
        """获取已同步的日期列表（断点续传）"""
        try:
            synced = pd.read_sql(
                f'SELECT DISTINCT trade_date FROM {table_name}', self.conn
            )
            return set(synced['trade_date'].astype(str).tolist())
        except Exception:
            return set()

    def sync_historical_daily(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史日线行情（多线程优化版）

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            max_workers: 并发线程数（默认4，建议不超过8）
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史日线: {start_date} to {end_date} (并发数: {max_workers})")

        # 获取交易日历
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        # 断点续传：跳过已同步日期
        synced_dates = self._get_synced_dates('daily')
        trade_dates = [d for d in trade_dates if d not in synced_dates]

        if not trade_dates:
            logger.info("所有日线数据已同步，无需更新")
            return

        logger.info(f"需要同步 {len(trade_dates)} 个交易日，已跳过 {len(synced_dates)} 个")

        # 创建进度跟踪器
        progress = ProgressTracker(len(trade_dates), "日线行情同步")

        # 多线程并发同步
        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(
                    self._sync_single_date,
                    date, 'daily', self.pro.daily, 'default'
                ): date for date in trade_dates
            }

            for future in as_completed(future_to_date):
                success, count, date = future.result()
                progress.update(success, date)
                if success:
                    total_records += count

        progress.print_summary()
        logger.info(f"日线行情同步完成，共 {total_records} 条记录")
    
    def sync_historical_daily_basic(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史每日基本面（多线程优化版）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史基本面: {start_date} to {end_date} (并发数: {max_workers})")

        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        # 断点续传
        synced_dates = self._get_synced_dates('daily_basic')
        trade_dates = [d for d in trade_dates if d not in synced_dates]

        if not trade_dates:
            logger.info("所有基本面数据已同步，无需更新")
            return

        progress = ProgressTracker(len(trade_dates), "每日基本面同步")

        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(
                    self._sync_single_date,
                    date, 'daily_basic', self.pro.daily_basic, 'default'
                ): date for date in trade_dates
            }

            for future in as_completed(future_to_date):
                success, count, date = future.result()
                progress.update(success, date)
                if success:
                    total_records += count

        progress.print_summary()
        logger.info(f"每日基本面同步完成，共 {total_records} 条记录")
    
    def sync_historical_stk_factor_pro(self, start_date='20230101', end_date=None, max_workers=2):
        """同步历史技术面因子（多线程优化版）

        stk_factor_pro限频30次/分钟(5000积分)，按trade_date逐日获取全市场数据。
        每次调用返回当日全市场约5000只股票的265个技术面字段。
        全部字段保存到DuckDB的stk_factor_pro表，不裁剪。

        Args:
            max_workers: 并发数（默认2，因为30次/分钟限制较严）
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史技术面因子: {start_date} to {end_date} (并发数: {max_workers})")
        logger.info("注意: stk_factor_pro 限频30次/分钟，自动适配速率")

        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        # 断点续传
        synced_dates = self._get_synced_dates('stk_factor_pro')
        trade_dates = [d for d in trade_dates if d not in synced_dates]

        if not trade_dates:
            logger.info("所有技术面因子数据已同步，无需更新")
            return

        logger.info(f"需要同步 {len(trade_dates)} 天，已跳过 {len(synced_dates)} 天")

        progress = ProgressTracker(len(trade_dates), "技术面因子同步")

        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(
                    self._sync_single_date,
                    date, 'stk_factor_pro', self.pro.stk_factor_pro, 'stk_factor_pro'
                ): date for date in trade_dates
            }

            for future in as_completed(future_to_date):
                success, count, date = future.result()
                progress.update(success, f"{date}({count}条)")
                if success:
                    total_records += count

        progress.print_summary()
        logger.info(f"技术面因子同步完成，共 {total_records} 条记录")
    
    def _get_stock_list(self):
        """获取全量股票代码列表"""
        try:
            stocks = pd.read_sql("SELECT ts_code FROM stock_basic", self.conn)
            return stocks['ts_code'].tolist()
        except Exception:
            df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code')
            return df['ts_code'].tolist()

    # 不支持 start_date/end_date 参数的接口（只能按 ts_code 全量拉取）
    NO_DATE_TABLES = {'dividend', 'forecast'}

    def _sync_fina_single(self, ts_code, api_func, table_name, start_date, end_date, rate_limiter):
        """单只股票财务数据同步（分页+指数退避重试）"""
        use_date = table_name not in self.NO_DATE_TABLES
        max_retries = 3
        for attempt in range(max_retries):
            try:
                rate_limiter.acquire()
                all_data = []
                offset = 0
                while True:
                    if use_date:
                        df = api_func(ts_code=ts_code, start_date=start_date, end_date=end_date,
                                      limit=self.batch_size, offset=offset)
                    else:
                        df = api_func(ts_code=ts_code, limit=self.batch_size, offset=offset)
                    if df is None or df.empty:
                        break
                    all_data.append(df)
                    if len(df) < self.batch_size:
                        break
                    offset += self.batch_size
                    time.sleep(0.12)
                if all_data:
                    result = pd.concat(all_data, ignore_index=True)
                    self._write_df(result, table_name)
                    return True, len(result)
                return True, 0
            except Exception as e:
                err_msg = str(e)
                # 匹配各种速率限制错误（400次/500次/每分钟等）
                if '每分钟最多访问' in err_msg or 'rate' in err_msg.lower():
                    wait = 60 * (attempt + 1)
                    logger.warning(f"{table_name} {ts_code} 速率限制，{wait}秒后重试 ({attempt+1}/{max_retries})")
                    time.sleep(wait)
                else:
                    logger.error(f"{table_name} {ts_code} 失败: {e}")
                    return False, 0
        logger.error(f"{table_name} {ts_code} 重试{max_retries}次后仍失败")
        return False, 0

    def _sync_fina_by_stock(self, api_func, table_name, label, start_year, end_year, max_workers=4):
        """按股票代码多线程同步财务数据"""
        start_date = f'{start_year}0101'
        end_date = f'{end_year}1231'
        stock_list = self._get_stock_list()

        # 断点续传：跳过已同步的股票
        try:
            synced = pd.read_sql(f"SELECT DISTINCT ts_code FROM {table_name}", self.conn)
            synced_set = set(synced['ts_code'].tolist())
            stock_list = [s for s in stock_list if s not in synced_set]
            logger.info(f"{label}: 已同步 {len(synced_set)} 只，剩余 {len(stock_list)} 只")
        except Exception:
            logger.info(f"{label}: 共 {len(stock_list)} 只股票待同步")

        # 优先匹配专用限流器，否则用 default
        rate_limiter = self.rate_limiters.get(table_name, self.rate_limiters.get('default', RateLimiter(500)))
        progress = ProgressTracker(len(stock_list), label)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._sync_fina_single, ts_code, api_func,
                                table_name, start_date, end_date, rate_limiter): ts_code
                for ts_code in stock_list
            }
            for future in as_completed(futures):
                ts_code = futures[future]
                try:
                    success, count = future.result()
                    progress.update(success, f"{ts_code}({count}条)")
                except Exception as e:
                    progress.update(False, ts_code)
                    logger.error(f"{table_name} {ts_code} 异常: {e}")

        progress.print_summary()

    def sync_historical_income(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史利润表数据（按股票多线程）"""
        logger.info(f"开始同步历史利润表: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.income, 'income', '利润表同步', start_year, end_year, max_workers)
        logger.info("历史利润表同步完成")

    def sync_historical_balancesheet(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史资产负债表数据（按股票多线程）"""
        logger.info(f"开始同步历史资产负债表: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.balancesheet, 'balancesheet', '资产负债表同步', start_year, end_year, max_workers)
        logger.info("历史资产负债表同步完成")

    def sync_historical_cashflow(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史现金流量表数据（按股票多线程）"""
        logger.info(f"开始同步历史现金流量表: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.cashflow, 'cashflow', '现金流量表同步', start_year, end_year, max_workers)
        logger.info("历史现金流量表同步完成")

    def sync_historical_fina_indicator(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史财务指标数据（按股票多线程）"""
        logger.info(f"开始同步历史财务指标: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.fina_indicator, 'fina_indicator', '财务指标同步', start_year, end_year, max_workers)
        logger.info("历史财务指标同步完成")
    
    def sync_historical_forecast(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史业绩预告数据（按股票多线程）"""
        logger.info(f"开始同步历史业绩预告: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.forecast, 'forecast', '业绩预告同步', start_year, end_year, max_workers)
        logger.info("历史业绩预告同步完成")

    def sync_historical_dividend(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史分红送股数据（按股票多线程）"""
        logger.info(f"开始同步历史分红送股: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.dividend, 'dividend', '分红送股同步', start_year, end_year, max_workers)
        logger.info("历史分红送股同步完成")
    
    def sync_historical_financials(self):
        """同步历史财务数据（10年）- 兼容旧版本"""
        self.sync_historical_income()
        self.sync_historical_balancesheet()
        self.sync_historical_cashflow()
        self.sync_historical_fina_indicator()
        logger.info("历史财务数据同步完成")
    
    def sync_historical_weekly(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史周线行情（多线程优化版）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史周线: {start_date} to {end_date} (并发数: {max_workers})")

        # 获取所有交易日，然后筛选出周五（Python端计算避免SQL字段不存在）
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_cal['cal_date_dt'] = pd.to_datetime(trade_cal['cal_date'])
        # 筛选周五（weekday=4）且在日期范围内的
        fridays = trade_cal[
            (trade_cal['cal_date_dt'].dt.weekday == 4) &
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        total = self._sync_date_range_mt(fridays, 'weekly', self.pro.weekly,
                                        "周线行情同步", max_workers)
        logger.info(f"周线行情同步完成，共 {total} 条记录")
    
    def sync_historical_monthly(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史月线行情（多线程优化版）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史月线: {start_date} to {end_date} (并发数: {max_workers})")

        # 获取所有交易日，Python端计算每月最后一个交易日
        trade_cal = pd.read_sql(
            'SELECT cal_date FROM trade_cal WHERE is_open=1',
            self.conn
        )
        trade_cal['year_month'] = trade_cal['cal_date'].str[:6]  # YYYYMM
        # 按年月分组取最后一个交易日
        last_days = trade_cal.groupby('year_month')['cal_date'].max().reset_index()
        trade_dates = last_days[
            (last_days['cal_date'] >= start_date) &
            (last_days['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        total = self._sync_date_range_mt(trade_dates, 'monthly', self.pro.monthly,
                                        "月线行情同步", max_workers)
        logger.info(f"月线行情同步完成，共 {total} 条记录")
    
    def sync_historical_index_daily(self, start_date='20230101', end_date=None):
        """同步历史指数日线（3年）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"开始同步历史指数日线: {start_date} to {end_date}")
        
        # 获取主要指数列表
        try:
            index_list = ['000001.SH', '000300.SH', '000905.SH', '399001.SZ', '399006.SZ']
            all_data = []
            for ts_code in index_list:
                df = self.pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if not df.empty:
                    all_data.append(df)
                time.sleep(0.12)
            
            if all_data:
                df_all = pd.concat(all_data, ignore_index=True)
                self._write_df(df_all, 'index_daily', if_exists='replace')
                logger.info(f"指数日线同步完成: {len(df_all)}条")
        except Exception as e:
            logger.error(f"同步指数日线失败: {e}")
    
    def _sync_date_range_mt(self, trade_dates, table_name, api_func, desc, max_workers=4):
        """通用多线程日期范围同步辅助方法"""
        if not trade_dates:
            logger.info(f"{desc}: 无需同步")
            return 0

        # 断点续传
        synced_dates = self._get_synced_dates(table_name)
        trade_dates = [d for d in trade_dates if d not in synced_dates]

        if not trade_dates:
            logger.info(f"{desc}数据已同步，无需更新")
            return 0

        logger.info(f"{desc}: 需要同步 {len(trade_dates)} 天，已跳过 {len(synced_dates)} 天")

        progress = ProgressTracker(len(trade_dates), desc)

        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(
                    self._sync_single_date,
                    date, table_name, api_func, table_name
                ): date for date in trade_dates
            }

            for future in as_completed(future_to_date):
                success, count, date = future.result()
                progress.update(success, f"{date}({count}条)")
                if success:
                    total_records += count

        progress.print_summary()
        return total_records

    def sync_historical_moneyflow(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史资金流向（多线程优化版）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史资金流向: {start_date} to {end_date} (并发数: {max_workers})")

        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        total = self._sync_date_range_mt(trade_dates, 'moneyflow', self.pro.moneyflow,
                                        "资金流向同步", max_workers)
        logger.info(f"历史资金流向同步完成，共 {total} 条记录")
    
    def sync_historical_cyq_perf(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史筹码分布（多线程优化版）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史筹码分布: {start_date} to {end_date} (并发数: {max_workers})")

        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        total = self._sync_date_range_mt(trade_dates, 'cyq_perf', self.pro.cyq_perf,
                                        "筹码分布同步", max_workers)
        logger.info(f"历史筹码分布同步完成，共 {total} 条记录")
    
    def sync_historical_stk_holdertrade(self, start_year=2016, end_year=2026):
        """同步历史股东交易数据（10年）"""
        logger.info(f"开始同步历史股东交易: {start_year}~{end_year}")
        
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                start_date = f'{year}{month:02d}01'
                end_date = f'{year}{month:02d}28'
                try:
                    df = self.pro.stk_holdertrade(start_date=start_date, end_date=end_date)
                    if not df.empty:
                        self._write_df(df, 'stk_holdertrade')
                        logger.info(f"股东交易 {year}-{month:02d}: {len(df)}条")
                    time.sleep(0.3)
                except Exception as e:
                    logger.error(f"同步股东交易{year}-{month:02d}失败: {e}")
        
        logger.info("历史股东交易同步完成")
    
    def sync_historical_top10_holders(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史十大股东数据"""
        logger.info(f"开始同步历史十大股东: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.top10_holders, 'top10_holders', '十大股东同步',
                                 start_year, end_year, max_workers)
        logger.info("历史十大股东同步完成")

    def sync_historical_top10_floatholders(self, start_year=2016, end_year=2026, max_workers=4):
        """同步历史前十大流通股东数据"""
        logger.info(f"开始同步历史十大流通股东: {start_year}~{end_year} (并发数: {max_workers})")
        self._sync_fina_by_stock(self.pro.top10_floatholders, 'top10_floatholders', '十大流通股东同步',
                                 start_year, end_year, max_workers)
        logger.info("历史十大流通股东同步完成")
    
    def _sync_hsgt_top10_single(self, trade_date, rate_limiter_name='hsgt_top10'):
        """同步单日沪深港通十大成交股（三个市场）"""
        rate_limiter = self._get_rate_limiter(rate_limiter_name)

        try:
            all_data = []
            # 沪股通、深股通、港股通
            for market_type in ['1', '2', '3']:
                rate_limiter.acquire()  # 每次 API 调用前单独限速
                try:
                    df = self._sync_with_retry(
                        self.pro.hsgt_top10,
                        trade_date=trade_date,
                        market_type=market_type
                    )
                    if df is not None and not df.empty:
                        all_data.append(df)
                except Exception as e:
                    logger.warning(f"  {trade_date} 市场{market_type}获取失败: {e}")

            if all_data:
                df_all = pd.concat(all_data, ignore_index=True)
                self._write_df(df_all, 'hsgt_top10')
                return True, len(df_all), trade_date
            return True, 0, trade_date
        except Exception as e:
            logger.error(f"获取 {trade_date} 失败: {e}")
            return False, 0, trade_date

    def sync_historical_hsgt_top10(self, start_date='20230101', end_date=None, max_workers=4):
        """同步历史沪深港通十大成交股（多线程优化版）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today

        logger.info(f"开始同步历史沪深港通十大成交股: {start_date} to {end_date} (并发数: {max_workers})")

        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        # 断点续传
        synced_dates = self._get_synced_dates('hsgt_top10')
        trade_dates = [d for d in trade_dates if d not in synced_dates]

        if not trade_dates:
            logger.info("沪深港通十大成交股数据已同步，无需更新")
            return

        progress = ProgressTracker(len(trade_dates), "沪深港通十大成交股同步")

        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(self._sync_hsgt_top10_single, date): date
                for date in trade_dates
            }

            for future in as_completed(future_to_date):
                success, count, date = future.result()
                progress.update(success, f"{date}({count}条)")
                if success:
                    total_records += count

        progress.print_summary()
        logger.info(f"沪深港通十大成交股同步完成，共 {total_records} 条记录")
    
    def sync_historical_hsgt_hold(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史沪深港通持股明细（多线程优化版）"""
        # 港交所 2024-08-20 起停止发布日度数据，改为季度披露
        MAX_DATE = '20240819'
        if end_date is None or end_date > MAX_DATE:
            end_date = MAX_DATE

        logger.info(f"开始同步历史沪深港通持股明细: {start_date} to {end_date} (并发数: {max_workers})")

        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()

        total = self._sync_date_range_mt(trade_dates, 'hk_hold', self.pro.hk_hold,
                                        "沪深港通持股明细同步", max_workers)
        logger.info(f"沪深港通持股明细同步完成，共 {total} 条记录")
    
    def sync_historical_stock_company(self):
        """同步上市公司基本信息（一次性）"""
        logger.info("开始同步上市公司基本信息...")
        
        try:
            df = self.pro.stock_company()
            if not df.empty:
                self._write_df(df, 'stock_company', if_exists='replace')
                logger.info(f"上市公司基本信息同步完成: {len(df)}条")
        except Exception as e:
            logger.error(f"同步上市公司基本信息失败: {e}")
    
    def sync_historical_namechange(self, start_year=2016, end_year=2026):
        """同步历史股票曾用名（10年）"""
        logger.info(f"开始同步历史股票曾用名: {start_year}~{end_year}")
        
        try:
            df = self.pro.namechange(start_date=f'{start_year}0101', end_date=f'{end_year}1231')
            if not df.empty:
                self._write_df(df, 'namechange', if_exists='replace')
                logger.info(f"股票曾用名同步完成: {len(df)}条")
        except Exception as e:
            logger.error(f"同步股票曾用名失败: {e}")
    
    def sync_historical_suspend(self, start_date='20230101', end_date=None):
        """同步历史停复牌信息（3年）"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步历史停复牌信息: {start_date} to {end_date}")

        try:
            df = self.pro.suspend_d(start_date=start_date, end_date=end_date)
            if not df.empty:
                self._write_df(df, 'suspend', if_exists='replace')
                logger.info(f"停复牌信息同步完成: {len(df)}条")
        except Exception as e:
            logger.error(f"同步停复牌信息失败: {e}")
    
    def sync_historical_concept(self):
        """同步东方财富概念板块目录到 concept 表。"""
        logger.info("开始同步东方财富概念板块列表...")
        try:
            service = get_eastmoney_board_service()
            concept_df, _ = _load_complete_concept_board_catalog(service)
            if concept_df is None or concept_df.empty:
                logger.warning("东方财富概念板块列表为空")
                return
            self._write_df(concept_df, "concept", if_exists="replace")
            logger.info(f"东方财富概念板块列表同步完成: {len(concept_df)} 条")
        except Exception as e:
            logger.error(f"同步东方财富概念板块列表失败: {e}")
    def sync_historical_concept_detail(self):
        """同步东方财富概念板块成分股到 concept_detail。"""
        logger.info("开始同步东方财富概念板块成分股...")
        try:
            concept_df = pd.read_sql("SELECT * FROM concept", self.conn)
            if concept_df.empty or len(concept_df) < _MIN_EXPECTED_CONCEPT_BOARD_COUNT:
                logger.info("概念板块目录为空，先刷新板块目录...")
                self.sync_historical_concept()
                concept_df = pd.read_sql("SELECT * FROM concept", self.conn)
            if concept_df.empty:
                logger.warning("概念板块目录仍为空，跳过成分股同步")
                return
        except Exception as e:
            logger.error(f"读取概念板块目录失败: {e}")
            return

        name_col = next((c for c in ["concept_name", "板块名称", "board_name"] if c in concept_df.columns), None)
        code_col = next((c for c in ["concept_code", "板块代码", "board_code"] if c in concept_df.columns), None)
        if not name_col or not code_col:
            logger.error(f"概念板块目录缺少必要字段: {list(concept_df.columns)}")
            return

        service = get_eastmoney_board_service()
        all_data = []
        progress = ProgressTracker(len(concept_df), "概念板块成分股同步")
        for _, row in concept_df.iterrows():
            concept_name = str(row.get(name_col) or "").strip()
            concept_code = service.normalize_board_code(row.get(code_col))
            if not concept_name or not concept_code:
                progress.update(False, concept_name or "empty_concept")
                continue
            try:
                df = service.fetch_board_members(
                    board_code=concept_code,
                    board_name=concept_name,
                    board_type="concept",
                )
                if df is not None and not df.empty:
                    all_data.append(service.to_member_table(df, "concept"))
                    progress.update(True, f"{concept_name}({len(df)})")
                else:
                    progress.update(True, concept_name)
            except Exception as e:
                progress.update(False, concept_name)
                logger.error(f"同步概念板块 {concept_name} 成分股失败: {e}")

        if all_data:
            df_all = pd.concat(all_data, ignore_index=True)
            self._write_df(df_all, "concept_detail", if_exists="replace")
            logger.info(f"东方财富概念板块成分股同步完成: {len(df_all)} 条")
        progress.print_summary()
    def sync_historical_top_list(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史龙虎榜每日明细（top_list，多线程）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史龙虎榜每日明细: {start_date} to {end_date} (并发数: {max_workers})")
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        total = self._sync_date_range_mt(trade_dates, 'top_list', self.pro.top_list,
                                         "龙虎榜每日明细同步", max_workers)
        logger.info(f"龙虎榜每日明细同步完成，共 {total} 条记录")

    def sync_historical_top_inst(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史龙虎榜机构成交明细（top_inst，多线程）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史龙虎榜机构明细: {start_date} to {end_date} (并发数: {max_workers})")
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        total = self._sync_date_range_mt(trade_dates, 'top_inst', self.pro.top_inst,
                                         "龙虎榜机构明细同步", max_workers)
        logger.info(f"龙虎榜机构明细同步完成，共 {total} 条记录")

    def sync_historical_margin(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史融资融券交易汇总（margin，多线程）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史融资融券汇总: {start_date} to {end_date} (并发数: {max_workers})")
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        total = self._sync_date_range_mt(trade_dates, 'margin', self.pro.margin,
                                         "融资融券汇总同步", max_workers)
        logger.info(f"融资融券汇总同步完成，共 {total} 条记录")

    def sync_historical_margin_detail(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史融资融券交易明细（margin_detail，多线程）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史融资融券明细: {start_date} to {end_date} (并发数: {max_workers})")
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        total = self._sync_date_range_mt(trade_dates, 'margin_detail', self.pro.margin_detail,
                                         "融资融券明细同步", max_workers)
        logger.info(f"融资融券明细同步完成，共 {total} 条记录")

    def sync_historical_share_float(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史限售股解禁（share_float，按公告日多线程）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史限售股解禁: {start_date} to {end_date} (并发数: {max_workers})")
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        total = self._sync_date_range_mt(trade_dates, 'share_float',
                                         lambda **kw: self.pro.share_float(ann_date=kw['trade_date']),
                                         "限售股解禁同步", max_workers)
        logger.info(f"限售股解禁同步完成，共 {total} 条记录")

    def sync_historical_repurchase(self, start_date='20160101', end_date=None):
        """同步历史股票回购数据（repurchase，按区间拉取）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史股票回购数据: {start_date} to {end_date}")
        try:
            df = self.pro.repurchase(start_date=start_date, end_date=end_date)
            if df is not None and not df.empty:
                self._write_df(df, 'repurchase', if_exists='replace')
                logger.info(f"股票回购同步完成，共 {len(df)} 条记录")
            else:
                logger.info("股票回购：无数据")
        except Exception as e:
            logger.error(f"股票回购同步失败: {e}")

    def sync_historical_block_trade(self, start_date='20160101', end_date=None, max_workers=4):
        """同步历史大宗交易（block_trade，多线程按日）"""
        today = datetime.now().strftime('%Y%m%d')
        if end_date is None or end_date > today:
            end_date = today
        logger.info(f"开始同步历史大宗交易: {start_date} to {end_date} (并发数: {max_workers})")
        trade_cal = pd.read_sql('SELECT cal_date FROM trade_cal WHERE is_open=1', self.conn)
        trade_dates = trade_cal[
            (trade_cal['cal_date'] >= start_date) &
            (trade_cal['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        total = self._sync_date_range_mt(trade_dates, 'block_trade', self.pro.block_trade,
                                         "大宗交易同步", max_workers)
        logger.info(f"大宗交易同步完成，共 {total} 条记录")

    def sync_historical_fund_portfolio(self, start_year=2024, end_year=2026, max_workers=4):
        """同步历史基金持仓（按基金代码多线程）"""
        logger.info(f"开始同步历史基金持仓: {start_year}~{end_year} (并发数: {max_workers})")
        start_date = f'{start_year}0101'
        end_date = f'{end_year}1231'

        # 获取基金代码列表
        try:
            fund_df = self.pro.fund_basic(market='E', status='L', fields='ts_code')
            fund_codes = fund_df['ts_code'].tolist()
            logger.info(f"获取基金列表: {len(fund_codes)} 只")
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
            return

        # 断点续传
        try:
            synced = pd.read_sql("SELECT DISTINCT ts_code FROM fund_portfolio", self.conn)
            synced_set = set(synced['ts_code'].tolist())
            fund_codes = [c for c in fund_codes if c not in synced_set]
            logger.info(f"基金持仓: 已同步 {len(synced_set)} 只，剩余 {len(fund_codes)} 只")
        except Exception:
            logger.info(f"基金持仓: 共 {len(fund_codes)} 只待同步")

        rate_limiter = self.rate_limiters.get('fund_portfolio', self.rate_limiters.get('default', RateLimiter(500)))
        progress = ProgressTracker(len(fund_codes), "基金持仓同步")

        def _sync_one(ts_code):
            return self._sync_fina_single(ts_code, self.pro.fund_portfolio,
                                          'fund_portfolio', start_date, end_date, rate_limiter)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_sync_one, ts_code): ts_code for ts_code in fund_codes}
            for future in as_completed(futures):
                ts_code = futures[future]
                try:
                    success, count = future.result()
                    progress.update(success, f"{ts_code}({count}条)")
                except Exception as e:
                    progress.update(False, ts_code)
                    logger.error(f"fund_portfolio {ts_code} 异常: {e}")

        progress.print_summary()
        logger.info("历史基金持仓同步完成")

    # ==================== 申万行业数据同步 ====================

    def sync_em_industry_board(self):
        """同步东方财富行业板块目录到 em_industry_board。"""
        logger.info("开始同步东方财富行业板块列表...")
        try:
            service = get_eastmoney_board_service()
            board_df = service.fetch_boards("industry")
            df = service.to_board_table(board_df, "industry")
            if df is None or df.empty:
                logger.warning("东方财富行业板块列表为空")
                return
            self._write_df(df, "em_industry_board", if_exists="replace")
            logger.info(f"东方财富行业板块列表同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"同步东方财富行业板块列表失败: {e}")
    def sync_em_industry_member(self):
        """同步东方财富行业板块成分股到 em_industry_member。"""
        logger.info("开始同步东方财富行业板块成分股...")
        try:
            board_df = pd.read_sql("SELECT * FROM em_industry_board", self.conn)
            if board_df.empty:
                logger.info("行业板块目录为空，先刷新板块目录...")
                self.sync_em_industry_board()
                board_df = pd.read_sql("SELECT * FROM em_industry_board", self.conn)
            if board_df.empty:
                logger.warning("行业板块目录仍为空，跳过成分股同步")
                return
        except Exception as e:
            logger.error(f"读取行业板块目录失败: {e}")
            return

        name_col = next((c for c in ["industry_name", "板块名称", "board_name"] if c in board_df.columns), None)
        code_col = next((c for c in ["industry_code", "板块代码", "board_code"] if c in board_df.columns), None)
        if not name_col or not code_col:
            logger.error(f"行业板块目录缺少必要字段: {list(board_df.columns)}")
            return

        service = get_eastmoney_board_service()
        all_data = []
        progress = ProgressTracker(len(board_df), "行业板块成分股同步")
        for _, row in board_df.iterrows():
            industry_name = str(row.get(name_col) or "").strip()
            industry_code = service.normalize_board_code(row.get(code_col))
            if not industry_name or not industry_code:
                progress.update(False, industry_name or "empty_industry")
                continue
            try:
                df = service.fetch_board_members(
                    board_code=industry_code,
                    board_name=industry_name,
                    board_type="industry",
                )
                if df is not None and not df.empty:
                    all_data.append(service.to_member_table(df, "industry"))
                    progress.update(True, f"{industry_name}({len(df)})")
                else:
                    progress.update(True, industry_name)
            except Exception as e:
                progress.update(False, industry_name)
                logger.error(f"同步行业板块 {industry_name} 成分股失败: {e}")

        if all_data:
            df_all = pd.concat(all_data, ignore_index=True)
            self._write_df(df_all, "em_industry_member", if_exists="replace")
            logger.info(f"东方财富行业板块成分股同步完成: {len(df_all)} 条")
        progress.print_summary()
    def sync_shenwan_industry(self):
        """同步申万行业分类 (index_classify)

        Tushare API: index_classify
        字段说明:
        - index_code: 指数代码 (如 '801010')
        - industry_name: 行业名称 (如 '农林牧渔')
        - level: 行业级别 (L1/L2/L3 对应一级/二级/三级)
        - index_code_full: 完整指数代码
        - src: 数据源 (申万编制)
        """
        logger.info("开始同步申万行业分类数据...")
        try:
            # 分别获取一级、二级、三级行业
            all_data = []
            for level in ['L1', 'L2', 'L3']:
                df = self._sync_with_retry(
                    self.pro.index_classify,
                    level=level,
                    src='SW2021'  # 申万2021版行业分类
                )
                if df is not None and not df.empty:
                    df['level'] = level  # 确保level字段
                    all_data.append(df)
                    logger.info(f"  申万行业{level}: {len(df)} 条")

            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                self._write_df(combined, 'shenwan_industry', if_exists='replace')
                logger.info(f"申万行业分类同步完成，共 {len(combined)} 条")
            else:
                logger.warning("申万行业分类：无数据")
        except Exception as e:
            logger.error(f"申万行业分类同步失败: {e}")

    def sync_shenwan_member(self, max_workers=4):
        """同步申万行业成分股 (index_member)

        Tushare API: index_member
        字段说明:
        - index_code: 申万行业指数代码
        - con_code: 成分股代码 (ts_code)
        - con_name: 成分股名称
        - in_date: 纳入日期
        - out_date: 剔除日期 (None表示当前仍在行业中)
        - is_new: 是否最新 (1=是, 0=已剔除)

        需要按每个行业指数代码分别获取
        """
        logger.info("开始同步申万行业成分股...")
        try:
            # 先获取申万行业列表
            industry_df = pd.read_sql(
                "SELECT DISTINCT index_code FROM shenwan_industry",
                self.conn
            )
            if industry_df.empty:
                logger.error("申万行业表为空，请先运行 sync_shenwan_industry()")
                return

            index_codes = industry_df['index_code'].tolist()
            logger.info(f"获取到 {len(index_codes)} 个申万行业")

            # 检查断点续传
            try:
                synced = pd.read_sql(
                    "SELECT DISTINCT index_code FROM shenwan_member",
                    self.conn
                )
                synced_set = set(synced['index_code'].tolist())
                index_codes = [c for c in index_codes if c not in synced_set]
                logger.info(f"已同步 {len(synced_set)} 个行业，剩余 {len(index_codes)} 个")
            except Exception:
                logger.info(f"从头开始同步 {len(index_codes)} 个行业")

            rate_limiter = self._get_rate_limiter('default')
            progress = ProgressTracker(len(index_codes), "申万行业成分同步")

            def _sync_one(index_code):
                try:
                    rate_limiter.acquire()
                    df = self._sync_with_retry(
                        self.pro.index_member,
                        index_code=index_code
                    )
                    if df is not None and not df.empty:
                        df['index_code'] = index_code  # 确保字段
                        # 写入队列（追加模式）
                        self._write_df(df, 'shenwan_member', if_exists='append')
                        return True, len(df)
                    return True, 0
                except Exception as e:
                    logger.error(f"  {index_code} 失败: {e}")
                    return False, 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_sync_one, code): code for code in index_codes}
                for future in as_completed(futures):
                    index_code = futures[future]
                    try:
                        success, count = future.result()
                        progress.update(success, f"{index_code}({count}条)")
                    except Exception as e:
                        progress.update(False, index_code)
                        logger.error(f"{index_code} 异常: {e}")

            progress.print_summary()
            logger.info("申万行业成分股同步完成")
        except Exception as e:
            logger.error(f"申万行业成分同步失败: {e}")

    def sync_shenwan_daily(self, start_date='20230101', end_date=None, max_workers=4):
        """同步申万行业指数日行情 (sw_daily 专用接口)

        Tushare API: sw_daily(trade_date='20230705') 或 sw_daily(ts_code='801010.SI', start_date='20230101')
        字段说明:
        - ts_code: 申万指数代码 (如 '801010.SI')
        - trade_date: 交易日期
        - name: 指数名称
        - open: 开盘点位
        - high: 最高点位
        - low: 最低点位
        - close: 收盘点位
        - change: 涨跌点位
        - pct_change: 涨跌幅
        - vol: 成交量（万股）
        - amount: 成交额（万元）
        - pe: 市盈率
        - pb: 市净率
        - float_mv: 流通市值（万元）
        - total_mv: 总市值（万元）

        申万指数代码格式: 6位数字.SI (如 801010.SI)
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"开始同步申万行业指数日行情: {start_date} to {end_date}")
        try:
            # 获取交易日列表
            trade_cal = pd.read_sql(
                'SELECT cal_date FROM trade_cal WHERE is_open=1',
                self.conn
            )
            trade_dates = trade_cal[
                (trade_cal['cal_date'] >= start_date) &
                (trade_cal['cal_date'] <= end_date)
            ]['cal_date'].tolist()

            # 使用默认限频 (500次/分钟足够)
            rate_limiter = self._get_rate_limiter('default')
            total_records = 0

            # 按交易日逐日获取（sw_daily支持按trade_date获取全市场行业指数数据）
            for trade_date in trade_dates:
                try:
                    rate_limiter.acquire()
                    df = self._sync_with_retry(
                        self.pro.sw_daily,
                        trade_date=trade_date
                    )
                    if df is not None and not df.empty:
                        self._write_df(df, 'shenwan_daily', if_exists='append')
                        total_records += len(df)
                        if len(df) > 0:
                            logger.info(f"  {trade_date}: {len(df)}条")
                except Exception as e:
                    logger.warning(f"  {trade_date} 获取失败: {e}")

            logger.info(f"申万行业指数日行情同步完成，共 {total_records} 条记录")
        except Exception as e:
            logger.error(f"申万行业指数日行情同步失败: {e}")

    def run_full_sync(self):
        """执行完整同步流程"""
        logger.info("=" * 60)
        logger.info("开始全量数据同步")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        self.create_tables()
        self.sync_stock_basic()
        self.sync_trade_cal()
        self.sync_historical_daily()
        self.sync_historical_daily_basic()
        self.sync_historical_stk_factor_pro()
        self.sync_historical_financials()
        self.sync_historical_fund_portfolio()

        # Eastmoney industry board taxonomy: runtime industry ecology uses this first
        self.sync_em_industry_board()
        self.sync_em_industry_member()

        # 申万行业数据同步
        self.sync_shenwan_industry()
        self.sync_shenwan_member()
        self.sync_shenwan_daily()

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"全量同步完成，耗时: {elapsed/60:.1f}分钟")
        logger.info("=" * 60)

if __name__ == "__main__":
    sync = FullDataSync()
    sync.sync_stock_basic()
