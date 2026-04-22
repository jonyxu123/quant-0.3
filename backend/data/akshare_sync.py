"""AKShare辅助数据同步模块 (v8.0)
负责拉取Tushare无法覆盖的数据。
"""
import akshare_proxy_patch
akshare_proxy_patch.install_patch(
    "101.201.173.125",
    auth_token="20260402BAQOIJJ3",
    retry=30,
    hook_domains=[
        "fund.eastmoney.com",
        "push2.eastmoney.com",
        "push2his.eastmoney.com",
        "emweb.securities.eastmoney.com",
    ],
)
import akshare as ak
import pandas as pd
import duckdb
import time
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from config import DB_PATH, TUSHARE_TOKEN
from backend.data.rate_limiter import safe_call


class AKShareSync:
    """AKShare辅助数据同步器"""
    
    def __init__(self, db_path=DB_PATH, tushare_token=TUSHARE_TOKEN):
        self.conn = duckdb.connect(db_path)
        self.tushare_token = tushare_token
    
    def sync_shareholder_num(self, symbol="000001"):
        """同步股东人数变化"""
        try:
            df = safe_call(ak.stock_zh_a_gdhs, symbol=symbol, default=pd.DataFrame())
            if df is not None and not df.empty:
                df['code'] = symbol
                df.to_sql('shareholder_num', self.conn, if_exists='append', index=False)
                logger.info(f"股东人数 {symbol}: {len(df)}期")
            return True
        except Exception as e:
            logger.error(f"股东人数同步失败({symbol}): {e}")
            return False
    
    def run_daily_akshare_sync(self):
        """每日AKShare增量同步（建议在16:00后执行）"""
        logger.info("=" * 60)
        logger.info("开始AKShare每日增量同步")
        logger.info("=" * 60)
        
        today = datetime.now().strftime('%Y%m%d')
        
        tasks = []
        
        success_count = 0
        for name, task in tasks:
            try:
                if task():
                    success_count += 1
                time.sleep(1.0)
            except Exception as e:
                logger.error(f"{name} 异常: {e}")
        
        logger.info(f"AKShare同步完成: {success_count}/{len(tasks)}项")
