"""筹码分布同步入口脚本

用法:
    python scripts/sync_cyq.py
    python scripts/sync_cyq.py --date 20241231
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
import time
from datetime import datetime
from loguru import logger
from config import DB_PATH, TUSHARE_TOKEN
from backend.data.incremental_sync import IncrementalSync


def main():
    parser = argparse.ArgumentParser(description="筹码分布同步")
    parser.add_argument("--date", default=None, help="指定日期 (YYYYMMDD, 默认今天)")
    args = parser.parse_args()

    date = args.date or datetime.now().strftime('%Y%m%d')
    sync = IncrementalSync()

    logger.info(f"开始筹码分布同步: {date}")
    try:
        pro = ts.pro_api(TUSHARE_TOKEN)
        df_cyq = pro.cyq_perf(trade_date=date)
        if not df_cyq.empty:
            df_cyq.to_sql('cyq_perf', sync.conn, if_exists='append', index=False)
            logger.info(f"筹码分布同步: {date} {len(df_cyq)}条")
        else:
            logger.info(f"筹码分布无数据: {date}")
    except Exception as e:
        logger.error(f"筹码分布同步失败: {e}")
    finally:
        sync.conn.close()

    logger.info("筹码分布同步流程结束")


if __name__ == "__main__":
    main()
