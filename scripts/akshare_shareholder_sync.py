"""AKShare股东数据同步入口脚本

用法:
    python scripts/akshare_shareholder_sync.py
    python scripts/akshare_shareholder_sync.py --symbol 000001
    python scripts/akshare_shareholder_sync.py --batch
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pandas as pd
from loguru import logger
from config import DB_PATH
from backend.data.akshare_sync import AKShareSync


def main():
    parser = argparse.ArgumentParser(description="AKShare股东数据同步")
    parser.add_argument("--symbol", default=None, help="指定股票代码 (如000001)")
    parser.add_argument("--batch", action="store_true", help="批量同步全市场股东人数")
    args = parser.parse_args()

    sync = AKShareSync()

    if args.batch:
        logger.info("开始批量股东人数同步")
        conn = duckdb.connect(DB_PATH)
        try:
            stocks = pd.read_sql("SELECT ts_code FROM stock_basic", conn)
            for i, row in stocks.iterrows():
                code = row['ts_code'].split('.')[0]
                sync.sync_shareholder_num(symbol=code)
                if (i + 1) % 100 == 0:
                    logger.info(f"已处理 {i+1}/{len(stocks)} 只股票")
        finally:
            conn.close()
        logger.info("批量股东人数同步完成")
    else:
        symbol = args.symbol or "000001"
        logger.info(f"开始股东人数同步: {symbol}")
        sync.sync_shareholder_num(symbol=symbol)
        logger.info(f"股东人数同步完成: {symbol}")


if __name__ == "__main__":
    main()
