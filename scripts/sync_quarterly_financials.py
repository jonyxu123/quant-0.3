"""季度财务数据同步入口脚本

用法:
    python scripts/sync_quarterly_financials.py --period 20241231
    python scripts/sync_quarterly_financials.py --year 2024
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.incremental_sync import IncrementalSync


def main():
    parser = argparse.ArgumentParser(description="季度财务数据同步")
    parser.add_argument("--period", default=None, help="指定报告期 (如20241231)")
    parser.add_argument("--year", default=None, help="指定年份，同步该年全部4个季度")
    args = parser.parse_args()

    sync = IncrementalSync()

    if args.year:
        year = args.year
        quarters = [f"{year}0331", f"{year}0630", f"{year}0930", f"{year}1231"]
        logger.info(f"开始{year}年全年度财务同步")
        for period in quarters:
            sync.sync_quarterly_financials(period)
        logger.info(f"{year}年全年度财务同步完成")
    elif args.period:
        logger.info(f"开始季度财务同步: {args.period}")
        sync.sync_quarterly_financials(args.period)
        logger.info(f"季度财务同步完成: {args.period}")
    else:
        from datetime import datetime
        now = datetime.now()
        year = str(now.year)
        # 自动判断当前季度
        month = now.month
        if month <= 3:
            period = f"{int(year)-1}1231"
        elif month <= 6:
            period = f"{year}0331"
        elif month <= 9:
            period = f"{year}0630"
        else:
            period = f"{year}0930"
        logger.info(f"自动检测当前应同步季度: {period}")
        sync.sync_quarterly_financials(period)
        logger.info(f"季度财务同步完成: {period}")


if __name__ == "__main__":
    main()
