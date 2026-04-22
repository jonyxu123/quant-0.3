"""基金持仓数据同步入口脚本

用法:
    python scripts/sync_fund_portfolio.py --end-date 20241231
    python scripts/sync_fund_portfolio.py --year 2024
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.incremental_sync import IncrementalSync


def main():
    parser = argparse.ArgumentParser(description="基金持仓数据同步")
    parser.add_argument("--end-date", default=None, help="指定报告期截止日 (如20241231)")
    parser.add_argument("--year", default=None, help="指定年份，同步该年全部4个季度")
    args = parser.parse_args()

    sync = IncrementalSync()

    if args.year:
        year = args.year
        quarters = [f"{year}0331", f"{year}0630", f"{year}0930", f"{year}1231"]
        logger.info(f"开始{year}年全年基金持仓同步")
        for end_date in quarters:
            sync.sync_quarterly_fund_portfolio(end_date)
        logger.info(f"{year}年全年基金持仓同步完成")
    elif args.end_date:
        logger.info(f"开始基金持仓同步: {args.end_date}")
        sync.sync_quarterly_fund_portfolio(args.end_date)
        logger.info(f"基金持仓同步完成: {args.end_date}")
    else:
        from datetime import datetime
        now = datetime.now()
        year = str(now.year)
        month = now.month
        if month <= 3:
            end_date = f"{int(year)-1}1231"
        elif month <= 6:
            end_date = f"{year}0331"
        elif month <= 9:
            end_date = f"{year}0630"
        else:
            end_date = f"{year}0930"
        logger.info(f"自动检测当前应同步季度: {end_date}")
        sync.sync_quarterly_fund_portfolio(end_date)
        logger.info(f"基金持仓同步完成: {end_date}")


if __name__ == "__main__":
    main()
