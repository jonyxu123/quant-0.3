"""AKShare每日同步入口脚本 (龙虎榜 + 融资融券)

用法:
    python scripts/akshare_daily_sync.py
    python scripts/akshare_daily_sync.py --date 20241231
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.akshare_sync import AKShareSync


def main():
    parser = argparse.ArgumentParser(description="AKShare每日同步 (龙虎榜+融资融券)")
    parser.add_argument("--date", default=None, help="指定日期 (YYYYMMDD, 默认今天)")
    args = parser.parse_args()

    sync = AKShareSync()
    date = args.date

    logger.info("开始AKShare每日同步 (龙虎榜+融资融券)")
    sync.sync_lhb_detail(date=date)
    sync.sync_margin_detail(date=date)
    logger.info("AKShare每日同步完成 (龙虎榜+融资融券)")


if __name__ == "__main__":
    main()
