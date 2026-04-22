"""增量数据同步入口脚本

用法:
    python scripts/incremental_sync.py
    python scripts/incremental_sync.py --date 20241231
    python scripts/incremental_sync.py --start 20241201 --end 20241231
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.incremental_sync import IncrementalSync


def main():
    parser = argparse.ArgumentParser(description="增量数据同步")
    parser.add_argument("--date", default=None, help="指定单日同步 (YYYYMMDD)")
    parser.add_argument("--start", default=None, help="批量同步起始日期")
    parser.add_argument("--end", default=None, help="批量同步结束日期")
    args = parser.parse_args()

    sync = IncrementalSync()

    if args.start and args.end:
        sync.sync_range_data(args.start, args.end)
    elif args.date:
        sync.sync_daily_data(args.date)
    else:
        sync.run_daily_sync()

    logger.info("增量同步流程结束")


if __name__ == "__main__":
    main()
