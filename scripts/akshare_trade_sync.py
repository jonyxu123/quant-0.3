"""AKShare交易同步入口脚本 (大宗交易 + 限售解禁)

用法:
    python scripts/akshare_trade_sync.py
    python scripts/akshare_trade_sync.py --start 20241201 --end 20241231
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.akshare_sync import AKShareSync


def main():
    parser = argparse.ArgumentParser(description="AKShare交易同步 (大宗交易+限售解禁)")
    parser.add_argument("--start", default=None, help="大宗交易起始日期 (默认30天前)")
    parser.add_argument("--end", default=None, help="大宗交易结束日期 (默认今天)")
    args = parser.parse_args()

    sync = AKShareSync()

    logger.info("开始AKShare交易同步 (大宗交易+限售解禁)")
    sync.sync_block_trade(start_date=args.start, end_date=args.end)
    sync.sync_restricted_release()
    logger.info("AKShare交易同步完成 (大宗交易+限售解禁)")


if __name__ == "__main__":
    main()
