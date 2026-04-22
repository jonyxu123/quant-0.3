"""AKShare北向资金同步入口脚本

用法:
    python scripts/akshare_north_sync.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.akshare_sync import AKShareSync


def main():
    sync = AKShareSync()

    logger.info("开始北向资金同步")
    sync.sync_north_hold()
    logger.info("北向资金同步完成")


if __name__ == "__main__":
    main()
