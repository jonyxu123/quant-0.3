"""数据质量检查入口脚本

用法:
    python scripts/data_quality_check.py --date 20241231
    python scripts/data_quality_check.py --date 20241231 --check-missing --start 20241201
"""
import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.quality_check import DataQualityChecker


def main():
    parser = argparse.ArgumentParser(description="数据质量检查")
    parser.add_argument("--date", required=True, help="检查日期 (YYYYMMDD)")
    parser.add_argument("--check-missing", action="store_true", help="检查缺失交易日")
    parser.add_argument("--start", default=None, help="缺失检查起始日期 (配合--check-missing)")
    args = parser.parse_args()

    checker = DataQualityChecker()

    logger.info(f"开始数据质量检查: {args.date}")

    # 生成质量报告
    report = checker.generate_report(args.date)
    logger.info(f"日线完整性: {report['daily_check']}")
    logger.info(f"技术面因子完整性: {report['stk_factor_pro_check']}")
    logger.info(f"AKShare数据完整性: {report['akshare_check']}")

    # 检查缺失交易日
    if args.check_missing and args.start:
        missing_report = checker.check_missing_dates(args.start, args.date)
        logger.info(f"缺失交易日检查: {missing_report}")

    # 输出JSON报告
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    logger.info("数据质量检查完成")


if __name__ == "__main__":
    main()
