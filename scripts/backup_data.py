"""数据备份入口脚本

用法:
    python scripts/backup_data.py
    python scripts/backup_data.py --output ./backups/
    python scripts/backup_data.py --tables daily daily_basic stk_factor_pro
"""
import argparse
import shutil
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from config import DB_PATH


def main():
    parser = argparse.ArgumentParser(description="数据备份")
    parser.add_argument("--output", default="./backups/", help="备份输出目录")
    parser.add_argument("--tables", nargs="+", default=None,
                        help="指定备份的表 (默认备份整个数据库文件)")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.tables:
        # 按表导出为CSV
        import duckdb
        import pandas as pd

        conn = duckdb.connect(DB_PATH, read_only=True)
        try:
            for table in args.tables:
                try:
                    df = pd.read_sql(f"SELECT * FROM {table}", conn)
                    csv_path = os.path.join(args.output, f"{table}_{timestamp}.csv")
                    df.to_csv(csv_path, index=False)
                    logger.info(f"备份表 {table}: {len(df)}行 -> {csv_path}")
                except Exception as e:
                    logger.error(f"备份表 {table} 失败: {e}")
        finally:
            conn.close()
    else:
        # 整库文件备份
        backup_path = os.path.join(args.output, f"stock_data_{timestamp}.duckdb")
        try:
            shutil.copy2(DB_PATH, backup_path)
            file_size_mb = os.path.getsize(backup_path) / (1024 * 1024)
            logger.info(f"数据库备份完成: {backup_path} ({file_size_mb:.1f}MB)")
        except Exception as e:
            logger.error(f"数据库备份失败: {e}")

    logger.info("备份流程结束")


if __name__ == "__main__":
    main()
