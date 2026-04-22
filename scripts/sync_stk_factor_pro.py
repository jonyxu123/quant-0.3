"""技术面因子增量同步入口脚本【v8.0新增】

同步Tushare stk_factor_pro接口数据（265字段全量存储）。
限频30次/分钟(5000积分)，按trade_date逐日获取全市场数据。

用法:
    python scripts/sync_stk_factor_pro.py
    python scripts/sync_stk_factor_pro.py --date 20241231
    python scripts/sync_stk_factor_pro.py --start 20241201 --end 20241231
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
import pandas as pd
import duckdb
import time
from datetime import datetime
from loguru import logger
from config import DB_PATH, TUSHARE_TOKEN


def main():
    parser = argparse.ArgumentParser(description="技术面因子增量同步【v8.0新增】")
    parser.add_argument("--date", default=None, help="指定单日同步 (YYYYMMDD)")
    parser.add_argument("--start", default=None, help="批量同步起始日期")
    parser.add_argument("--end", default=None, help="批量同步结束日期")
    args = parser.parse_args()

    pro = ts.pro_api(TUSHARE_TOKEN)
    conn = duckdb.connect(DB_PATH)
    today = datetime.now().strftime('%Y%m%d')

    try:
        if args.date:
            # 单日同步
            trade_date = args.date
            logger.info(f"同步技术面因子: {trade_date}")
            df = pro.stk_factor_pro(trade_date=trade_date)
            if not df.empty:
                df.to_sql('stk_factor_pro', conn, if_exists='append', index=False)
                logger.info(f"技术面因子 {trade_date}: {len(df)}条")
            else:
                logger.info(f"技术面因子 {trade_date}: 无数据")

        elif args.start and args.end:
            # 批量同步（带断点续传）
            start_date, end_date = args.start, args.end
            logger.info(f"批量同步技术面因子: {start_date} to {end_date}")

            trade_cal = pd.read_sql(
                'SELECT cal_date FROM trade_cal WHERE is_open=1 AND cal_date >= ? AND cal_date <= ? ORDER BY cal_date',
                conn, params=[start_date, end_date]
            )
            trade_dates = trade_cal['cal_date'].tolist()

            # 断点续传
            try:
                synced = pd.read_sql(
                    'SELECT DISTINCT trade_date FROM stk_factor_pro', conn
                )
                synced_dates = set(synced['trade_date'].tolist())
                trade_dates = [d for d in trade_dates if d not in synced_dates]
                logger.info(f"跳过已同步{len(synced_dates)}天，剩余{len(trade_dates)}天")
            except Exception:
                pass

            total = len(trade_dates)
            for i, trade_date in enumerate(trade_dates):
                try:
                    df = pro.stk_factor_pro(trade_date=trade_date)
                    if not df.empty:
                        df.to_sql('stk_factor_pro', conn, if_exists='append', index=False)
                        logger.info(f"[{i+1}/{total}] {trade_date}: {len(df)}条技术面因子")
                    time.sleep(2.0)  # 限频30次/分钟
                except Exception as e:
                    logger.error(f"获取{trade_date}技术面因子失败: {e}")
                    time.sleep(5.0)

        else:
            # 默认同步今天
            logger.info(f"同步技术面因子: {today}")
            try:
                df = pro.stk_factor_pro(trade_date=today)
                if not df.empty:
                    df.to_sql('stk_factor_pro', conn, if_exists='append', index=False)
                    logger.info(f"技术面因子 {today}: {len(df)}条")
                else:
                    logger.info(f"技术面因子 {today}: 无数据")
            except Exception as e:
                logger.error(f"技术面因子同步失败: {e}")

    finally:
        conn.close()

    logger.info("技术面因子同步流程结束")


if __name__ == "__main__":
    main()
