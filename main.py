"""多因子选股系统 v8.0 - CLI主入口

用法:
    python main.py sync-full                    # 全量同步所有数据
    python main.py sync-full --interface daily --start 20230101 --end 20241231  # 按接口同步
    python main.py sync-daily                   # 每日增量同步
    python main.py compute-factors --date T     # 计算因子
    python main.py preprocess --start S --end E # 因子预处理
    python main.py select-stocks --date T --top 50  # 选股
    python main.py backtest --strategy A --start 20240101 --end 20241231  # 回测
    python main.py ic-monitor                   # IC监控
    python main.py quality-check --date T       # 数据质量检查
"""
import argparse
import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loguru import logger

# 配置 logstyles 主题（可选: Tokyo Night Storm, Catpuccin Mocha, Darcula, GitHub Dark 等）
try:
    from logstyles import LogStyles
    # 使用 Standard 格式，避免 Detailed 的字段不匹配问题
    formatter = LogStyles.get_formatter(
        theme_name='Tokyo Night Storm',
        format_name='Detailed'
    )

    # 应用配置（serialize=False 避免字典被误解析）
    logger.remove()
    logger.add(
        sys.stdout,
        format=formatter,
        colorize=True,
        enqueue=True,
        serialize=False,
        diagnose=False,
        backtrace=False,
        catch=False  # 不捕获格式错误，直接打印
    )
    print(f"[Logstyles] 主题已启用: Tokyo Night Storm")
except Exception as e:
    # 出错时使用默认样式
    print(f"[Logstyles] 配置失败: {e}")


# ============================================================
# 子命令处理函数
# ============================================================

def cmd_sync_full(args):
    """全量数据同步（支持按接口和日期区间，多线程优化版）"""
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # 直接调用scripts/full_sync.py的main函数
    from scripts.full_sync import main as full_sync_main

    # 构建参数列表
    original_argv = sys.argv.copy()

    try:
        # 重新构建参数
        new_argv = ['full_sync.py']

        # 处理 --list-interfaces 参数
        if hasattr(args, 'list_interfaces') and args.list_interfaces:
            new_argv.append('--list-interfaces')
        else:
            if hasattr(args, 'interface') and args.interface != 'all':
                new_argv.extend(['--interface', args.interface])
            if hasattr(args, 'start'):
                new_argv.extend(['--start', args.start])
            if hasattr(args, 'end'):
                new_argv.extend(['--end', args.end])
            if hasattr(args, 'start_year'):
                new_argv.extend(['--start-year', str(args.start_year)])
            if hasattr(args, 'end_year'):
                new_argv.extend(['--end-year', str(args.end_year)])
            if hasattr(args, 'workers'):
                new_argv.extend(['--workers', str(args.workers)])

        sys.argv = new_argv
        full_sync_main()
    finally:
        sys.argv = original_argv


def cmd_sync_daily(args):
    """每日增量同步"""
    from backend.data.incremental_sync import IncrementalSync
    from backend.data.akshare_sync import AKShareSync

    # Tushare增量
    inc_sync = IncrementalSync()
    inc_sync.run_daily_sync()

    # AKShare增量
    ak_sync = AKShareSync()
    ak_sync.run_daily_akshare_sync()


def cmd_compute_factors(args):
    """计算因子"""
    from backend.factors.factor_engine import FactorEngine
    import duckdb

    trade_date = args.date or datetime.now().strftime('%Y%m%d')
    engine = FactorEngine()

    logger.info(f"开始计算因子: {trade_date}")
    factor_df = engine.compute_all(trade_date)

    # 关闭只读连接，再开写连接
    db_path = engine.db_path
    engine.conn.close()

    if factor_df.empty:
        logger.warning(f"{trade_date} 因子计算结果为空")
        return

    factor_df = factor_df.reset_index()
    if 'index' in factor_df.columns and 'ts_code' not in factor_df.columns:
        factor_df = factor_df.rename(columns={'index': 'ts_code'})
    factor_df['trade_date'] = trade_date

    # 写入DuckDB（独占写连接）
    conn = duckdb.connect(db_path)
    try:
        # 删除同日已有数据再写入
        # 删除旧表结构（因子列数可能已变化）
        try:
            conn.execute("DROP TABLE IF EXISTS factor_raw")
        except Exception:
            pass
        conn.execute("CREATE TABLE factor_raw AS SELECT * FROM factor_df")
        logger.info(f"因子计算完成: {trade_date}, {len(factor_df)}只股票, {len(factor_df.columns)-1}个因子")
    finally:
        conn.close()


def cmd_preprocess(args):
    """因子预处理"""
    from backend.preprocessing.preprocess import preprocess_factors_chunked
    import duckdb

    conn = duckdb.connect(DB_PATH)
    try:
        result = preprocess_factors_chunked(
            conn,
            start_date=args.start,
            end_date=args.end
        )
        logger.info(f"预处理结果: {result}")
    finally:
        conn.close()


def cmd_select_stocks(args):
    """选股"""
    from backend.factors.factor_engine import FactorEngine
    from backend.strategy.strategy_engine import StrategyEngine

    trade_date = args.date or datetime.now().strftime('%Y%m%d')
    top_n = args.top or 50
    strategy_id = args.strategy or 'D'

    engine = FactorEngine()
    strategy = StrategyEngine(strategy_id)

    logger.info(f"开始选股: {trade_date}, 策略{strategy_id}, Top{top_n}")
    factor_df = engine.compute_all(trade_date)

    if factor_df.empty:
        logger.warning(f"{trade_date} 无因子数据，无法选股")
        return

    result = strategy.run_pipeline(strategy_id, factor_df, n=top_n)
    selected = result['selected']

    if selected.empty:
        logger.warning("选股结果为空")
        return

    logger.info(f"选股完成: {len(selected)}只")
    print(f"\n策略{strategy_id} Top{top_n} ({trade_date}):")
    print("-" * 80)
    cols = ['rank', 'strategy_score']
    available_cols = [c for c in cols if c in selected.columns]
    print(selected[available_cols].to_string())


def cmd_backtest(args):
    """回测"""
    from backend.backtest.backtester import Backtester

    backtester = Backtester()
    result = backtester.run_backtest(
        strategy_id=args.strategy,
        start_date=args.start,
        end_date=args.end,
        top_n=args.top or 50
    )

    report = backtester.generate_report(args.strategy, result)
    print(report)


def cmd_ic_monitor(args):
    """IC监控"""
    from backend.ic_monitor.ic_monitor import ICMonitor

    monitor = ICMonitor()
    report = monitor.generate_report()

    logger.info(f"IC监控报告: 有效因子{report['active_factors']}/{report['total_factors']}, "
                f"淘汰{report['culled_factors']}个")

    # 输出详细报告
    print("\n因子IC监控报告")
    print("=" * 60)
    print(f"总因子数: {report['total_factors']}")
    print(f"有效因子: {report['active_factors']}")
    print(f"淘汰因子: {report['culled_factors']}")
    print()

    for factor_id, detail in report.get('factor_details', {}).items():
        status = "✓ 有效" if detail['is_active'] else "✗ 淘汰"
        print(f"  {factor_id:20s} IC均值={detail['ic_mean']:+.4f}  "
              f"IC_IR={detail['ic_ir']:+.4f}  {status}")


def cmd_quality_check(args):
    """数据质量检查"""
    from backend.data.quality_check import DataQualityChecker

    trade_date = args.date or datetime.now().strftime('%Y%m%d')
    checker = DataQualityChecker()

    report = checker.generate_report(trade_date)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="多因子选股系统 v8.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py sync-full                    # 全量同步所有数据
  python main.py sync-full --interface daily --start 20230101 --end 20241231  # 按接口同步
  python main.py sync-full --interface income,balancesheet --start-year 2018 --end-year 2024  # 同步财务数据
  python main.py sync-full --list-interfaces  # 查看可用接口列表
  python main.py sync-daily                   # 每日增量同步
  python main.py compute-factors --date 20241231  # 计算因子
  python main.py preprocess --start 20240101 --end 20241231  # 因子预处理
  python main.py select-stocks --date 20241231 --top 50  # 选股
  python main.py backtest --strategy A --start 20240101 --end 20241231  # 回测
  python main.py ic-monitor                   # IC监控
  python main.py quality-check --date 20241231  # 数据质量检查
"""
    )
    subparsers = parser.add_subparsers(dest="command", help="可用子命令")

    # sync-full
    p_sync_full = subparsers.add_parser("sync-full", help="全量数据同步（支持按接口和日期区间，多线程优化版）")
    p_sync_full.add_argument("--interface", default="all",
                           help="要同步的接口，多个用逗号分隔，或'all'同步所有（默认: all）")
    p_sync_full.add_argument("--list-interfaces", action="store_true",
                           help="列出所有可用接口")
    p_sync_full.add_argument("--start", default="20230101",
                           help="起始日期YYYYMMDD（默认: 20230101）")
    p_sync_full.add_argument("--end", default=None,
                           help="结束日期YYYYMMDD（默认: 今天）")
    p_sync_full.add_argument("--start-year", type=int, default=2016,
                           help="起始年份（用于财务数据，默认: 2016）")
    p_sync_full.add_argument("--end-year", type=int, default=2026,
                           help="结束年份（用于财务数据，默认: 2026）")
    p_sync_full.add_argument("--workers", type=int, default=4,
                           help="并发线程数（默认: 4，日线等接口建议4-8，stk_factor_pro建议2）")
    p_sync_full.set_defaults(func=cmd_sync_full)

    # sync-daily
    p_sync_daily = subparsers.add_parser("sync-daily", help="每日增量同步")
    p_sync_daily.set_defaults(func=cmd_sync_daily)

    # compute-factors
    p_compute = subparsers.add_parser("compute-factors", help="计算因子")
    p_compute.add_argument("--date", default=None, help="交易日期 (YYYYMMDD, 默认今天)")
    p_compute.set_defaults(func=cmd_compute_factors)

    # preprocess
    p_preprocess = subparsers.add_parser("preprocess", help="因子预处理")
    p_preprocess.add_argument("--start", required=True, help="起始日期 (YYYYMMDD)")
    p_preprocess.add_argument("--end", required=True, help="结束日期 (YYYYMMDD)")
    p_preprocess.set_defaults(func=cmd_preprocess)

    # select-stocks
    p_select = subparsers.add_parser("select-stocks", help="选股")
    p_select.add_argument("--date", default=None, help="交易日期 (YYYYMMDD, 默认今天)")
    p_select.add_argument("--top", type=int, default=50, help="选股数量 (默认50)")
    p_select.add_argument("--strategy", default="D", help="策略ID (A-AD, 默认D)")
    p_select.set_defaults(func=cmd_select_stocks)

    # backtest
    p_backtest = subparsers.add_parser("backtest", help="回测")
    p_backtest.add_argument("--strategy", required=True, help="策略ID (A-AD)")
    p_backtest.add_argument("--start", required=True, help="回测开始日期 (YYYYMMDD)")
    p_backtest.add_argument("--end", required=True, help="回测结束日期 (YYYYMMDD)")
    p_backtest.add_argument("--top", type=int, default=50, help="选股数量 (默认50)")
    p_backtest.set_defaults(func=cmd_backtest)

    # ic-monitor
    p_ic = subparsers.add_parser("ic-monitor", help="IC监控")
    p_ic.set_defaults(func=cmd_ic_monitor)

    # quality-check
    p_quality = subparsers.add_parser("quality-check", help="数据质量检查")
    p_quality.add_argument("--date", default=None, help="检查日期 (YYYYMMDD, 默认今天)")
    p_quality.set_defaults(func=cmd_quality_check)

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
