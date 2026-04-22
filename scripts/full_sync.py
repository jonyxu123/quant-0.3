"""全量数据同步入口脚本（支持按接口和日期区间同步）- 优化版

用法:
    # 全量同步所有数据（默认3年）
    python scripts/full_sync.py
    
    # 全量同步所有数据，指定日期区间
    python scripts/full_sync.py --start 20230101 --end 20241231
    
    # 只同步特定接口，4线程并发
    python scripts/full_sync.py --interface daily --start 20230101 --end 20241231 --workers 4
    
    # 同步多个接口，8线程并发（适合基础接口）
    python scripts/full_sync.py --interface daily,financial --start 20230101 --workers 8
    
    # 技术面因子（限频30次/分，建议2线程）
    python scripts/full_sync.py --interface stk_factor_pro --workers 2
    
    # 查看可用接口列表
    python scripts/full_sync.py --list-interfaces

优化特性:
    - 多线程并发同步（自动速率限制）
    - 断点续传（跳过已同步数据）
    - 详细进度显示（成功率、预计剩余时间）
    - 自动重试机制
"""
import argparse
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from backend.data.full_sync import FullDataSync
from backend.data.akshare_sync import AKShareSync


INTERFACE_MAP = {
    # 基础信息 (无多线程优化)
    'stock_basic': ('sync_stock_basic', [], {}, False),
    'trade_cal': ('sync_trade_cal', [], {}, False),
    'stock_company': ('sync_historical_stock_company', [], {}, False),
    'namechange': ('sync_historical_namechange', [], {'start_year': 2016, 'end_year': 2026}, False),
    'suspend': ('sync_historical_suspend', ['start_date', 'end_date'], {}, False),
    
    # 行情数据 (支持多线程)
    'daily': ('sync_historical_daily', ['start_date', 'end_date'], {}, True),
    'daily_basic': ('sync_historical_daily_basic', ['start_date', 'end_date'], {}, True),
    'weekly': ('sync_historical_weekly', ['start_date', 'end_date'], {}, True),
    'monthly': ('sync_historical_monthly', ['start_date', 'end_date'], {}, True),
    'index_daily': ('sync_historical_index_daily', ['start_date', 'end_date'], {}, False),
    
    # 技术面因子 (支持多线程，限频30次/分)
    'stk_factor_pro': ('sync_historical_stk_factor_pro', ['start_date', 'end_date'], {}, True),
    
    # 财务数据
    'income': ('sync_historical_income', ['start_year', 'end_year'], {}, True),
    'balancesheet': ('sync_historical_balancesheet', ['start_year', 'end_year'], {}, True),
    'cashflow': ('sync_historical_cashflow', ['start_year', 'end_year'], {}, True),
    'fina_indicator': ('sync_historical_fina_indicator', ['start_year', 'end_year'], {}, True),
    'forecast': ('sync_historical_forecast', ['start_year', 'end_year'], {}, True),
    'dividend': ('sync_historical_dividend', ['start_year', 'end_year'], {}, True),
    
    # 市场参考 (支持多线程)
    'moneyflow': ('sync_historical_moneyflow', ['start_date', 'end_date'], {}, True),
    'cyq_perf': ('sync_historical_cyq_perf', ['start_date', 'end_date'], {}, True),
    'stk_holdertrade': ('sync_historical_stk_holdertrade', ['start_year', 'end_year'], {}, False),
    
    # 机构数据
    'fund_portfolio': ('sync_historical_fund_portfolio', ['start_year', 'end_year'], {}, True),
    'top10_holders': ('sync_historical_top10_holders', ['start_year', 'end_year'], {}, True),
    'top10_floatholders': ('sync_historical_top10_floatholders', ['start_year', 'end_year'], {}, True),
    'hsgt_top10': ('sync_historical_hsgt_top10', ['start_date', 'end_date'], {}, True),
    'hk_hold': ('sync_historical_hsgt_hold', ['start_date', 'end_date'], {}, True),
    'top_list': ('sync_historical_top_list', ['start_date', 'end_date'], {}, True),
    'top_inst': ('sync_historical_top_inst', ['start_date', 'end_date'], {}, True),
    'margin': ('sync_historical_margin', ['start_date', 'end_date'], {}, True),
    'margin_detail': ('sync_historical_margin_detail', ['start_date', 'end_date'], {}, True),
    'share_float': ('sync_historical_share_float', ['start_date', 'end_date'], {}, True),
    'repurchase': ('sync_historical_repurchase', ['start_date', 'end_date'], {}, False),
    'block_trade': ('sync_historical_block_trade', ['start_date', 'end_date'], {}, True),
    
    # 概念板块
    'concept': ('sync_historical_concept', [], {}, False),
    'concept_detail': ('sync_historical_concept_detail', [], {}, False),
    
    # 组合接口
    'all': ('run_full_sync', [], {}, False),
    'financial': ('sync_historical_financials', [], {}, False),
}

# AKShare 接口映射
# 格式: 接口名 -> (方法名, 参数列表, 默认值, 是否支持日期循环)
# 日期循环类：需要按交易日逐日调用（传 date 参数）
# 区间/一次性类：直接调用（传 start_date/end_date 或无参数）
AK_INTERFACE_MAP = {
    # 组合
    'ak_all': ('run_daily_akshare_sync', [], {}),
}


def list_interfaces():
    """列出所有可用接口"""
    print("可用接口列表：")
    print("=" * 80)
    
    categories = {
        '基础信息': ['stock_basic', 'trade_cal', 'stock_company', 'namechange', 'suspend'],
        '行情数据': ['daily', 'daily_basic', 'weekly', 'monthly', 'index_daily'],
        '技术面因子': ['stk_factor_pro'],
        '财务数据': ['income', 'balancesheet', 'cashflow', 'fina_indicator', 'forecast', 'dividend'],
        '市场参考': ['moneyflow', 'cyq_perf', 'stk_holdertrade'],
        '机构数据': ['fund_portfolio', 'top10_holders', 'top10_floatholders', 'hsgt_top10', 'hk_hold', 'top_list', 'top_inst'],
        '两融数据': ['margin', 'margin_detail'],
        '参考数据': ['share_float', 'repurchase', 'block_trade'],
        '概念板块': ['concept', 'concept_detail'],
        '组合接口': ['all', 'financial'],
    }
    
    for category, interfaces in categories.items():
        print(f"\n{category}:")
        for interface in interfaces:
            method_name, params, defaults, supports_mt = INTERFACE_MAP[interface]
            param_str = ', '.join(params) if params else '无参数'
            mt_flag = " [并发]" if supports_mt else ""
            print(f"  {interface:20} - {method_name:30} 参数: {param_str}{mt_flag}")

    print("\n[AKShare 接口]:")
    ak_categories = {
        '组合': ['ak_all'],
    }
    for category, interfaces in ak_categories.items():
        print(f"\n  {category}:")
        for interface in interfaces:
            method_name, params, _ = AK_INTERFACE_MAP[interface]
            param_str = params if isinstance(params, str) else (', '.join(params) if params else '无参数')
            print(f"    {interface:20} - {method_name:30} 参数: {param_str}")

    print("\n优化建议：")
    print("  - 日线/资金流向/筹码分布等支持并发，--workers 4~8 可加速 3-6 倍")
    print("  - stk_factor_pro 限频30次/分，建议 --workers 2")
    print("  - AKShare 接口不支持多线程，按日期顺序同步")
    print("\n示例：")
    print("  python scripts/full_sync.py --interface daily --start 20230101 --end 20241231 --workers 8")
    print("  python scripts/full_sync.py --interface ak_lhb --start 20230101 --end 20241231")
    print("  python scripts/full_sync.py --interface ak_all")


def main():
    parser = argparse.ArgumentParser(description="全量数据同步（支持按接口和日期区间，多线程优化版）")
    parser.add_argument("--interface", default="all", 
                       help="要同步的接口，多个用逗号分隔，或'all'同步所有（默认: all）")
    parser.add_argument("--list-interfaces", action="store_true", 
                       help="列出所有可用接口")
    
    # 日期参数
    parser.add_argument("--start", default="20230101", 
                       help="起始日期YYYYMMDD（默认: 20230101）")
    parser.add_argument("--end", default=None, 
                       help="结束日期YYYYMMDD（默认: 今天）")
    parser.add_argument("--start-year", type=int, default=2016, 
                       help="起始年份（用于财务数据，默认: 2016）")
    parser.add_argument("--end-year", type=int, default=2026, 
                       help="结束年份（用于财务数据，默认: 2026）")
    
    # 并发参数
    parser.add_argument("--workers", type=int, default=4,
                       help="并发线程数（默认: 4，基础接口建议4-8，stk_factor_pro建议2）")
    
    args = parser.parse_args()
    
    if args.list_interfaces:
        list_interfaces()
        return
    
    if args.end is None:
        args.end = datetime.now().strftime('%Y%m%d')
    
    sync = FullDataSync(max_workers=args.workers)
    sync.create_tables()
    ak_sync = None  # 懒加载，仅在需要 AKShare 接口时初始化

    interfaces = [i.strip() for i in args.interface.split(',')]

    for interface in interfaces:
        # ── AKShare 接口路由 ──
        if interface in AK_INTERFACE_MAP:
            if ak_sync is None:
                ak_sync = AKShareSync()
            method_name, params, defaults = AK_INTERFACE_MAP[interface]
            method = getattr(ak_sync, method_name)

            if params == 'date_loop':
                # 按交易日逐日调用
                logger.info(f"开始同步 AKShare 接口: {interface} ({method_name}) 日期循环 {args.start}~{args.end}")
                from datetime import datetime as _dt
                cur = _dt.strptime(args.start, '%Y%m%d')
                end_dt = _dt.strptime(args.end, '%Y%m%d')
                success, fail = 0, 0
                while cur <= end_dt:
                    date_str = cur.strftime('%Y%m%d')
                    try:
                        method(date=date_str)
                        success += 1
                    except Exception as e:
                        logger.error(f"{interface} {date_str} 失败: {e}")
                        fail += 1
                    cur += timedelta(days=1)
                logger.info(f"接口 {interface} 日期循环完成: 成功{success} 失败{fail}")
            else:
                kwargs = {**defaults}
                for param in params:
                    if param == 'start_date':
                        kwargs[param] = args.start
                    elif param == 'end_date':
                        kwargs[param] = args.end
                logger.info(f"开始同步 AKShare 接口: {interface} ({method_name})")
                logger.info("参数: " + str(kwargs).replace('{', '[').replace('}', ']'))
                try:
                    method(**kwargs)
                    logger.info(f"接口 {interface} 同步完成")
                except Exception as e:
                    logger.error(f"接口 {interface} 同步失败: {e}")
            continue

        # ── Tushare 接口路由 ──
        if interface not in INTERFACE_MAP:
            logger.error(f"未知接口: {interface}")
            logger.info("使用 --list-interfaces 查看可用接口列表")
            continue

        method_name, params, defaults, supports_mt = INTERFACE_MAP[interface]
        method = getattr(sync, method_name)

        # 构建参数
        kwargs = {}
        for param in params:
            if param == 'start_date':
                kwargs[param] = args.start
            elif param == 'end_date':
                kwargs[param] = args.end
            elif param == 'start_year':
                kwargs[param] = args.start_year
            elif param == 'end_year':
                kwargs[param] = args.end_year

        # 应用默认值
        kwargs.update(defaults)

        # 对支持多线程的接口传递 workers 参数
        if supports_mt:
            kwargs['max_workers'] = args.workers

        logger.info(f"开始同步接口: {interface} ({method_name})")
        logger.info("参数: " + str(kwargs).replace('{', '[').replace('}', ']'))
        if supports_mt:
            logger.info(f"并发模式: {args.workers} 线程")

        try:
            method(**kwargs)
            logger.info(f"接口 {interface} 同步完成")
        except Exception as e:
            logger.error(f"接口 {interface} 同步失败: {e}")

    logger.info("全量同步流程结束")


if __name__ == "__main__":
    main()
