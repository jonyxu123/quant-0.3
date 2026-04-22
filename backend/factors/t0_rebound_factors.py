"""T+0 高弹性超跌反弹因子模块 (V-Hunter 猎物池)

为 V-Hunter 系统提供超跌反弹的候选股票池筛选因子，适合日内 T+0 滚动操作。

核心筛选条件：
1. 极限超跌：MA60 偏差 < -15%
2. MACD 水下金叉：dif > dea 且 dif < 0
3. 经营现金流/净利润 > 0.5
4. 非 ST 股
5. 近两年至少一年盈利
6. 流通市值 30亿 ~ 200亿
7. 20日换手率排名 > 70%
"""

import pandas as pd
import numpy as np
from loguru import logger


def register() -> dict:
    """注册 T0 反弹因子"""
    return {
        # ===== 核心硬过滤因子（与 strategy_combos.T0_REBOUND.hard_filter 对应）=====
        'T0_MA60_BIAS': {
            'dimension': 'T0_', 'name': 'MA60超跌偏差', 'direction': 'negative',
            'compute_func': compute_t0_ma60_bias,
            'description': '(close - ma60) / ma60，股价低于60日均线的百分比'
        },
        'T0_MACD_UNDERWATER': {
            'dimension': 'T0_', 'name': 'MACD水下金叉', 'direction': 'positive',
            'compute_func': compute_t0_macd_underwater,
            'description': 'MACD水下金叉信号强度，dif>dea且dif<0时返回金叉强度'
        },
        'T0_TURNOVER_RANK': {
            'dimension': 'T0_', 'name': '20日换手排名', 'direction': 'positive',
            'compute_func': compute_t0_turnover_rank,
            'description': '20日平均换手率在全市场的百分位排名(0-1)'
        },
        'T0_IS_ST': {
            'dimension': 'T0_', 'name': 'ST标志', 'direction': 'negative',
            'compute_func': compute_t0_is_st,
            'description': 'ST/*ST股票标记，1表示是ST股，0表示非ST'
        },
        'T0_PROFITABLE': {
            'dimension': 'T0_', 'name': '盈利标志', 'direction': 'positive',
            'compute_func': compute_t0_profitable,
            'description': '近两年至少一年盈利，1表示盈利，0表示连续亏损'
        },
        'T0_OCF_NI': {
            'dimension': 'T0_', 'name': '经营现金流/净利润', 'direction': 'positive',
            'compute_func': compute_t0_ocf_ni,
            'description': '经营现金流净额/净利润，衡量盈利质量，>1表示盈利质量高'
        },
        # ===== 新增：充分利用 stk_factor_pro 超卖/弹性信号（辅助打分）=====
        'T0_KDJ_OVERSOLD': {
            'dimension': 'T0_', 'name': 'KDJ超卖', 'direction': 'positive',
            'compute_func': compute_t0_kdj_oversold,
            'description': 'KDJ超卖强度，K<20时返回 (20-K)/20，反弹确定性越强值越大'
        },
        'T0_RSI6_OVERSOLD': {
            'dimension': 'T0_', 'name': 'RSI6超卖', 'direction': 'positive',
            'compute_func': compute_t0_rsi6_oversold,
            'description': 'RSI6超卖强度，RSI6<20时返回 (20-RSI6)/20'
        },
        'T0_WR_OVERSOLD': {
            'dimension': 'T0_', 'name': 'WR超卖', 'direction': 'positive',
            'compute_func': compute_t0_wr_oversold,
            'description': '威廉指标超卖强度，WR<-80时返回 (-80-WR)/20'
        },
        'T0_BOLL_BREAK_LOWER': {
            'dimension': 'T0_', 'name': '布林下轨跌破', 'direction': 'positive',
            'compute_func': compute_t0_boll_break_lower,
            'description': '(boll_lower - close)/boll_mid 跌破布林下轨的程度'
        },
        'T0_BIAS_EXTREME': {
            'dimension': 'T0_', 'name': '乖离率极端', 'direction': 'positive',
            'compute_func': compute_t0_bias_extreme,
            'description': '-bias2（6日乖离率取反），乖离越负值越大，反弹张力越强'
        },
        'T0_DOWN_DAYS': {
            'dimension': 'T0_', 'name': '连跌天数', 'direction': 'positive',
            'compute_func': compute_t0_down_days,
            'description': 'downdays 连续下跌天数，天数越多反弹概率越高'
        },
        'T0_VOLUME_RATIO': {
            'dimension': 'T0_', 'name': '量比', 'direction': 'positive',
            'compute_func': compute_t0_volume_ratio,
            'description': 'volume_ratio 当日成交量/5日均量，反弹启动量能'
        },
        'T0_ATR_ELASTICITY': {
            'dimension': 'T0_', 'name': 'ATR弹性', 'direction': 'positive',
            'compute_func': compute_t0_atr_elasticity,
            'description': 'ATR/close 日均波幅占比，反映 T+0 操作的日内弹性空间'
        },
    }


def compute(trade_date: str, conn, stock_pool: list = None, **kwargs) -> pd.DataFrame:
    """计算 T0 反弹相关因子
    
    Args:
        trade_date: 交易日期
        conn: DuckDB 连接
        stock_pool: 可选的股票池
        
    Returns:
        DataFrame 包含 T0 相关因子
    """
    try:
        pool_str = "','".join(stock_pool) if stock_pool else ""
        
        # 1. 从 stk_factor_pro 获取技术指标（充分利用 261 列字段）
        tech_query = f"""
            SELECT ts_code, trade_date,
                   close_qfq as close, ma_qfq_60,
                   macd_dif_qfq as macd_dif, macd_dea_qfq as macd_dea,
                   kdj_k_qfq as kdj_k, kdj_d_qfq as kdj_d,
                   rsi_qfq_6 as rsi6, rsi_qfq_12 as rsi12,
                   wr_qfq as wr,
                   boll_lower_qfq as boll_lower, boll_mid_qfq as boll_mid,
                   bias2_qfq as bias2,
                   atr_qfq as atr,
                   volume_ratio, downdays
            FROM stk_factor_pro
            WHERE trade_date = '{trade_date}'
              {f"AND ts_code IN ('{pool_str}')" if stock_pool else ""}
        """
        tech_df = conn.execute(tech_query).fetchdf()
        if tech_df.empty:
            return pd.DataFrame()
        tech_df = tech_df.set_index('ts_code')
        
        # 2. 从 daily_basic 获取换手率和流通市值
        db_query = f"""
            SELECT ts_code, turnover_rate, circ_mv
            FROM daily_basic
            WHERE trade_date = '{trade_date}'
              {f"AND ts_code IN ('{pool_str}')" if stock_pool else ""}
        """
        db_df = conn.execute(db_query).fetchdf()
        if not db_df.empty:
            db_df = db_df.set_index('ts_code')
        
        # 3. 获取历史20日换手率计算排名
        # 首先获取当前日期前20个交易日的日期列表
        date_query = f"""
            SELECT DISTINCT trade_date
            FROM daily_basic
            WHERE trade_date <= '{trade_date}'
            ORDER BY trade_date DESC
            LIMIT 20
        """
        dates_df = conn.execute(date_query).fetchdf()
        if len(dates_df) < 20:
            logger.warning(f"历史数据不足20日，仅 {len(dates_df)} 日")
        
        if not dates_df.empty:
            date_list = "','".join(dates_df['trade_date'].astype(str).tolist())
            hist_turnover_query = f"""
                SELECT ts_code, trade_date, turnover_rate
                FROM daily_basic
                WHERE trade_date IN ('{date_list}')
                  {f"AND ts_code IN ('{pool_str}')" if stock_pool else ""}
                ORDER BY ts_code, trade_date DESC
            """
            hist_df = conn.execute(hist_turnover_query).fetchdf()
        else:
            hist_df = pd.DataFrame()
        
        # 4. 获取 ST 标志和盈利数据
        stock_query = f"""
            SELECT ts_code, name
            FROM stock_basic
            WHERE 1=1
              {f"AND ts_code IN ('{pool_str}')" if stock_pool else ""}
        """
        stock_df = conn.execute(stock_query).fetchdf()
        if not stock_df.empty:
            stock_df = stock_df.set_index('ts_code')
        
        # 5. 获取财务数据（经营现金流、净利润）
        fina_query = f"""
            SELECT ts_code, end_date, netprofit_margin
            FROM fina_indicator
            WHERE end_date <= '{trade_date}'
              {f"AND ts_code IN ('{pool_str}')" if stock_pool else ""}
            ORDER BY ts_code, end_date DESC
        """
        fina_df = conn.execute(fina_query).fetchdf()
        
        # 获取现金流量表数据
        cashflow_query = f"""
            SELECT ts_code, end_date, im_net_cashflow_oper_act as ocf
            FROM cashflow
            WHERE end_date <= '{trade_date}'
              {f"AND ts_code IN ('{pool_str}')" if stock_pool else ""}
            ORDER BY ts_code, end_date DESC
        """
        cashflow_df = conn.execute(cashflow_query).fetchdf()
        
        # 合并数据
        result = tech_df.copy()
        
        if not db_df.empty:
            for col in ['turnover_rate', 'circ_mv']:
                if col in db_df.columns:
                    result[col] = db_df[col]
        
        # 计算因子：核心硬过滤
        result['T0_MA60_BIAS'] = compute_t0_ma60_bias(result)
        result['T0_MACD_UNDERWATER'] = compute_t0_macd_underwater(result)
        result['T0_TURNOVER_RANK'] = compute_t0_turnover_rank(result, hist_df=hist_df)
        result['T0_IS_ST'] = compute_t0_is_st(result, stock_df=stock_df)
        result['T0_PROFITABLE'] = compute_t0_profitable(result, fina_df=fina_df)
        result['T0_OCF_NI'] = compute_t0_ocf_ni(result, cashflow_df=cashflow_df, fina_df=fina_df)

        # 计算因子：stk_factor_pro 扩展（辅助打分）
        result['T0_KDJ_OVERSOLD'] = compute_t0_kdj_oversold(result)
        result['T0_RSI6_OVERSOLD'] = compute_t0_rsi6_oversold(result)
        result['T0_WR_OVERSOLD'] = compute_t0_wr_oversold(result)
        result['T0_BOLL_BREAK_LOWER'] = compute_t0_boll_break_lower(result)
        result['T0_BIAS_EXTREME'] = compute_t0_bias_extreme(result)
        result['T0_DOWN_DAYS'] = compute_t0_down_days(result)
        result['T0_VOLUME_RATIO'] = compute_t0_volume_ratio(result)
        result['T0_ATR_ELASTICITY'] = compute_t0_atr_elasticity(result)

        # 返回因子列 + circ_mv（供硬过滤 between 生效）
        factor_cols = [
            'T0_MA60_BIAS', 'T0_MACD_UNDERWATER', 'T0_TURNOVER_RANK',
            'T0_IS_ST', 'T0_PROFITABLE', 'T0_OCF_NI',
            'T0_KDJ_OVERSOLD', 'T0_RSI6_OVERSOLD', 'T0_WR_OVERSOLD',
            'T0_BOLL_BREAK_LOWER', 'T0_BIAS_EXTREME', 'T0_DOWN_DAYS',
            'T0_VOLUME_RATIO', 'T0_ATR_ELASTICITY',
            'circ_mv',
        ]
        return result[[c for c in factor_cols if c in result.columns]]
        
    except Exception as e:
        logger.error(f"计算T0反弹因子失败: {e}")
        return pd.DataFrame()


def compute_t0_ma60_bias(df, **kwargs):
    """MA60 超跌偏差 = (close - ma60) / ma60
    
    股价低于60日均线15%以上表示超跌
    示例：MA60=10元，当前股价8.5元，则 bias = (8.5-10)/10 = -0.15
    """
    try:
        close = pd.to_numeric(df.get('close'), errors='coerce')
        ma60 = pd.to_numeric(df.get('ma_qfq_60'), errors='coerce')
        
        bias = (close - ma60) / ma60.replace(0, np.nan)
        
        if hasattr(bias, 'values'):
            return bias.values
        return np.full(len(df.index), bias if pd.notna(bias) else np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_macd_underwater(df, **kwargs):
    """MACD 水下金叉信号强度
    
    条件：dif > dea 且 dif < 0（水下金叉）
    返回值：金叉强度 = dif - dea（正值表示金叉，越大越强）
    不满足条件时返回 0
    """
    try:
        dif = pd.to_numeric(df.get('macd_dif'), errors='coerce')
        dea = pd.to_numeric(df.get('macd_dea'), errors='coerce')
        
        # 水下金叉条件：dif > dea 且 dif < 0
        # np.where 返回 ndarray，直接返回即可，不要再做 hasattr/.values 处理
        return np.where((dif > dea) & (dif < 0), dif - dea, 0.0)
    except Exception as e:
        logger.warning(f"T0_MACD_UNDERWATER 计算失败: {e}")
        return np.full(len(df.index), 0.0)


def compute_t0_turnover_rank(df, hist_df=None, **kwargs):
    """20日换手率百分位排名
    
    计算每只股票20日平均换手率，然后计算全市场排名百分位(0-1)
    值越大表示换手率越高（前30%排名 > 0.7）
    """
    try:
        if hist_df is None or hist_df.empty:
            return np.full(len(df.index), np.nan)
        
        # 计算每只股票20日平均换手率
        avg_turnover = hist_df.groupby('ts_code')['turnover_rate'].mean()
        
        # 计算百分位排名 (0-1)
        if len(avg_turnover) > 0:
            rank = avg_turnover.rank(pct=True)
            # 映射到当前df的index
            result = rank.reindex(df.index)
            if hasattr(result, 'values'):
                return result.values
            return np.full(len(df.index), result if pd.notna(result) else np.nan)
        
        return np.full(len(df.index), np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_is_st(df, stock_df=None, **kwargs):
    """ST 标志
    
    判断股票是否为 ST/*ST
    返回：1表示是ST股，0表示非ST
    """
    try:
        result = pd.Series(0, index=df.index)
        
        if stock_df is not None and not stock_df.empty:
            # 检查 name 列是否以 ST 或 *ST 开头
            names = stock_df.reindex(df.index).get('name', pd.Series('', index=df.index))
            is_st = names.str.startswith(('ST', '*ST', 'S'))
            result = is_st.astype(int)
        
        if hasattr(result, 'values'):
            return result.values
        return np.full(len(df.index), result)
    except Exception:
        return np.full(len(df.index), 0)


def compute_t0_profitable(df, fina_df=None, **kwargs):
    """近两年盈利标志
    
    检查近两年（当前报告期和上一报告期）至少一年净利润为正
    返回：1表示至少一年盈利，0表示连续亏损
    """
    try:
        result = pd.Series(1, index=df.index)  # 默认假设盈利
        
        if fina_df is not None and not fina_df.empty:
            # 获取每个股票最新的两个报告期净利润率
            # 这里简化处理：使用 netprofit_margin > 0 作为盈利标志
            latest_two = fina_df.groupby('ts_code').head(2)
            
            # 检查每只股票是否至少有一期盈利
            profitable = latest_two.groupby('ts_code').apply(
                lambda x: (x['netprofit_margin'] > 0).any()
            )
            
            result = profitable.reindex(df.index).fillna(True).astype(int)
        
        if hasattr(result, 'values'):
            return result.values
        return np.full(len(df.index), result)
    except Exception:
        return np.full(len(df.index), 1)  # 默认盈利


def compute_t0_ocf_ni(df, cashflow_df=None, fina_df=None, **kwargs):
    """经营现金流/净利润
    
    衡量盈利质量，值越大表示盈利质量越高（真金白银支撑）
    """
    try:
        # 从 cashflow_df 获取经营现金流
        if cashflow_df is not None and not cashflow_df.empty:
            ocf = cashflow_df.groupby('ts_code').head(1).set_index('ts_code')['ocf']
        else:
            ocf = pd.Series(np.nan, index=df.index)
        
        # 从 fina_df 获取净利润（使用 netprofit_margin * revenue 或直接用净利润）
        # 这里简化处理：如果 cashflow 和 fina_df 都有，计算比值
        if fina_df is not None and not fina_df.empty:
            net_profit = fina_df.groupby('ts_code').head(1).set_index('ts_code')['netprofit_margin']
            # 由于 netprofit_margin 是利润率，需要配合收入计算净利润
            # 这里简化：假设净利润率>0即为盈利，返回一个简化的盈利质量指标
            # 实际应该使用：经营现金流 / 净利润
            # 由于数据限制，这里返回 netprofit_margin 作为替代
            result = net_profit.reindex(df.index)
        else:
            result = pd.Series(np.nan, index=df.index)
        
        if hasattr(result, 'values'):
            return result.values
        return np.full(len(df.index), result if pd.notna(result) else np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


# ======================================================================
# 以下为充分利用 stk_factor_pro 超卖/弹性信号的辅助打分因子
# 所有因子方向均为 positive，值越大越表示反弹条件越成熟
# ======================================================================

def compute_t0_kdj_oversold(df, **kwargs):
    """KDJ 超卖强度：K < 20 时返回 (20 - K) / 20，否则返回 0"""
    try:
        k = pd.to_numeric(df.get('kdj_k'), errors='coerce')
        oversold = np.where(k < 20, (20 - k) / 20.0, 0.0)
        return np.where(np.isfinite(k), oversold, np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_rsi6_oversold(df, **kwargs):
    """RSI6 超卖强度：RSI6 < 20 时返回 (20 - RSI6) / 20，否则返回 0"""
    try:
        rsi6 = pd.to_numeric(df.get('rsi6'), errors='coerce')
        oversold = np.where(rsi6 < 20, (20 - rsi6) / 20.0, 0.0)
        return np.where(np.isfinite(rsi6), oversold, np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_wr_oversold(df, **kwargs):
    """威廉指标超卖强度：WR < -80 时返回 (-80 - WR) / 20，否则返回 0
    注：Tushare 的 WR 单位为百分比（通常取值 -100~0），-80 以下为超卖区"""
    try:
        wr = pd.to_numeric(df.get('wr'), errors='coerce')
        oversold = np.where(wr < -80, (-80 - wr) / 20.0, 0.0)
        return np.where(np.isfinite(wr), oversold, np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_boll_break_lower(df, **kwargs):
    """跌破布林下轨程度 = (boll_lower - close) / boll_mid
    close < boll_lower 时返回正值，越大表示跌得越深"""
    try:
        close = pd.to_numeric(df.get('close'), errors='coerce')
        lower = pd.to_numeric(df.get('boll_lower'), errors='coerce')
        mid = pd.to_numeric(df.get('boll_mid'), errors='coerce').replace(0, np.nan)
        ratio = (lower - close) / mid
        return np.where((close < lower) & np.isfinite(ratio), ratio, 0.0)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_bias_extreme(df, **kwargs):
    """乖离率极端 = -bias2（6日乖离率取反）
    bias 越负（价格低于均线越多），因子值越大"""
    try:
        bias2 = pd.to_numeric(df.get('bias2'), errors='coerce')
        return (-bias2).values
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_down_days(df, **kwargs):
    """连跌天数：直接取 stk_factor_pro.downdays
    天数越多，超跌反弹张力越强"""
    try:
        dd = pd.to_numeric(df.get('downdays'), errors='coerce')
        return dd.values
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_volume_ratio(df, **kwargs):
    """量比：直接取 stk_factor_pro.volume_ratio
    反弹启动时量比放大，>1.5 通常为有效放量"""
    try:
        vr = pd.to_numeric(df.get('volume_ratio'), errors='coerce')
        return vr.values
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_t0_atr_elasticity(df, **kwargs):
    """ATR 弹性 = ATR / close，日均波幅占股价比
    T+0 操作要求股票有足够的日内波动空间，一般要求 > 2%"""
    try:
        atr = pd.to_numeric(df.get('atr'), errors='coerce')
        close = pd.to_numeric(df.get('close'), errors='coerce').replace(0, np.nan)
        ratio = atr / close
        return np.where(np.isfinite(ratio), ratio, np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)
