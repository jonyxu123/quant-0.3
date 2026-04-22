"""
龙头选股策略专用因子模块
包含分层过滤策略所需的所有专用因子

Layer 1: F-Score ≥ 7 (财务审计)
Layer 2: 换手率 2%-12% + 流通市值 50-300亿 (弹性过滤)
Layer 3: RS 排名 > 80 (动能排名)
Layer 4: Hurst 指数 > 0.6 + Z-Score 0.5~2.0 + OBV趋势 (趋势纯度)
Layer 5: MACD/MTM 共振 + VWAP 确认 (入场触发)
"""

import pandas as pd
import numpy as np
from loguru import logger


def register() -> dict:
    """注册龙头策略专用因子"""
    return {
        # === 质量因子 ===
        'D_F_SCORE': {
            'dimension': 'D_',
            'name': 'Piotroski F-Score (9维财务质量评分)',
            'direction': 'positive',
            'compute_func': compute_d_f_score
        },
        'D_EARNINGS_QUALITY': {
            'dimension': 'D_',
            'name': '盈利质量 (经营现金流/净利润)',
            'direction': 'positive',
            'compute_func': compute_d_earnings_quality
        },
        
        # === 动量因子 ===
        'D_RS_RATING': {
            'dimension': 'D_',
            'name': 'RS排名 (全市场百分位 0-100)',
            'direction': 'positive',
            'compute_func': compute_d_rs_rating
        },
        'D_RS_Q4_ACCEL': {
            'dimension': 'D_',
            'name': 'Q4涨幅加速 (Q4-Q3涨幅差)',
            'direction': 'positive',
            'compute_func': compute_d_rs_q4_accel
        },
        
        # === 趋势纯度因子 ===
        'D_HURST_EXP': {
            'dimension': 'D_',
            'name': 'Hurst指数 (趋势持续性)',
            'direction': 'positive',
            'compute_func': compute_d_hurst_exp
        },
        'D_Z_SCORE_60': {
            'dimension': 'D_',
            'name': 'Z-Score 60日 (价格偏离度)',
            'direction': 'positive',
            'compute_func': compute_d_z_score_60
        },
        'D_PRICE_VS_52W_HIGH': {
            'dimension': 'D_',
            'name': '距52周高点比例',
            'direction': 'positive',
            'compute_func': compute_d_price_vs_52w_high
        },
        
        # === 流动性因子 ===
        'D_TURNRATE': {
            'dimension': 'D_',
            'name': '日换手率 (%)',
            'direction': 'positive',
            'compute_func': compute_d_turnrate
        },
        'D_CIRC_MCAP': {
            'dimension': 'D_',
            'name': '流通市值 (亿元)',
            'direction': 'positive',
            'compute_func': compute_d_circ_mcap
        },
        'D_OBV_TREND': {
            'dimension': 'D_',
            'name': 'OBV趋势 (5日/20日均值比)',
            'direction': 'positive',
            'compute_func': compute_d_obv_trend
        },
        'D_OBV_BREAKOUT': {
            'dimension': 'D_',
            'name': 'OBV突破20日新高',
            'direction': 'positive',
            'compute_func': compute_d_obv_breakout
        },
        
        # === 技术因子 ===
        'D_MACD_DIF': {
            'dimension': 'D_',
            'name': 'MACD DIF值',
            'direction': 'positive',
            'compute_func': compute_d_macd_dif
        },
        'D_MACD_HIST_DELTA': {
            'dimension': 'D_',
            'name': 'MACD柱变化 (今日-昨日)',
            'direction': 'positive',
            'compute_func': compute_d_macd_hist_delta
        },
        'D_MACD_GOLDEN_CROSS': {
            'dimension': 'D_',
            'name': 'MACD金叉信号',
            'direction': 'positive',
            'compute_func': compute_d_macd_golden_cross
        },
        'D_MTM_BREAKOUT': {
            'dimension': 'D_',
            'name': 'MTM突破信号',
            'direction': 'positive',
            'compute_func': compute_d_mtm_breakout
        },
        'D_MTM_SLOPE': {
            'dimension': 'D_',
            'name': 'MTM斜率',
            'direction': 'positive',
            'compute_func': compute_d_mtm_slope
        },
        'D_VWAP_BIAS': {
            'dimension': 'D_',
            'name': 'VWAP偏离度',
            'direction': 'positive',
            'compute_func': compute_d_vwap_bias
        },
        
        # === 风控因子 ===
        'D_ATR_14': {
            'dimension': 'D_',
            'name': '14日ATR (止损计算)',
            'direction': 'negative',
            'compute_func': compute_d_atr_14
        },
        'D_BOLL_POSITION': {
            'dimension': 'D_',
            'name': 'BOLL带位置 (0~1)',
            'direction': 'positive',
            'compute_func': compute_d_boll_position
        },
        'D_MA20_DISTANCE': {
            'dimension': 'D_',
            'name': '距MA20距离 (%)',
            'direction': 'positive',
            'compute_func': compute_d_ma20_distance
        },
        
        # === 安全因子 ===
        'D_DEBT_RATIO_DELTA': {
            'dimension': 'D_',
            'name': '资产负债率同比变化',
            'direction': 'negative',
            'compute_func': compute_d_debt_ratio_delta
        },
        'D_CURR_RATIO_DELTA': {
            'dimension': 'D_',
            'name': '流动比率同比变化',
            'direction': 'positive',
            'compute_func': compute_d_curr_ratio_delta
        },
        'D_ROA_DELTA': {
            'dimension': 'D_',
            'name': 'ROA同比变化',
            'direction': 'positive',
            'compute_func': compute_d_roa_delta
        },
    }


def compute(trade_date: str, conn, stock_pool: list = None) -> pd.DataFrame:
    """计算所有龙头策略专用因子"""
    try:
        # 硬性过滤：排除ST股和上市不足1年的次新股
        one_year_ago = str(int(trade_date) - 10000)  # 约1年前
        
        if stock_pool is None:
            stock_pool_df = conn.execute(f"""
                SELECT ts_code FROM stock_basic 
                WHERE list_date IS NOT NULL 
                  AND list_date <= '{one_year_ago}'  -- 排除次新股(<1年)
                  AND name NOT LIKE '%ST%'            -- 排除ST股
                  AND name NOT LIKE '%*ST%'
            """).fetchdf()
            stock_pool = stock_pool_df['ts_code'].tolist()
        else:
            # 过滤传入的股票池，排除ST和次新股
            pool_str = "','".join(stock_pool)
            filtered_df = conn.execute(f"""
                SELECT ts_code FROM stock_basic 
                WHERE ts_code IN ('{pool_str}')
                  AND list_date IS NOT NULL 
                  AND list_date <= '{one_year_ago}'  -- 排除次新股(<1年)
                  AND name NOT LIKE '%ST%'            -- 排除ST股
                  AND name NOT LIKE '%*ST%'
            """).fetchdf()
            stock_pool = filtered_df['ts_code'].tolist()
        
        if not stock_pool:
            return pd.DataFrame()
        
        pool_str = "','".join(stock_pool)
        
        # 获取日线数据 (用于计算技术指标)
        daily_df = conn.execute(f"""
            SELECT ts_code, close, open, high, low, vol, amount
            FROM daily
            WHERE trade_date = '{trade_date}'
              AND ts_code IN ('{pool_str}')
        """).fetchdf()
        daily_df = daily_df.set_index('ts_code') if not daily_df.empty else pd.DataFrame()
        
        # 获取日线基础数据（含换手率，供 D_TURNRATE 使用）
        basic_df = conn.execute(f"""
            SELECT ts_code, total_mv, circ_mv, float_share, turnover_rate
            FROM daily_basic
            WHERE trade_date = '{trade_date}'
              AND ts_code IN ('{pool_str}')
        """).fetchdf()
        basic_df = basic_df.set_index('ts_code') if not basic_df.empty else pd.DataFrame()
        
        # 获取财务报表数据 (用于F-Score)
        # 注意：ocfps和eps不在fina_indicator表中，需要从其他表获取或移除
        fina_df = conn.execute(f"""
            SELECT ts_code, roe, roa, grossprofit_margin, 
                   debt_to_assets, current_ratio, quick_ratio
            FROM fina_indicator
            WHERE end_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, end_date DESC
        """).fetchdf()
        fina_df = fina_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not fina_df.empty else pd.DataFrame()
        
        # 获取资产负债表 (用于资产负债率同比)
        balance_df = conn.execute(f"""
            SELECT ts_code, total_liab, total_assets, total_hldr_eqy_exc_min_int
            FROM balancesheet
            WHERE end_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, end_date DESC
        """).fetchdf()
        balance_df = balance_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not balance_df.empty else pd.DataFrame()
        
        # 获取去年同期的财务数据 (用于计算同比变化)
        # 取前一年年末的数据 (如1231) 进行对比，确保口径一致
        prev_year_end = str(int(trade_date[:4]) - 1) + '1231'
        prev_fina_df = conn.execute(f"""
            SELECT ts_code, roa, debt_to_assets, current_ratio
            FROM fina_indicator
            WHERE end_date <= '{prev_year_end}'
              AND end_date >= '{str(int(trade_date[:4]) - 1)}0101'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, end_date DESC
        """).fetchdf()
        prev_fina_df = prev_fina_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not prev_fina_df.empty else pd.DataFrame()
        
        # 获取历史日线数据 (用于计算技术指标)
        hist_daily = conn.execute(f"""
            SELECT ts_code, trade_date, close, open, high, low, vol, amount
            FROM daily
            WHERE trade_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
              AND trade_date >= '{int(trade_date) - 10000}'
            ORDER BY ts_code, trade_date DESC
        """).fetchdf()

        # 性能优化：一次 groupby 成 dict，避免每个因子对全表做 5506 次 boolean 扫描
        # 原 `hist_daily[hist_daily['ts_code'] == ts_code]` → `hist_grouped.get(ts_code, ...)`
        if not hist_daily.empty:
            hist_grouped = {ts: g for ts, g in hist_daily.groupby('ts_code', sort=False)}
        else:
            hist_grouped = {}
        
        # 获取历史52周数据
        weeks_52_ago = str(int(trade_date) - 10000)
        hist_52w = conn.execute(f"""
            SELECT ts_code, MAX(high) as high_52w, MIN(low) as low_52w
            FROM daily
            WHERE trade_date >= '{weeks_52_ago}'
              AND trade_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            GROUP BY ts_code
        """).fetchdf()
        hist_52w = hist_52w.set_index('ts_code') if not hist_52w.empty else pd.DataFrame()
        
        # 获取现金流数据 (用于盈利质量)
        # 注意：使用实际存在的字段 im_net_cashflow_oper_act
        cashflow_df = conn.execute(f"""
            SELECT ts_code, im_net_cashflow_oper_act as net_cash_flows_oper_act
            FROM cashflow
            WHERE end_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, end_date DESC
        """).fetchdf()
        cashflow_df = cashflow_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not cashflow_df.empty else pd.DataFrame()
        
        # 获取利润表数据 (用于盈利质量计算的净利润)
        income_df = conn.execute(f"""
            SELECT ts_code, n_income_attr_p as net_profit
            FROM income
            WHERE end_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, end_date DESC
        """).fetchdf()
        income_df = income_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not income_df.empty else pd.DataFrame()
        
        # 合并基础数据
        df = pd.DataFrame(index=stock_pool)
        for src in [daily_df, basic_df, fina_df, balance_df, hist_52w, cashflow_df, income_df]:
            if not src.empty:
                common_idx = df.index.intersection(src.index)
                for col in src.columns:
                    df.loc[common_idx, col] = src.loc[common_idx, col]
        
        # 计算所有因子
        result = pd.DataFrame(index=df.index)
        
        # 质量因子
        result['D_F_SCORE'] = compute_d_f_score(df)
        result['D_EARNINGS_QUALITY'] = compute_d_earnings_quality(df)
        
        # 动量因子
        result['D_RS_RATING'] = compute_d_rs_rating(df, hist_grouped, trade_date)
        result['D_RS_Q4_ACCEL'] = compute_d_rs_q4_accel(df, hist_grouped, trade_date)
        
        # 趋势纯度
        result['D_HURST_EXP'] = compute_d_hurst_exp(df, hist_grouped)
        result['D_Z_SCORE_60'] = compute_d_z_score_60(df, hist_grouped)
        result['D_PRICE_VS_52W_HIGH'] = compute_d_price_vs_52w_high(df)
        
        # 流动性
        result['D_TURNRATE'] = compute_d_turnrate(df)
        result['D_CIRC_MCAP'] = compute_d_circ_mcap(df)
        result['D_OBV_TREND'] = compute_d_obv_trend(df, hist_grouped)
        result['D_OBV_BREAKOUT'] = compute_d_obv_breakout(df, hist_grouped)
        
        # 技术指标
        result['D_MACD_DIF'] = compute_d_macd_dif(df, hist_grouped)
        result['D_MACD_HIST_DELTA'] = compute_d_macd_hist_delta(df, hist_grouped)
        result['D_MACD_GOLDEN_CROSS'] = compute_d_macd_golden_cross(df, hist_grouped)
        result['D_MTM_BREAKOUT'] = compute_d_mtm_breakout(df, hist_grouped)
        result['D_MTM_SLOPE'] = compute_d_mtm_slope(df, hist_grouped)
        result['D_VWAP_BIAS'] = compute_d_vwap_bias(df, hist_grouped)
        
        # 风控
        result['D_ATR_14'] = compute_d_atr_14(df, hist_grouped)
        result['D_BOLL_POSITION'] = compute_d_boll_position(df, hist_grouped)
        result['D_MA20_DISTANCE'] = compute_d_ma20_distance(df, hist_grouped)
        
        # 安全因子
        result['D_DEBT_RATIO_DELTA'] = compute_d_debt_ratio_delta(df, prev_fina_df)
        result['D_CURR_RATIO_DELTA'] = compute_d_curr_ratio_delta(df, prev_fina_df)
        result['D_ROA_DELTA'] = compute_d_roa_delta(df, prev_fina_df)
        
        return result
        
    except Exception as e:
        logger.error(f"计算龙头策略因子失败：{e}")
        return pd.DataFrame()


# ==================== 因子计算函数 ====================

# 用于 hist_grouped.get() 的缺省返回值，避免每次新建 DataFrame
_EMPTY_DF = pd.DataFrame()

def compute_d_f_score(df, **kwargs):
    """
    Piotroski F-Score: 9维财务质量评分
    
    基于可用字段的简化版:
    1. ROA > 0 (盈利能力)
    2. 毛利率 > 0 (盈利空间)
    3. 毛利率 > 20% (较好水平)
    4. 流动比率 > 1 (短期偿债)
    5. 流动比率 > 1.5 (较好水平)
    6. 负债率 < 60% (杠杆适中)
    7. 负债率 < 40% (较低杠杆)
    8. ROE > 0 (股东回报)
    9. ROE > 8% (较好回报)
    
    评分: 0-9分，≥7分优秀
    """
    score = pd.Series(0, index=df.index)
    
    try:
        # 1. ROA > 0
        roa = pd.to_numeric(df.get('roa'), errors='coerce')
        score += (roa > 0).astype(int)
        
        # 2. 毛利率 > 0
        gpm = pd.to_numeric(df.get('grossprofit_margin'), errors='coerce')
        score += (gpm > 0).astype(int)
        
        # 3. 毛利率 > 20% (较好)
        score += (gpm > 20).astype(int)
        
        # 4. 流动比率 > 1
        curr = pd.to_numeric(df.get('current_ratio'), errors='coerce')
        score += (curr > 1).astype(int)
        
        # 5. 流动比率 > 1.5 (较好)
        score += (curr > 1.5).astype(int)
        
        # 6. 负债率 < 60%
        debt = pd.to_numeric(df.get('debt_to_assets'), errors='coerce')
        score += (debt < 60).astype(int)
        
        # 7. 负债率 < 40% (更优)
        score += (debt < 40).astype(int)
        
        # 8. ROE > 0
        roe = pd.to_numeric(df.get('roe'), errors='coerce')
        score += (roe > 0).astype(int)
        
        # 9. ROE > 8% (较好)
        score += (roe > 8).astype(int)
        
    except Exception as e:
        logger.warning(f"F-Score计算错误: {e}")
    
    return score.values


def compute_d_earnings_quality(df, **kwargs):
    """盈利质量 = 经营现金流 / 净利润，>0.5优秀"""
    try:
        oper_cf = pd.to_numeric(df.get('net_cash_flows_oper_act'), errors='coerce')
        net_profit = pd.to_numeric(df.get('net_profit'), errors='coerce')
        
        # 避免除零
        quality = np.where(
            (net_profit > 0) & (net_profit != 0),
            oper_cf / net_profit,
            np.nan
        )
        return quality
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_rs_rating(df, hist_grouped, trade_date, **kwargs):
    """
    RS排名 (Relative Strength Rating)
    计算股票过去52周涨幅在全市场的百分位排名 (0-100)
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        # 计算每只股票过去52周涨幅
        returns = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF)
            if len(stock_data) >= 20:  # 至少需要20天数据
                latest_close = stock_data.iloc[0]['close']
                # 找到52周前的价格
                year_ago_idx = min(len(stock_data) - 1, 250)  # 约250个交易日
                year_ago_close = stock_data.iloc[year_ago_idx]['close'] if year_ago_idx < len(stock_data) else stock_data.iloc[-1]['close']
                
                if year_ago_close > 0:
                    returns[ts_code] = (latest_close - year_ago_close) / year_ago_close * 100
        
        if not returns:
            return pd.Series(np.nan, index=df.index).values
        
        # 转换为Series并计算百分位排名
        returns_series = pd.Series(returns)
        rs_rating = returns_series.rank(pct=True) * 100
        
        # 对齐到df.index
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in rs_rating.index:
                result[ts_code] = rs_rating[ts_code]
        
        return result.values
    except Exception as e:
        logger.warning(f"RS排名计算错误: {e}")
        return pd.Series(np.nan, index=df.index).values


def compute_d_rs_q4_accel(df, hist_grouped, trade_date, **kwargs):
    """
    Q4涨幅加速 = Q4季度涨幅 - Q3季度涨幅
    使用最近60日和最近120-60日的涨幅差
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        accel = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 120:
                # Q4: 最近60日
                q4_return = (stock_data.iloc[-1]['close'] - stock_data.iloc[-60]['close']) / stock_data.iloc[-60]['close'] * 100
                # Q3: 60-120日前
                q3_return = (stock_data.iloc[-60]['close'] - stock_data.iloc[-120]['close']) / stock_data.iloc[-120]['close'] * 100
                accel[ts_code] = q4_return - q3_return
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in accel:
                result[ts_code] = accel[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_hurst_exp(df, hist_grouped, **kwargs):
    """
    Hurst指数: 使用R/S分析法计算H值
    H > 0.6: 趋势持续性强
    H = 0.5: 随机游走
    H < 0.4: 均值回归
    
    使用梯度数据窗口：优先250天，不足时降级到120天，再不足则返回NaN
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        hurst_values = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            
            # 梯度数据窗口：优先250天，不足则降级到120天
            window_days = None
            tau = []
            
            if len(stock_data) >= 250:
                window_days = 250
                tau = [4, 8, 16, 32, 64, 128]
            elif len(stock_data) >= 120:
                window_days = 120
                tau = [4, 8, 16, 24, 32]
            else:
                # 数据不足，跳过
                continue
            
            # 取最近 window_days 天
            prices = stock_data['close'].values[-window_days:]
            
            # 计算对数收益率
            log_returns = np.log(prices[1:] / prices[:-1])
            
            if len(log_returns) > 30:
                rs_values = []
                
                for lag in tau:
                    # 确保至少有2个chunk，保证统计意义
                    if len(log_returns) >= lag * 2:
                        chunks = len(log_returns) // lag
                        rs_chunk = []
                        for i in range(chunks):
                            chunk = log_returns[i*lag:(i+1)*lag]
                            mean_chunk = np.mean(chunk)
                            dev_chunk = chunk - mean_chunk
                            cumdev = np.cumsum(dev_chunk)
                            r = np.max(cumdev) - np.min(cumdev)
                            s = np.std(chunk)
                            if s > 0:
                                rs_chunk.append(r / s)
                        if rs_chunk:
                            rs_values.append(np.mean(rs_chunk))
                
                if len(rs_values) >= 3:
                    # 线性拟合log(R/S) vs log(lag)
                    log_tau = np.log(tau[:len(rs_values)])
                    log_rs = np.log(rs_values)
                    # 斜率就是Hurst指数
                    hurst = np.polyfit(log_tau, log_rs, 1)[0]
                    hurst_values[ts_code] = hurst
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in hurst_values:
                result[ts_code] = hurst_values[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_z_score_60(df, hist_grouped, **kwargs):
    """
    Z-Score 60日 = (现价 - MA60) / StdDev(60)
    0.5~2.0之间最优 (适度偏离，不过度)
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        z_scores = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 60:
                # 取最近 60 天（升序后末尾即为最新）
                prices = stock_data['close'].values[-60:]
                current_price = prices[-1]
                ma60 = np.mean(prices)
                std60 = np.std(prices)
                if std60 > 0:
                    z_scores[ts_code] = (current_price - ma60) / std60
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in z_scores:
                result[ts_code] = z_scores[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_price_vs_52w_high(df, **kwargs):
    """距52周高点比例 = 现价 / 52周最高价"""
    try:
        close = pd.to_numeric(df.get('close'), errors='coerce')
        high_52w = pd.to_numeric(df.get('high_52w'), errors='coerce')
        
        ratio = np.where(
            high_52w > 0,
            close / high_52w,
            np.nan
        )
        return ratio
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_turnrate(df, **kwargs):
    """日换手率 (%)"""
    turnrate = pd.to_numeric(df.get('turnover_rate'), errors='coerce')
    if turnrate is None:
        return np.full(len(df.index), np.nan)
    if hasattr(turnrate, 'values'):
        return turnrate.values
    # 标量值
    return np.full(len(df.index), turnrate)


def compute_d_circ_mcap(df, **kwargs):
    """流通市值 (亿元)"""
    circ_mv = pd.to_numeric(df.get('circ_mv'), errors='coerce')
    if circ_mv is None:
        return np.full(len(df.index), np.nan)
    # 转换为亿元
    if hasattr(circ_mv, 'values'):
        return (circ_mv / 10000).values
    # 标量值
    return np.full(len(df.index), circ_mv / 10000)


def compute_d_obv_trend(df, hist_grouped, **kwargs):
    """
    OBV趋势 = 5日OBV均值 / 20日OBV均值
    >1 表示OBV在上升，资金流入
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        obv_trend = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 20:
                # 计算OBV
                closes = stock_data['close'].values
                volumes = stock_data['vol'].values
                
                obv = [volumes[0]]
                for i in range(1, len(closes)):
                    if closes[i] > closes[i-1]:
                        obv.append(obv[-1] + volumes[i])
                    elif closes[i] < closes[i-1]:
                        obv.append(obv[-1] - volumes[i])
                    else:
                        obv.append(obv[-1])
                
                obv = np.array(obv)
                if len(obv) >= 20:
                    obv_5 = np.mean(obv[-5:])
                    obv_20 = np.mean(obv[-20:])
                    if obv_20 != 0:
                        obv_trend[ts_code] = obv_5 / obv_20
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in obv_trend:
                result[ts_code] = obv_trend[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_obv_breakout(df, hist_grouped, **kwargs):
    """
    OBV突破: 今日OBV是否创20日新高
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        obv_break = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 21:
                closes = stock_data['close'].values
                volumes = stock_data['vol'].values
                
                # 计算OBV
                obv = [volumes[0]]
                for i in range(1, len(closes)):
                    if closes[i] > closes[i-1]:
                        obv.append(obv[-1] + volumes[i])
                    elif closes[i] < closes[i-1]:
                        obv.append(obv[-1] - volumes[i])
                    else:
                        obv.append(obv[-1])
                
                obv = np.array(obv)
                if len(obv) >= 21:
                    today_obv = obv[-1]
                    max_20d = np.max(obv[-21:-1])  # 过去20日最高(不含今日)
                    obv_break[ts_code] = 1 if today_obv > max_20d else 0
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in obv_break:
                result[ts_code] = obv_break[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_macd_dif(df, hist_grouped, **kwargs):
    """MACD DIF值 = EMA12 - EMA26"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        dif_values = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 26:
                closes = stock_data['close'].values
                
                # 计算EMA12和EMA26
                ema12 = _calculate_ema(closes, 12)
                ema26 = _calculate_ema(closes, 26)
                
                if ema12 is not None and ema26 is not None:
                    dif_values[ts_code] = ema12 - ema26
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in dif_values:
                result[ts_code] = dif_values[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_macd_hist_delta(df, hist_grouped, **kwargs):
    """MACD柱变化 = 今日MACD柱 - 昨日MACD柱"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        hist_delta = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 27:
                closes = stock_data['close'].values
                
                # 计算DIF序列 (EMA12 - EMA26)
                dif_series = _get_ema_series(closes, 12) - _get_ema_series(closes, 26)
                
                if len(dif_series) >= 9:
                    # 计算DEA序列 (DIF的EMA9)
                    dea_series = _get_ema_series(dif_series, 9)
                    
                    if len(dea_series) >= 2 and len(dif_series) >= len(dea_series):
                        # MACD柱 = DIF - DEA (对齐后)
                        dif_aligned = dif_series[-len(dea_series):]
                        macd_hist = dif_aligned - dea_series
                        
                        if len(macd_hist) >= 2:
                            hist_delta[ts_code] = macd_hist[-1] - macd_hist[-2]
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in hist_delta:
                result[ts_code] = hist_delta[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_macd_golden_cross(df, hist_grouped, **kwargs):
    """MACD金叉信号: DIF上穿DEA"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        golden_cross = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 27:
                closes = stock_data['close'].values
                
                # 计算MACD
                dif_series = _get_ema_series(closes, 12) - _get_ema_series(closes, 26)
                dea_series = _get_ema_series(dif_series, 9)
                
                if len(dif_series) >= 2 and len(dea_series) >= 2:
                    # 今日DIF > DEA 且 昨日DIF <= DEA
                    today_cross = (dif_series[-1] > dea_series[-1]) and (dif_series[-2] <= dea_series[-2])
                    golden_cross[ts_code] = 1 if today_cross else 0
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in golden_cross:
                result[ts_code] = golden_cross[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_mtm_breakout(df, hist_grouped, **kwargs):
    """MTM突破: MTM(10) > MTM_MA(10)"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        mtm_break = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 20:
                closes = stock_data['close'].values
                
                # MTM(10) = 今日收盘价 - 10日前收盘价
                if len(closes) >= 10:
                    mtm = closes[-1] - closes[-10]
                    # MTM的10日均线
                    mtm_ma = np.mean([closes[i] - closes[i-10] for i in range(10, len(closes))])
                    mtm_break[ts_code] = 1 if mtm > mtm_ma else 0
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in mtm_break:
                result[ts_code] = mtm_break[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_mtm_slope(df, hist_grouped, **kwargs):
    """MTM斜率: (MTM今日 - MTM昨日) / MTM昨日"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        mtm_slope = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 12:
                closes = stock_data['close'].values
                
                if len(closes) >= 11:
                    mtm_today = closes[-1] - closes[-11]
                    mtm_yesterday = closes[-2] - closes[-12]
                    if mtm_yesterday != 0:
                        mtm_slope[ts_code] = (mtm_today - mtm_yesterday) / abs(mtm_yesterday)
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in mtm_slope:
                result[ts_code] = mtm_slope[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_vwap_bias(df, hist_grouped, **kwargs):
    """
    VWAP偏离 = (现价 - VWAP) / VWAP
    VWAP = sum(成交额) / sum(成交量) 
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        vwap_bias = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 1:
                # 今日VWAP
                amount = stock_data.iloc[-1]['amount']
                vol = stock_data.iloc[-1]['vol']
                close = stock_data.iloc[-1]['close']
                
                if vol > 0:
                    vwap = amount / vol
                    vwap_bias[ts_code] = (close - vwap) / vwap
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in vwap_bias:
                result[ts_code] = vwap_bias[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_atr_14(df, hist_grouped, **kwargs):
    """14日ATR (Average True Range)"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        atr_values = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 15:
                highs = stock_data['high'].values[-15:]
                lows = stock_data['low'].values[-15:]
                closes = stock_data['close'].values[-15:]
                
                # 计算True Range
                tr_values = []
                for i in range(1, len(highs)):
                    tr1 = highs[i] - lows[i]
                    tr2 = abs(highs[i] - closes[i-1])
                    tr3 = abs(lows[i] - closes[i-1])
                    tr_values.append(max(tr1, tr2, tr3))
                
                # 14日ATR
                if len(tr_values) >= 14:
                    atr_values[ts_code] = np.mean(tr_values[-14:])
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in atr_values:
                result[ts_code] = atr_values[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_boll_position(df, hist_grouped, **kwargs):
    """
    BOLL带位置 = (Price - BOLL_mid) / (BOLL_upper - BOLL_mid)
    0~1之间，0.5为中轨
    """
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        boll_pos = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 20:
                closes = stock_data['close'].values[-20:]
                price = closes[-1]
                
                ma20 = np.mean(closes)
                std20 = np.std(closes)
                
                upper = ma20 + 2 * std20
                lower = ma20 - 2 * std20
                
                if upper != lower:
                    pos = (price - ma20) / (upper - ma20)
                    boll_pos[ts_code] = max(0, min(1, pos))  # 限制在0-1
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in boll_pos:
                result[ts_code] = boll_pos[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_ma20_distance(df, hist_grouped, **kwargs):
    """距MA20距离 = (Price - MA20) / MA20"""
    try:
        if not hist_grouped:
            return pd.Series(np.nan, index=df.index).values
        
        ma20_dist = {}
        for ts_code in df.index:
            stock_data = hist_grouped.get(ts_code, _EMPTY_DF).sort_values('trade_date')
            if len(stock_data) >= 20:
                closes = stock_data['close'].values[-20:]
                price = closes[-1]
                ma20 = np.mean(closes)
                
                if ma20 != 0:
                    ma20_dist[ts_code] = (price - ma20) / ma20
        
        result = pd.Series(np.nan, index=df.index)
        for ts_code in df.index:
            if ts_code in ma20_dist:
                result[ts_code] = ma20_dist[ts_code]
        
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index).values


def compute_d_debt_ratio_delta(df, prev_fina_df, **kwargs):
    """资产负债率同比变化 = 今年 - 去年"""
    try:
        curr_debt = pd.to_numeric(df.get('debt_to_assets'), errors='coerce')
        if not prev_fina_df.empty:
            prev_debt = pd.to_numeric(prev_fina_df.get('debt_to_assets'), errors='coerce')
            prev_debt = prev_debt.reindex(df.index) if hasattr(prev_debt, 'reindex') else pd.Series(prev_debt, index=df.index)
        else:
            prev_debt = pd.Series(np.nan, index=df.index)
        
        delta = curr_debt - prev_debt
        return delta.values if hasattr(delta, 'values') else np.full(len(df.index), np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_d_curr_ratio_delta(df, prev_fina_df, **kwargs):
    """流动比率同比变化 = 今年 - 去年"""
    try:
        curr_ratio = pd.to_numeric(df.get('current_ratio'), errors='coerce')
        if not prev_fina_df.empty:
            prev_ratio = pd.to_numeric(prev_fina_df.get('current_ratio'), errors='coerce')
            prev_ratio = prev_ratio.reindex(df.index) if hasattr(prev_ratio, 'reindex') else pd.Series(prev_ratio, index=df.index)
        else:
            prev_ratio = pd.Series(np.nan, index=df.index)
        
        delta = curr_ratio - prev_ratio
        return delta.values if hasattr(delta, 'values') else np.full(len(df.index), np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


def compute_d_roa_delta(df, prev_fina_df, **kwargs):
    """ROA同比变化 = 今年 - 去年"""
    try:
        curr_roa = pd.to_numeric(df.get('roa'), errors='coerce')
        if not prev_fina_df.empty:
            prev_roa = pd.to_numeric(prev_fina_df.get('roa'), errors='coerce')
            prev_roa = prev_roa.reindex(df.index) if hasattr(prev_roa, 'reindex') else pd.Series(prev_roa, index=df.index)
        else:
            prev_roa = pd.Series(np.nan, index=df.index)
        
        delta = curr_roa - prev_roa
        return delta.values if hasattr(delta, 'values') else np.full(len(df.index), np.nan)
    except Exception:
        return np.full(len(df.index), np.nan)


# ==================== 辅助函数 ====================

def _calculate_ema(prices, period):
    """计算EMA"""
    if len(prices) < period:
        return None
    
    multiplier = 2 / (period + 1)
    ema = np.mean(prices[:period])  # 初始用SMA
    
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    
    return ema


def _get_ema_series(prices, period):
    """获取EMA序列"""
    if len(prices) < period:
        return np.array([])
    
    multiplier = 2 / (period + 1)
    ema_values = []
    ema = np.mean(prices[:period])
    ema_values.append(ema)
    
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
        ema_values.append(ema)
    
    return np.array(ema_values)
