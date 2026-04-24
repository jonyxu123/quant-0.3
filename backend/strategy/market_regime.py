"""
市场环境识别模块 - 基于信号强度评分机制识别当前市场环境
基于《多因子选股系统_v8.0》第5章 identify_market_regime() 函数

核心机制:
- 信号强度评分(0-100分)，分数越高越匹配
- 避免硬优先级链导致信号被屏蔽
- 支持特殊信号优先 + 标准市场环境映射
"""

from scripts.akshare_proxy_patch_free import install_patch
install_patch(proxy_api_scheme="socks5h", proxy_api_url="http://bapi.51daili.com/getapi2?linePoolIndex=0,1&packid=2&time=1&qty=1&port=2&format=txt&dt=4&ct=1&dtc=1&usertype=17&uid=42083&accessName=sword721&accessPassword=CE2BFE18E746F92ECDB5479063290EAE&skey=autoaddwhiteip",
    log_file="akshare_proxy_patch.log",
    log_console=False,
    hook_domains=[
    "fund.eastmoney.com",
    "push2.eastmoney.com",
    "push2his.eastmoney.com",
    "emweb.securities.eastmoney.com",
    ],)

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Tuple

from .strategy_combos import STRATEGY_COMBOS


class MarketRegime:
    """
    市场环境识别器

    基于信号强度评分机制识别当前市场环境，返回建议策略ID。
    支持从市场指数数据和个股数据中提取多维信号，综合评分后选择最优策略。

    Attributes:
        market_index_df: 市场指数数据 (ts_code, close, date等)
        individual_universe: 个股数据 (close, MA20等)
        signal_scores: 各策略信号评分字典
        market_state: 当前市场状态字典
    """

    def __init__(self, market_index_df: pd.DataFrame = None,
                 individual_universe: pd.DataFrame = None):
        """
        初始化市场环境识别器

        Args:
            market_index_df: 市场指数数据，需包含 ts_code, close 列
            individual_universe: 个股数据，需包含 close, MA20 列
        """
        self.market_index_df = market_index_df
        self.individual_universe = individual_universe
        self.signal_scores: Dict[str, float] = {}
        self.market_state: Dict[str, any] = {}

    # ============================================================
    # 辅助数据获取函数
    # ============================================================

    @staticmethod
    def get_index_return(index_code: str, days: int,
                         market_index_df: pd.DataFrame) -> float:
        """计算指数近N日收益率"""
        df = market_index_df[market_index_df['ts_code'] == index_code].copy()
        if len(df) < days:
            return 1.0
        return df['close'].iloc[-1] / df['close'].iloc[-days]

    @staticmethod
    def get_market_sentiment_extreme() -> float:
        """
        计算市场情绪极端程度（0-1，0.5为中性）

        综合多个情绪指标:
        1. 换手率Z-score
        2. 创新高股票比例
        3. 涨跌比
        4. 融券卖出占比
        5. 千股千评评分

        Returns:
            float: 情绪得分，0=极度悲观，1=极度乐观，0.5=中性
        """
        # TODO: 集成实际情绪指标数据
        return 0.5  # 默认中性

    @staticmethod
    def is_earning_alert_season() -> bool:
        """
        判断是否处于业绩预告密集发布期

        Returns:
            bool: 是否为财报季（1月、4月、7月、8月、10月）
        """
        current_month = datetime.now().month
        return current_month in [1, 4, 7, 8, 10]

    @staticmethod
    def get_lhb_activity_score() -> float:
        """
        获取龙虎榜活跃度评分（0-100）

        Returns:
            float: 龙虎榜活跃度评分
        """
        try:
            import akshare as ak
            df = ak.stock_lhb_detail_daily(date=datetime.now().strftime('%Y%m%d'))
            if df is None or df.empty:
                return 0
            return min(len(df) / 30 * 100, 100)  # 归一化到0-100
        except Exception:
            return 0

    @staticmethod
    def get_north_net_flow(days: int = 5) -> float:
        """
        获取北向资金净流入金额（元）

        Args:
            days: 统计天数

        Returns:
            float: 净流入金额（元）
        """
        try:
            import akshare as ak
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is None or df.empty or len(df) < days:
                return 0
            return df.iloc[-days:, :]['当日净流入'].sum() * 10000  # 万元→元
        except Exception:
            return 0

    @staticmethod
    def get_sector_return_dispersion() -> float:
        """
        计算行业收益离散度（用于检测轮动强度）

        Returns:
            float: 行业收益率标准差
        """
        try:
            import akshare as ak
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                return 0
            returns = df['涨跌幅'].astype(float)
            return returns.std()
        except Exception:
            return 0

    # ============================================================
    # 市场状态计算
    # ============================================================

    def _compute_market_state(self) -> dict:
        """
        计算当前市场状态指标

        Returns:
            dict: 包含 is_bull, is_bear, is_high_vol, breadth, small_strong,
                  vol_20, ma20_slope, sentiment_extreme 等指标
        """
        df = self.market_index_df[self.market_index_df['ts_code'] == '000300.SH'].copy()

        if len(df) < 25:
            # 数据不足，返回默认中性状态
            return {
                'is_bull': False, 'is_bear': False, 'is_high_vol': False,
                'breadth': 0.5, 'small_strong': False,
                'vol_20': 0.15, 'ma20_slope': 0.0,
                'sentiment_extreme': 0.5,
                'north_flow_5d': 0, 'north_flow_20d': 0,
                'sector_dispersion': 0.1,
                'event_season': False, 'lhb_active': 0,
            }

        # 计算MA20和斜率
        df['MA20'] = df['close'].rolling(20).mean()
        ma20_slope = (df['MA20'].iloc[-1] - df['MA20'].iloc[-5]) / df['MA20'].iloc[-5]

        # 判断牛熊
        is_bull = df['close'].iloc[-1] > df['MA20'].iloc[-1] and ma20_slope > 0.01
        is_bear = df['close'].iloc[-1] < df['MA20'].iloc[-1] and ma20_slope < -0.01

        # 计算波动率
        ret = df['close'].pct_change()
        vol_20 = ret.rolling(20).std().iloc[-1] * np.sqrt(252)
        is_high_vol = vol_20 > 0.25

        # 计算市场广度
        if self.individual_universe is not None and 'MA20' in self.individual_universe.columns:
            breadth = (self.individual_universe['close'] > self.individual_universe['MA20']).mean()
        else:
            breadth = 0.5

        # 判断小盘风格
        small_vs_large = self.get_index_return('000905.SH', 20, self.market_index_df) / \
                         max(self.get_index_return('000300.SH', 20, self.market_index_df), 1e-8)
        small_strong = small_vs_large > 1.05

        # 获取辅助数据
        sentiment_extreme = self.get_market_sentiment_extreme()
        north_flow_5d = self.get_north_net_flow(days=5)
        north_flow_20d = self.get_north_net_flow(days=20)
        sector_dispersion = self.get_sector_return_dispersion()
        event_season = self.is_earning_alert_season()
        lhb_active = self.get_lhb_activity_score()

        state = {
            'is_bull': is_bull,
            'is_bear': is_bear,
            'is_high_vol': is_high_vol,
            'breadth': breadth,
            'small_strong': small_strong,
            'vol_20': vol_20,
            'ma20_slope': ma20_slope,
            'sentiment_extreme': sentiment_extreme,
            'north_flow_5d': north_flow_5d,
            'north_flow_20d': north_flow_20d,
            'sector_dispersion': sector_dispersion,
            'event_season': event_season,
            'lhb_active': lhb_active,
        }

        self.market_state = state
        return state

    # ============================================================
    # 信号强度评分
    # ============================================================

    def _compute_signal_scores(self, state: dict) -> Dict[str, float]:
        """
        基于市场状态计算各策略的信号强度评分（0-100分）

        Args:
            state: 市场状态字典

        Returns:
            Dict[str, float]: 各策略ID的信号评分
        """
        signal_scores = {}

        is_bull = state['is_bull']
        is_bear = state['is_bear']
        is_high_vol = state['is_high_vol']
        breadth = state['breadth']
        small_strong = state['small_strong']
        vol_20 = state['vol_20']
        sentiment_extreme = state['sentiment_extreme']
        north_flow_5d = state['north_flow_5d']
        north_flow_20d = state['north_flow_20d']
        sector_dispersion = state['sector_dispersion']
        event_season = state['event_season']
        lhb_active = state['lhb_active']

        # --- 信号A: 价值防御型 ---
        if is_bear and breadth < 0.45:
            signal_scores['A'] = (1 - breadth) * 80 + 20

        # --- 信号B: 成长进攻型 ---
        if is_bull and not is_high_vol:
            signal_scores['B'] = breadth * 70 + 30

        # --- 信号C: 质量优先型 ---
        if not is_bull and not is_bear and 0.4 < breadth < 0.6:
            signal_scores['C'] = 65

        # --- 信号D: 均衡综合型 ---
        if not is_bull and not is_bear:
            signal_scores['D'] = 55

        # --- 信号E: 小盘成长增强型 ---
        if small_strong and is_bull:
            signal_scores['E'] = breadth * 80 + 20

        # --- 信号F: 主升浪启动型 ---
        if is_bull and not is_high_vol and breadth > 0.6:
            signal_scores['F'] = breadth * 70 + 30

        # --- 信号G: 超跌反弹型 ---
        if is_bear and breadth < 0.3:
            signal_scores['G'] = (1 - breadth) * 90 + 10

        # --- 信号H: 综合趋势确认型 ---
        if is_bull and vol_20 < 0.2:
            signal_scores['H'] = breadth * 60 + (1 - vol_20 / 0.2) * 40

        # --- 信号I: 机构抱团型 ---
        if north_flow_5d > 10e8 and breadth > 0.5:
            signal_scores['I'] = min(north_flow_5d / 50e8 * 50 + breadth * 50, 100)

        # --- 信号J: 高股息收息型 ---
        if is_bear and breadth < 0.4:
            signal_scores['J'] = (1 - breadth) * 70 + 30

        # --- 信号K: 筹码集中突破型 ---
        if is_bull and breadth > 0.6:
            signal_scores['K'] = breadth * 60 + 30

        # --- 信号L: 盈利预测上调型 ---
        if event_season and not is_bear:
            signal_scores['L'] = 70

        # --- 信号M: 事件驱动机会型 ---
        if event_season and lhb_active > 30:
            signal_scores['M'] = lhb_active  # 直接用龙虎榜活跃度作为评分

        # --- 信号N: 北向资金聪明钱型 ---
        if north_flow_5d > 10e8 and north_flow_20d > 50e8:
            signal_scores['N'] = min(north_flow_5d / 50e8 * 50 + north_flow_20d / 200e8 * 50, 100)

        # --- 信号O: 行业轮动动量型 ---
        if sector_dispersion > 0.10 and not is_bear:
            signal_scores['O'] = min(sector_dispersion / 0.15 * 60, 100)

        # --- 信号P: 低波红利防御增强型 ---
        extreme_vol = vol_20 > 0.30 or is_bear
        if extreme_vol and breadth < 0.40:
            signal_scores['P'] = (1 - breadth) * 100

        # --- 信号Q: CAN SLIM成长龙头型 ---
        if is_bull and not is_high_vol and breadth > 0.6 and north_flow_5d > 10e8:
            signal_scores['Q'] = breadth * 80 + min(north_flow_5d / 50e8 * 20, 20)

        # --- 信号R: 格雷厄姆深度价值型 ---
        if is_bear and breadth < 0.3 and vol_20 > 0.2:
            signal_scores['R'] = (1 - breadth) * 60 + min(vol_20 / 0.3 * 40, 40)

        # --- 信号S: 巴菲特GARP型 ---
        if not is_bull and not is_bear and not is_high_vol and 0.4 < breadth < 0.6:
            signal_scores['S'] = 70  # 震荡市中等评分

        # --- 信号T: 林奇PEG成长型 ---
        if small_strong and is_bull and breadth > 0.5:
            signal_scores['T'] = breadth * 80 + 20

        # --- 信号U: 格林布拉特神奇公式 ---
        if is_bear and breadth < 0.4 and vol_20 < 0.3:
            signal_scores['U'] = (1 - breadth) * 50 + (1 - vol_20 / 0.3) * 50

        # --- 信号V: 索罗斯宏观对冲型 ---
        if is_high_vol and sector_dispersion > 0.15:
            signal_scores['V'] = min(vol_20 / 0.3 * 50 + sector_dispersion / 0.2 * 50, 100)

        # --- 信号W: 唐奇安趋势跟踪型 ---
        if is_bull and vol_20 < 0.2 and breadth > 0.7:
            signal_scores['W'] = breadth * 70 + (1 - vol_20 / 0.2) * 30

        # --- 信号X: 波动率溢价策略 ---
        if is_high_vol and not is_bull and breadth < 0.5:
            signal_scores['X'] = (vol_20 / 0.3 * 50) + ((1 - breadth) * 50)

        # --- 信号Y: 情绪反转策略 ---
        if sentiment_extreme > 0.7 or sentiment_extreme < 0.3:
            signal_scores['Y'] = abs(sentiment_extreme - 0.5) * 200

        # --- 信号Z: 日历效应策略 ---
        current_month = datetime.now().month
        if current_month in [1, 2, 3, 11, 12]:  # 春季躁动和年末估值切换
            signal_scores['Z'] = 80
        elif current_month in [5, 6]:  # 五穷六绝
            signal_scores['Z'] = 70

        # --- 信号AA: 公司治理增强策略 ---
        if not is_bull and not is_bear and vol_20 < 0.2:
            signal_scores['AA'] = 75  # 震荡低波环境

        # --- 信号AB: 行业轮动增强策略 ---
        if sector_dispersion > 0.12 and not is_bear:
            signal_scores['AB'] = min(sector_dispersion / 0.15 * 80, 100)

        # --- 信号AC: 风险平价多策略（全市场适用）---
        signal_scores['AC'] = 60  # 基础评分

        # --- 信号AD: 因子轮动策略（全市场适用）---
        signal_scores['AD'] = 65  # 基础评分

        self.signal_scores = signal_scores
        return signal_scores

    # ============================================================
    # 核心识别方法
    # ============================================================

    def identify_regime(self, market_index_df: pd.DataFrame = None,
                        individual_universe: pd.DataFrame = None) -> str:
        """
        基于信号强度评分识别当前市场环境，返回建议策略组合ID

        改进机制：采用信号强度评分+排序机制，避免硬优先级链导致信号被屏蔽。

        Args:
            market_index_df: 市场指数数据（可选，覆盖初始化数据）
            individual_universe: 个股数据（可选，覆盖初始化数据）

        Returns:
            str: 建议策略ID，如 'A', 'B', ..., 'AD'
        """
        # 更新数据源
        if market_index_df is not None:
            self.market_index_df = market_index_df
        if individual_universe is not None:
            self.individual_universe = individual_universe

        # Step 1: 计算市场状态
        state = self._compute_market_state()

        # Step 2: 计算信号评分
        signal_scores = self._compute_signal_scores(state)

        # Step 3: 策略选择逻辑
        # 如果有强特殊信号（评分>60），优先使用
        if signal_scores:
            best_signal = max(signal_scores, key=signal_scores.get)
            if signal_scores[best_signal] > 60:
                return best_signal

        # Step 4: 标准市场环境映射（A-AD，30套策略全覆盖）
        is_bull = state['is_bull']
        is_bear = state['is_bear']
        is_high_vol = state['is_high_vol']
        breadth = state['breadth']
        small_strong = state['small_strong']
        vol_20 = state['vol_20']
        sentiment_extreme = state['sentiment_extreme']

        if is_bull and not is_high_vol:
            if small_strong:
                return 'E'  # 小盘成长
            else:
                return 'F'  # 主升浪
        elif is_bull and is_high_vol:
            return 'B'  # 牛市高波动→成长进攻
        elif is_bear:
            if breadth < 0.3:
                return 'G'  # 超跌反弹
            elif breadth < 0.45:
                return 'A'  # 价值防御
            else:
                return 'J'  # 高股息防御
        elif is_high_vol:
            # 高波动环境
            if sentiment_extreme > 0.7:
                return 'Y'  # 情绪过热→情绪反转
            else:
                return 'X'  # 波动率溢价
        else:
            # 震荡市根据风格选择
            if small_strong:
                return 'D'  # 均衡
            else:
                # 震荡低波环境
                if vol_20 < 0.15:
                    return 'AA'  # 公司治理增强
                else:
                    return 'C'  # 质量优先

    # ============================================================
    # 辅助查询方法
    # ============================================================

    def get_signal_scores(self) -> Dict[str, float]:
        """
        获取最近一次识别的信号评分

        Returns:
            Dict[str, float]: 各策略的信号评分（按分数降序排列）
        """
        return dict(sorted(self.signal_scores.items(), key=lambda x: x[1], reverse=True))

    def get_market_state(self) -> dict:
        """
        获取最近一次识别的市场状态

        Returns:
            dict: 市场状态指标字典
        """
        return self.market_state.copy()

    def get_regime_description(self, strategy_id: str) -> str:
        """
        获取策略对应的市场环境描述

        Args:
            strategy_id: 策略ID

        Returns:
            str: 市场环境描述
        """
        combo = STRATEGY_COMBOS.get(strategy_id.upper())
        if combo:
            return f"策略{strategy_id}({combo['name']}): {combo['regime']}"
        return f"策略{strategy_id}: 未知策略"

    def get_top_n_strategies(self, n: int = 5) -> list:
        """
        获取信号评分最高的N个策略

        Args:
            n: 返回策略数量

        Returns:
            list[tuple]: [(strategy_id, score), ...] 按评分降序排列
        """
        sorted_scores = sorted(self.signal_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_scores[:n]


# ============================================================
# 兼容旧版函数式接口
# ============================================================

def identify_market_regime(market_index_df: pd.DataFrame,
                           individual_universe: pd.DataFrame) -> str:
    """
    基于信号强度评分识别当前市场环境（函数式接口，兼容旧版代码）

    Args:
        market_index_df: 市场指数数据
        individual_universe: 个股数据

    Returns:
        str: 建议策略ID
    """
    regime = MarketRegime(market_index_df, individual_universe)
    return regime.identify_regime()


if __name__ == '__main__':
    # 测试代码 - 使用模拟数据
    print("=" * 60)
    print("市场环境识别模块测试")
    print("=" * 60)

    # 创建模拟市场数据
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=60, freq='D')
    closes = 3800 + np.cumsum(np.random.randn(60) * 20)
    mock_index_df = pd.DataFrame({
        'ts_code': ['000300.SH'] * 60,
        'close': closes,
        'date': dates,
    })
    # 添加中证500数据
    mock_index_df = pd.concat([
        mock_index_df,
        pd.DataFrame({
            'ts_code': ['000905.SH'] * 60,
            'close': closes * 0.8 + np.cumsum(np.random.randn(60) * 15),
            'date': dates,
        })
    ], ignore_index=True)

    # 创建模拟个股数据
    n_stocks = 500
    mock_individual = pd.DataFrame({
        'close': np.random.uniform(10, 100, n_stocks),
        'MA20': np.random.uniform(10, 100, n_stocks),
    })

    # 初始化识别器
    regime = MarketRegime(mock_index_df, mock_individual)

    # 识别市场环境
    strategy_id = regime.identify_regime()
    print(f"\n建议策略: {strategy_id}")
    print(f"策略描述: {regime.get_regime_description(strategy_id)}")

    # 查看市场状态
    state = regime.get_market_state()
    print(f"\n市场状态:")
    for k, v in state.items():
        print(f"  {k}: {v}")

    # 查看信号评分
    scores = regime.get_signal_scores()
    print(f"\n信号评分 (Top 10):")
    for sid, score in list(scores.items())[:10]:
        print(f"  {sid:>3s}: {score:.1f}分")

    # Top 5 策略
    top5 = regime.get_top_n_strategies(5)
    print(f"\nTop 5 策略:")
    for sid, score in top5:
        print(f"  {sid:>3s}: {score:.1f}分 - {regime.get_regime_description(sid)}")
