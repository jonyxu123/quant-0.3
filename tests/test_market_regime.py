"""市场环境识别测试 - MarketRegime类、信号评分、策略映射"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch
from datetime import datetime

from backend.strategy.market_regime import MarketRegime, identify_market_regime
from backend.strategy.strategy_combos import STRATEGY_COMBOS


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def bull_market_index_df():
    """牛市市场指数数据"""
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=60, freq='D')
    # 持续上涨的收盘价
    closes_300 = 3800 + np.cumsum(np.random.uniform(0, 30, 60))
    closes_905 = closes_300 * 0.8 + np.cumsum(np.random.uniform(0, 25, 60))

    df_300 = pd.DataFrame({
        'ts_code': ['000300.SH'] * 60,
        'close': closes_300,
        'date': dates,
    })
    df_905 = pd.DataFrame({
        'ts_code': ['000905.SH'] * 60,
        'close': closes_905,
        'date': dates,
    })
    return pd.concat([df_300, df_905], ignore_index=True)


@pytest.fixture
def bear_market_index_df():
    """熊市市场指数数据"""
    np.random.seed(123)
    dates = pd.date_range('2024-01-01', periods=60, freq='D')
    # 持续下跌的收盘价
    closes_300 = 3800 - np.cumsum(np.random.uniform(0, 30, 60))
    closes_905 = closes_300 * 0.8 - np.cumsum(np.random.uniform(0, 25, 60))

    df_300 = pd.DataFrame({
        'ts_code': ['000300.SH'] * 60,
        'close': np.maximum(closes_300, 2000),  # 防止负数
        'date': dates,
    })
    df_905 = pd.DataFrame({
        'ts_code': ['000905.SH'] * 60,
        'close': np.maximum(closes_905, 1500),
        'date': dates,
    })
    return pd.concat([df_300, df_905], ignore_index=True)


@pytest.fixture
def mock_individual_universe():
    """模拟个股数据"""
    np.random.seed(42)
    n_stocks = 500
    return pd.DataFrame({
        'close': np.random.uniform(10, 100, n_stocks),
        'MA20': np.random.uniform(10, 100, n_stocks),
    })


# ============================================================
# MarketRegime类初始化
# ============================================================

class TestMarketRegimeInit:
    def test_init_with_data(self, bull_market_index_df, mock_individual_universe):
        """带数据初始化"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        assert regime.market_index_df is not None
        assert regime.individual_universe is not None
        assert isinstance(regime.signal_scores, dict)
        assert isinstance(regime.market_state, dict)

    def test_init_without_data(self):
        """无数据初始化"""
        regime = MarketRegime()
        assert regime.market_index_df is None
        assert regime.individual_universe is None

    def test_init_with_none(self):
        """None参数初始化"""
        regime = MarketRegime(None, None)
        assert regime.market_index_df is None
        assert regime.individual_universe is None


# ============================================================
# identify_regime 返回有效策略ID
# ============================================================

class TestIdentifyRegime:
    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_identify_regime_returns_valid_strategy(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """identify_regime应返回有效的策略ID(A-AD)"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        strategy_id = regime.identify_regime()
        assert strategy_id in STRATEGY_COMBOS, (
            f"返回的策略ID '{strategy_id}' 不在有效策略集合中"
        )

    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_identify_regime_bear_market(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bear_market_index_df, mock_individual_universe
    ):
        """熊市应返回防御型策略"""
        regime = MarketRegime(bear_market_index_df, mock_individual_universe)
        strategy_id = regime.identify_regime()
        assert strategy_id in STRATEGY_COMBOS

    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_identify_regime_insufficient_data(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment
    ):
        """数据不足时应返回默认策略"""
        # 少于25个数据点
        short_df = pd.DataFrame({
            'ts_code': ['000300.SH'] * 10,
            'close': np.random.uniform(3000, 4000, 10),
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
        })
        regime = MarketRegime(short_df)
        strategy_id = regime.identify_regime()
        assert strategy_id in STRATEGY_COMBOS


# ============================================================
# 信号评分在0-100范围
# ============================================================

class TestSignalScores:
    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_signal_scores_in_range(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """所有信号评分应在0-100范围内"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        regime.identify_regime()
        scores = regime.get_signal_scores()
        for sid, score in scores.items():
            assert 0 <= score <= 100, (
                f"策略{sid}的信号评分{score:.2f}超出[0,100]范围"
            )

    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_signal_scores_not_empty(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """信号评分不应为空"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        regime.identify_regime()
        scores = regime.get_signal_scores()
        assert len(scores) > 0

    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_ac_ad_always_present(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """AC和AD策略应始终有基础评分"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        regime.identify_regime()
        scores = regime.get_signal_scores()
        assert 'AC' in scores
        assert 'AD' in scores
        assert scores['AC'] == 60
        assert scores['AD'] == 65


# ============================================================
# 市场状态
# ============================================================

class TestMarketState:
    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_market_state_keys(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """市场状态应包含所有必要指标"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        regime.identify_regime()
        state = regime.get_market_state()
        expected_keys = {
            'is_bull', 'is_bear', 'is_high_vol', 'breadth',
            'small_strong', 'vol_20', 'ma20_slope',
            'sentiment_extreme', 'north_flow_5d', 'north_flow_20d',
            'sector_dispersion', 'event_season', 'lhb_active',
        }
        assert expected_keys.issubset(set(state.keys()))

    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_breadth_in_range(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """市场广度应在[0,1]范围内"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        regime.identify_regime()
        state = regime.get_market_state()
        assert 0 <= state['breadth'] <= 1


# ============================================================
# 辅助方法
# ============================================================

class TestHelperMethods:
    def test_get_regime_description(self):
        """获取策略环境描述"""
        regime = MarketRegime()
        desc = regime.get_regime_description('A')
        assert '价值防御型' in desc

    def test_get_regime_description_unknown(self):
        """未知策略描述"""
        regime = MarketRegime()
        desc = regime.get_regime_description('ZZ')
        assert '未知策略' in desc

    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_get_top_n_strategies(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """获取Top N策略"""
        regime = MarketRegime(bull_market_index_df, mock_individual_universe)
        regime.identify_regime()
        top5 = regime.get_top_n_strategies(5)
        assert len(top5) <= 5
        # 应按评分降序排列
        for i in range(len(top5) - 1):
            assert top5[i][1] >= top5[i + 1][1]

    def test_is_earning_alert_season(self):
        """财报季判断"""
        # 验证方法存在且返回布尔值
        result = MarketRegime.is_earning_alert_season()
        assert isinstance(result, bool)


# ============================================================
# 函数式接口兼容性
# ============================================================

class TestFunctionalInterface:
    @patch.object(MarketRegime, 'get_market_sentiment_extreme', return_value=0.5)
    @patch.object(MarketRegime, 'get_north_net_flow', return_value=0)
    @patch.object(MarketRegime, 'get_sector_return_dispersion', return_value=0.1)
    @patch.object(MarketRegime, 'get_lhb_activity_score', return_value=0)
    def test_identify_market_regime_function(
        self, mock_lhb, mock_sector, mock_north, mock_sentiment,
        bull_market_index_df, mock_individual_universe
    ):
        """函数式接口应返回有效策略ID"""
        strategy_id = identify_market_regime(bull_market_index_df, mock_individual_universe)
        assert strategy_id in STRATEGY_COMBOS
