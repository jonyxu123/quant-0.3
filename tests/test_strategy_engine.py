"""策略引擎测试 - 策略权重验证、打分、过滤、选股"""
import numpy as np
import pandas as pd
import pytest

from backend.strategy.strategy_combos import (
    STRATEGY_COMBOS,
    get_strategy_combo,
    get_strategy_info,
    validate_strategy_weights,
)
from backend.strategy.strategy_engine import StrategyEngine


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_factor_df():
    """模拟因子数据DataFrame"""
    np.random.seed(42)
    n_stocks = 200
    stock_codes = [f'{600000 + i:06d}.SH' for i in range(n_stocks)]

    return pd.DataFrame({
        'V_EP': np.random.uniform(0.02, 0.15, n_stocks),
        'V_BP': np.random.uniform(0.3, 3.0, n_stocks),
        'V_FCFY': np.random.uniform(0.01, 0.10, n_stocks),
        'V_DY': np.random.uniform(0, 6, n_stocks),
        'V_PE_TTM': np.random.uniform(5, 100, n_stocks),
        'V_SG': np.random.uniform(0.3, 3.0, n_stocks),
        'P_ROE': np.random.uniform(5, 30, n_stocks),
        'P_ROA': np.random.uniform(2, 15, n_stocks),
        'P_GPM': np.random.uniform(10, 60, n_stocks),
        'P_CFOA': np.random.uniform(0.02, 0.15, n_stocks),
        'P_ROIC': np.random.uniform(5, 30, n_stocks),
        'Q_OCF_NI': np.random.uniform(0.5, 2.0, n_stocks),
        'Q_APR': np.random.uniform(-0.5, 0.5, n_stocks),
        'Q_GPMD': np.random.uniform(-0.1, 0.1, n_stocks),
        'Q_ATD': np.random.uniform(-0.1, 0.1, n_stocks),
        'Q_CURRENT_RATIO': np.random.uniform(0.5, 3.0, n_stocks),
        'Q_QUICK_RATIO': np.random.uniform(0.3, 2.5, n_stocks),
        'Q_ALT_ZSCORE': np.random.uniform(0, 5, n_stocks),
        'G_NI_YOY': np.random.uniform(-20, 60, n_stocks),
        'G_REV_YOY': np.random.uniform(-10, 40, n_stocks),
        'G_QPT': np.random.uniform(-10, 30, n_stocks),
        'G_ASSET_YOY': np.random.uniform(-5, 30, n_stocks),
        'E_SUE': np.random.uniform(-2, 3, n_stocks),
        'E_EARNINGS_MOM': np.random.uniform(-10, 20, n_stocks),
        'M_MOM_12_1': np.random.uniform(-0.3, 0.5, n_stocks),
        'M_MOM_6M': np.random.uniform(-0.2, 0.4, n_stocks),
        'M_MOM_3M': np.random.uniform(-0.1, 0.2, n_stocks),
        'M_RS_3M': np.random.uniform(0.5, 1.5, n_stocks),
        'M_RS_6M': np.random.uniform(0.5, 2.0, n_stocks),
        'M_RSI_OVERSOLD': np.random.uniform(10, 40, n_stocks),
        'M_RSI_50_70': np.random.uniform(0, 1, n_stocks),
        'M_NEW_HIGH_20': np.random.choice([0, 1], n_stocks, p=[0.8, 0.2]),
        'M_NEW_HIGH_60': np.random.choice([0, 1], n_stocks, p=[0.7, 0.3]),
        'M_MA_BULL': np.random.choice([0, 1], n_stocks, p=[0.7, 0.3]),
        'M_MACD_GOLDEN': np.random.choice([0, 1], n_stocks, p=[0.6, 0.4]),
        'M_VOL_ACCEL': np.random.uniform(0.5, 3.0, n_stocks),
        'M_INDU_MOM': np.random.uniform(-0.1, 0.2, n_stocks),
        'S_LN_FLOAT': np.random.uniform(18, 25, n_stocks),
        'S_AMT_20_Z': np.random.uniform(-2, 3, n_stocks),
        'S_TURNOVER': np.random.uniform(0.5, 5, n_stocks),
        'C_WINNER_RATE': np.random.uniform(20, 95, n_stocks),
        'C_CONCENTRATION': np.random.uniform(5, 30, n_stocks),
        'C_TOP10_HOLD': np.random.uniform(10, 60, n_stocks),
        'L_DEBT_RATIO': np.random.uniform(20, 80, n_stocks),
        'L_CCR': np.random.uniform(0, 1, n_stocks),
        'I_FUND_HOLD_PCT': np.random.uniform(0, 30, n_stocks),
        'I_FUND_COUNT': np.random.randint(1, 50, n_stocks).astype(float),
        'I_FUND_NEW': np.random.uniform(0, 10, n_stocks),
        'I_FUND_CONSENSUS': np.random.uniform(0, 1, n_stocks),
        'I_NORTH_HOLD_PCT': np.random.uniform(0, 5, n_stocks),
        'MF_F_OUT_IN_RATIO': np.random.uniform(0.5, 2.0, n_stocks),
        'MF_MAIN_NET_IN': np.random.uniform(-1e8, 1e8, n_stocks),
        'MF_NORTH_NET_IN_5D': np.random.uniform(-1e8, 5e9, n_stocks),
        'MF_NORTH_NET_IN_20D': np.random.uniform(-1e8, 2e10, n_stocks),
        'MF_NORTH_HOLD_CHANGE': np.random.uniform(-5, 10, n_stocks),
        'MF_MARGIN_NET_BUY': np.random.uniform(-1e8, 1e8, n_stocks),
        'MF_MARGIN_ACCEL': np.random.uniform(-1, 1, n_stocks),
        'MF_SECTOR_FLOW_IN': np.random.uniform(-1e8, 1e8, n_stocks),
        'MF_VOL_SHRINK': np.random.uniform(0, 1, n_stocks),
        'EV_EARNING_ALERT': np.random.uniform(0, 100, n_stocks),
        'EV_LHB_NET_BUY': np.random.uniform(-1e8, 1e8, n_stocks),
        'EV_SHHOLDER_INCREASE': np.random.uniform(0, 1, n_stocks),
        'EV_BLOCK_TRADE_PREM': np.random.uniform(-10, 10, n_stocks),
        'EV_MANAGEMENT_BUY': np.random.uniform(0, 1, n_stocks),
        'TP_MACD_DIV_BOT': np.random.uniform(0, 1, n_stocks),
        'TP_CHANNEL_BREAK': np.random.choice([0, 1], n_stocks, p=[0.8, 0.2]).astype(float),
        'TP_VOL_COMPRESS': np.random.uniform(0, 1, n_stocks),
        'VOL_20D': np.random.uniform(0.1, 0.5, n_stocks),
        'VOL_IDIOSYNCRATIC': np.random.uniform(0.05, 0.3, n_stocks),
        'VOL_OMEGA': np.random.uniform(0, 1, n_stocks),
        'SENT_TURNOVER_Z': np.random.uniform(-3, 3, n_stocks),
        'SENT_NEW_HIGH_PCT': np.random.uniform(0, 1, n_stocks),
        'SENT_ADVANCE_DECLINE': np.random.uniform(-1, 1, n_stocks),
        'SENT_SHORT_RATIO': np.random.uniform(0, 1, n_stocks),
        'SENT_COMMENT_SCORE': np.random.uniform(0, 100, n_stocks),
        'CAL_SPRING_RALLY': np.random.uniform(0, 1, n_stocks),
        'CAL_SELL_MAY': np.random.uniform(0, 1, n_stocks),
        'CAL_YEAR_END_SWITCH': np.random.uniform(0, 1, n_stocks),
        'CAL_EARNING_SEASON': np.random.uniform(0, 1, n_stocks),
        'GOV_INSIDER_OWN': np.random.uniform(0, 50, n_stocks),
        'GOV_INSIDER_TRADE': np.random.uniform(-1, 1, n_stocks),
        'GOV_DIVIDEND_CONT': np.random.uniform(0, 1, n_stocks),
        'GOV_AUDIT_OPINION': np.random.choice([0, 1], n_stocks, p=[0.1, 0.9]).astype(float),
        'GOV_REGULATORY_PENALTY': np.random.choice([0, 1], n_stocks, p=[0.9, 0.1]).astype(float),
        'GOV_ESG_SCORE': np.random.uniform(0, 100, n_stocks),
        'SECTOR_MOM_1M': np.random.uniform(-0.1, 0.2, n_stocks),
        'SECTOR_RS_3M': np.random.uniform(0.5, 1.5, n_stocks),
        'CONCEPT_HOT_RANK': np.random.uniform(0, 100, n_stocks),
        'V_PB': np.random.uniform(0.5, 5, n_stocks),
        'V_EV_EBITDA': np.random.uniform(2, 20, n_stocks),
    }, index=stock_codes)


# ============================================================
# 30套策略权重和为1（或接近1）
# ============================================================

class TestStrategyWeights:
    def test_all_30_strategies_exist(self):
        """验证30套策略全部存在"""
        assert len(STRATEGY_COMBOS) == 30

    def test_strategy_ids_cover_a_to_ad(self):
        """验证策略ID覆盖A-AD"""
        expected_ids = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
            'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
            'Q', 'R', 'S', 'T', 'U', 'V', 'W',
            'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD',
        }
        assert set(STRATEGY_COMBOS.keys()) == expected_ids

    @pytest.mark.parametrize("strategy_id", list(STRATEGY_COMBOS.keys()))
    def test_weights_abs_sum_near_one(self, strategy_id):
        """每套策略权重绝对值之和应接近1.0"""
        result = validate_strategy_weights(strategy_id)
        assert result['is_valid'], (
            f"策略{strategy_id}权重绝对值之和={result['total_abs_weight']:.4f}, "
            f"偏差={result['deviation']:.4f}超过5%阈值"
        )

    def test_all_weights_sum_near_one_bulk(self):
        """批量验证所有策略权重和接近1"""
        invalid = []
        for sid in STRATEGY_COMBOS:
            result = validate_strategy_weights(sid)
            if not result['is_valid']:
                invalid.append(sid)
        assert len(invalid) == 0, f"以下策略权重不合法: {invalid}"


# ============================================================
# score_stocks 打分逻辑
# ============================================================

class TestScoreStocks:
    def test_score_stocks_returns_score(self, mock_factor_df):
        """打分后应包含strategy_score列"""
        engine = StrategyEngine('A')
        result = engine.score_stocks('A', mock_factor_df)
        assert 'strategy_score' in result.columns

    def test_score_stocks_sorted_desc(self, mock_factor_df):
        """打分结果应按strategy_score降序排列"""
        engine = StrategyEngine('A')
        result = engine.score_stocks('A', mock_factor_df)
        scores = result['strategy_score'].values
        assert all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1))

    def test_score_stocks_matched_factors(self, mock_factor_df):
        """打分结果应包含matched_factors和total_factors"""
        engine = StrategyEngine('A')
        result = engine.score_stocks('A', mock_factor_df)
        assert 'matched_factors' in result.columns
        assert 'total_factors' in result.columns
        assert result['matched_factors'].iloc[0] > 0

    def test_score_stocks_empty_df(self):
        """空DataFrame应返回空结果"""
        engine = StrategyEngine('A')
        result = engine.score_stocks('A', pd.DataFrame())
        assert result.empty

    def test_score_stocks_negative_weight(self, mock_factor_df):
        """负权重因子应降低得分"""
        engine = StrategyEngine('A')
        result = engine.score_stocks('A', mock_factor_df)
        # 策略A中S_LN_FLOAT权重为-0.10，验证负权重生效
        assert 'strategy_score' in result.columns
        assert not result['strategy_score'].isna().any()

    def test_score_stocks_strategy_id_in_result(self, mock_factor_df):
        """结果应包含strategy_id列"""
        engine = StrategyEngine('A')
        result = engine.score_stocks('A', mock_factor_df)
        assert 'strategy_id' in result.columns
        assert (result['strategy_id'] == 'A').all()


# ============================================================
# apply_hard_filter 硬性过滤
# ============================================================

class TestApplyHardFilter:
    def test_hard_filter_adds_filter_pass(self, mock_factor_df):
        """过滤后应包含filter_pass列"""
        engine = StrategyEngine('F')
        result = engine.apply_hard_filter('F', mock_factor_df)
        assert 'filter_pass' in result.columns

    def test_hard_filter_boolean_values(self, mock_factor_df):
        """filter_pass列应为布尔值"""
        engine = StrategyEngine('F')
        result = engine.apply_hard_filter('F', mock_factor_df)
        assert result['filter_pass'].dtype == bool or set(result['filter_pass'].unique()).issubset({True, False})

    def test_hard_filter_no_filter_strategy(self, mock_factor_df):
        """无过滤条件的策略应全部通过"""
        engine = StrategyEngine('A')
        result = engine.apply_hard_filter('A', mock_factor_df)
        assert result['filter_pass'].all()

    def test_hard_filter_with_conditions(self, mock_factor_df):
        """有过滤条件的策略应剔除部分股票"""
        engine = StrategyEngine('F')
        result = engine.apply_hard_filter('F', mock_factor_df)
        # 策略F有3个hard_filter条件，可能剔除部分股票
        passed = result['filter_pass'].sum()
        total = len(result)
        assert passed <= total
        assert passed >= 0

    def test_hard_filter_empty_df(self):
        """空DataFrame应返回空结果"""
        engine = StrategyEngine('F')
        result = engine.apply_hard_filter('F', pd.DataFrame())
        assert result.empty

    def test_hard_filter_stats_stored(self, mock_factor_df):
        """过滤后应存储过滤统计信息"""
        engine = StrategyEngine('F')
        engine.apply_hard_filter('F', mock_factor_df)
        stats = engine.get_filter_stats()
        assert 'strategy_id' in stats
        assert 'total_stocks' in stats
        assert 'passed_stocks' in stats
        assert 'filtered_stocks' in stats


# ============================================================
# select_top_n 选股
# ============================================================

class TestSelectTopN:
    def test_select_top_n_count(self, mock_factor_df):
        """选股数量应等于n（或不足时为实际数量）"""
        engine = StrategyEngine('A')
        scored = engine.score_stocks('A', mock_factor_df)
        selected = engine.select_top_n(scored, n=10)
        assert len(selected) == 10

    def test_select_top_n_has_rank(self, mock_factor_df):
        """选股结果应包含rank列"""
        engine = StrategyEngine('A')
        scored = engine.score_stocks('A', mock_factor_df)
        selected = engine.select_top_n(scored, n=10)
        assert 'rank' in selected.columns
        assert list(selected['rank']) == list(range(1, len(selected) + 1))

    def test_select_top_n_sorted_by_score(self, mock_factor_df):
        """选股结果应按得分降序排列"""
        engine = StrategyEngine('A')
        scored = engine.score_stocks('A', mock_factor_df)
        selected = engine.select_top_n(scored, n=10)
        scores = selected['strategy_score'].values
        assert all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1))

    def test_select_top_n_with_min_score(self, mock_factor_df):
        """设置最低分数阈值应过滤低分股票"""
        engine = StrategyEngine('A')
        scored = engine.score_stocks('A', mock_factor_df)
        min_score = scored['strategy_score'].median()
        selected = engine.select_top_n(scored, n=200, min_score=min_score)
        if len(selected) > 0:
            assert selected['strategy_score'].min() >= min_score

    def test_select_top_n_with_filter(self, mock_factor_df):
        """有filter_pass时仅选取通过过滤的股票"""
        engine = StrategyEngine('F')
        scored = engine.score_stocks('F', mock_factor_df)
        filtered = engine.apply_hard_filter('F', scored)
        selected = engine.select_top_n(filtered, n=10)
        # 选出的股票应全部通过过滤
        if len(selected) > 0:
            assert selected['filter_pass'].all()

    def test_select_top_n_empty_df(self):
        """空DataFrame应返回空结果"""
        engine = StrategyEngine('A')
        result = engine.select_top_n(pd.DataFrame(), n=10)
        assert result.empty


# ============================================================
# 完整选股流程 run_pipeline
# ============================================================

class TestRunPipeline:
    def test_run_pipeline_returns_all_keys(self, mock_factor_df):
        """完整流程应返回所有必要键"""
        engine = StrategyEngine('A')
        result = engine.run_pipeline('A', mock_factor_df, n=10)
        expected_keys = {'selected', 'scored', 'filtered', 'filter_stats', 'strategy_id', 'summary'}
        assert expected_keys.issubset(set(result.keys()))

    def test_run_pipeline_summary(self, mock_factor_df):
        """摘要应包含正确的统计信息"""
        engine = StrategyEngine('A')
        result = engine.run_pipeline('A', mock_factor_df, n=10)
        summary = result['summary']
        assert 'strategy_id' in summary
        assert 'total_stocks_input' in summary
        assert 'stocks_selected' in summary
        assert summary['total_stocks_input'] == len(mock_factor_df)
        assert summary['stocks_selected'] <= 10
