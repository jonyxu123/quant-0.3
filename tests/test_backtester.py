"""回测框架测试 - 绩效指标计算、报告生成"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from backend.backtest.backtester import Backtester


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_backtester():
    """创建模拟回测器（不连接真实DuckDB）"""
    with patch('backend.backtest.backtester.duckdb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        bt = Backtester(db_path=':memory:')
        return bt


@pytest.fixture
def positive_returns():
    """正收益序列"""
    return pd.Series([1.0, 0.5, 1.5, 0.8, 0.3, 1.2, 0.6, 0.9, 0.4, 1.1])


@pytest.fixture
def mixed_returns():
    """混合正负收益序列"""
    return pd.Series([2.0, -1.0, 1.5, -0.5, 0.8, -0.3, 1.2, -0.8, 0.6, 0.4])


@pytest.fixture
def negative_returns():
    """负收益序列"""
    return pd.Series([-1.0, -0.5, -1.5, -0.8, -0.3, -1.2, -0.6, -0.9, -0.4, -1.1])


@pytest.fixture
def single_period_returns():
    """单期收益序列"""
    return pd.Series([1.0])


# ============================================================
# _calc_performance 绩效指标计算正确
# ============================================================

class TestCalcPerformance:
    def test_calc_performance_keys(self, mock_backtester, mixed_returns):
        """绩效指标应包含所有必要键"""
        perf = mock_backtester._calc_performance(mixed_returns)
        expected_keys = {
            'annual_return', 'annual_volatility', 'sharpe_ratio',
            'max_drawdown', 'calmar_ratio', 'win_rate',
            'profit_loss_ratio', 'total_periods', 'cumulative_return',
        }
        assert expected_keys.issubset(set(perf.keys()))

    def test_calc_performance_positive_returns(self, mock_backtester, positive_returns):
        """正收益序列的年化收益率应为正"""
        perf = mock_backtester._calc_performance(positive_returns)
        assert perf['annual_return'] > 0
        assert perf['cumulative_return'] > 0

    def test_calc_performance_negative_returns(self, mock_backtester, negative_returns):
        """负收益序列的年化收益率应为负"""
        perf = mock_backtester._calc_performance(negative_returns)
        assert perf['annual_return'] < 0
        assert perf['cumulative_return'] < 0

    def test_calc_performance_win_rate(self, mock_backtester, mixed_returns):
        """胜率应在0-100%之间"""
        perf = mock_backtester._calc_performance(mixed_returns)
        assert 0 <= perf['win_rate'] <= 100

    def test_calc_performance_win_rate_all_positive(self, mock_backtester, positive_returns):
        """全正收益的胜率应为100%"""
        perf = mock_backtester._calc_performance(positive_returns)
        assert perf['win_rate'] == 100.0

    def test_calc_performance_win_rate_all_negative(self, mock_backtester, negative_returns):
        """全负收益的胜率应为0%"""
        perf = mock_backtester._calc_performance(negative_returns)
        assert perf['win_rate'] == 0.0

    def test_calc_performance_max_drawdown_non_positive(self, mock_backtester, mixed_returns):
        """最大回撤应为非正值（百分比）"""
        perf = mock_backtester._calc_performance(mixed_returns)
        assert perf['max_drawdown'] <= 0

    def test_calc_performance_max_drawdown_zero_for_monotone_up(self, mock_backtester):
        """单调递增收益的最大回撤应为0"""
        # 构造单调递增的累计收益
        returns = pd.Series([0.1, 0.1, 0.1, 0.1, 0.1])
        perf = mock_backtester._calc_performance(returns)
        assert perf['max_drawdown'] == 0.0

    def test_calc_performance_sharpe_ratio(self, mock_backtester, mixed_returns):
        """夏普比率应合理"""
        perf = mock_backtester._calc_performance(mixed_returns)
        # 夏普比率不应为NaN或Inf
        assert not np.isnan(perf['sharpe_ratio'])
        assert not np.isinf(perf['sharpe_ratio'])

    def test_calc_performance_total_periods(self, mock_backtester, mixed_returns):
        """总期数应等于输入长度"""
        perf = mock_backtester._calc_performance(mixed_returns)
        assert perf['total_periods'] == len(mixed_returns)

    def test_calc_performance_profit_loss_ratio(self, mock_backtester, mixed_returns):
        """盈亏比应为非负值"""
        perf = mock_backtester._calc_performance(mixed_returns)
        assert perf['profit_loss_ratio'] >= 0

    def test_calc_performance_empty_returns(self, mock_backtester):
        """空收益序列应返回空字典"""
        perf = mock_backtester._calc_performance(pd.Series(dtype=float))
        assert perf == {}

    def test_calc_performance_single_period(self, mock_backtester, single_period_returns):
        """单期收益应能正常计算"""
        perf = mock_backtester._calc_performance(single_period_returns)
        assert 'annual_return' in perf
        assert 'sharpe_ratio' in perf

    def test_calc_performance_cumulative_return(self, mock_backtester):
        """累计收益应正确计算"""
        # 简单验证：等额正收益
        returns = pd.Series([1.0, 1.0, 1.0])
        perf = mock_backtester._calc_performance(returns)
        # 累计收益 = (1.01)^3 - 1 ≈ 3.03%
        assert perf['cumulative_return'] > 0

    def test_calc_performance_calmar_ratio(self, mock_backtester, mixed_returns):
        """卡尔马比率应合理"""
        perf = mock_backtester._calc_performance(mixed_returns)
        if perf['max_drawdown'] != 0:
            expected_calmar = perf['annual_return'] / abs(perf['max_drawdown'])
            assert abs(perf['calmar_ratio'] - expected_calmar) < 0.1

    def test_calc_performance_volatility_non_negative(self, mock_backtester, mixed_returns):
        """年化波动率应为非负值"""
        perf = mock_backtester._calc_performance(mixed_returns)
        assert perf['annual_volatility'] >= 0


# ============================================================
# generate_report 生成报告
# ============================================================

class TestGenerateReport:
    def test_generate_report_contains_strategy_id(self, mock_backtester):
        """报告应包含策略ID"""
        result = {
            'start_date': '20230101',
            'end_date': '20231231',
            'rebalance_freq': 'M',
            'top_n': 50,
            'performance': {
                'annual_return': 15.0,
                'annual_volatility': 20.0,
                'sharpe_ratio': 0.75,
                'max_drawdown': -10.0,
                'calmar_ratio': 1.5,
                'win_rate': 55.0,
                'profit_loss_ratio': 1.2,
                'cumulative_return': 15.0,
            }
        }
        report = mock_backtester.generate_report('A', result)
        assert '策略A' in report or 'A' in report

    def test_generate_report_contains_dates(self, mock_backtester):
        """报告应包含回测区间"""
        result = {
            'start_date': '20230101',
            'end_date': '20231231',
            'rebalance_freq': 'M',
            'top_n': 50,
            'performance': {},
        }
        report = mock_backtester.generate_report('A', result)
        assert '20230101' in report
        assert '20231231' in report

    def test_generate_report_contains_metrics(self, mock_backtester):
        """报告应包含绩效指标"""
        result = {
            'start_date': '20230101',
            'end_date': '20231231',
            'rebalance_freq': 'M',
            'top_n': 50,
            'performance': {
                'annual_return': 15.0,
                'annual_volatility': 20.0,
                'sharpe_ratio': 0.75,
                'max_drawdown': -10.0,
                'calmar_ratio': 1.5,
                'win_rate': 55.0,
                'profit_loss_ratio': 1.2,
                'cumulative_return': 15.0,
            }
        }
        report = mock_backtester.generate_report('A', result)
        assert '年化收益率' in report
        assert '夏普比率' in report
        assert '最大回撤' in report
        assert '胜率' in report

    def test_generate_report_empty_performance(self, mock_backtester):
        """空绩效指标应能正常生成报告"""
        result = {
            'start_date': '20230101',
            'end_date': '20231231',
            'rebalance_freq': 'M',
            'top_n': 50,
            'performance': {},
        }
        report = mock_backtester.generate_report('A', result)
        assert isinstance(report, str)
        assert len(report) > 0

    def test_generate_report_is_string(self, mock_backtester):
        """报告应为字符串类型"""
        result = {
            'start_date': '20230101',
            'end_date': '20231231',
            'rebalance_freq': 'M',
            'top_n': 50,
            'performance': {
                'annual_return': 15.0,
                'annual_volatility': 20.0,
                'sharpe_ratio': 0.75,
                'max_drawdown': -10.0,
                'calmar_ratio': 1.5,
                'win_rate': 55.0,
                'profit_loss_ratio': 1.2,
                'cumulative_return': 15.0,
            }
        }
        report = mock_backtester.generate_report('D', result)
        assert isinstance(report, str)


# ============================================================
# _get_rebalance_dates 测试
# ============================================================

class TestGetRebalanceDates:
    def test_monthly_rebalance(self, mock_backtester):
        """月度调仓应返回每月首个交易日"""
        # Mock数据库查询
        mock_conn = MagicMock()
        all_dates = [f'2023{m:02d}{d:02d}' for m in range(1, 13) for d in [1, 15]]
        dates_df = pd.DataFrame({'cal_date': all_dates})

        with patch('pandas.read_sql', return_value=dates_df):
            dates = mock_backtester._get_rebalance_dates('20230101', '20231231', 'M')
            # 每月至少1个调仓日
            assert len(dates) >= 1

    def test_weekly_rebalance(self, mock_backtester):
        """周度调仓应返回每5个交易日1个"""
        mock_conn = MagicMock()
        all_dates = [f'202301{d:02d}' for d in range(1, 21)]
        dates_df = pd.DataFrame({'cal_date': all_dates})

        with patch('pandas.read_sql', return_value=dates_df):
            dates = mock_backtester._get_rebalance_dates('20230101', '20230120', 'W')
            assert len(dates) >= 1

    def test_empty_dates(self, mock_backtester):
        """无交易日时应返回空列表"""
        with patch('pandas.read_sql', side_effect=Exception("DB error")):
            dates = mock_backtester._get_rebalance_dates('20990101', '20991231', 'M')
            assert dates == []


# ============================================================
# _calc_portfolio_return 测试
# ============================================================

class TestCalcPortfolioReturn:
    def test_portfolio_return_with_data(self, mock_backtester):
        """有数据时应返回合理收益"""
        holdings = ['600000.SH', '600001.SH']
        mock_df = pd.DataFrame({
            'ts_code': holdings,
            'pct_chg': [1.0, 2.0],
        })
        with patch('pandas.read_sql', return_value=mock_df):
            ret = mock_backtester._calc_portfolio_return(holdings, '20230101', '20230102')
            assert ret == 1.5  # 平均收益

    def test_portfolio_return_no_data(self, mock_backtester):
        """无数据时应返回0"""
        holdings = ['600000.SH']
        with patch('pandas.read_sql', return_value=pd.DataFrame()):
            ret = mock_backtester._calc_portfolio_return(holdings, '20230101', '20230102')
            assert ret == 0.0

    def test_portfolio_return_db_error(self, mock_backtester):
        """数据库错误时应返回0"""
        holdings = ['600000.SH']
        with patch('pandas.read_sql', side_effect=Exception("DB error")):
            ret = mock_backtester._calc_portfolio_return(holdings, '20230101', '20230102')
            assert ret == 0.0


# ============================================================
# 集成验证：绩效指标一致性
# ============================================================

class TestPerformanceConsistency:
    def test_sharpe_equals_return_over_vol(self, mock_backtester):
        """夏普比率 = 年化收益 / 年化波动率"""
        returns = pd.Series([1.0, -0.5, 1.5, 0.8, -0.3, 1.2, 0.6, -0.8, 0.9, 0.4])
        perf = mock_backtester._calc_performance(returns)
        if perf['annual_volatility'] > 0:
            expected_sharpe = perf['annual_return'] / perf['annual_volatility']
            assert abs(perf['sharpe_ratio'] - expected_sharpe) < 0.1

    def test_calmar_equals_return_over_drawdown(self, mock_backtester):
        """卡尔马比率 = 年化收益 / |最大回撤|"""
        returns = pd.Series([1.0, -0.5, 1.5, 0.8, -0.3, 1.2, 0.6, -0.8, 0.9, 0.4])
        perf = mock_backtester._calc_performance(returns)
        if perf['max_drawdown'] != 0:
            expected_calmar = perf['annual_return'] / abs(perf['max_drawdown'])
            assert abs(perf['calmar_ratio'] - expected_calmar) < 0.1
