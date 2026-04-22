"""预处理模块测试 - 截面缺失值填充、MAD去极值、Z-Score标准化"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

from backend.preprocessing.preprocess import (
    _fillna_cross_section,
    _winsorize_mad_cross_section,
    _zscore_cross_section,
    _identify_factor_cols,
    preprocess_single_date,
)


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def sample_df():
    """构造含NaN、极端值的多行业截面DataFrame"""
    np.random.seed(42)
    n_per_industry = 20
    industries = ['银行', '医药', '电子']
    rows = []
    for ind in industries:
        for i in range(n_per_industry):
            row = {
                'trade_date': '20240101',
                'industry': ind,
                'ts_code': f'{ind}_{i:03d}',
                'V_EP': np.random.normal(0.05, 0.02),
                'P_ROE': np.random.normal(12, 5),
                'G_NI_YOY': np.random.normal(15, 10),
            }
            rows.append(row)

    df = pd.DataFrame(rows)

    # 人为注入NaN
    df.loc[0, 'V_EP'] = np.nan
    df.loc[1, 'P_ROE'] = np.nan
    df.loc[25, 'G_NI_YOY'] = np.nan

    # 人为注入极端值
    df.loc[2, 'V_EP'] = 999.0
    df.loc[3, 'P_ROE'] = -500.0

    return df


@pytest.fixture
def factor_cols():
    return ['V_EP', 'P_ROE', 'G_NI_YOY']


@pytest.fixture
def mock_conn():
    """模拟DuckDB连接"""
    conn = MagicMock()
    return conn


# ============================================================
# _identify_factor_cols 测试
# ============================================================

class TestIdentifyFactorCols:
    def test_identify_factor_cols(self):
        """识别以维度前缀开头的因子列"""
        columns = ['trade_date', 'ts_code', 'industry', 'V_EP', 'P_ROE', 'G_NI_YOY', 'M_MOM_12_1']
        factor_cols = _identify_factor_cols(columns)
        assert 'V_EP' in factor_cols
        assert 'P_ROE' in factor_cols
        assert 'G_NI_YOY' in factor_cols
        assert 'M_MOM_12_1' in factor_cols
        assert 'trade_date' not in factor_cols
        assert 'industry' not in factor_cols

    def test_no_factor_cols(self):
        """无非因子列时返回空列表"""
        columns = ['trade_date', 'ts_code', 'industry']
        factor_cols = _identify_factor_cols(columns)
        assert factor_cols == []


# ============================================================
# _fillna_cross_section 测试
# ============================================================

class TestFillnaCrossSection:
    def test_fillna_within_industry(self, sample_df, factor_cols):
        """行业内NaN应被中位数填充"""
        result = _fillna_cross_section(sample_df.copy(), factor_cols)
        # 按行业分组，行业内不应有NaN
        for col in factor_cols:
            for ind in result['industry'].unique():
                subset = result[result['industry'] == ind]
                assert not subset[col].isna().any(), (
                    f"列{col}在行业{ind}中仍有NaN"
                )

    def test_fillna_preserves_non_nan(self, sample_df, factor_cols):
        """非NaN值不应被修改"""
        original = sample_df.copy()
        result = _fillna_cross_section(sample_df.copy(), factor_cols)
        for col in factor_cols:
            mask = original[col].notna()
            pd.testing.assert_series_equal(
                result.loc[mask, col], original.loc[mask, col],
                check_names=False
            )

    def test_fillna_uses_median(self):
        """验证填充值确实是行业内中位数"""
        df = pd.DataFrame({
            'industry': ['A', 'A', 'A', 'A'],
            'V_EP': [1.0, 2.0, np.nan, 4.0],
        })
        result = _fillna_cross_section(df, ['V_EP'])
        # 行业A中位数 = median(1,2,4) = 2.0
        assert result.loc[2, 'V_EP'] == 2.0


# ============================================================
# _winsorize_mad_cross_section 测试
# ============================================================

class TestWinsorizeMadCrossSection:
    def test_winsorize_clips_extremes(self, sample_df, factor_cols):
        """MAD去极值后不应有超出截尾范围的值"""
        result = _winsorize_mad_cross_section(sample_df.copy(), factor_cols, n=3.0)
        for col in factor_cols:
            series = result[col]
            median = series.median()
            mad = (series - median).abs().median()
            upper = median + 3.0 * 1.4826 * mad
            lower = median - 3.0 * 1.4826 * mad
            # 去极值后所有值应在[lower, upper]范围内
            assert series.min() >= lower - 1e-10, (
                f"列{col}最小值{series.min():.4f}低于下界{lower:.4f}"
            )
            assert series.max() <= upper + 1e-10, (
                f"列{col}最大值{series.max():.4f}高于上界{upper:.4f}"
            )

    def test_winsorize_no_new_nan(self, sample_df, factor_cols):
        """MAD去极值不应引入新的NaN（原有NaN保留）"""
        original_nan_mask = sample_df[factor_cols].isna()
        result = _winsorize_mad_cross_section(sample_df.copy(), factor_cols)
        result_nan_mask = result[factor_cols].isna()
        # 去极值后NaN位置应与原来一致（不新增NaN）
        for col in factor_cols:
            assert not (result_nan_mask[col] & ~original_nan_mask[col]).any(), (
                f"列{col}在去极值后新增了NaN"
            )

    def test_winsorize_preserves_normal_values(self):
        """正常范围内的值不应被修改"""
        df = pd.DataFrame({
            'industry': ['A'] * 10,
            'V_EP': [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9],
        })
        original = df['V_EP'].copy()
        result = _winsorize_mad_cross_section(df, ['V_EP'])
        # 正常值不应被clip
        pd.testing.assert_series_equal(result['V_EP'], original, check_names=False)

    def test_winsorize_extreme_value_clipped(self):
        """极端值应被截尾"""
        df = pd.DataFrame({
            'industry': ['A'] * 10,
            'V_EP': [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 999.0],
        })
        result = _winsorize_mad_cross_section(df, ['V_EP'], n=3.0)
        # 999.0应被截尾
        assert result['V_EP'].max() < 999.0


# ============================================================
# _zscore_cross_section 测试
# ============================================================

class TestZscoreCrossSection:
    def test_zscore_mean_approx_zero(self, sample_df, factor_cols):
        """Z-Score标准化后均值应接近0"""
        # 先填充NaN，否则zscore会传播NaN
        df = _fillna_cross_section(sample_df.copy(), factor_cols)
        result = _zscore_cross_section(df, factor_cols)
        for col in factor_cols:
            mean = result[col].mean()
            assert abs(mean) < 1e-6, (
                f"列{col}的Z-Score均值{mean:.6f}偏离0"
            )

    def test_zscore_std_approx_one(self, sample_df, factor_cols):
        """Z-Score标准化后标准差应接近1"""
        df = _fillna_cross_section(sample_df.copy(), factor_cols)
        result = _zscore_cross_section(df, factor_cols)
        for col in factor_cols:
            std = result[col].std()
            assert abs(std - 1.0) < 0.05, (
                f"列{col}的Z-Score标准差{std:.4f}偏离1"
            )

    def test_zscore_no_nan(self, sample_df, factor_cols):
        """Z-Score标准化不应引入NaN（输入无NaN时）"""
        df = _fillna_cross_section(sample_df.copy(), factor_cols)
        result = _zscore_cross_section(df, factor_cols)
        for col in factor_cols:
            assert not result[col].isna().any(), f"列{col}在Z-Score后出现NaN"


# ============================================================
# preprocess_single_date 测试
# ============================================================

class TestPreprocessSingleDate:
    def test_preprocess_single_date_pipeline(self, mock_conn, sample_df, factor_cols):
        """单期处理流程：fillna → winsorize → zscore → 写回DuckDB"""
        # 配置mock连接
        mock_conn.execute.return_value.fetchdf.return_value = sample_df.copy()
        mock_conn.execute.return_value.df = sample_df.copy()

        # 由于to_sql需要真实连接，我们mock掉DataFrame.to_sql
        with pytest.MonkeyPatch.context() as m:
            written_dfs = []
            def mock_to_sql(self_df, name, con, **kwargs):
                written_dfs.append(self_df.copy())

            m.setattr(pd.DataFrame, 'to_sql', mock_to_sql)

            result = preprocess_single_date(mock_conn, '20240101')

            # 应返回处理行数
            assert result == len(sample_df)

            # 验证写回的DataFrame经过了完整处理
            if written_dfs:
                processed_df = written_dfs[0]
                for col in factor_cols:
                    # Z-Score后均值≈0
                    mean = processed_df[col].mean()
                    assert abs(mean) < 0.1, (
                        f"列{col}处理后均值{mean:.4f}偏离0"
                    )

    def test_preprocess_empty_date(self, mock_conn):
        """空数据日期应返回0"""
        empty_df = pd.DataFrame()
        mock_conn.execute.return_value.fetchdf.return_value = empty_df
        result = preprocess_single_date(mock_conn, '20991231')
        assert result == 0


# ============================================================
# 集成验证：完整预处理流水线
# ============================================================

class TestPreprocessIntegration:
    def test_full_pipeline_no_nan_in_industry(self, sample_df, factor_cols):
        """完整流水线后，行业内不应有NaN"""
        df = _fillna_cross_section(sample_df.copy(), factor_cols)
        df = _winsorize_mad_cross_section(df, factor_cols)
        df = _zscore_cross_section(df, factor_cols)

        for col in factor_cols:
            for ind in df['industry'].unique():
                subset = df[df['industry'] == ind]
                assert not subset[col].isna().any(), (
                    f"列{col}在行业{ind}中仍有NaN"
                )

    def test_full_pipeline_zscore_properties(self, sample_df, factor_cols):
        """完整流水线后，Z-Score属性应满足"""
        df = _fillna_cross_section(sample_df.copy(), factor_cols)
        df = _winsorize_mad_cross_section(df, factor_cols)
        df = _zscore_cross_section(df, factor_cols)

        for col in factor_cols:
            mean = df[col].mean()
            std = df[col].std()
            assert abs(mean) < 0.1, f"列{col}均值{mean:.4f}偏离0"
            assert abs(std - 1.0) < 0.1, f"列{col}标准差{std:.4f}偏离1"

    def test_full_pipeline_no_extreme_outliers(self, sample_df, factor_cols):
        """完整流水线后，不应有超出MAD截尾范围的极端值"""
        df = _fillna_cross_section(sample_df.copy(), factor_cols)
        df = _winsorize_mad_cross_section(df, factor_cols)

        for col in factor_cols:
            series = df[col]
            median = series.median()
            mad = (series - median).abs().median()
            upper = median + 3.0 * 1.4826 * mad
            lower = median - 3.0 * 1.4826 * mad
            assert series.min() >= lower - 1e-10
            assert series.max() <= upper + 1e-10
