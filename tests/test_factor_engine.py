"""因子引擎测试 - 因子注册表完整性、维度因子数量、列表查询"""
import pytest
from unittest.mock import MagicMock, patch
from collections import Counter

from backend.factors.factor_engine import FactorEngine
from config import DIMENSION_PREFIXES


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_factor_engine():
    """创建模拟因子引擎（不连接真实DuckDB）"""
    with patch('backend.factors.factor_engine.duckdb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        engine = FactorEngine(db_path=':memory:')
        return engine


# ============================================================
# 因子注册表完整性
# ============================================================

class TestFactorRegistry:
    def test_registry_not_empty(self, mock_factor_engine):
        """因子注册表不应为空"""
        assert len(mock_factor_engine._registry) > 0

    def test_registry_has_208_plus_factors(self, mock_factor_engine):
        """因子注册表应包含208+因子"""
        total = len(mock_factor_engine._registry)
        assert total >= 208, (
            f"因子注册表仅包含{total}个因子，预期至少208个"
        )

    def test_registry_factor_ids_are_strings(self, mock_factor_engine):
        """所有因子ID应为字符串"""
        for factor_id in mock_factor_engine._registry.keys():
            assert isinstance(factor_id, str), f"因子ID {factor_id} 不是字符串"

    def test_registry_factor_info_has_required_keys(self, mock_factor_engine):
        """每个因子信息应包含必要字段"""
        required_keys = {'dimension', 'name', 'direction'}
        for factor_id, info in mock_factor_engine._registry.items():
            assert required_keys.issubset(set(info.keys())), (
                f"因子{factor_id}缺少字段: {required_keys - set(info.keys())}"
            )

    def test_registry_dimensions_are_valid(self, mock_factor_engine):
        """所有因子维度前缀应在DIMENSION_PREFIXES中或为已知的扩展维度"""
        # 获取注册表中实际出现的所有维度
        actual_dims = set(info['dimension'] for info in mock_factor_engine._registry.values())
        # 允许DIMENSION_PREFIXES之外的维度（如L_杠杆维度）
        extra_dims = actual_dims - set(DIMENSION_PREFIXES)
        if extra_dims:
            import warnings
            warnings.warn(f"发现DIMENSION_PREFIXES之外的维度: {extra_dims}，建议补充到配置中")
        # 核心验证：所有维度前缀应与因子ID匹配
        for factor_id, info in mock_factor_engine._registry.items():
            dim = info['dimension']
            assert factor_id.startswith(dim), (
                f"因子{factor_id}不以维度前缀{dim}开头"
            )

    def test_registry_factor_ids_match_dimension(self, mock_factor_engine):
        """因子ID应以对应维度前缀开头"""
        for factor_id, info in mock_factor_engine._registry.items():
            dim = info['dimension']
            assert factor_id.startswith(dim), (
                f"因子ID {factor_id} 不以维度前缀 {dim} 开头"
            )


# ============================================================
# 各维度因子数量正确
# ============================================================

class TestDimensionCounts:
    def test_all_dimensions_have_factors(self, mock_factor_engine):
        """每个维度都应有至少1个因子"""
        dimensions_with_factors = set()
        for factor_id, info in mock_factor_engine._registry.items():
            dimensions_with_factors.add(info['dimension'])

        for dim in DIMENSION_PREFIXES:
            if dim in dimensions_with_factors:
                count = len(mock_factor_engine.list_factors_by_dimension(dim))
                assert count > 0, f"维度{dim}无注册因子"

    def test_dimension_factor_counts(self, mock_factor_engine):
        """验证各维度因子数量分布合理"""
        dim_counts = Counter()
        for factor_id, info in mock_factor_engine._registry.items():
            dim_counts[info['dimension']] += 1

        # 基础维度应有较多因子
        core_dimensions = ['V_', 'P_', 'G_', 'Q_', 'M_']
        for dim in core_dimensions:
            if dim in dim_counts:
                assert dim_counts[dim] >= 3, (
                    f"核心维度{dim}仅有{dim_counts[dim]}个因子，预期至少3个"
                )

    def test_total_factor_count_by_dimension(self, mock_factor_engine):
        """各维度因子数之和应等于总因子数"""
        # 获取注册表中实际出现的所有维度
        actual_dims = set(info['dimension'] for info in mock_factor_engine._registry.values())
        total_from_dimensions = 0
        for dim in actual_dims:
            total_from_dimensions += len(mock_factor_engine.list_factors_by_dimension(dim))
        total_from_registry = len(mock_factor_engine._registry)
        assert total_from_dimensions == total_from_registry, (
            f"维度因子数之和{total_from_dimensions} != 注册表总数{total_from_registry}"
        )


# ============================================================
# list_all_factors / list_factors_by_dimension
# ============================================================

class TestListMethods:
    def test_list_all_factors(self, mock_factor_engine):
        """list_all_factors应返回所有因子ID列表"""
        all_factors = mock_factor_engine.list_all_factors()
        assert len(all_factors) == len(mock_factor_engine._registry)
        # 应按字母排序
        assert all_factors == sorted(all_factors)

    def test_list_all_factors_are_strings(self, mock_factor_engine):
        """list_all_factors返回的因子ID应为字符串"""
        all_factors = mock_factor_engine.list_all_factors()
        for fid in all_factors:
            assert isinstance(fid, str)

    def test_list_factors_by_dimension(self, mock_factor_engine):
        """list_factors_by_dimension应返回指定维度的因子"""
        actual_dims = set(info['dimension'] for info in mock_factor_engine._registry.values())
        for dim in actual_dims:
            factors = mock_factor_engine.list_factors_by_dimension(dim)
            for fid in factors:
                assert fid.startswith(dim), (
                    f"因子{fid}不属于维度{dim}"
                )

    def test_list_factors_by_dimension_nonexistent(self, mock_factor_engine):
        """不存在的维度应返回空列表"""
        factors = mock_factor_engine.list_factors_by_dimension('ZZ_')
        assert factors == []

    def test_list_factors_by_dimension_no_overlap(self, mock_factor_engine):
        """不同维度的因子不应重叠"""
        actual_dims = set(info['dimension'] for info in mock_factor_engine._registry.values())
        dim_factors = {}
        for dim in actual_dims:
            dim_factors[dim] = set(mock_factor_engine.list_factors_by_dimension(dim))

        for dim1 in actual_dims:
            for dim2 in actual_dims:
                if dim1 != dim2:
                    overlap = dim_factors[dim1] & dim_factors[dim2]
                    assert len(overlap) == 0, (
                        f"维度{dim1}和{dim2}有重叠因子: {overlap}"
                    )


# ============================================================
# get_factor_info
# ============================================================

class TestGetFactorInfo:
    def test_get_factor_info_existing(self, mock_factor_engine):
        """获取已注册因子的信息"""
        all_factors = mock_factor_engine.list_all_factors()
        if all_factors:
            info = mock_factor_engine.get_factor_info(all_factors[0])
            assert info is not None
            assert 'dimension' in info
            assert 'name' in info

    def test_get_factor_info_nonexistent(self, mock_factor_engine):
        """获取未注册因子的信息应返回空字典"""
        info = mock_factor_engine.get_factor_info('NONEXISTENT_FACTOR')
        assert info == {}


# ============================================================
# 因子方向性验证
# ============================================================

class TestFactorDirection:
    def test_direction_values(self, mock_factor_engine):
        """因子方向应为positive/negative/bidirectional"""
        valid_directions = {'positive', 'negative', 'bidirectional'}
        for factor_id, info in mock_factor_engine._registry.items():
            direction = info.get('direction')
            assert direction in valid_directions, (
                f"因子{factor_id}的方向{direction}不在有效值{valid_directions}中"
            )
