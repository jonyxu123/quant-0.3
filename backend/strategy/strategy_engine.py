"""
策略打分引擎模块 - 基于策略权重对股票进行打分、过滤和选股
基于《多因子选股系统_v8.0》第5-6章策略打分逻辑

核心功能:
- score_stocks(): 策略打分 - 根据策略权重对因子数据加权求和
- apply_hard_filter(): 硬性过滤 - 根据策略硬性过滤条件剔除不合规股票
- select_top_n(): 选股Top N - 按综合得分排序选取Top N股票
"""

import numpy as np
import pandas as pd
from typing import Optional, Dict, List, Tuple

from .strategy_combos import (
    STRATEGY_COMBOS,
    get_strategy_combo,
    get_strategy_info,
)


class StrategyEngine:
    """
    策略打分引擎

    负责根据策略配置对股票因子数据进行打分、过滤和排序选股。

    Attributes:
        strategy_id: 当前使用的策略ID
        strategy_info: 当前策略的完整信息
    """

    # 支持的比较运算符
    OPERATORS = {
        '>': lambda x, v: x > v,
        '>=': lambda x, v: x >= v,
        '<': lambda x, v: x < v,
        '<=': lambda x, v: x <= v,
        '==': lambda x, v: x == v,
        '!=': lambda x, v: x != v,
        # 区间过滤：value 传 [low, high]，闭区间
        'between': lambda x, v: x.between(v[0], v[1]) if hasattr(x, 'between') else (x >= v[0]) & (x <= v[1]),
    }

    def __init__(self, strategy_id: str = 'D'):
        """
        初始化策略打分引擎

        Args:
            strategy_id: 策略ID，默认使用均衡综合型(D)
        """
        self.strategy_id = strategy_id.upper()
        self.strategy_info = get_strategy_info(self.strategy_id)
        if self.strategy_info is None:
            # 如果策略不存在，回退到均衡综合型
            self.strategy_id = 'D'
            self.strategy_info = get_strategy_info('D')

    # ============================================================
    # 核心方法: 策略打分
    # ============================================================

    def score_stocks(self, strategy_id: str = None,
                     factor_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        策略打分 - 根据策略权重对因子数据加权求和

        打分逻辑:
        1. 获取策略权重配置
        2. 对因子数据进行标准化（如果尚未标准化）
        3. 加权求和: score = Σ(weight_i × factor_i)
        4. 负权重因子: weight为负表示负向因子（值越小越好）

        Args:
            strategy_id: 策略ID（可选，默认使用初始化时的策略）
            factor_df: 因子数据DataFrame，index为股票代码，columns为因子ID

        Returns:
            pd.DataFrame: 包含原始因子数据和新增的 'strategy_score' 列
                          按得分降序排列
        """
        if factor_df is None or factor_df.empty:
            return pd.DataFrame()

        # 确定使用的策略
        sid = (strategy_id or self.strategy_id).upper()
        weights = get_strategy_combo(sid)

        # 获取因子数据中可用的因子（与策略权重取交集）
        available_factors = set(factor_df.columns) & set(weights.keys())
        if not available_factors:
            # 没有可用因子，返回零分
            result_df = factor_df.copy()
            result_df['strategy_score'] = 0.0
            result_df['strategy_id'] = sid
            result_df['matched_factors'] = 0
            result_df['total_factors'] = len(weights)
            return result_df

        # 加权求和
        score = pd.Series(0.0, index=factor_df.index)
        for factor_id, weight in weights.items():
            if factor_id in factor_df.columns:
                # 负权重直接乘（负权重×因子值 = 负向因子得分）
                factor_values = factor_df[factor_id].fillna(0)
                score += weight * factor_values

        # 构建结果
        result_df = factor_df.copy()
        result_df['strategy_score'] = score
        result_df['strategy_id'] = sid
        result_df['matched_factors'] = len(available_factors)
        result_df['total_factors'] = len(weights)

        # 按得分降序排列
        result_df = result_df.sort_values('strategy_score', ascending=False)

        return result_df

    # ============================================================
    # 核心方法: 硬性过滤
    # ============================================================

    def apply_hard_filter(self, strategy_id: str = None,
                          factor_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        硬性过滤 - 根据策略硬性过滤条件剔除不合规股票

        过滤逻辑:
        1. 获取策略的hard_filter配置
        2. 对每个过滤条件，检查因子值是否满足
        3. 不满足任一条件的股票被剔除
        4. 无hard_filter的策略不做过滤

        hard_filter格式:
        {
            'factor_id': {'op': '>', 'value': threshold},
            ...
        }
        op支持: >, >=, <, <=, ==, !=

        Args:
            strategy_id: 策略ID（可选，默认使用初始化时的策略）
            factor_df: 因子数据DataFrame，index为股票代码，columns为因子ID

        Returns:
            pd.DataFrame: 过滤后的因子数据，新增 'filter_pass' 列
                          True=通过所有过滤条件
        """
        if factor_df is None or factor_df.empty:
            return pd.DataFrame()

        # 确定使用的策略
        sid = (strategy_id or self.strategy_id).upper()
        info = get_strategy_info(sid)
        if info is None:
            # 策略不存在，不做过滤
            result_df = factor_df.copy()
            result_df['filter_pass'] = True
            return result_df

        hard_filter = info.get('hard_filter', {})
        if not hard_filter:
            # 无过滤条件，全部通过
            result_df = factor_df.copy()
            result_df['filter_pass'] = True
            return result_df

        # 逐条应用过滤条件
        pass_mask = pd.Series(True, index=factor_df.index)
        filter_details = {}

        for factor_id, condition in hard_filter.items():
            op = condition.get('op', '>')
            value = condition.get('value', 0)

            # 处理特殊字段（如 V_PE_TTM_upper 映射到 V_PE_TTM）
            actual_factor = factor_id.replace('_upper', '')
            if actual_factor not in factor_df.columns:
                # 因子不存在于数据中，跳过该条件（不剔除）
                filter_details[factor_id] = 'skipped (factor not found)'
                continue

            # 应用比较运算
            if op not in self.OPERATORS:
                filter_details[factor_id] = f'skipped (unknown op: {op})'
                continue

            factor_values = factor_df[actual_factor]
            condition_mask = self.OPERATORS[op](factor_values, value)
            pass_mask = pass_mask & condition_mask

            # 记录过滤统计
            pass_count = condition_mask.sum()
            total_count = len(condition_mask)
            filter_details[factor_id] = f'{op} {value}: {pass_count}/{total_count} passed'

        # 构建结果
        result_df = factor_df.copy()
        result_df['filter_pass'] = pass_mask

        # 存储过滤详情（作为实例属性，方便查询）
        self._last_filter_details = filter_details
        self._last_filter_stats = {
            'strategy_id': sid,
            'total_stocks': len(factor_df),
            'passed_stocks': int(pass_mask.sum()),
            'filtered_stocks': int((~pass_mask).sum()),
            'filter_rate': round(1 - pass_mask.mean(), 4),
            'details': filter_details,
        }

        return result_df

    # ============================================================
    # 核心方法: 选股Top N
    # ============================================================

    def select_top_n(self, score_df: pd.DataFrame = None,
                     n: int = 50,
                     score_col: str = 'strategy_score',
                     min_score: float = None) -> pd.DataFrame:
        """
        选股Top N - 按综合得分排序选取Top N股票

        选股逻辑:
        1. 如果有filter_pass列，仅选取通过过滤的股票
        2. 按得分降序排列
        3. 可选: 设置最低分数阈值
        4. 选取Top N

        Args:
            score_df: 打分后的DataFrame（需包含score_col列）
            n: 选取股票数量
            score_col: 得分列名，默认 'strategy_score'
            min_score: 最低分数阈值（可选）

        Returns:
            pd.DataFrame: Top N股票数据，新增 'rank' 列表示排名
        """
        if score_df is None or score_df.empty:
            return pd.DataFrame()

        df = score_df.copy()

        # 如果有过滤标记，仅保留通过过滤的股票
        if 'filter_pass' in df.columns:
            df = df[df['filter_pass'] == True]

        # 检查得分列是否存在
        if score_col not in df.columns:
            # 尝试使用strategy_score
            if 'strategy_score' in df.columns:
                score_col = 'strategy_score'
            else:
                return pd.DataFrame()

        # 应用最低分数阈值
        if min_score is not None:
            df = df[df[score_col] >= min_score]

        # 按得分降序排列
        df = df.sort_values(score_col, ascending=False)

        # 选取Top N
        df = df.head(n)

        # 添加排名列
        df['rank'] = range(1, len(df) + 1)

        return df

    # ============================================================
    # 组合方法: 完整选股流程
    # ============================================================

    def run_pipeline(self, strategy_id: str = None,
                     factor_df: pd.DataFrame = None,
                     n: int = 50,
                     min_score: float = None,
                     apply_filter: bool = True) -> Dict:
        """
        完整选股流程: 打分 → 过滤 → 选股

        Args:
            strategy_id: 策略ID
            factor_df: 因子数据
            n: 选股数量
            min_score: 最低分数阈值
            apply_filter: 是否应用硬性过滤

        Returns:
            Dict: 包含以下键:
                - 'selected': 选中的Top N股票DataFrame
                - 'scored': 打分后的完整DataFrame
                - 'filtered': 过滤后的DataFrame
                - 'filter_stats': 过滤统计信息
                - 'strategy_id': 使用的策略ID
                - 'summary': 选股摘要
        """
        sid = (strategy_id or self.strategy_id).upper()

        # Step 1: 策略打分
        scored_df = self.score_stocks(sid, factor_df)

        # Step 2: 硬性过滤
        if apply_filter:
            filtered_df = self.apply_hard_filter(sid, scored_df)
        else:
            filtered_df = scored_df.copy()
            filtered_df['filter_pass'] = True

        # Step 3: 选股Top N
        selected_df = self.select_top_n(filtered_df, n=n, min_score=min_score)

        # 构建摘要
        summary = {
            'strategy_id': sid,
            'strategy_name': self.strategy_info['name'] if self.strategy_info else 'Unknown',
            'total_stocks_input': len(factor_df) if factor_df is not None else 0,
            'stocks_scored': len(scored_df),
            'stocks_passed_filter': int(filtered_df['filter_pass'].sum()) if 'filter_pass' in filtered_df.columns else len(filtered_df),
            'stocks_selected': len(selected_df),
            'score_range': (
                round(selected_df['strategy_score'].min(), 4) if len(selected_df) > 0 else None,
                round(selected_df['strategy_score'].max(), 4) if len(selected_df) > 0 else None,
            ),
        }

        return {
            'selected': selected_df,
            'scored': scored_df,
            'filtered': filtered_df,
            'filter_stats': getattr(self, '_last_filter_stats', {}),
            'strategy_id': sid,
            'summary': summary,
        }

    # ============================================================
    # 辅助方法
    # ============================================================

    def get_filter_stats(self) -> dict:
        """
        获取最近一次过滤的统计信息

        Returns:
            dict: 过滤统计信息
        """
        return getattr(self, '_last_filter_stats', {})

    def multi_strategy_compare(self, strategy_ids: List[str],
                               factor_df: pd.DataFrame,
                               n: int = 20) -> pd.DataFrame:
        """
        多策略对比 - 对同一批股票用多个策略打分，对比选股结果

        Args:
            strategy_ids: 策略ID列表
            factor_df: 因子数据
            n: 每个策略选股数量

        Returns:
            pd.DataFrame: 各策略Top N选股结果对比
        """
        results = []
        for sid in strategy_ids:
            pipeline = self.run_pipeline(sid, factor_df, n=n)
            selected = pipeline['selected']
            if not selected.empty:
                for rank, (idx, row) in enumerate(selected.iterrows(), 1):
                    results.append({
                        'strategy_id': sid,
                        'stock_code': idx,
                        'rank': rank,
                        'strategy_score': row.get('strategy_score', 0),
                    })

        return pd.DataFrame(results)

    def score_distribution(self, score_df: pd.DataFrame,
                           score_col: str = 'strategy_score',
                           bins: int = 10) -> pd.DataFrame:
        """
        分析得分分布

        Args:
            score_df: 打分后的DataFrame
            score_col: 得分列名
            bins: 分桶数

        Returns:
            pd.DataFrame: 得分分布统计
        """
        if score_col not in score_df.columns:
            return pd.DataFrame()

        scores = score_df[score_col]
        dist = pd.cut(scores, bins=bins).value_counts().sort_index()
        dist_df = pd.DataFrame({
            'score_range': dist.index.astype(str),
            'count': dist.values,
            'pct': (dist.values / dist.sum() * 100).round(2),
        })
        return dist_df


if __name__ == '__main__':
    # 测试代码 - 使用模拟数据
    print("=" * 60)
    print("策略打分引擎测试")
    print("=" * 60)

    # 创建模拟因子数据
    np.random.seed(42)
    n_stocks = 200
    stock_codes = [f'{600000+i:06d}.SH' for i in range(n_stocks)]

    mock_factors = pd.DataFrame({
        'V_EP': np.random.uniform(0.02, 0.15, n_stocks),
        'V_BP': np.random.uniform(0.3, 3.0, n_stocks),
        'V_FCFY': np.random.uniform(0.01, 0.10, n_stocks),
        'P_ROE': np.random.uniform(5, 30, n_stocks),
        'P_ROA': np.random.uniform(2, 15, n_stocks),
        'P_GPM': np.random.uniform(10, 60, n_stocks),
        'P_CFOA': np.random.uniform(0.02, 0.15, n_stocks),
        'Q_OCF_NI': np.random.uniform(0.5, 2.0, n_stocks),
        'Q_APR': np.random.uniform(-0.5, 0.5, n_stocks),
        'G_NI_YOY': np.random.uniform(-20, 60, n_stocks),
        'G_REV_YOY': np.random.uniform(-10, 40, n_stocks),
        'E_SUE': np.random.uniform(-2, 3, n_stocks),
        'M_MOM_12_1': np.random.uniform(-0.3, 0.5, n_stocks),
        'M_RS_3M': np.random.uniform(0.5, 1.5, n_stocks),
        'M_RS_6M': np.random.uniform(0.5, 2.0, n_stocks),
        'S_LN_FLOAT': np.random.uniform(18, 25, n_stocks),
        'S_AMT_20_Z': np.random.uniform(-2, 3, n_stocks),
        'C_WINNER_RATE': np.random.uniform(20, 95, n_stocks),
        'L_DEBT_RATIO': np.random.uniform(20, 80, n_stocks),
        'V_DY': np.random.uniform(0, 6, n_stocks),
        'V_PE_TTM': np.random.uniform(5, 100, n_stocks),
        'V_SG': np.random.uniform(0.3, 3.0, n_stocks),
        'M_MA_BULL': np.random.choice([0, 1], n_stocks, p=[0.7, 0.3]),
        'M_VOL_ACCEL': np.random.uniform(0.5, 3.0, n_stocks),
        'M_NEW_HIGH_20': np.random.choice([0, 1], n_stocks, p=[0.8, 0.2]),
        'M_MACD_GOLDEN': np.random.choice([0, 1], n_stocks, p=[0.6, 0.4]),
        'I_FUND_HOLD_PCT': np.random.uniform(0, 30, n_stocks),
        'I_FUND_COUNT': np.random.randint(1, 50, n_stocks),
    }, index=stock_codes)

    # 测试1: 策略A打分
    print("\n--- 测试1: 价值防御型(A)打分 ---")
    engine = StrategyEngine('A')
    scored = engine.score_stocks('A', mock_factors)
    print(f"打分完成: {len(scored)} 只股票")
    print(f"得分范围: [{scored['strategy_score'].min():.4f}, {scored['strategy_score'].max():.4f}]")
    print(f"匹配因子: {scored['matched_factors'].iloc[0]}/{scored['total_factors'].iloc[0]}")
    print(f"\nTop 5:")
    print(scored[['strategy_score', 'V_EP', 'P_ROE', 'V_BP']].head().to_string())

    # 测试2: 策略F硬性过滤
    print("\n--- 测试2: 主升浪启动型(F)硬性过滤 ---")
    filtered = engine.apply_hard_filter('F', mock_factors)
    print(f"过滤结果: {filtered['filter_pass'].sum()}/{len(filtered)} 通过")
    stats = engine.get_filter_stats()
    if stats:
        for k, v in stats['details'].items():
            print(f"  {k}: {v}")

    # 测试3: 选股Top N
    print("\n--- 测试3: 选股Top 10 ---")
    top10 = engine.select_top_n(scored, n=10)
    print(top10[['rank', 'strategy_score']].to_string())

    # 测试4: 完整流程
    print("\n--- 测试4: 完整选股流程(策略S-巴菲特GARP型) ---")
    result = engine.run_pipeline('S', mock_factors, n=10)
    print(f"摘要: {result['summary']}")
    if not result['selected'].empty:
        print(f"\n选中股票:")
        print(result['selected'][['rank', 'strategy_score', 'P_ROE', 'P_GPM']].to_string())

    # 测试5: 多策略对比
    print("\n--- 测试5: 多策略对比(A/B/C/D) ---")
    compare = engine.multi_strategy_compare(['A', 'B', 'C', 'D'], mock_factors, n=5)
    for sid in ['A', 'B', 'C', 'D']:
        subset = compare[compare['strategy_id'] == sid]
        print(f"\n策略{sid} Top 5:")
        for _, row in subset.iterrows():
            print(f"  #{int(row['rank']):2d} {row['stock_code']}: {row['strategy_score']:.4f}")
