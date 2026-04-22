"""T+0 超跌反弹高弹性选股策略 (V-Hunter 猎物池)

核心定位：为 V-Hunter 系统提供超跌反弹的候选股票池，适合日内 T+0 滚动操作。

筛选条件：
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
from dataclasses import dataclass
from typing import Dict, List, Tuple
from loguru import logger


@dataclass
class T0ReboundConfig:
    """T+0 反弹策略配置参数"""
    
    # 技术筛选条件
    max_ma60_bias: float = -0.15  # MA60 偏差上限（必须小于此值）
    min_circ_mv: float = 30e8  # 最小流通市值 30亿
    max_circ_mv: float = 200e8  # 最大流通市值 200亿
    min_turnover_rank: float = 0.7  # 20日换手率排名至少前30%
    
    # 基本面筛选
    min_ocf_to_ni: float = 0.5  # 经营现金流/净利润最小 0.5
    require_profitable: bool = True  # 要求至少一年盈利
    
    # 输出配置
    max_picks: int = 30  # 最大选出数量
    sort_by: str = 'ma60_bias'  # 按超跌程度排序


class T0ReboundSelector:
    """T+0 超跌反弹选股器 - 分层过滤选股系统"""
    
    def __init__(self, config: T0ReboundConfig = None):
        self.config = config or T0ReboundConfig()
        self.layer_stats = {}
    
    def run_pipeline(self, factors_df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """执行选股流水线
        
        Args:
            factors_df: 包含所有因子的 DataFrame
            
        Returns:
            {
                'high_elasticity': [...],  # 高弹性备选池
                'stats': {...}  # 各层过滤统计
            }
        """
        df = factors_df.copy()
        initial_count = len(df)
        
        # === 第一层：基础清洗 ===
        df = self._layer_basic_clean(df)
        layer1_count = len(df)
        
        # === 第二层：技术面筛选 ===
        df = self._layer_tech_screen(df)
        layer2_count = len(df)
        
        # === 第三层：基本面排雷 ===
        df = self._layer_fundamental_screen(df)
        layer3_count = len(df)
        
        # === 第四层：流动性筛选 ===
        df = self._layer_liquidity_screen(df)
        layer4_count = len(df)
        
        # === 第五层：排序输出 ===
        result_df = self._layer_sort_output(df)
        
        # 构建统计信息
        stats = {
            'initial': initial_count,
            'layer1_basic_clean': layer1_count,
            'layer2_tech_screen': layer2_count,
            'layer3_fundamental': layer3_count,
            'layer4_liquidity': layer4_count,
            'final': len(result_df),
            'retention_rate': f"{len(result_df)/initial_count*100:.1f}%" if initial_count > 0 else "0%"
        }
        
        # 转换为列表格式
        high_elasticity = self._format_results(result_df)
        
        return {
            'high_elasticity': high_elasticity,
            'stats': stats
        }
    
    def _layer_basic_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """基础清洗：剔除ST、停牌、次新股"""
        initial = len(df)
        
        # 剔除 ST 股
        if 'T0_IS_ST' in df.columns:
            df = df[df['T0_IS_ST'] == 0]
        
        # 剔除停牌（假设 turnover_rate 为0或NaN表示停牌）
        if 'turnover_rate' in df.columns:
            df = df[df['turnover_rate'] > 0]
        
        logger.info(f"基础清洗: {initial} -> {len(df)} 只")
        return df
    
    def _layer_tech_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """技术面筛选：极限超跌 + MACD水下金叉"""
        initial = len(df)
        
        # 1. 极限超跌: MA60 偏差 < -15%
        if 'T0_MA60_BIAS' in df.columns:
            df = df[df['T0_MA60_BIAS'] < self.config.max_ma60_bias]
        
        # 2. MACD 水下金叉: T0_MACD_UNDERWATER > 0
        if 'T0_MACD_UNDERWATER' in df.columns:
            df = df[df['T0_MACD_UNDERWATER'] > 0]
        
        logger.info(f"技术面筛选: {initial} -> {len(df)} 只")
        return df
    
    def _layer_fundamental_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """基本面排雷：现金流质量 + 盈利要求"""
        initial = len(df)
        
        # 3. 经营现金流/净利润 > 0.5 (使用 Q_OCF_NI 或 T0_OCF_NI)
        ocf_col = None
        for col in ['Q_OCF_NI', 'T0_OCF_NI']:
            if col in df.columns:
                ocf_col = col
                break
        
        if ocf_col:
            df = df[df[ocf_col] > self.config.min_ocf_to_ni]
        
        # 4. 盈利要求
        if self.config.require_profitable and 'T0_PROFITABLE' in df.columns:
            df = df[df['T0_PROFITABLE'] == 1]
        
        logger.info(f"基本面筛选: {initial} -> {len(df)} 只")
        return df
    
    def _layer_liquidity_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """流动性筛选：市值 + 换手率排名"""
        initial = len(df)
        
        # 5. 流通市值 30亿 ~ 200亿
        if 'circ_mv' in df.columns:
            df = df[df['circ_mv'].between(
                self.config.min_circ_mv, 
                self.config.max_circ_mv
            )]
        
        # 6. 20日换手率排名 > 70%
        if 'T0_TURNOVER_RANK' in df.columns:
            df = df[df['T0_TURNOVER_RANK'] > self.config.min_turnover_rank]
        
        logger.info(f"流动性筛选: {initial} -> {len(df)} 只")
        return df
    
    def _layer_sort_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """排序输出：按超跌程度"""
        # 按 MA60 偏差排序（越超跌越优先）
        if 'T0_MA60_BIAS' in df.columns:
            df = df.sort_values('T0_MA60_BIAS', ascending=True)
        
        # 限制最大数量
        if len(df) > self.config.max_picks:
            df = df.head(self.config.max_picks)
        
        return df
    
    def _format_results(self, df: pd.DataFrame) -> List[Dict]:
        """格式化结果为字典列表"""
        results = []
        for idx, row in df.iterrows():
            stock = {
                'ts_code': idx,
                'ma60_bias': round(row.get('T0_MA60_BIAS', 0), 4) if pd.notna(row.get('T0_MA60_BIAS')) else None,
                'macd_underwater': round(row.get('T0_MACD_UNDERWATER', 0), 4) if pd.notna(row.get('T0_MACD_UNDERWATER')) else None,
                'turnover_rank': round(row.get('T0_TURNOVER_RANK', 0), 4) if pd.notna(row.get('T0_TURNOVER_RANK')) else None,
                'circ_mv': round(row.get('circ_mv', 0) / 1e8, 2) if pd.notna(row.get('circ_mv')) else None,  # 转为亿元
            }
            results.append(stock)
        return results
    
    def get_layer_explanation(self) -> List[Dict]:
        """获取各层筛选逻辑说明"""
        return [
            {
                'layer': 1,
                'name': '基础清洗',
                'rules': [
                    '剔除 ST/*ST 股票',
                    '剔除停牌股票',
                ]
            },
            {
                'layer': 2,
                'name': '技术面筛选',
                'rules': [
                    f'MA60 偏差 < {self.config.max_ma60_bias*100:.0f}% (极限超跌)',
                    'MACD 水下金叉信号 (dif>dea 且 dif<0)',
                ]
            },
            {
                'layer': 3,
                'name': '基本面排雷',
                'rules': [
                    f'经营现金流/净利润 > {self.config.min_ocf_to_ni}',
                    '近两年至少一年盈利',
                ]
            },
            {
                'layer': 4,
                'name': '流动性筛选',
                'rules': [
                    f'流通市值 {self.config.min_circ_mv/1e8:.0f}亿 ~ {self.config.max_circ_mv/1e8:.0f}亿',
                    f'20日换手率排名 > {self.config.min_turnover_rank*100:.0f}% (前30%)',
                ]
            },
            {
                'layer': 5,
                'name': '排序输出',
                'rules': [
                    '按超跌程度排序 (MA60偏差越小越优先)',
                    f'最多输出 {self.config.max_picks} 只股票',
                ]
            }
        ]


def run_t0_rebound_selection(trade_date: str, conn, stock_pool: list = None) -> Dict:
    """便捷函数: 执行 T+0 反弹选股策略
    
    Args:
        trade_date: 交易日期
        conn: DuckDB 连接
        stock_pool: 可选的股票池
        
    Returns:
        选股结果字典
    """
    from backend.factors.t0_rebound_factors import compute as compute_t0_factors
    
    # 计算所有 T0 因子
    logger.info(f"开始计算 T0 反弹因子: {trade_date}")
    factors_df = compute_t0_factors(trade_date, conn, stock_pool)
    
    if factors_df.empty:
        logger.warning("T0 反弹因子计算结果为空")
        return {'high_elasticity': [], 'stats': {}}
    
    # 执行选股流水线
    selector = T0ReboundSelector()
    results = selector.run_pipeline(factors_df)
    
    return results


def get_t0_rebound_strategy_config() -> Dict:
    """获取 T0 反弹策略的配置，用于添加到 STRATEGY_COMBOS
    
    Returns:
        策略配置字典
    """
    return {
        'id': 'T0_REBOUND',
        'name': 'V-Hunter T+0高弹性',
        'regime': '超跌反弹、日内T+0操作，适合高波动市场环境',
        'weights': {
            'T0_MA60_BIAS': 0.30,      # 超跌程度最重要
            'T0_MACD_UNDERWATER': 0.25,  # 水下金叉确认
            'T0_TURNOVER_RANK': 0.25,  # 流动性保障
            'Q_OCF_NI': 0.10,          # 基本面质量
            'T0_PROFITABLE': 0.10,     # 盈利要求
        },
        'filters': {
            'max_ma60_bias': -0.15,
            'min_circ_mv': 30e8,
            'max_circ_mv': 200e8,
            'min_turnover_rank': 0.7,
            'min_ocf_to_ni': 0.5,
            'exclude_st': True,
        },
        'description': '超跌反弹高波动T+0策略，筛选极限超跌、MACD水下金叉、高流动性的小市值股票'
    }


# ==================== 策略添加到策略组合 ====================

def register_strategy():
    """注册 T0 反弹策略到策略组合"""
    from backend.strategy.strategy_combos import STRATEGY_COMBOS, STRATEGY_VERSIONS
    
    config = get_t0_rebound_strategy_config()
    strategy_id = config['id']
    
    # 添加到 STRATEGY_COMBOS
    STRATEGY_COMBOS[strategy_id] = config
    
    # 添加到最新版本
    if 'v9.0' in STRATEGY_VERSIONS:
        if strategy_id not in STRATEGY_VERSIONS['v9.0']:
            STRATEGY_VERSIONS['v9.0'].append(strategy_id)
    
    logger.info(f"T0 反弹策略已注册: {strategy_id}")
    return config


if __name__ == '__main__':
    print("V-Hunter T+0 高弹性选股策略")
    print("=" * 60)
    
    config = T0ReboundConfig()
    selector = T0ReboundSelector(config)
    
    print("\n分层选股逻辑:")
    for layer in selector.get_layer_explanation():
        print(f"\n第{layer['layer']}层: {layer['name']}")
        for rule in layer['rules']:
            print(f"  - {rule}")
    
    print("\n策略配置:")
    print(f"  MA60偏差阈值: {config.max_ma60_bias*100:.0f}%")
    print(f"  流通市值: {config.min_circ_mv/1e8:.0f}亿 ~ {config.max_circ_mv/1e8:.0f}亿")
    print(f"  换手率排名: > {config.min_turnover_rank*100:.0f}%")
    print(f"  最大输出: {config.max_picks}只")
