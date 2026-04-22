"""
龙头选股策略引擎 - 分层过滤选股系统

选股流程:
Layer 1: F-Score ≥ 7 + 盈利质量 > 0.5 (财务审计)
Layer 2: 换手率 2%-12% + 流通市值 50-300亿 (弹性过滤)
Layer 3: RS 排名 > 80 (动能排名)
Layer 4: Hurst 指数 > 0.6 + Z-Score 0.5~2.0 + OBV趋势 > 1 (趋势纯度)
Layer 5: MACD DIF > 0 + MACD柱变化 > 0 + VWAP偏离 0~1.5% (入场触发)

输出: strong_buy (前20) + watch_list (前50)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from loguru import logger


@dataclass
class DragonSelectionConfig:
    """龙头选股策略配置参数"""
    # Layer 1: 财务审计
    min_f_score: int = 7
    min_earnings_quality: float = 0.5
    
    # Layer 2: 弹性过滤
    min_turnrate: float = 2.0  # %
    max_turnrate: float = 12.0  # %
    min_circ_mcap: float = 50.0  # 亿元
    max_circ_mcap: float = 300.0  # 亿元
    
    # Layer 3: 动能排名
    min_rs_rating: float = 80.0  # 全市场百分位前20%
    
    # Layer 4: 趋势纯度
    min_hurst_exp: float = 0.6
    min_z_score: float = 0.5
    max_z_score: float = 2.0
    min_obv_trend: float = 1.0
    
    # Layer 5: 入场触发
    min_macd_dif: float = 0.0
    min_macd_hist_delta: float = 0.0
    min_vwap_bias: float = 0.0
    max_vwap_bias: float = 0.015  # 1.5%
    
    # 输出
    strong_buy_count: int = 20
    watch_list_count: int = 50


class DragonStockSelector:
    """
    龙头选股器 - 分层过滤选股系统
    
    适用于: 追求高成长、高确定性的趋势龙头股
    市场环境: 牛市、震荡市中的强势行情
    """
    
    def __init__(self, config: DragonSelectionConfig = None):
        self.config = config or DragonSelectionConfig()
        self.layer_stats = {}  # 记录每层的过滤统计
    
    def run_pipeline(self, factors_df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """
        执行分层选股流水线
        
        Args:
            factors_df: 包含所有龙头策略因子的DataFrame
            
        Returns:
            {
                'strong_buy': [{'symbol': '000001.SZ', 'name': '...', 'score': 95, 'layers_passed': 5, ...}, ...],
                'watch_list': [...],
                'stats': {...}
            }
        """
        if factors_df.empty:
            return {'strong_buy': [], 'watch_list': [], 'stats': {}}
        
        df = factors_df.copy()
        total_stocks = len(df)
        self.layer_stats = {'total_input': total_stocks}
        
        logger.info(f"龙头选股开始，初始股票池: {total_stocks}只")
        
        # Layer 1: 财务审计层
        df = self._apply_layer1_financial_audit(df)
        layer1_count = len(df)
        self.layer_stats['layer1_financial'] = layer1_count
        logger.info(f"Layer 1 财务审计: {layer1_count}只通过 (F-Score ≥ {self.config.min_f_score})")
        
        # Layer 2: 弹性过滤层
        df = self._apply_layer2_elastic_filter(df)
        layer2_count = len(df)
        self.layer_stats['layer2_elastic'] = layer2_count
        logger.info(f"Layer 2 弹性过滤: {layer2_count}只通过 (换手率{self.config.min_turnrate}%-{self.config.max_turnrate}%, 市值{self.config.min_circ_mcap}-{self.config.max_circ_mcap}亿)")
        
        # Layer 3: 动能排名层
        df = self._apply_layer3_momentum_rank(df)
        layer3_count = len(df)
        self.layer_stats['layer3_momentum'] = layer3_count
        logger.info(f"Layer 3 动能排名: {layer3_count}只通过 (RS > {self.config.min_rs_rating})")
        
        # Layer 4: 趋势纯度层
        df = self._apply_layer4_trend_purity(df)
        layer4_count = len(df)
        self.layer_stats['layer4_trend'] = layer4_count
        logger.info(f"Layer 4 趋势纯度: {layer4_count}只通过 (Hurst > {self.config.min_hurst_exp}, Z-Score {self.config.min_z_score}~{self.config.max_z_score})")
        
        # Layer 5: 入场触发层
        df_strong = self._apply_layer5_entry_trigger(df)
        layer5_count = len(df_strong)
        self.layer_stats['layer5_entry'] = layer5_count
        logger.info(f"Layer 5 入场触发: {layer5_count}只通过 (MACD共振 + VWAP确认)")
        
        # 生成结果
        results = self._generate_results(df_strong, df)
        results['stats'] = self.layer_stats
        
        logger.info(f"龙头选股完成: 强力买入 {len(results['strong_buy'])}只, 观察名单 {len(results['watch_list'])}只")
        
        return results
    
    def _apply_layer1_financial_audit(self, df: pd.DataFrame) -> pd.DataFrame:
        """Layer 1: 财务审计 - F-Score和盈利质量"""
        mask = (
            (df.get('D_F_SCORE', pd.Series(0, index=df.index)) >= self.config.min_f_score) &
            (df.get('D_EARNINGS_QUALITY', pd.Series(0, index=df.index)) > self.config.min_earnings_quality)
        )
        return df[mask].copy()
    
    def _apply_layer2_elastic_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Layer 2: 弹性过滤 - 换手率和流通市值"""
        turnrate = df.get('D_TURNRATE', pd.Series(0, index=df.index))
        circ_mcap = df.get('D_CIRC_MCAP', pd.Series(0, index=df.index))
        
        mask = (
            turnrate.between(self.config.min_turnrate, self.config.max_turnrate) &
            circ_mcap.between(self.config.min_circ_mcap, self.config.max_circ_mcap)
        )
        return df[mask].copy()
    
    def _apply_layer3_momentum_rank(self, df: pd.DataFrame) -> pd.DataFrame:
        """Layer 3: 动能排名 - RS评分"""
        rs_rating = df.get('D_RS_RATING', pd.Series(0, index=df.index))
        
        mask = rs_rating > self.config.min_rs_rating
        return df[mask].copy()
    
    def _apply_layer4_trend_purity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Layer 4: 趋势纯度 - Hurst + Z-Score + OBV趋势"""
        hurst = df.get('D_HURST_EXP', pd.Series(0, index=df.index))
        z_score = df.get('D_Z_SCORE_60', pd.Series(0, index=df.index))
        obv_trend = df.get('D_OBV_TREND', pd.Series(0, index=df.index))
        
        mask = (
            (hurst > self.config.min_hurst_exp) &
            z_score.between(self.config.min_z_score, self.config.max_z_score) &
            (obv_trend > self.config.min_obv_trend)
        )
        return df[mask].copy()
    
    def _apply_layer5_entry_trigger(self, df: pd.DataFrame) -> pd.DataFrame:
        """Layer 5: 入场触发 - MACD + VWAP"""
        macd_dif = df.get('D_MACD_DIF', pd.Series(0, index=df.index))
        macd_hist_delta = df.get('D_MACD_HIST_DELTA', pd.Series(0, index=df.index))
        vwap_bias = df.get('D_VWAP_BIAS', pd.Series(0, index=df.index))
        
        mask = (
            (macd_dif > self.config.min_macd_dif) &
            (macd_hist_delta > self.config.min_macd_hist_delta) &
            vwap_bias.between(self.config.min_vwap_bias, self.config.max_vwap_bias)
        )
        return df[mask].copy()
    
    def _generate_results(self, df_strong: pd.DataFrame, df_watch: pd.DataFrame) -> Dict:
        """生成选股结果"""
        
        def stock_to_dict(row, layers_passed):
            """将股票数据转换为字典格式"""
            return {
                'symbol': row.name,
                'score': round(row.get('D_RS_RATING', 0), 2),
                'layers_passed': layers_passed,
                'f_score': round(row.get('D_F_SCORE', 0), 0),
                'rs_rating': round(row.get('D_RS_RATING', 0), 2),
                'hurst_exp': round(row.get('D_HURST_EXP', 0), 3),
                'z_score': round(row.get('D_Z_SCORE_60', 0), 3),
                'obv_trend': round(row.get('D_OBV_TREND', 0), 3),
                'macd_dif': round(row.get('D_MACD_DIF', 0), 4),
                'vwap_bias': round(row.get('D_VWAP_BIAS', 0), 4),
                'turnrate': round(row.get('D_TURNRATE', 0), 2),
                'circ_mcap': round(row.get('D_CIRC_MCAP', 0), 2),
            }
        
        # 强力买入列表 (通过所有5层)
        strong_buy = []
        if not df_strong.empty:
            # 按RS评分排序
            df_sorted = df_strong.sort_values('D_RS_RATING', ascending=False)
            top_n = df_sorted.head(self.config.strong_buy_count)
            
            for idx, row in top_n.iterrows():
                strong_buy.append(stock_to_dict(row, layers_passed=5))
        
        # 观察名单 (通过前4层但未通过第5层，或直接通过第5层的其他股票)
        watch_list = []
        if not df_watch.empty:
            # 排除已经在strong_buy中的
            watch_candidates = df_watch[~df_watch.index.isin(df_strong.index)]
            
            if not watch_candidates.empty:
                df_watch_sorted = watch_candidates.sort_values('D_RS_RATING', ascending=False)
                top_watch = df_watch_sorted.head(self.config.watch_list_count)
                
                for idx, row in top_watch.iterrows():
                    watch_list.append(stock_to_dict(row, layers_passed=4))
        
        return {
            'strong_buy': strong_buy,
            'watch_list': watch_list
        }
    
    def get_layer_explanation(self) -> List[Dict]:
        """获取各层选股逻辑说明"""
        return [
            {
                'layer': 1,
                'name': '财务审计层',
                'description': 'F-Score ≥ 7 且 盈利质量 > 0.5',
                'purpose': '筛选财务健康、盈利真实的公司'
            },
            {
                'layer': 2,
                'name': '弹性过滤层',
                'description': f'换手率 {self.config.min_turnrate}%-{self.config.max_turnrate}%, 流通市值 {self.config.min_circ_mcap}-{self.config.max_circ_mcap}亿',
                'purpose': '确保适度流动性和合理市值规模'
            },
            {
                'layer': 3,
                'name': '动能排名层',
                'description': f'RS评分 > {self.config.min_rs_rating} (全市场前20%)',
                'purpose': '选择具有强劲相对强度的龙头股'
            },
            {
                'layer': 4,
                'name': '趋势纯度层',
                'description': f'Hurst指数 > {self.config.min_hurst_exp}, Z-Score在{self.config.min_z_score}~{self.config.max_z_score}之间, OBV趋势 > {self.config.min_obv_trend}',
                'purpose': '确保趋势持续性和量价配合'
            },
            {
                'layer': 5,
                'name': '入场触发层',
                'description': 'MACD DIF > 0 且 MACD柱增加 且 VWAP偏离 0~1.5%',
                'purpose': '捕捉技术共振的入场时机'
            }
        ]


def run_dragon_selection(trade_date: str, conn, stock_pool: list = None) -> Dict:
    """
    便捷函数: 执行龙头选股策略
    
    Args:
        trade_date: 交易日期 YYYYMMDD
        conn: 数据库连接
        stock_pool: 可选的股票池，默认全市场
        
    Returns:
        选股结果字典
    """
    from backend.factors.dragon_factors import compute as compute_dragon_factors
    
    # 计算所有龙头策略因子
    logger.info(f"开始计算龙头策略因子: {trade_date}")
    factors_df = compute_dragon_factors(trade_date, conn, stock_pool)
    
    if factors_df.empty:
        logger.warning("龙头策略因子计算结果为空")
        return {'strong_buy': [], 'watch_list': [], 'stats': {}}
    
    # 执行选股流水线
    selector = DragonStockSelector()
    results = selector.run_pipeline(factors_df)
    
    return results


# ==================== 策略添加到策略组合 ====================

def get_dragon_strategy_config() -> Dict:
    """
    获取龙头策略的配置，用于添加到STRATEGY_COMBOS
    """
    return {
        'id': 'DRAGON',
        'name': '龙头趋势共振型',
        'regime': '牛市主升浪、强势震荡市，追求高确定性趋势龙头股',
        'weights': {
            'D_F_SCORE': 0.15,
            'D_RS_RATING': 0.20,
            'D_HURST_EXP': 0.15,
            'D_Z_SCORE_60': 0.10,
            'D_OBV_TREND': 0.10,
            'D_MACD_DIF': 0.10,
            'D_MACD_HIST_DELTA': 0.10,
            'D_VWAP_BIAS': 0.05,
            'D_EARNINGS_QUALITY': 0.05,
        },
        'hard_filter': {
            'D_F_SCORE': {'op': '>=', 'value': 7},
            'D_RS_RATING': {'op': '>', 'value': 80},
            'D_HURST_EXP': {'op': '>', 'value': 0.6},
            'D_Z_SCORE_60': {'op': 'between', 'value': [0.5, 2.0]},
            'D_TURNRATE': {'op': 'between', 'value': [2.0, 12.0]},
            'D_CIRC_MCAP': {'op': 'between', 'value': [50.0, 300.0]},
        }
    }


if __name__ == '__main__':
    # 测试代码
    print("=" * 60)
    print("龙头选股策略引擎")
    print("=" * 60)
    
    config = DragonSelectionConfig()
    selector = DragonStockSelector(config)
    
    print("\n分层选股逻辑:")
    for layer in selector.get_layer_explanation():
        print(f"\n  Layer {layer['layer']}: {layer['name']}")
        print(f"    条件: {layer['description']}")
        print(f"    目的: {layer['purpose']}")
    
    print(f"\n配置参数:")
    print(f"  F-Score ≥ {config.min_f_score}")
    print(f"  RS排名 > {config.min_rs_rating}")
    print(f"  Hurst指数 > {config.min_hurst_exp}")
    print(f"  换手率 {config.min_turnrate}%-{config.max_turnrate}%")
    print(f"  流通市值 {config.min_circ_mcap}-{config.max_circ_mcap}亿")
