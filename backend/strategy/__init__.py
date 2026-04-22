"""
策略模块 - 30套策略权重配置、市场环境识别、策略打分引擎

模块结构:
- strategy_combos: 30套策略(A-AD)的因子权重配置
- market_regime: 市场环境识别（信号强度评分机制）
- strategy_engine: 策略打分引擎（打分、过滤、选股）
"""

from .strategy_combos import (
    STRATEGY_COMBOS,
    STRATEGY_VERSIONS,
    RISK_PARITY_CONFIG,
    get_strategy_combo,
    list_all_strategies,
    get_strategy_info,
    get_strategies_by_version,
    validate_strategy_weights,
)

from .market_regime import (
    MarketRegime,
    identify_market_regime,
)

from .strategy_engine import (
    StrategyEngine,
)

__all__ = [
    # 策略配置
    'STRATEGY_COMBOS',
    'STRATEGY_VERSIONS',
    'RISK_PARITY_CONFIG',
    'get_strategy_combo',
    'list_all_strategies',
    'get_strategy_info',
    'get_strategies_by_version',
    'validate_strategy_weights',
    # 市场环境识别
    'MarketRegime',
    'identify_market_regime',
    # 策略打分引擎
    'StrategyEngine',
]
