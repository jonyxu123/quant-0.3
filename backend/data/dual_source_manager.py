"""Tushare + AKShare 双源数据管理器"""
import duckdb
import numpy as np
from loguru import logger
from config import DB_PATH, TUSHARE_TOKEN, DIMENSION_PREFIXES


class DualSourceManager:
    """Tushare + AKShare 双源数据管理器
    
    优先级：Tushare > AKShare > 计算衍生
    """
    
    def __init__(self, tushare_token=TUSHARE_TOKEN, db_path=DB_PATH):
        self.tushare_token = tushare_token
        self.db_path = db_path
    
    def get_factor_value(self, factor_id, trade_date, code):
        """统一因子值获取入口，自动路由到正确的数据源"""
        conn = duckdb.connect(self.db_path)
        try:
            value = self._query_factor(conn, factor_id, trade_date, code)
            return value
        finally:
            conn.close()
    
    def _query_factor(self, conn, factor_id, trade_date, code):
        """根据因子ID从对应数据源查询并计算因子值"""
        # 估值因子 (V_*) - 来自 daily_basic / fina_indicator
        if factor_id.startswith('V_'):
            return self._calc_value_factor(conn, factor_id, trade_date, code)
        
        # 盈利/成长/质量因子 (P_/G_/Q_/L_*)
        elif factor_id.startswith(('P_', 'G_', 'Q_', 'L_')):
            return self._calc_fundamental_factor(conn, factor_id, trade_date, code)
        
        # 动量/技术形态因子 (M_/TP_*) - 来自 daily / stk_factor_pro
        elif factor_id.startswith(('M_', 'TP_')):
            return self._calc_technical_factor(conn, factor_id, trade_date, code)
        
        # 技术增强因子 (TECH_*) - 来自 stk_factor_pro
        elif factor_id.startswith('TECH_'):
            return self._calc_tech_enhanced_factor(conn, factor_id, trade_date, code)
        
        # 分析师预期因子 (E_*)
        elif factor_id.startswith('E_'):
            return self._calc_analyst_factor(conn, factor_id, trade_date, code)
        
        # 机构持仓因子 (I_*)
        elif factor_id.startswith('I_'):
            return self._calc_institution_factor(conn, factor_id, trade_date, code)
        
        # 筹码分布因子 (C_*)
        elif factor_id.startswith('C_'):
            return self._calc_chip_factor(conn, factor_id, trade_date, code)
        
        # 事件驱动因子 (EV_*) - 来自 AKShare
        elif factor_id.startswith('EV_'):
            return self._calc_event_factor(conn, factor_id, trade_date, code)
        
        # 增强资金流向因子 (MF_*) - 来自 AKShare
        elif factor_id.startswith('MF_'):
            return self._calc_money_flow_factor(conn, factor_id, trade_date, code)
        
        # 波动率因子 (VOL_*)
        elif factor_id.startswith('VOL_'):
            return self._calc_volatility_factor(conn, factor_id, trade_date, code)
        
        # 情绪因子 (SENT_*)
        elif factor_id.startswith('SENT_'):
            return self._calc_sentiment_factor(conn, factor_id, trade_date, code)
        
        # 日历因子 (CAL_*)
        elif factor_id.startswith('CAL_'):
            return self._calc_calendar_factor(conn, factor_id, trade_date, code)
        
        # 治理因子 (GOV_*)
        elif factor_id.startswith('GOV_'):
            return self._calc_governance_factor(conn, factor_id, trade_date, code)
        
        # 其他v8.0新增维度
        elif factor_id.startswith(('ANALYST_', 'SECTOR_', 'CONCEPT_', 'MICRO_',
                                   'FQ_', 'DIV_', 'CORP_', 'QV_', 'MS_', 'IM_')):
            return self._calc_v8_factor(conn, factor_id, trade_date, code)
        
        return np.nan
    
    # --- 各类因子的具体计算方法（由因子引擎模块实现） ---
    
    def _calc_value_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 value_factors.py 实现
    
    def _calc_fundamental_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 profitability/growth/quality_factors.py 实现
    
    def _calc_technical_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 momentum/tech_pattern_factors.py 实现
    
    def _calc_tech_enhanced_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 tech_enhanced_factors.py 实现
    
    def _calc_analyst_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 analyst_factors.py 实现
    
    def _calc_institution_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 institution_factors.py 实现
    
    def _calc_chip_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 chip_factors.py 实现
    
    def _calc_event_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 event_factors.py 实现
    
    def _calc_money_flow_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 money_flow_factors.py 实现
    
    def _calc_volatility_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 volatility_factors.py 实现
    
    def _calc_sentiment_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 sentiment_factors.py 实现
    
    def _calc_calendar_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 calendar_factors.py 实现
    
    def _calc_governance_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由 governance_factors.py 实现
    
    def _calc_v8_factor(self, conn, factor_id, trade_date, code):
        return np.nan  # 由v8.0新增维度模块实现
