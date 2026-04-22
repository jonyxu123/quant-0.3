"""因子IC动态监控与自动淘汰机制"""
import duckdb
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from loguru import logger
from config import DB_PATH, IC_CULL_THRESHOLD, IC_IR_CULL_THRESHOLD, IC_CULL_PERSIST_MONTHS


class ICMonitor:
    """因子IC动态监控与自动淘汰"""
    
    def __init__(self, db_path=DB_PATH):
        self.conn = duckdb.connect(db_path)
        self.active_factors = set()
        self.ic_history = {}  # {factor_id: [ic_values]}
    
    def calc_ic(self, factor_values: pd.Series, next_returns: pd.Series) -> float:
        """计算IC (Spearman秩相关系数)"""
        valid = factor_values.notna() & next_returns.notna()
        if valid.sum() < 30:
            return np.nan
        try:
            ic, _ = spearmanr(factor_values[valid], next_returns[valid])
            return ic
        except Exception:
            return np.nan
    
    def calc_ic_ir(self, ic_series: pd.Series) -> float:
        """计算IC_IR = IC均值 / IC标准差"""
        std = ic_series.std()
        if std == 0 or np.isnan(std):
            return 0.0
        return ic_series.mean() / std
    
    def monitor_and_cull(self, factor_df: pd.DataFrame,
                         return_df: pd.DataFrame) -> dict:
        """监控并淘汰失效因子
        
        淘汰条件（D8决策）：
        - |IC均值| < 0.02 持续3个月
        - IC_IR < 0.3 持续3个月
        """
        report = {'culled': [], 'active': [], 'warning': []}
        
        for factor_id in factor_df.columns:
            if factor_id not in return_df.columns and 'next_return' not in return_df.columns:
                continue
            
            next_ret = return_df.get('next_return', return_df.get(factor_id, pd.Series()))
            ic = self.calc_ic(factor_df[factor_id], next_ret)
            
            if np.isnan(ic):
                continue
            
            # 记录IC历史
            if factor_id not in self.ic_history:
                self.ic_history[factor_id] = []
            self.ic_history[factor_id].append(ic)
            
            ic_series = pd.Series(self.ic_history[factor_id])
            ic_mean = abs(ic_series.mean())
            ic_ir = self.calc_ic_ir(ic_series)
            
            # 判断淘汰条件
            recent_n = min(len(self.ic_history[factor_id]), IC_CULL_PERSIST_MONTHS)
            recent_ics = ic_series.iloc[-recent_n:]
            
            should_cull = False
            if (recent_ics.abs() < IC_CULL_THRESHOLD).all():
                should_cull = True
            if (recent_ics.std() > 0 and 
                abs(recent_ics.mean() / recent_ics.std()) < IC_IR_CULL_THRESHOLD):
                should_cull = True
            
            if should_cull:
                self.active_factors.discard(factor_id)
                report['culled'].append({
                    'factor_id': factor_id,
                    'ic_mean': ic_mean,
                    'ic_ir': ic_ir,
                    'reason': 'low_ic' if ic_mean < IC_CULL_THRESHOLD else 'low_ic_ir'
                })
            elif ic_mean < IC_CULL_THRESHOLD * 2 or ic_ir < IC_IR_CULL_THRESHOLD * 2:
                report['warning'].append({
                    'factor_id': factor_id,
                    'ic_mean': ic_mean,
                    'ic_ir': ic_ir
                })
            else:
                self.active_factors.add(factor_id)
                report['active'].append({
                    'factor_id': factor_id,
                    'ic_mean': ic_mean,
                    'ic_ir': ic_ir
                })
        
        return report
    
    def generate_report(self) -> dict:
        """生成因子有效性报告"""
        report = {
            'total_factors': len(self.ic_history),
            'active_factors': len(self.active_factors),
            'culled_factors': len(self.ic_history) - len(self.active_factors),
            'factor_details': {}
        }
        
        for factor_id, ics in self.ic_history.items():
            ic_series = pd.Series(ics)
            report['factor_details'][factor_id] = {
                'ic_mean': ic_series.mean(),
                'ic_std': ic_series.std(),
                'ic_ir': self.calc_ic_ir(ic_series),
                'ic_positive_rate': (ic_series > 0).mean(),
                'is_active': factor_id in self.active_factors,
                'sample_count': len(ics)
            }
        
        return report
    
    def get_active_factors(self) -> list:
        """获取当前有效因子列表"""
        return sorted(list(self.active_factors))
    
    def batch_calc_ic(self, factor_df: pd.DataFrame,
                      return_df: pd.DataFrame) -> pd.DataFrame:
        """批量计算所有因子的IC值"""
        results = []
        for factor_id in factor_df.columns:
            next_ret = return_df.get('next_return', pd.Series(dtype=float))
            if next_ret.empty:
                continue
            ic = self.calc_ic(factor_df[factor_id], next_ret)
            results.append({'factor_id': factor_id, 'ic': ic})
        
        return pd.DataFrame(results).set_index('factor_id') if results else pd.DataFrame()
