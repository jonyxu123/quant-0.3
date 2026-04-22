"""数据完整性检查"""
import pandas as pd
import duckdb
from datetime import datetime
from loguru import logger
from config import DB_PATH


class DataQualityChecker:
    """数据完整性检查"""
    
    def __init__(self, db_path=DB_PATH):
        self.conn = duckdb.connect(db_path)
    
    def check_daily_completeness(self, trade_date):
        """检查日线数据完整性"""
        total_stocks = pd.read_sql(
            'SELECT COUNT(*) as cnt FROM stock_basic WHERE list_date <= ?',
            self.conn, params=[trade_date]
        ).iloc[0, 0]
        
        synced_stocks = pd.read_sql(
            'SELECT COUNT(DISTINCT ts_code) as cnt FROM daily WHERE trade_date = ?',
            self.conn, params=[trade_date]
        ).iloc[0, 0]
        
        completeness = synced_stocks / total_stocks if total_stocks > 0 else 0
        
        return {
            'trade_date': trade_date,
            'expected': total_stocks,
            'actual': synced_stocks,
            'completeness': completeness,
            'status': 'OK' if completeness > 0.95 else 'WARNING'
        }
    
    def check_missing_dates(self, start_date, end_date):
        """检查缺失的交易日"""
        expected_dates = pd.read_sql(
            'SELECT cal_date FROM trade_cal WHERE is_open=1 AND cal_date >= ? AND cal_date <= ?',
            self.conn, params=[start_date, end_date]
        )['cal_date'].tolist()
        
        actual_dates = pd.read_sql(
            'SELECT DISTINCT trade_date FROM daily WHERE trade_date >= ? AND trade_date <= ?',
            self.conn, params=[start_date, end_date]
        )['trade_date'].tolist()
        
        missing = set(expected_dates) - set(actual_dates)
        
        return {
            'expected_count': len(expected_dates),
            'actual_count': len(actual_dates),
            'missing_dates': sorted(list(missing)),
            'missing_count': len(missing)
        }
    
    def check_stk_factor_pro_completeness(self, trade_date):
        """检查stk_factor_pro技术面因子完整性【v8.0新增】"""
        try:
            synced = pd.read_sql(
                'SELECT COUNT(DISTINCT ts_code) as cnt FROM stk_factor_pro WHERE trade_date = ?',
                self.conn, params=[trade_date]
            ).iloc[0, 0]
            
            daily_count = pd.read_sql(
                'SELECT COUNT(DISTINCT ts_code) as cnt FROM daily WHERE trade_date = ?',
                self.conn, params=[trade_date]
            ).iloc[0, 0]
            
            completeness = synced / daily_count if daily_count > 0 else 0
            return {
                'trade_date': trade_date,
                'daily_stocks': daily_count,
                'stk_factor_pro_stocks': synced,
                'completeness': completeness,
                'status': 'OK' if completeness > 0.90 else 'WARNING'
            }
        except Exception as e:
            return {'trade_date': trade_date, 'status': 'ERROR', 'message': str(e)}
    
    def check_akshare_completeness(self, trade_date):
        """检查AKShare数据完整性"""
        results = {}
        for table in ['lhb_detail', 'margin_detail', 'block_trade', 'north_hold']:
            try:
                count = pd.read_sql(
                    f'SELECT COUNT(*) as cnt FROM {table} WHERE date = ? OR trade_date = ?',
                    self.conn, params=[trade_date, trade_date]
                ).iloc[0, 0]
                results[table] = {'count': count, 'status': 'OK' if count > 0 else 'EMPTY'}
            except Exception:
                results[table] = {'count': 0, 'status': 'MISSING'}
        return results
    
    def generate_report(self, trade_date):
        """生成数据质量报告"""
        report = {
            'daily_check': self.check_daily_completeness(trade_date),
            'stk_factor_pro_check': self.check_stk_factor_pro_completeness(trade_date),
            'akshare_check': self.check_akshare_completeness(trade_date),
            'timestamp': datetime.now().isoformat()
        }
        logger.info(f"数据质量报告: {report}")
        return report
