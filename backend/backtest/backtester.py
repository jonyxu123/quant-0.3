"""回测框架与绩效评估"""
import duckdb
import pandas as pd
import numpy as np
from loguru import logger
from config import DB_PATH, DEFAULT_TOP_N


class Backtester:
    """策略回测框架"""
    
    def __init__(self, db_path=DB_PATH):
        self.conn = duckdb.connect(db_path)
    
    def run_backtest(self, strategy_id: str, start_date: str, end_date: str,
                     rebalance_freq: str = 'M', top_n: int = DEFAULT_TOP_N) -> dict:
        """运行回测
        
        Args:
            strategy_id: 策略ID (A-AD)
            start_date: 回测开始日期
            end_date: 回测结束日期
            rebalance_freq: 调仓频率 ('W'周/'M'月/'Q'季)
            top_n: 每期选股数量
        """
        from backend.factors.factor_engine import FactorEngine
        from backend.preprocessing.preprocess import preprocess_single_date
        from backend.strategy.strategy_engine import StrategyEngine
        
        engine = FactorEngine(self.db_path)
        strategy = StrategyEngine()
        
        # 获取交易日序列
        trade_dates = self._get_rebalance_dates(start_date, end_date, rebalance_freq)
        
        portfolio_returns = []
        holdings_history = []
        
        for i, trade_date in enumerate(trade_dates):
            try:
                # 计算因子
                factor_df = engine.compute_all(trade_date)
                if factor_df.empty:
                    continue
                
                # 策略打分+选股
                scored_df = strategy.score_stocks(strategy_id, factor_df)
                filtered_df = strategy.apply_hard_filter(strategy_id, scored_df)
                selected = strategy.select_top_n(strategy_id, filtered_df, n=top_n)
                
                if selected.empty:
                    continue
                
                # 计算下期收益
                next_date = trade_dates[i + 1] if i + 1 < len(trade_dates) else None
                if next_date:
                    returns = self._calc_portfolio_return(
                        selected.index.tolist(), trade_date, next_date
                    )
                    portfolio_returns.append(returns)
                    holdings_history.append({
                        'trade_date': trade_date,
                        'holdings': selected.index.tolist(),
                        'score_mean': selected.get('strategy_score', pd.Series([0])).mean()
                    })
                
            except Exception as e:
                logger.error(f"回测{trade_date}失败: {e}")
                continue
        
        # 计算绩效指标
        if portfolio_returns:
            returns_series = pd.Series(portfolio_returns)
            performance = self._calc_performance(returns_series)
        else:
            performance = {}
        
        return {
            'strategy_id': strategy_id,
            'start_date': start_date,
            'end_date': end_date,
            'rebalance_freq': rebalance_freq,
            'top_n': top_n,
            'performance': performance,
            'holdings_history': holdings_history,
            'total_periods': len(trade_dates),
            'valid_periods': len(portfolio_returns)
        }
    
    def _get_rebalance_dates(self, start_date, end_date, freq='M'):
        """获取调仓日期序列"""
        try:
            dates_df = pd.read_sql(
                'SELECT cal_date FROM trade_cal WHERE is_open=1 AND cal_date >= ? AND cal_date <= ? ORDER BY cal_date',
                self.conn, params=[start_date, end_date]
            )
            all_dates = dates_df['cal_date'].tolist()
        except Exception:
            return []
        
        if freq == 'W':
            return all_dates[::5]
        elif freq == 'M':
            monthly = []
            current_month = None
            for d in all_dates:
                month = d[:6]
                if month != current_month:
                    monthly.append(d)
                    current_month = month
            return monthly
        elif freq == 'Q':
            quarterly = []
            current_q = None
            for d in all_dates:
                q = d[:4] + str((int(d[4:6]) - 1) // 3 + 1)
                if q != current_q:
                    quarterly.append(d)
                    current_q = q
            return quarterly
        return all_dates
    
    def _calc_portfolio_return(self, holdings, start_date, end_date):
        """计算组合收益"""
        try:
            placeholders = ','.join([f"'{h}'" for h in holdings])
            df = pd.read_sql(
                f"SELECT ts_code, pct_chg FROM daily WHERE trade_date = '{end_date}' AND ts_code IN ({placeholders})",
                self.conn
            )
            if df.empty:
                return 0.0
            return df['pct_chg'].mean()
        except Exception:
            return 0.0
    
    def _calc_performance(self, returns: pd.Series) -> dict:
        """计算绩效指标"""
        if returns.empty:
            return {}
        
        cumulative = (1 + returns / 100).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        
        annual_factor = 252 / max(1, len(returns))
        
        annual_return = (cumulative.iloc[-1] ** annual_factor - 1) * 100 if cumulative.iloc[-1] > 0 else 0
        annual_vol = returns.std() * np.sqrt(annual_factor)
        sharpe = annual_return / annual_vol if annual_vol > 0 else 0
        max_drawdown = drawdown.min() * 100
        calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        win_rate = (returns > 0).mean() * 100
        avg_win = returns[returns > 0].mean() if (returns > 0).any() else 0
        avg_loss = abs(returns[returns < 0].mean()) if (returns < 0).any() else 0
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
        
        return {
            'annual_return': round(annual_return, 2),
            'annual_volatility': round(annual_vol, 2),
            'sharpe_ratio': round(sharpe, 2),
            'max_drawdown': round(max_drawdown, 2),
            'calmar_ratio': round(calmar, 2),
            'win_rate': round(win_rate, 2),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'total_periods': len(returns),
            'cumulative_return': round((cumulative.iloc[-1] - 1) * 100, 2)
        }
    
    def generate_report(self, strategy_id: str, result: dict) -> str:
        """生成回测报告"""
        perf = result.get('performance', {})
        lines = [
            f"回测报告 - 策略{strategy_id}",
            f"回测区间: {result['start_date']} ~ {result['end_date']}",
            f"调仓频率: {result['rebalance_freq']}  选股数量: {result['top_n']}",
            "=" * 50,
            f"年化收益率: {perf.get('annual_return', 0):.2f}%",
            f"年化波动率: {perf.get('annual_volatility', 0):.2f}%",
            f"夏普比率:   {perf.get('sharpe_ratio', 0):.2f}",
            f"最大回撤:   {perf.get('max_drawdown', 0):.2f}%",
            f"卡尔马比率: {perf.get('calmar_ratio', 0):.2f}",
            f"胜率:       {perf.get('win_rate', 0):.2f}%",
            f"盈亏比:     {perf.get('profit_loss_ratio', 0):.2f}",
            f"累计收益:   {perf.get('cumulative_return', 0):.2f}%",
        ]
        return '\n'.join(lines)
