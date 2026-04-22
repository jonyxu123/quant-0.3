"""因子预处理模块 - 分块截面预处理（解决OOM问题）

OOM风险：5000股×2430日×208因子×8字节≈19.2GB，加Pandas开销约29GB，16G/32G必OOM。
解决方案：预处理三步操作均为截面操作，按trade_date逐期从DuckDB读取→处理→写回，单期仅~8MB。
"""
import duckdb
import numpy as np
import pandas as pd
import time
from loguru import logger
from config import DB_PATH, DIMENSION_PREFIXES


def _identify_factor_cols(columns):
    """识别因子列（以维度前缀开头的数值列）"""
    return [c for c in columns if any(c.startswith(p) for p in DIMENSION_PREFIXES)]


def _fillna_cross_section(df: pd.DataFrame, factor_cols: list) -> pd.DataFrame:
    """截面缺失值填充：按行业内中位数填充"""
    for col in factor_cols:
        df[col] = df.groupby('industry')[col].transform(
            lambda x: x.fillna(x.median())
        )
    return df


def _winsorize_mad_cross_section(df: pd.DataFrame, factor_cols: list, n: float = 3.0) -> pd.DataFrame:
    """截面MAD去极值"""
    for col in factor_cols:
        series = df[col]
        median = series.median()
        mad = (series - median).abs().median()
        upper = median + n * 1.4826 * mad
        lower = median - n * 1.4826 * mad
        df[col] = series.clip(lower, upper)
    return df


def _zscore_cross_section(df: pd.DataFrame, factor_cols: list) -> pd.DataFrame:
    """截面Z-Score标准化（单期调用时整表即一个截面）"""
    for col in factor_cols:
        mean = df[col].mean()
        std = df[col].std()
        df[col] = (df[col] - mean) / (std + 1e-8)
    return df


def preprocess_single_date(conn, trade_date: str, table: str = 'factor_raw',
                           output_table: str = 'factor_preprocessed') -> int:
    """单期截面预处理（内存峰值：5000行×208列≈8MB，不会OOM）"""
    df = conn.execute(
        f"SELECT * FROM {table} WHERE trade_date = '{trade_date}'"
    ).fetchdf()
    
    if df.empty:
        return 0
    
    factor_cols = _identify_factor_cols(df.columns)
    
    df = _fillna_cross_section(df, factor_cols)
    df = _winsorize_mad_cross_section(df, factor_cols)
    df = _zscore_cross_section(df, factor_cols)
    
    df.to_sql(output_table, conn, if_exists='append', index=False)
    return len(df)


def preprocess_factors_chunked(conn, start_date: str = None, end_date: str = None,
                               table: str = 'factor_raw',
                               output_table: str = 'factor_preprocessed',
                               chunk_size: int = 50) -> dict:
    """分块截面预处理主入口（解决OOM问题）
    
    按交易日逐期处理，每期仅加载~8MB到内存。
    支持断点续传：检查output_table已有日期，跳过已处理期。
    """
    t0 = time.time()
    
    where = []
    if start_date:
        where.append(f"trade_date >= '{start_date}'")
    if end_date:
        where.append(f"trade_date <= '{end_date}'")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    
    dates_df = conn.execute(
        f"SELECT DISTINCT trade_date FROM {table} {where_clause} ORDER BY trade_date"
    ).fetchdf()
    trade_dates = dates_df['trade_date'].tolist()
    
    # 断点续传
    try:
        done_df = conn.execute(
            f"SELECT DISTINCT trade_date FROM {output_table}"
        ).fetchdf()
        done_dates = set(done_df['trade_date'].tolist())
        trade_dates = [d for d in trade_dates if d not in done_dates]
        logger.info(f"跳过已处理{len(done_dates)}期，剩余{len(trade_dates)}期")
    except Exception:
        pass
    
    total_rows = 0
    chunk_rows = 0
    for i, trade_date in enumerate(trade_dates):
        n = preprocess_single_date(conn, trade_date, table, output_table)
        total_rows += n
        chunk_rows += n
        if (i + 1) % chunk_size == 0:
            logger.info(f"已处理 {i+1}/{len(trade_dates)} 期，本块{chunk_rows}行")
            chunk_rows = 0
    
    elapsed = time.time() - t0
    logger.info(f"预处理完成: {len(trade_dates)}期, {total_rows}行, 耗时{elapsed:.1f}秒")
    return {'total_dates': len(trade_dates), 'total_rows': total_rows, 'elapsed_sec': elapsed}
