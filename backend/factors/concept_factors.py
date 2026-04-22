""""概念因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'CONCEPT_HOT_RANK': {

            'dimension': 'CONCEPT_',

            'name': '概念热度排名',

            'direction': 'positive',

            'compute_func': compute_concept_hot_rank

        },

        'CONCEPT_MOM_5D': {

            'dimension': 'CONCEPT_',

            'name': '概念5日动',

            'direction': 'positive',

            'compute_func': compute_concept_mom_5d

        },

        'CONCEPT_COVER_R': {

            'dimension': 'CONCEPT_',

            'name': '概念覆盖比例',

            'direction': 'positive',

            'compute_func': compute_concept_cover_r

        },

        'CONCEPT_TURNOVER': {

            'dimension': 'CONCEPT_',

            'name': '概念换手',

            'direction': 'bidirectional',

            'compute_func': compute_concept_turnover

        },

    }

def compute(trade_date: str, conn, stock_pool: list = None) -> pd.DataFrame:

    """"计算该维度全部因子"""

    try:

        if stock_pool is None:

            stock_pool_df = conn.execute(

                "SELECT ts_code FROM stock_basic WHERE list_date IS NOT NULL"

            ).fetchdf()

            stock_pool = stock_pool_df['ts_code'].tolist()

        if not stock_pool:

            return pd.DataFrame()

        pool_str = "','".join(stock_pool)

        # 从concept取概念数        
        try:

            # concept_detail 字段：代码(6位无后缀)、名称、concept_name
            # 通过 stock_basic 做 JOIN 转换为 ts_code
            concept_df = conn.execute(f"""

                SELECT sb.ts_code, cd.concept_name

                FROM concept_detail cd

                JOIN stock_basic sb ON sb.ts_code LIKE (cd."代码" || '%')

                WHERE sb.ts_code IN ('{pool_str}')

            """).fetchdf()

        except Exception:

            concept_df = pd.DataFrame()

        try:

            idx_df = conn.execute(f"""

                SELECT ts_code, trade_date, close, pct_chg

                FROM index_daily

                WHERE trade_date <= '{trade_date}'

                ORDER BY ts_code, trade_date DESC

            """).fetchdf()

        except Exception:

            idx_df = pd.DataFrame()

        # 构建数据

        df = pd.DataFrame(index=stock_pool)

        # 计算概念统计

        concept_stats = _compute_concept_stats(concept_df, idx_df, df.index, trade_date)

        result = pd.DataFrame(index=df.index)

        result['CONCEPT_HOT_RANK'] = compute_concept_hot_rank(df, concept_stats=concept_stats)

        result['CONCEPT_MOM_5D'] = compute_concept_mom_5d(df, concept_stats=concept_stats)

        result['CONCEPT_COVER_R'] = compute_concept_cover_r(df, concept_stats=concept_stats)

        result['CONCEPT_TURNOVER'] = compute_concept_turnover(df, concept_stats=concept_stats)

        return result

    except Exception as e:

        logger.error(f"计算概念因子失败: {e}")

        return pd.DataFrame()

def _compute_concept_stats(concept_df: pd.DataFrame, idx_df: pd.DataFrame, index, trade_date: str) -> dict:

    """"计算概念统计"""

    stats = {}

    try:

        if concept_df.empty:

            return stats

        for ts_code in index:

            concepts = concept_df[concept_df['ts_code'] == ts_code]

            stats[ts_code] = {

                'count': len(concepts),

                'hot_rank': 0,

                'mom_5d': 0,

                'turnover': 0,

            }

    except Exception as e:

        logger.warning(f"计算概念统计失败: {e}")

    return stats

def compute_concept_hot_rank(df, concept_stats=None, **kwargs):

    """"CONCEPT_HOT_RANK = 概念热度排名"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if concept_stats:

            for ts_code in df.index:

                if ts_code in concept_stats:

                    result.loc[ts_code] = concept_stats[ts_code]['hot_rank']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_concept_mom_5d(df, concept_stats=None, **kwargs):

    """"CONCEPT_MOM_5D = 概念5日动"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if concept_stats:

            for ts_code in df.index:

                if ts_code in concept_stats:

                    result.loc[ts_code] = concept_stats[ts_code]['mom_5d']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_concept_cover_r(df, concept_stats=None, **kwargs):

    # CONCEPT_COVER_R概念覆盖比例简

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if concept_stats:

            max_count = max(s['count'] for s in concept_stats.values()) if concept_stats else 1

            if max_count == 0:

                max_count = 1

            for ts_code in df.index:

                if ts_code in concept_stats:

                    result.loc[ts_code] = concept_stats[ts_code]['count'] / max_count

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_concept_turnover(df, concept_stats=None, **kwargs):

    """"CONCEPT_TURNOVER = 概念换手"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if concept_stats:

            for ts_code in df.index:

                if ts_code in concept_stats:

                    result.loc[ts_code] = concept_stats[ts_code]['turnover']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

