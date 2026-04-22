"""日历因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

from datetime import datetime, timedelta

def register() -> dict:

    # """注册因子到全局注册"""

    return {

        'CAL_SPRING_RALLY': {

            'dimension': 'CAL_',

            'name': '春季躁动',

            'direction': 'positive',

            'compute_func': compute_cal_spring_rally

        },

        'CAL_SELL_MAY': {

            'dimension': 'CAL_',

            'name': 'Sell in May',

            'direction': 'negative',

            'compute_func': compute_cal_sell_may

        },

        'CAL_YEAR_END_SWITCH': {

            'dimension': 'CAL_',

            'name': '年末调仓',

            'direction': 'bidirectional',

            'compute_func': compute_cal_year_end_switch

        },

        'CAL_EARNING_SEASON': {

            'dimension': 'CAL_',

            'name': '财报',

            'direction': 'bidirectional',

            'compute_func': compute_cal_earning_season

        },

        'CAL_HOLIDAY_EFFECT': {

            'dimension': 'CAL_',

            'name': '节假日效',

            'direction': 'positive',

            'compute_func': compute_cal_holiday_effect

        },

    }


def compute_cal_spring_rally(df, trade_date=None, **kwargs):
    try:
        if trade_date is None:
            return pd.Series(0, index=df.index).astype(float)
        month = int(trade_date[4:6])
        value = 1.0 if month in (1, 2, 3) else 0.0
        return pd.Series(value, index=df.index).astype(float)
    except Exception:
        return pd.Series(np.nan, index=df.index)


def compute_cal_sell_may(df, trade_date=None, **kwargs):
    try:
        if trade_date is None:
            return pd.Series(0, index=df.index).astype(float)
        month = int(trade_date[4:6])
        value = 1.0 if 5 <= month <= 10 else 0.0
        return pd.Series(value, index=df.index).astype(float)
    except Exception:
        return pd.Series(np.nan, index=df.index)


def compute_cal_year_end_switch(df, trade_date=None, **kwargs):
    try:
        if trade_date is None:
            return pd.Series(0, index=df.index).astype(float)
        month = int(trade_date[4:6])
        value = 1.0 if month in (12, 1) else 0.0
        return pd.Series(value, index=df.index).astype(float)
    except Exception:
        return pd.Series(np.nan, index=df.index)


def compute_cal_earning_season(df, trade_date=None, **kwargs):
    try:
        if trade_date is None:
            return pd.Series(0, index=df.index).astype(float)
        month = int(trade_date[4:6])
        value = 1.0 if month in (4, 8, 10) else 0.0
        return pd.Series(value, index=df.index).astype(float)
    except Exception:
        return pd.Series(np.nan, index=df.index)


def compute_cal_holiday_effect(df, trade_date=None, **kwargs):
    try:
        if trade_date is None:
            return pd.Series(0, index=df.index).astype(float)
        month = int(trade_date[4:6])
        day = int(trade_date[6:8])
        is_near_holiday = (
            (month == 1 and day >= 20) or
            (month == 2 and day <= 15) or
            (month == 10 and day <= 7)
        )
        value = 1.0 if is_near_holiday else 0.0
        return pd.Series(value, index=df.index).astype(float)
    except Exception:
        return pd.Series(np.nan, index=df.index)


def compute(trade_date: str, conn, stock_pool: list = None) -> pd.DataFrame:
    try:
        if stock_pool is None:
            stock_pool_df = conn.execute(
                "SELECT ts_code FROM stock_basic WHERE list_date IS NOT NULL"
            ).fetchdf()
            stock_pool = stock_pool_df['ts_code'].tolist()
        if not stock_pool:
            return pd.DataFrame()
        df = pd.DataFrame(index=stock_pool)
        result = pd.DataFrame(index=df.index)
        result['CAL_SPRING_RALLY'] = compute_cal_spring_rally(df, trade_date=trade_date)
        result['CAL_SELL_MAY'] = compute_cal_sell_may(df, trade_date=trade_date)
        result['CAL_YEAR_END_SWITCH'] = compute_cal_year_end_switch(df, trade_date=trade_date)
        result['CAL_EARNING_SEASON'] = compute_cal_earning_season(df, trade_date=trade_date)
        result['CAL_HOLIDAY_EFFECT'] = compute_cal_holiday_effect(df, trade_date=trade_date)
        return result
    except Exception as e:
        logger.error(f"计算日历因子失败: {e}")
        return pd.DataFrame()

