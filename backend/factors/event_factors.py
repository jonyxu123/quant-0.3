""""事件因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'EV_LHB_NET_BUY': {

            'dimension': 'EV_',

            'name': '龙虎榜净买入',

            'direction': 'positive',

            'compute_func': compute_ev_lhb_net_buy

        },

        'EV_BLOCK_TRADE_PREM': {

            'dimension': 'EV_',

            'name': '大宗交易溢价',

            'direction': 'positive',

            'compute_func': compute_ev_block_trade_prem

        },

        'EV_SHHOLDER_INCREASE': {

            'dimension': 'EV_',

            'name': '股东增持事件',

            'direction': 'positive',

            'compute_func': compute_ev_shholder_increase

        },

        'EV_SHHOLDER_DECREASE': {

            'dimension': 'EV_',

            'name': '股东减持事件',

            'direction': 'negative',

            'compute_func': compute_ev_shholder_decrease

        },

        'EV_LOCKUP_RELEASE': {

            'dimension': 'EV_',

            'name': '解禁事件',

            'direction': 'negative',

            'compute_func': compute_ev_lockup_release

        },

        'EV_REPURCHASE_PLAN': {

            'dimension': 'EV_',

            'name': '回购计划事件',

            'direction': 'positive',

            'compute_func': compute_ev_repurchase_plan

        },

        'EV_EARNING_SURPRISE': {

            'dimension': 'EV_',

            'name': '业绩惊喜事件',

            'direction': 'positive',

            'compute_func': compute_ev_earning_surprise

        },

        'EV_CONCEPT_HOT': {

            'dimension': 'EV_',

            'name': '概念热度事件',

            'direction': 'positive',

            'compute_func': compute_ev_concept_hot

        },

        'EV_REPORT_DATE': {

            'dimension': 'EV_',

            'name': '财报发布日期事件',

            'direction': 'bidirectional',

            'compute_func': compute_ev_report_date

        },

        'EV_RESEARCH_REPORT': {

            'dimension': 'EV_',

            'name': '研报覆盖事件',

            'direction': 'positive',

            'compute_func': compute_ev_research_report

        },

        'EV_INSTITUTION_SURVEY': {

            'dimension': 'EV_',

            'name': '机构调研事件',

            'direction': 'positive',

            'compute_func': compute_ev_institution_survey

        },

        'EV_DIVIDEND_INC': {

            'dimension': 'EV_',

            'name': '分红增加事件',

            'direction': 'positive',

            'compute_func': compute_ev_dividend_inc

        },

        'EV_ST_SPLIT': {

            'dimension': 'EV_',

            'name': 'ST/拆股事件',

            'direction': 'negative',

            'compute_func': compute_ev_st_split

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

        # 从forecast取业绩预        
        try:

            fc_df = conn.execute(f""""

                SELECT ts_code, ann_date, predict_net_profit

                FROM forecast

                WHERE ann_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, ann_date DESC

            """).fetchdf()

        except Exception:

            fc_df = pd.DataFrame()

        # 构建数据

        df = pd.DataFrame(index=stock_pool)

        result = pd.DataFrame(index=df.index)

        result['EV_LHB_NET_BUY'] = compute_ev_lhb_net_buy(df)

        result['EV_BLOCK_TRADE_PREM'] = compute_ev_block_trade_prem(df)

        result['EV_SHHOLDER_INCREASE'] = compute_ev_shholder_increase(df)

        result['EV_SHHOLDER_DECREASE'] = compute_ev_shholder_decrease(df)

        result['EV_LOCKUP_RELEASE'] = compute_ev_lockup_release(df)

        result['EV_REPURCHASE_PLAN'] = compute_ev_repurchase_plan(df)

        result['EV_EARNING_SURPRISE'] = compute_ev_earning_surprise(df)

        result['EV_CONCEPT_HOT'] = compute_ev_concept_hot(df)

        result['EV_REPORT_DATE'] = compute_ev_report_date(df)

        result['EV_RESEARCH_REPORT'] = compute_ev_research_report(df)

        result['EV_INSTITUTION_SURVEY'] = compute_ev_institution_survey(df)

        result['EV_DIVIDEND_INC'] = compute_ev_dividend_inc(df)

        result['EV_ST_SPLIT'] = compute_ev_st_split(df)

        return result

    except Exception as e:

        logger.error(f"计算事件因子失败: {e}")

        return pd.DataFrame()

def compute_ev_lhb_net_buy(df, **kwargs):

    """"EV_LHB_NET_BUY = 龙虎榜净买入(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_block_trade_prem(df, **kwargs):

    """"EV_BLOCK_TRADE_PREM = 大宗交易溢价(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_shholder_increase(df, **kwargs):

    """"EV_SHHOLDER_INCREASE = 股东增持事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_shholder_decrease(df, **kwargs):

    """"EV_SHHOLDER_DECREASE = 股东减持事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_lockup_release(df, **kwargs):

    """"EV_LOCKUP_RELEASE = 解禁事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_repurchase_plan(df, **kwargs):

    """"EV_REPURCHASE_PLAN = 回购计划事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_earning_surprise(df, **kwargs):

    """"EV_EARNING_SURPRISE = 业绩惊喜事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_concept_hot(df, **kwargs):

    """"EV_CONCEPT_HOT = 概念热度事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_report_date(df, **kwargs):

    """"EV_REPORT_DATE = 财报发布日期事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_research_report(df, **kwargs):

    """"EV_RESEARCH_REPORT = 研报覆盖事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_institution_survey(df, **kwargs):

    """"EV_INSTITUTION_SURVEY = 机构调研事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_dividend_inc(df, **kwargs):

    """"EV_DIVIDEND_INC = 分红增加事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ev_st_split(df, **kwargs):

    """"EV_ST_SPLIT = ST/拆股事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

