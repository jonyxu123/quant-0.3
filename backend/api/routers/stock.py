"""股票详情路由。"""

from __future__ import annotations

import os
import sys

from fastapi import APIRouter, HTTPException, Query
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import DB_PATH
from backend.data.duckdb_manager import open_duckdb_conn

router = APIRouter()


def _get_conn():
    return open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)


def _df_to_records(df):
    if df is None or df.empty:
        return []
    return df.where(df.notna(), None).to_dict(orient="records")


@router.get("/search")
def search_stocks(
    q: str = Query(..., min_length=1, description="搜索关键词：代码/名称/拼音"),
    limit: int = Query(20, ge=1, le=100),
):
    """按代码、名称、拼音模糊搜索股票。"""
    conn = _get_conn()
    try:
        kw = q.strip().upper()
        df = conn.execute(
            """
            SELECT ts_code, symbol, name, industry, area, market
            FROM stock_basic
            WHERE UPPER(ts_code) LIKE ?
               OR UPPER(symbol)  LIKE ?
               OR name           LIKE ?
               OR UPPER(cnspell) LIKE ?
            ORDER BY
              CASE
                WHEN UPPER(symbol) = ?       THEN 0
                WHEN UPPER(ts_code) = ?      THEN 0
                WHEN name = ?                THEN 1
                WHEN UPPER(symbol) LIKE ?    THEN 2
                WHEN name LIKE ?             THEN 3
                ELSE 9
              END,
              ts_code
            LIMIT ?
            """,
            [
                f"%{kw}%",
                f"%{kw}%",
                f"%{q}%",
                f"{kw}%",
                kw,
                kw,
                q,
                f"{kw}%",
                f"{q}%",
                limit,
            ],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/basic/{ts_code}")
def get_stock_basic(ts_code: str):
    """获取股票基础信息、公司信息和最新行情。"""
    conn = _get_conn()
    try:
        basic = conn.execute(
            """
            SELECT ts_code, symbol, name, area, industry, market, list_date, act_name
            FROM stock_basic
            WHERE ts_code = ?
            """,
            [ts_code],
        ).fetchdf()
        if basic.empty:
            raise HTTPException(status_code=404, detail="股票不存在")

        company = conn.execute(
            """
            SELECT com_name, chairman, manager, secretary, reg_capital, setup_date,
                   province, city, employees, website, introduction, main_business, business_scope
            FROM stock_company
            WHERE ts_code = ?
            """,
            [ts_code],
        ).fetchdf()

        latest = conn.execute(
            """
            SELECT trade_date, close, pe, pe_ttm, pb, ps_ttm, dv_ttm,
                   total_share, float_share, free_share, total_mv, circ_mv,
                   turnover_rate, turnover_rate_f, volume_ratio
            FROM daily_basic
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [ts_code],
        ).fetchdf()

        daily_latest = conn.execute(
            """
            SELECT trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
            FROM daily
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [ts_code],
        ).fetchdf()

        return {
            "basic": _df_to_records(basic)[0],
            "company": _df_to_records(company)[0] if not company.empty else None,
            "latest_basic": _df_to_records(latest)[0] if not latest.empty else None,
            "latest_daily": _df_to_records(daily_latest)[0] if not daily_latest.empty else None,
        }
    finally:
        conn.close()


@router.get("/chip/{ts_code}")
def get_stock_chip(
    ts_code: str,
    days: int = Query(120, ge=20, le=500, description="数据天数"),
):
    """获取筹码分布数据。"""
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT trade_date,
                   his_low, his_high,
                   cost_5pct, cost_15pct, cost_50pct,
                   cost_85pct, cost_95pct,
                   weight_avg, winner_rate
            FROM cyq_perf
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, days],
        ).fetchdf()
        if not df.empty:
            df = df.sort_values("trade_date")
        return {"ts_code": ts_code, "count": len(df), "data": _df_to_records(df)}
    except Exception as e:
        logger.error(f"获取筹码分布失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取筹码分布失败: {e}")
    finally:
        conn.close()


@router.get("/kline/{ts_code}")
def get_stock_kline(
    ts_code: str,
    period: str = Query("daily", description="周期: daily / weekly / monthly"),
    days: int = Query(250, ge=20, le=1000, description="K线数量"),
    fq: str = Query("qfq", description="复权类型: none / qfq / hfq"),
):
    """获取 K 线数据。"""
    conn = _get_conn()
    try:
        if period == "weekly":
            sql = """
                SELECT trade_date, open, high, low, close, vol, amount, pct_chg
                FROM weekly WHERE ts_code = ?
                ORDER BY trade_date DESC LIMIT ?
            """
            params = [ts_code, days]
        elif period == "monthly":
            sql = """
                SELECT trade_date, open, high, low, close, vol, amount, pct_chg
                FROM monthly WHERE ts_code = ?
                ORDER BY trade_date DESC LIMIT ?
            """
            params = [ts_code, days]
        else:
            ps = "_bfq" if fq == "none" else f"_{fq}"
            ind = ps
            sql = f"""
                SELECT trade_date,
                       open{ps} as open,
                       high{ps} as high,
                       low{ps} as low,
                       close{ps} as close,
                       vol, amount, pct_chg,
                       ma{ind}_5 as ma5, ma{ind}_10 as ma10,
                       ma{ind}_20 as ma20, ma{ind}_30 as ma30,
                       ma{ind}_60 as ma60,
                       macd_dif{ind} as macd_dif,
                       macd_dea{ind} as macd_dea,
                       macd{ind} as macd,
                       kdj_k{ind} as kdj_k,
                       kdj_d{ind} as kdj_d,
                       kdj{ind} as kdj_j,
                       boll_upper{ind} as boll_upper,
                       boll_mid{ind} as boll_mid,
                       boll_lower{ind} as boll_lower,
                       rsi{ind}_6 as rsi6,
                       rsi{ind}_12 as rsi12,
                       rsi{ind}_24 as rsi24
                FROM stk_factor_pro
                WHERE ts_code = ?
                ORDER BY trade_date DESC LIMIT ?
            """
            params = [ts_code, days]

        df = conn.execute(sql, params).fetchdf()
        if not df.empty:
            df = df.sort_values("trade_date")
        return {
            "ts_code": ts_code,
            "period": period,
            "fq": fq,
            "count": len(df),
            "klines": _df_to_records(df),
        }
    except Exception as e:
        logger.error(f"获取K线失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取K线失败: {e}")
    finally:
        conn.close()


@router.get("/fina/{ts_code}")
def get_stock_fina(ts_code: str, limit: int = Query(8, ge=1, le=20)):
    """获取最近财务指标。"""
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT end_date, ann_date,
                   eps, roe, roa, grossprofit_margin, netprofit_margin,
                   debt_to_assets, current_ratio, quick_ratio,
                   netprofit_yoy, or_yoy, op_yoy
            FROM fina_indicator
            WHERE ts_code = ?
            ORDER BY end_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    except Exception as e:
        logger.error(f"获取财务指标失败: {e}")
        return {"count": 0, "data": []}
    finally:
        conn.close()


@router.get("/block_trade/{ts_code}")
def get_block_trade(ts_code: str, limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT trade_date, price, vol, amount, buyer, seller
            FROM block_trade
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/share_float/{ts_code}")
def get_share_float(ts_code: str, limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT ann_date, float_date, float_share, float_ratio, holder_name, share_type
            FROM share_float
            WHERE ts_code = ?
            ORDER BY float_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/top_list/{ts_code}")
def get_top_list(ts_code: str, limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT trade_date, name, close, pct_change, turnover_rate,
                   l_buy, l_sell, l_amount, net_amount, net_rate, reason
            FROM top_list
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/top_inst/{ts_code}")
def get_top_inst(ts_code: str, limit: int = Query(30, ge=1, le=200)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT trade_date, exalter, side, buy, sell, net_buy, buy_rate, sell_rate, reason
            FROM top_inst
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/holder_trade/{ts_code}")
def get_holder_trade(ts_code: str, limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT ann_date, holder_name, holder_type, in_de,
                   change_vol, change_ratio, after_share, after_ratio, avg_price
            FROM stk_holdertrade
            WHERE ts_code = ?
            ORDER BY ann_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/moneyflow/{ts_code}")
def get_moneyflow(ts_code: str, days: int = Query(30, ge=1, le=120)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT trade_date,
                   buy_sm_amount, sell_sm_amount,
                   buy_md_amount, sell_md_amount,
                   buy_lg_amount, sell_lg_amount,
                   buy_elg_amount, sell_elg_amount,
                   net_mf_amount
            FROM moneyflow
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, days],
        ).fetchdf()
        if not df.empty:
            df = df.sort_values("trade_date")
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/dividend/{ts_code}")
def get_dividend(ts_code: str, limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT end_date, ann_date, div_proc, stk_div, stk_bo_rate,
                   cash_div, cash_div_tax, record_date, ex_date, pay_date
            FROM dividend
            WHERE ts_code = ?
            ORDER BY end_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/forecast/{ts_code}")
def get_forecast(ts_code: str, limit: int = Query(10, ge=1, le=50)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT ann_date, end_date, type, p_change_min, p_change_max,
                   net_profit_min, net_profit_max, summary, change_reason
            FROM forecast
            WHERE ts_code = ?
            ORDER BY end_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/top_holders/{ts_code}")
def get_top_holders(ts_code: str, float_only: bool = False, limit: int = 10):
    conn = _get_conn()
    try:
        table = "top10_floatholders" if float_only else "top10_holders"
        latest = conn.execute(
            f"SELECT MAX(end_date) FROM {table} WHERE ts_code = ?",
            [ts_code],
        ).fetchone()
        if not latest or not latest[0]:
            return {"end_date": None, "count": 0, "data": []}
        df = conn.execute(
            f"""
            SELECT holder_name, hold_amount, hold_ratio, hold_float_ratio, hold_change, holder_type
            FROM {table}
            WHERE ts_code = ? AND end_date = ?
            ORDER BY hold_ratio DESC
            LIMIT ?
            """,
            [ts_code, latest[0], limit],
        ).fetchdf()
        return {"end_date": latest[0], "count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/margin/{ts_code}")
def get_margin(ts_code: str, days: int = Query(30, ge=1, le=120)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT trade_date, rzye, rqye, rzmre, rzche, rzrqye
            FROM margin_detail
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, days],
        ).fetchdf()
        if not df.empty:
            df = df.sort_values("trade_date")
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()


@router.get("/repurchase/{ts_code}")
def get_repurchase(ts_code: str, limit: int = Query(10, ge=1, le=50)):
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT ann_date, end_date, proc, exp_date, vol, amount, high_limit, low_limit
            FROM repurchase
            WHERE ts_code = ?
            ORDER BY ann_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        ).fetchdf()
        return {"count": len(df), "data": _df_to_records(df)}
    finally:
        conn.close()

