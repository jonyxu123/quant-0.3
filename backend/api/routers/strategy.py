"""策略选股与回测路由"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
import duckdb
import pandas as pd
from datetime import datetime
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import DB_PATH
from backend.strategy.strategy_combos import list_all_strategies, get_strategy_info, STRATEGY_VERSIONS

router = APIRouter()

# ─── 全局任务状态 ───
_task_status = {"running": False, "message": "", "result": None}


class SelectRequest(BaseModel):
    trade_date: Optional[str] = None
    strategy_ids: List[str] = ["D"]   # 单策略传一个，多策略传多个
    top_n: int = 50
    apply_filter: bool = True
    combine_mode: str = "union"        # union=并集取票, intersect=交集, score=加权平均分


@router.get("/list")
def list_strategies():
    """获取所有30套策略列表，按版本分组"""
    all_s = list_all_strategies()
    grouped: dict = {}
    for ver, ids in STRATEGY_VERSIONS.items():
        grouped[ver] = [s for s in all_s if s["id"] in ids]
    return {"strategies": all_s, "grouped": grouped, "versions": list(STRATEGY_VERSIONS.keys())}


@router.get("/info/{strategy_id}")
def strategy_info(strategy_id: str):
    """获取单个策略详情（权重、过滤条件）"""
    info = get_strategy_info(strategy_id.upper())
    if not info:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")
    return info


@router.get("/latest_date")
def latest_factor_date():
    """获取因子库最新日期（factor_raw 优先，降级到 daily_basic）"""
    conn = duckdb.connect(DB_PATH)
    try:
        try:
            row = conn.execute("SELECT MAX(trade_date) FROM factor_raw").fetchone()
            if row and row[0]:
                return {"latest_date": str(row[0]), "source": "factor_raw"}
        except Exception:
            pass
        # 降级：从 daily_basic 取最新日期
        row = conn.execute("SELECT MAX(trade_date) FROM daily_basic").fetchone()
        return {"latest_date": str(row[0]) if row and row[0] else None, "source": "daily_basic"}
    except Exception:
        return {"latest_date": None}
    finally:
        conn.close()


@router.post("/select")
def select_stocks(request: SelectRequest):
    """
    执行选股 - 直接读取 factor_raw 缓存表，秒级响应
    - 单策略：strategy_ids=["D"]
    - 多策略组合：strategy_ids=["A","D","X"]，combine_mode 控制合并方式
    """
    from backend.strategy.strategy_engine import StrategyEngine

    # ── 1. 确定交易日 ──
    trade_date = request.trade_date
    conn = duckdb.connect(DB_PATH)
    try:
        if not trade_date:
            row = conn.execute("SELECT MAX(trade_date) FROM factor_raw").fetchone()
            trade_date = str(row[0]) if row and row[0] else None
            if not trade_date:
                return {"trade_date": None, "stocks": [], "count": 0,
                        "error": "factor_raw 表为空，请先执行因子计算"}

        # ── 2. 从 factor_raw 直接读取已计算的因子数据 ──
        factor_df = conn.execute(
            f"SELECT * FROM factor_raw WHERE trade_date = '{trade_date}'"
        ).fetchdf()
    except Exception as e:
        return {"trade_date": trade_date, "stocks": [], "count": 0,
                "error": f"读取因子数据失败: {e}"}
    finally:
        conn.close()

    if factor_df.empty:
        return {"trade_date": trade_date, "stocks": [], "count": 0,
                "error": f"{trade_date} 无因子数据，请先运行 python main.py compute-factors --date {trade_date}"}

    # 设置 ts_code 为 index，去掉 trade_date 列
    if 'ts_code' in factor_df.columns:
        factor_df = factor_df.set_index('ts_code')
    elif 'index' in factor_df.columns:
        factor_df = factor_df.rename(columns={'index': 'ts_code'}).set_index('ts_code')
    if 'trade_date' in factor_df.columns:
        factor_df = factor_df.drop(columns=['trade_date'])

    # factor_raw 中因子列可能以 VARCHAR 存储，强制转为 float
    factor_df = factor_df.apply(pd.to_numeric, errors='coerce')

    try:
        se = StrategyEngine()
        strategy_ids = [s.upper() for s in request.strategy_ids]

        if len(strategy_ids) == 1:
            result = se.run_pipeline(
                strategy_id=strategy_ids[0],
                factor_df=factor_df,
                n=request.top_n,
                apply_filter=request.apply_filter,
            )
            selected = result["selected"]
            summary = result["summary"]
        else:
            selected, summary = _multi_strategy_select(
                se, strategy_ids, factor_df,
                top_n=request.top_n,
                apply_filter=request.apply_filter,
                combine_mode=request.combine_mode,
            )

        stocks = _enrich_stocks(selected, trade_date, factor_df)
        return {
            "trade_date": trade_date,
            "strategy_ids": strategy_ids,
            "combine_mode": request.combine_mode if len(strategy_ids) > 1 else "single",
            "stocks": stocks,
            "count": len(stocks),
            "summary": summary,
        }
    except Exception as e:
        logger.exception(f"选股失败: {e}")
        return {"trade_date": trade_date, "stocks": [], "count": 0, "error": str(e)}


def _multi_strategy_select(se, strategy_ids, factor_df, top_n, apply_filter, combine_mode):
    """多策略组合选股"""
    import pandas as pd

    all_results = {}
    for sid in strategy_ids:
        r = se.run_pipeline(sid, factor_df, n=top_n, apply_filter=apply_filter)
        all_results[sid] = r["selected"]

    if combine_mode == "intersect":
        # 交集：所有策略都选中的股票
        sets = [set(df.index) for df in all_results.values() if not df.empty]
        common = set.intersection(*sets) if sets else set()
        # 取平均得分
        score_dfs = []
        for sid, df in all_results.items():
            if not df.empty:
                s = df[["strategy_score"]].rename(columns={"strategy_score": sid})
                score_dfs.append(s)
        if score_dfs and common:
            merged = pd.concat(score_dfs, axis=1).loc[list(common)]
            merged["avg_score"] = merged.mean(axis=1)
            merged = merged.sort_values("avg_score", ascending=False).head(top_n)
            merged["rank"] = range(1, len(merged) + 1)
            merged["strategy_score"] = merged["avg_score"]
            selected = merged
        else:
            selected = pd.DataFrame()

    elif combine_mode == "score":
        # 加权平均：所有策略得分归一化后平均
        import numpy as np
        score_dfs = []
        for sid, df in all_results.items():
            if not df.empty and "strategy_score" in df.columns:
                s = df[["strategy_score"]].copy()
                # Min-max 归一化
                mn, mx = s["strategy_score"].min(), s["strategy_score"].max()
                s["strategy_score"] = (s["strategy_score"] - mn) / (mx - mn + 1e-9)
                score_dfs.append(s.rename(columns={"strategy_score": sid}))
        if score_dfs:
            merged = pd.concat(score_dfs, axis=1).fillna(0)
            merged["strategy_score"] = merged.mean(axis=1)
            merged = merged.sort_values("strategy_score", ascending=False).head(top_n)
            merged["rank"] = range(1, len(merged) + 1)
            selected = merged
        else:
            selected = pd.DataFrame()

    else:
        # 并集 union：合并所有策略结果，出现次数多优先，次数相同则取最高分
        import pandas as pd
        records = []
        for sid, df in all_results.items():
            if not df.empty:
                for code, row in df.iterrows():
                    records.append({
                        "ts_code": code,
                        "strategy_score": row.get("strategy_score", 0),
                        "strategy_id": sid,
                    })
        if records:
            rdf = pd.DataFrame(records)
            agg = rdf.groupby("ts_code").agg(
                appear_count=("strategy_id", "count"),
                max_score=("strategy_score", "max"),
                strategies=("strategy_id", lambda x: ",".join(sorted(x))),
            ).sort_values(["appear_count", "max_score"], ascending=[False, False])
            agg = agg.head(top_n)
            agg["rank"] = range(1, len(agg) + 1)
            agg["strategy_score"] = agg["max_score"]
            selected = agg
        else:
            selected = pd.DataFrame()

    summary = {
        "strategy_ids": strategy_ids,
        "combine_mode": combine_mode,
        "stocks_selected": len(selected),
        "per_strategy_counts": {sid: len(df) for sid, df in all_results.items()},
    }
    return selected, summary


def _enrich_stocks(selected_df, trade_date: str, factors_df: pd.DataFrame = None):
    """拼接股票名称、行业等信息，并包含因子值"""
    import pandas as pd
    if selected_df is None or selected_df.empty:
        return []

    codes = list(selected_df.index)
    names = {}
    try:
        conn = duckdb.connect(DB_PATH)
        placeholders = ",".join([f"'{c}'" for c in codes])
        rows = conn.execute(
            f"SELECT ts_code, name, industry FROM stock_basic WHERE ts_code IN ({placeholders})"
        ).fetchall()
        names = {r[0]: {"name": r[1], "industry": r[2]} for r in rows}
        conn.close()
    except Exception:
        pass

    result = []
    for i, (code, row) in enumerate(selected_df.iterrows()):
        meta = names.get(code, {})
        item = {
            "rank": int(row.get("rank", i + 1)),
            "ts_code": code,
            "name": meta.get("name", ""),
            "industry": meta.get("industry", ""),
            "strategy_score": round(float(row.get("strategy_score", 0)), 4),
        }
        # union 模式额外字段
        if "appear_count" in row:
            item["appear_count"] = int(row["appear_count"])
        if "strategies" in row:
            item["strategies"] = str(row["strategies"])
        
        # 添加因子值 - 包含该股票的所有因子原始值
        if factors_df is not None and code in factors_df.index:
            stock_factors = factors_df.loc[code]
            # 转换为可序列化的格式，保留3位小数
            factor_values = {}
            for col in factors_df.columns:
                val = stock_factors[col]
                if pd.notna(val):
                    # 根据因子类型格式化
                    if isinstance(val, (int, float)):
                        factor_values[col] = round(float(val), 3)
                    else:
                        factor_values[col] = str(val)
            item["factor_values"] = factor_values
        
        result.append(item)
    return result
