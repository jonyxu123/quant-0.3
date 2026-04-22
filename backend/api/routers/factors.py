"""因子管理路由。"""

from __future__ import annotations

import os
import sys
import threading
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks
from loguru import logger
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import DB_PATH
from backend.data.duckdb_manager import open_duckdb_conn

router = APIRouter()

_task_state = {
    "running": False,
    "trade_date": None,
    "progress": "",
    "error": None,
    "done": False,
}
_task_lock = threading.Lock()


class FactorComputeRequest(BaseModel):
    trade_date: Optional[str] = None


def _nan_safe_records(df):
    if df is None or df.empty:
        return []
    out = df.where(df.notna(), None).to_dict(orient="records")
    return out


@router.get("/registry")
def list_factor_registry():
    """获取所有已注册因子。"""
    from backend.factors.factor_engine import FactorEngine

    engine = FactorEngine(DB_PATH)
    reg = engine._registry
    result = [
        {
            "factor_id": k,
            "dimension": v["dimension"],
            "name": v.get("name", ""),
            "direction": v.get("direction", ""),
        }
        for k, v in sorted(reg.items())
    ]
    engine.conn.close()
    return {"count": len(result), "factors": result}


@router.get("/dimensions")
def list_dimensions():
    """按维度聚合因子数量。"""
    from backend.factors.factor_engine import FactorEngine

    engine = FactorEngine(DB_PATH)
    reg = engine._registry
    engine.conn.close()
    dim_map: dict[str, int] = {}
    for _, v in reg.items():
        d = v["dimension"]
        dim_map[d] = dim_map.get(d, 0) + 1
    return {"dimensions": [{"dimension": d, "count": c} for d, c in sorted(dim_map.items())]}


@router.get("/dates")
def list_computed_dates():
    """列出 factor_raw 已计算日期。"""
    conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
    try:
        rows = conn.execute(
            """
            SELECT trade_date, COUNT(*) AS stock_count
            FROM factor_raw
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 60
            """
        ).fetchall()
        return {"dates": [{"trade_date": r[0], "stock_count": r[1]} for r in rows]}
    except Exception:
        return {"dates": []}
    finally:
        conn.close()


@router.get("/latest_date")
def latest_available_date():
    """获取 daily_basic 最新交易日，以及 factor_raw 最新计算日。"""
    conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
    try:
        row = conn.execute("SELECT MAX(trade_date) FROM daily_basic").fetchone()
        latest_computed = None
        try:
            latest_computed = conn.execute("SELECT MAX(trade_date) FROM factor_raw").fetchone()[0]
        except Exception:
            pass
        return {
            "latest_data_date": str(row[0]) if row and row[0] else None,
            "latest_computed_date": str(latest_computed) if latest_computed else None,
        }
    finally:
        conn.close()


@router.get("/coverage/{trade_date}")
def factor_coverage(trade_date: str):
    """计算指定日期各因子覆盖率（非 NaN 比例）。"""
    conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
    try:
        df = conn.execute("SELECT * FROM factor_raw WHERE trade_date = ?", [trade_date]).fetchdf()
        if df.empty:
            return {"trade_date": trade_date, "coverage": []}
        total = len(df)
        skip_cols = {"trade_date", "ts_code", "index"}
        coverage = []
        for col in df.columns:
            if col in skip_cols:
                continue
            valid = int(df[col].notna().sum())
            coverage.append(
                {
                    "factor_id": col,
                    "valid_count": valid,
                    "total": total,
                    "coverage_pct": round(valid / total * 100, 1) if total > 0 else 0,
                }
            )
        coverage.sort(key=lambda x: -x["coverage_pct"])
        return {"trade_date": trade_date, "stock_count": total, "coverage": coverage}
    except Exception as e:
        return {"error": str(e), "coverage": []}
    finally:
        conn.close()


@router.get("/data/{trade_date}")
def get_factor_data(trade_date: str, page: int = 1, page_size: int = 50, search: str = ""):
    """分页获取指定日期因子数据。"""
    conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
    try:
        where = "WHERE trade_date = ?"
        params: list[object] = [trade_date]
        if search:
            where += " AND ts_code LIKE ?"
            params.append(f"%{search}%")
        total = conn.execute(f"SELECT COUNT(*) FROM factor_raw {where}", params).fetchone()[0]
        offset = (page - 1) * page_size
        params2 = list(params)
        params2.extend([page_size, offset])
        df = conn.execute(
            f"SELECT * FROM factor_raw {where} LIMIT ? OFFSET ?",
            params2,
        ).fetchdf()
        cols = ["ts_code"] + [c for c in df.columns if c not in {"ts_code", "trade_date", "index"}]
        df = df[[c for c in cols if c in df.columns]]
        return {
            "trade_date": trade_date,
            "total": total,
            "page": page,
            "page_size": page_size,
            "columns": list(df.columns),
            "data": _nan_safe_records(df),
        }
    except Exception as e:
        return {"error": str(e), "data": []}
    finally:
        conn.close()


@router.get("/task_status")
def task_status():
    """查询因子计算任务状态。"""
    return dict(_task_state)


@router.post("/compute")
async def compute_factors(request: FactorComputeRequest, background_tasks: BackgroundTasks):
    """触发因子计算任务（后台执行）。"""
    with _task_lock:
        if _task_state["running"]:
            return {"message": "任务已在运行中", "trade_date": _task_state["trade_date"]}

    trade_date = request.trade_date
    if not trade_date:
        conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
        try:
            row = conn.execute("SELECT MAX(trade_date) FROM daily_basic").fetchone()
            trade_date = str(row[0]) if row and row[0] else datetime.now().strftime("%Y%m%d")
        finally:
            conn.close()

    def run_compute():
        from backend.factors.factor_engine import FactorEngine

        with _task_lock:
            _task_state.update(
                {
                    "running": True,
                    "trade_date": trade_date,
                    "progress": "初始化因子引擎",
                    "error": None,
                    "done": False,
                }
            )
        try:
            engine = FactorEngine(DB_PATH)
            factor_count = len(engine._registry)
            _task_state["progress"] = f"开始计算 {trade_date}，因子数 {factor_count}"
            factor_df = engine.compute_all(trade_date)
            db_path = engine.db_path
            engine.conn.close()

            if factor_df.empty:
                _task_state.update({"running": False, "error": "因子结果为空", "done": True})
                return

            factor_df = factor_df.reset_index()
            if "index" in factor_df.columns and "ts_code" not in factor_df.columns:
                factor_df = factor_df.rename(columns={"index": "ts_code"})
            factor_df["trade_date"] = trade_date

            _task_state["progress"] = "写入数据库"
            conn = open_duckdb_conn(db_path, retries=3, base_sleep_sec=0.08)
            try:
                conn.register("factor_df", factor_df)
                try:
                    conn.execute("DROP TABLE IF EXISTS factor_raw")
                except Exception:
                    pass
                conn.execute("CREATE TABLE factor_raw AS SELECT * FROM factor_df")
            finally:
                try:
                    conn.unregister("factor_df")
                except Exception:
                    pass
                conn.close()

            stock_count = len(factor_df)
            factor_cols = len(factor_df.columns) - 1
            _task_state.update(
                {
                    "running": False,
                    "done": True,
                    "progress": f"完成：{trade_date}，股票 {stock_count}，字段 {factor_cols}",
                }
            )
            logger.info(_task_state["progress"])
        except Exception as e:
            logger.exception(f"因子计算失败: {e}")
            _task_state.update(
                {
                    "running": False,
                    "error": str(e),
                    "done": True,
                    "progress": "计算失败",
                }
            )

    background_tasks.add_task(run_compute)
    with _task_lock:
        _task_state.update(
            {
                "running": True,
                "trade_date": trade_date,
                "progress": "任务已提交",
                "error": None,
                "done": False,
            }
        )
    return {"message": "因子计算任务已启动", "trade_date": trade_date}


@router.delete("/data/{trade_date}")
def delete_factor_data(trade_date: str):
    """删除指定交易日因子数据。"""
    try:
        conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
        conn.execute("DELETE FROM factor_raw WHERE trade_date = ?", [trade_date])
        conn.close()
        return {"message": f"已删除 {trade_date} 的因子数据"}
    except Exception as e:
        return {"error": str(e)}

