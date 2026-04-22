"""数据同步路由"""
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import DB_PATH
from backend.data.duckdb_manager import open_duckdb_conn
from backend.data.full_sync import FullDataSync
from backend.data.incremental_sync import IncrementalSync

router = APIRouter()

class SyncRequest(BaseModel):
    interface: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    workers: int = 4

sync_status = {
    "running": False,
    "current_task": None,
    "progress": 0,
    "message": ""
}

@router.get("/interfaces")
def list_interfaces():
    """获取所有可用接口列表"""
    categories = {
        '基础信息': ['stock_basic', 'trade_cal', 'stock_company', 'namechange', 'suspend'],
        '行情数据': ['daily', 'daily_basic', 'weekly', 'monthly', 'index_daily'],
        '技术面因子': ['stk_factor_pro'],
        '财务数据': ['income', 'balancesheet', 'cashflow', 'fina_indicator', 'forecast', 'dividend'],
        '市场参考': ['moneyflow', 'cyq_perf', 'stk_holdertrade'],
        '机构数据': ['fund_portfolio', 'top10_holders', 'top10_floatholders', 'hsgt_top10', 'hk_hold', 'top_list', 'top_inst'],
        '两融数据': ['margin', 'margin_detail'],
        '参考数据': ['share_float', 'repurchase', 'block_trade'],
        '概念板块': ['concept', 'concept_detail'],
    }
    return {"categories": categories}

@router.get("/status")
def get_sync_status():
    """获取同步状态"""
    # 注意：不使用 read_only=True，因为后台同步可能正在写入
    # DuckDB 不允许以不同配置同时连接同一数据库
    conn = None
    try:
        conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
        tables = conn.execute("SHOW TABLES").fetchall()
        stats = {}
        for (table_name,) in tables:
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                last_date = conn.execute(
                    f'SELECT MAX(trade_date) FROM "{table_name}"'
                ).fetchone()[0]
                stats[table_name] = {"count": count, "last_date": str(last_date) if last_date else None}
            except Exception:
                pass
        return {
            "sync_running": sync_status["running"],
            "current_task": sync_status["current_task"],
            "progress": sync_status["progress"],
            "message": sync_status["message"],
            "tables": stats
        }
    except Exception as e:
        logger.warning(f"sync status read failed: {e}")
        return {
            "sync_running": sync_status["running"],
            "current_task": sync_status["current_task"],
            "progress": sync_status["progress"],
            "message": f'{sync_status["message"]} | status_db_unavailable: {e}',
            "tables": {},
            "db_unavailable": True,
        }
    finally:
        if conn is not None:
            conn.close()

@router.post("/full")
async def sync_full(request: SyncRequest, background_tasks: BackgroundTasks):
    """全量同步"""
    if sync_status["running"]:
        raise HTTPException(status_code=409, detail="同步任务正在运行中")
    
    def run_sync():
        sync_status["running"] = True
        sync_status["current_task"] = request.interface
        sync_status["progress"] = 0
        try:
            syncer = FullDataSync()
            method = getattr(syncer, f'sync_historical_{request.interface}', None)
            if not method:
                raise ValueError(f"未知接口: {request.interface}")
            
            kwargs = {"max_workers": request.workers}
            if request.start_date:
                kwargs["start_date"] = request.start_date
            if request.end_date:
                kwargs["end_date"] = request.end_date
            if request.start_year:
                kwargs["start_year"] = request.start_year
            if request.end_year:
                kwargs["end_year"] = request.end_year
            
            method(**kwargs)
            sync_status["message"] = f"{request.interface} 同步完成"
            sync_status["progress"] = 100
        except Exception as e:
            logger.error(f"同步失败: {e}")
            sync_status["message"] = f"同步失败: {e}"
        finally:
            sync_status["running"] = False
    
    background_tasks.add_task(run_sync)
    return {"message": "同步任务已启动", "interface": request.interface}

@router.post("/incremental")
async def sync_incremental(background_tasks: BackgroundTasks):
    """增量同步"""
    if sync_status["running"]:
        raise HTTPException(status_code=409, detail="同步任务正在运行中")
    
    def run_sync():
        sync_status["running"] = True
        sync_status["current_task"] = "incremental"
        sync_status["progress"] = 0
        syncer = None
        try:
            syncer = IncrementalSync()
            syncer.run_daily_sync()
            sync_status["message"] = "增量同步完成"
            sync_status["progress"] = 100
        except Exception as e:
            logger.error(f"增量同步失败: {e}")
            sync_status["message"] = f"增量同步失败: {e}"
        finally:
            if syncer is not None and hasattr(syncer, "close"):
                try:
                    syncer.close()
                except Exception:
                    pass
            sync_status["running"] = False
    
    background_tasks.add_task(run_sync)
    return {"message": "增量同步任务已启动"}
