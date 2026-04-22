"""系统监控路由"""
from fastapi import APIRouter
import duckdb
import psutil
from datetime import datetime
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import DB_PATH

router = APIRouter()

@router.get("/system")
def get_system_info():
    """获取系统资源信息"""
    return {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory": {
            "total": psutil.virtual_memory().total / (1024**3),
            "used": psutil.virtual_memory().used / (1024**3),
            "percent": psutil.virtual_memory().percent
        },
        "disk": {
            "total": psutil.disk_usage('/').total / (1024**3),
            "used": psutil.disk_usage('/').used / (1024**3),
            "percent": psutil.disk_usage('/').percent
        }
    }

@router.get("/database")
def get_database_info():
    """获取数据库统计信息"""
    conn = duckdb.connect(DB_PATH)
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        table_stats = []
        total_rows = 0
        
        for (table_name,) in tables:
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                total_rows += count
                table_stats.append({"table": table_name, "rows": count})
            except Exception:
                pass
        
        return {
            "total_tables": len(tables),
            "total_rows": total_rows,
            "tables": sorted(table_stats, key=lambda x: x['rows'], reverse=True)[:20]
        }
    finally:
        conn.close()

@router.get("/latest_data")
def get_latest_data():
    """获取最新数据日期"""
    conn = duckdb.connect(DB_PATH)
    try:
        latest = {}
        for table in ['daily', 'daily_basic', 'moneyflow', 'factor_raw']:
            try:
                result = conn.execute(
                    f'SELECT MAX(trade_date) FROM "{table}"'
                ).fetchone()[0]
                latest[table] = str(result) if result else None
            except Exception:
                latest[table] = None
        return latest
    finally:
        conn.close()


@router.get("/tables")
def list_tables():
    """获取所有表名及行数"""
    conn = duckdb.connect(DB_PATH)
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        result = []
        for (name,) in tables:
            try:
                cnt = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            except Exception:
                cnt = 0
            result.append({"name": name, "rows": cnt})
        result.sort(key=lambda x: x["name"])
        return {"tables": result}
    finally:
        conn.close()


@router.get("/tables/{table_name}/columns")
def get_columns(table_name: str):
    """获取表字段列表"""
    conn = duckdb.connect(DB_PATH)
    try:
        cols = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        return {"columns": [{"name": c[0], "type": c[1]} for c in cols]}
    except Exception as e:
        return {"columns": [], "error": str(e)}
    finally:
        conn.close()


@router.get("/tables/{table_name}/data")
def get_table_data(table_name: str, page: int = 1, page_size: int = 50, search: str = ""):
    """分页查询表数据"""
    conn = duckdb.connect(DB_PATH)
    try:
        offset = (page - 1) * page_size
        total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        # 获取列名
        cols_raw = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        col_names = [c[0] for c in cols_raw]

        # 搜索过滤（对第一个字符串字段做 LIKE）
        where = ""
        if search:
            str_cols = [c[0] for c in cols_raw if "VARCHAR" in c[1].upper() or "TEXT" in c[1].upper()]
            if str_cols:
                conds = " OR ".join([f'CAST("{c}" AS VARCHAR) LIKE \'%{search}%\'' for c in str_cols[:3]])
                where = f"WHERE {conds}"
                total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}" {where}').fetchone()[0]

        rows = conn.execute(
            f'SELECT * FROM "{table_name}" {where} LIMIT {page_size} OFFSET {offset}'
        ).fetchall()

        return {
            "columns": col_names,
            "rows": [list(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, (total + page_size - 1) // page_size),
        }
    except Exception as e:
        return {"columns": [], "rows": [], "total": 0, "page": 1, "page_size": page_size, "total_pages": 1, "error": str(e)}
    finally:
        conn.close()
