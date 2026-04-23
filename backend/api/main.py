"""FastAPI 主应用 - 量化系统 Web API"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from loguru import logger
import asyncio
import sys, os, math, json
import warnings

warnings.filterwarnings("ignore", message=r".*RequestsDependencyWarning.*")
warnings.filterwarnings("ignore", message=r"urllib3 .* doesn't match a supported version!")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.api.routers import sync, factors, strategy, monitor, stock, watchlist, realtime_monitor
from backend.realtime_runtime import persistence_service, pool_service, runtime_jobs


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """
    启动/关闭钩子：
      - 启动时加载 Layer 2 日线指标缓存一次
      - 每日 20:00 检查 DB 最新 trade_date 是否为今天，是则刷新缓存
      - 启动 tick 持久化后台线程
    """
    import datetime

    # 0. 提前建表（主线程单连接，避免后续多线程并发 DDL 冲突）
    try:
        _init_conn = persistence_service._get_conn()
        _init_conn.close()
        logger.info("[启动] DB 表结构初始化完成")
    except Exception as e:
        logger.warning(f"[启动] DB 表结构初始化失败: {e}")

    # 0.5 gm 启动前注入初始订阅：Pool1 + Pool2 合并去重（不依赖前端 WS）
    try:
        from config import TICK_PROVIDER
        if str(TICK_PROVIDER).lower() == "gm":
            with persistence_service._ro_conn_ctx() as conn:
                rows = conn.execute(
                    """
                    SELECT DISTINCT ts_code
                    FROM monitor_pools
                    WHERE pool_id IN (1, 2)
                    ORDER BY ts_code
                    """
                ).fetchall()
            init_codes = [str(r[0]) for r in rows if r and r[0]]
            if init_codes:
                from backend.realtime.gm_tick import seed_subscribed_codes
                n = seed_subscribed_codes(init_codes, overwrite=True)
                logger.info(f"[启动] gm 初始订阅已注入(Pool1+Pool2 去重): {n}")
    except Exception as e:
        logger.warning(f"[启动] gm 初始订阅注入失败: {e}")

    # 1. 启动时加载一次日线指标缓存
    try:
        n = pool_service._refresh_daily_factors_cache(pool_id=None)
        logger.info(f"[启动] Layer2 日线指标缓存初始化完成: {n} 只股票")
    except Exception as e:
        logger.warning(f"[启动] Layer2 日线指标缓存初始化失败: {e}")

    # 2. 每日 20:00 刷新任务（仅当 DB 最新数据是今天才刷新）
    async def _daily_refresh_loop():
        while True:
            now = datetime.datetime.now()
            # 计算下次 20:00
            next_run = now.replace(hour=20, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += datetime.timedelta(days=1)
            wait_seconds = (next_run - now).total_seconds()
            logger.info(f"[定时] 下次日线缓存检查时间: {next_run} (等待 {wait_seconds/3600:.1f} 小时)")
            await asyncio.sleep(wait_seconds)

            # 检查 DB 最新 trade_date 是否为今天
            try:
                today_str = datetime.date.today().strftime('%Y%m%d')
                latest = pool_service._get_latest_factor_date()
                if latest == today_str:
                    n = pool_service._refresh_daily_factors_cache(pool_id=None)
                    logger.info(f"[定时] DB 最新数据是今天({today_str})，日线缓存已刷新: {n} 只股票")
                else:
                    logger.info(f"[定时] DB 最新数据日期={latest}，不是今天({today_str})，跳过刷新")
            except Exception as e:
                logger.warning(f"[定时] 日线缓存检查失败: {e}")

    # 3. 启动 tick 历史持久化线程
    try:
        runtime_jobs._start_tick_persistence()
    except Exception as e:
        logger.warning(f"[启动] tick 持久化线程启动失败: {e}")

    # 4. 启动逐笔缓存刷新线程（供 Layer2 主力行为分析用）
    try:
        runtime_jobs._start_txn_refresher(interval=3.0)
    except Exception as e:
        logger.warning(f"[启动] 逐笔刷新线程启动失败: {e}")

    # 5. 启动 T+0 在线质量监控线程
    try:
        runtime_jobs._start_t0_quality_monitor(interval=1.0)
    except Exception as e:
        logger.warning(f"[启动] T+0 质量监控线程启动失败: {e}")

    # 6. 启动 T+0 特征漂移监控线程（每30分钟检测一次）
    try:
        runtime_jobs._start_drift_monitor(check_interval_min=30.0)
    except Exception as e:
        logger.warning(f"[启动] T+0 漂移监控线程启动失败: {e}")

    # 7. 动态阈值闭环线程
    try:
        runtime_jobs._start_threshold_closed_loop(interval_min=float(getattr(runtime_jobs, "_CLOSED_LOOP_INTERVAL_MIN", 15.0)))
    except Exception as e:
        logger.warning(f"[启动] 动态阈值闭环线程启动失败: {e}")

    refresh_task = asyncio.create_task(_daily_refresh_loop())
    logger.info("[启动] 每日 20:00 日线缓存检查任务已启动")

    yield

    refresh_task.cancel()
    logger.info("[关闭] 后台任务已取消")


class _NaNSafeEncoder(json.JSONEncoder):
    """将 NaN / Inf 转为 null，避免 JSON 序列化失败"""
    def iterencode(self, o, _one_shot=False):
        return super().iterencode(self._clean(o), _one_shot)

    def _clean(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        if isinstance(obj, dict):
            return {k: self._clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._clean(v) for v in obj]
        return obj


class _NaNSafeResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, cls=_NaNSafeEncoder, ensure_ascii=False).encode("utf-8")


app = FastAPI(
    title="Quant System API",
    description="多因子量化选股系统 Web API",
    version="8.0.0",
    default_response_class=_NaNSafeResponse,
    lifespan=_lifespan,
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(sync.router, prefix="/api/sync", tags=["数据同步"])
app.include_router(factors.router, prefix="/api/factors", tags=["因子计算"])
app.include_router(strategy.router, prefix="/api/strategy", tags=["策略回测"])
app.include_router(monitor.router, prefix="/api/monitor", tags=["监控面板"])
app.include_router(stock.router, prefix="/api/stock", tags=["股票详情"])
app.include_router(watchlist.router, prefix="/api/watchlist", tags=["自选股"])
app.include_router(realtime_monitor.router, prefix="/api/realtime", tags=["盯盘"])

@app.get("/")
def root():
    return {"message": "Quant System API v8.0", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    logger.info("启动 FastAPI 服务器: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
