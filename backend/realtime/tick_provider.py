"""
Tick 数据源抽象基类

盯盘模块的 tick 数据支持多种数据源：
  - mootdx: 通达信协议（主动查询模式）
  - gm: 掘金量化（subscribe 订阅模式）

逐笔成交、分钟线、指数数据固定使用 mootdx，不在此抽象范围内。

使用方式：
  from backend.realtime.tick_provider import get_tick_provider
  provider = get_tick_provider()  # 根据 config.TICK_PROVIDER 自动选择
  tick = provider.get_tick('600519.SH')
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional
from loguru import logger
import threading


class TickProvider(ABC):
    """Tick 数据源抽象基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider 标识（如 'mootdx', 'gm'）"""
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Provider 显示名称（如 '通达信', '掘金量化'）"""
        ...

    @abstractmethod
    def get_tick(self, ts_code: str) -> dict:
        """
        获取单只股票最新 tick + 五档盘口。

        Returns:
            {
                'ts_code': str,
                'name': str,
                'price': float,
                'open': float,
                'high': float,
                'low': float,
                'pre_close': float,
                'volume': int,
                'amount': float,
                'pct_chg': float,
                'timestamp': int,
                'bids': [(price, vol), ...],  # 买1~买5
                'asks': [(price, vol), ...],  # 卖1~卖5
                'is_mock': bool,
            }
        """
        ...

    def subscribe_symbols(self, ts_codes: list[str]):
        """
        订阅股票列表（仅 gm 等订阅模式需要实现）。
        mootdx 等主动查询模式无需实现，默认空操作。
        """
        pass

    def unsubscribe_symbols(self, ts_codes: list[str]):
        """
        取消订阅股票列表（仅 gm 等订阅模式需要实现）。
        """
        pass

    def start(self):
        """
        启动 Provider（仅 gm 等订阅模式需要实现，启动后台线程）。
        mootdx 等主动查询模式无需实现。
        """
        pass

    def stop(self):
        """
        停止 Provider（仅 gm 等订阅模式需要实现）。
        """
        pass

    def bulk_update_daily_factors(self, factors_map: dict):
        """批量更新日线指标缓存（仅 gm 订阅模式需要实现）"""
        pass

    def bulk_update_chip_features(self, features_map: dict):
        """批量更新筹码特征缓存（Pool1 阶段2预留，默认空操作）"""
        pass

    def bulk_update_stock_pools(self, mapping: dict):
        """批量更新股票→池映射（仅 gm 订阅模式需要实现）"""
        pass

    def bulk_update_stock_industry(self, mapping: dict):
        """批量更新股票行业映射。默认空实现。"""
        pass

    def bulk_update_stock_concepts(self, mapping: dict):
        """批量更新股票到东方财富概念板块映射。默认空实现。"""
        pass

    def bulk_update_concept_snapshots(self, mapping: dict):
        """批量更新东方财富概念板块生态快照。默认空实现。"""
        pass

    def bulk_update_industry_snapshots(self, mapping: dict):
        """批量更新东方财富行业板块生态快照。默认空实现。"""
        pass

    def bulk_update_instrument_profiles(self, mapping: dict):
        """批量更新标的制度画像。默认空实现。"""
        pass

    def get_cached_signals(self, ts_code: str) -> Optional[dict]:
        """
        获取 ts_code 最新缓存的信号评估结果（毫秒级）。
        仅 gm 等订阅模式支持；mootdx 等主动查询模式返回 None。
        返回 None 表示不支持缓存或无缓存数据。
        """
        return None

    def get_cached_transactions(self, ts_code: str) -> Optional[list]:
        """
        获取 ts_code 最近缓存的逐笔成交数据。
        仅 gm 等订阅模式支持；mootdx 等主动查询模式返回 None。
        返回 None 表示不支持缓存或无缓存数据。
        """
        return None

    def get_cached_tick(self, ts_code: str) -> Optional[dict]:
        """
        获取 ts_code 最新缓存的 tick（不发起网络请求）。
        支持缓存的 Provider 应覆盖此方法；默认返回 None。
        """
        return None

    def get_cached_bars(self, ts_code: str) -> Optional[list]:
        """
        获取 ts_code 缓存的分钟线（不发起网络请求）。
        支持缓存的 Provider 应覆盖此方法；默认返回 None。
        """
        return None

    def get_prev_price(self, ts_code: str) -> Optional[float]:
        """
        获取 ts_code 最近一笔用于“跨越确认”的上一价格（非当前价）。
        默认返回 None，支持缓存的 Provider 可覆盖实现。
        """
        return None

    def bulk_update_recent_txns(self, mapping: dict):
        """批量更新逐笔成交缓存。{ts_code: [txn, ...]}。默认空操作。"""
        pass

    def get_pool1_observe_stats(self) -> Optional[dict]:
        """
        获取 Pool1 两阶段可观测统计（轻量接口）。
        返回 None 表示当前 provider 不支持或暂无统计。
        """
        return None

    def get_pool1_position_state(self, ts_code: str) -> Optional[dict]:
        return None

    def get_pool1_position_storage_status(self) -> Optional[dict]:
        return None

    def get_pool2_t0_inventory_state(self, ts_code: str) -> Optional[dict]:
        return None

    def get_pool2_t0_inventory_storage_status(self) -> Optional[dict]:
        return None

    def update_pool2_t0_inventory_state(self, ts_code: str, updates: dict) -> Optional[dict]:
        return None


# ============================================================
# 工厂函数
# ============================================================
_instance: Optional[TickProvider] = None
_init_lock = threading.Lock()  # FastAPI 单进程多线程，用线程锁即可


def get_tick_provider() -> TickProvider:
    """
    获取全局 TickProvider 单例。
    根据 config.TICK_PROVIDER 选择实现，启动时确定，运行期间不切换。
    使用线程锁确保多线程环境下只初始化一次。
    """
    global _instance
    if _instance is not None:
        return _instance

    with _init_lock:
        # 双重检查
        if _instance is not None:
            return _instance

        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import TICK_PROVIDER

        if TICK_PROVIDER == 'gm':
            from backend.realtime.gm_tick import GmTickProvider
            _instance = GmTickProvider()
            logger.info("Tick 数据源: 掘金量化(gm) subscribe 模式")
        else:
            from backend.realtime.mootdx_tick import MootdxTickProvider
            _instance = MootdxTickProvider()
            logger.info("Tick 数据源: 通达信(mootdx) 主动查询模式")

        # 启动 Provider（gm 需要启动后台订阅进程）
        _instance.start()
        return _instance
