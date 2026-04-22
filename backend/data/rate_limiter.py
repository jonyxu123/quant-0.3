"""Tushare/AKShare接口限流装饰器"""
import time
from functools import wraps
from loguru import logger


def rate_limit(calls_per_minute=200):
    """接口限流装饰器
    
    Args:
        calls_per_minute: 每分钟允许的最大调用次数
    """
    min_interval = 60.0 / calls_per_minute
    
    def decorator(func):
        last_call = [0.0]
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_call[0]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            result = func(*args, **kwargs)
            last_call[0] = time.time()
            return result
        
        return wrapper
    return decorator


def safe_call(func, *args, default=None, max_retries=3, **kwargs):
    """安全调用装饰器，用于AKShare等不稳定接口
    
    Args:
        func: 要调用的函数
        default: 失败时返回的默认值
        max_retries: 最大重试次数
    """
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.warning(f"{func.__name__} 第{attempt+1}次调用失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))  # 递增等待
            else:
                logger.error(f"{func.__name__} 重试{max_retries}次后仍失败")
                return default
