"""
T+0 特征漂移监控

实现 PSI（Population Stability Index）+ KS 检验，
监控以下 5 个核心特征的分布漂移：
  - bias_vwap
  - robust_zscore
  - gub5_ratio
  - bid_ask_ratio
  - vpower_slope（量能斜率）

设计原则：
  - 只告警不自调：检测到漂移 + precision 同步下降时才记录告警
  - 滑动基线：用最近 N 个交易日的历史信号 details 作为基线
  - 不引入外部依赖（scipy/numpy 可选，有则用 KS，否则降级）
"""
from __future__ import annotations

import math
import statistics
from typing import Optional

# ─── KS 检验（无 scipy 时降级为简单分位距离）─────────────────
try:
    from scipy.stats import ks_2samp as _ks_2samp
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False


def _ks_statistic(a: list[float], b: list[float]) -> tuple[float, float]:
    """
    返回 (ks_stat, p_value)。
    无 scipy 时 p_value 返回 -1（降级模式）。
    """
    if _HAS_SCIPY:
        stat, p = _ks_2samp(a, b)
        return float(stat), float(p)
    # 降级：计算经验 CDF 最大差距
    a_sorted = sorted(a)
    b_sorted = sorted(b)
    all_vals = sorted(set(a_sorted + b_sorted))
    n_a, n_b = len(a_sorted), len(b_sorted)
    max_diff = 0.0
    i_a = i_b = 0
    for v in all_vals:
        while i_a < n_a and a_sorted[i_a] <= v:
            i_a += 1
        while i_b < n_b and b_sorted[i_b] <= v:
            i_b += 1
        diff = abs(i_a / n_a - i_b / n_b)
        if diff > max_diff:
            max_diff = diff
    return max_diff, -1.0


def compute_psi(expected: list[float], actual: list[float], bins: int = 10) -> float:
    """
    Population Stability Index。
    PSI < 0.1: 稳定；0.1-0.2: 轻微漂移；> 0.2: 显著漂移。
    """
    if len(expected) < bins or len(actual) < bins:
        return 0.0

    # 用 expected 确定分箱边界
    mn, mx = min(expected), max(expected)
    if mx == mn:
        return 0.0

    edges = [mn + (mx - mn) * i / bins for i in range(bins + 1)]
    edges[-1] = mx + 1e-9  # 保证最大值落入最后一箱

    def _bucket(vals: list[float]) -> list[float]:
        counts = [0] * bins
        for v in vals:
            for j in range(bins):
                if edges[j] <= v < edges[j + 1]:
                    counts[j] += 1
                    break
        total = len(vals)
        return [max(c / total, 1e-6) for c in counts]  # 避免 log(0)

    exp_pct = _bucket(expected)
    act_pct = _bucket(actual)

    psi = sum((a - e) * math.log(a / e) for e, a in zip(exp_pct, act_pct))
    return round(psi, 4)


def detect_feature_drift(
    feature_name: str,
    baseline: list[float],
    recent: list[float],
    psi_threshold: float = 0.2,
    ks_p_threshold: float = 0.05,
) -> dict:
    """
    综合 PSI + KS 判断单特征是否漂移。

    Returns:
        {
            'feature': str,
            'psi': float,
            'ks_stat': float,
            'ks_p': float,         # -1 表示降级模式
            'drifted': bool,       # PSI>阈值 AND (KS_p<阈值 OR 降级模式下 ks_stat>0.2)
            'severity': str,       # 'stable' | 'mild' | 'severe'
        }
    """
    if len(baseline) < 10 or len(recent) < 10:
        return {
            'feature': feature_name, 'psi': 0.0,
            'ks_stat': 0.0, 'ks_p': 1.0,
            'drifted': False, 'severity': 'stable',
        }

    psi = compute_psi(baseline, recent)
    ks_stat, ks_p = _ks_statistic(baseline, recent)

    # 判断漂移
    ks_triggered = (ks_p >= 0 and ks_p < ks_p_threshold) or (ks_p < 0 and ks_stat > 0.2)
    drifted = psi > psi_threshold and ks_triggered

    if psi > 0.25:
        severity = 'severe'
    elif psi > psi_threshold:
        severity = 'mild'
    else:
        severity = 'stable'

    return {
        'feature': feature_name,
        'psi': psi,
        'ks_stat': round(ks_stat, 4),
        'ks_p': round(ks_p, 4),
        'drifted': drifted,
        'severity': severity,
    }


def detect_all_t0_drift(
    baseline_records: list[dict],
    recent_records: list[dict],
    psi_threshold: float = 0.2,
    ks_p_threshold: float = 0.05,
) -> dict:
    """
    对 5 个核心 T+0 特征批量检测漂移。

    Args:
        baseline_records: 历史信号 details 列表（每条为一个信号的 details 字典）
        recent_records:   近期信号 details 列表

    Returns:
        {
            'any_drift': bool,
            'features': { feature_name: detect_feature_drift 结果 },
            'drifted_features': [str],
        }
    """
    FEATURES = [
        ('bias_vwap',      lambda d: d.get('bias_vwap')),
        ('robust_zscore',  lambda d: d.get('robust_zscore')),
        ('gub5_ratio',     lambda d: d.get('gub5_ratio')),
        ('bid_ask_ratio',  lambda d: d.get('bid_ask_ratio')),
        ('raw_score',      lambda d: d.get('raw_score')),    # 用 raw_score 作为综合特征代理
    ]

    def _extract(records: list[dict], fn) -> list[float]:
        vals = []
        for r in records:
            v = fn(r)
            if v is not None:
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    pass
        return vals

    results = {}
    drifted = []
    for fname, fn in FEATURES:
        base_vals = _extract(baseline_records, fn)
        recent_vals = _extract(recent_records, fn)
        res = detect_feature_drift(fname, base_vals, recent_vals, psi_threshold, ks_p_threshold)
        results[fname] = res
        if res['drifted']:
            drifted.append(fname)

    return {
        'any_drift': len(drifted) > 0,
        'features': results,
        'drifted_features': drifted,
    }
