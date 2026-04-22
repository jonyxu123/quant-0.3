"""StockTradingApp v8.0 嶉ㄥ眬閰嶇疆"""
import os
from dotenv import load_dotenv

load_dotenv()

# Tushare
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

# DuckDB
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH_ENV = os.getenv("DB_PATH")
if _DB_PATH_ENV:
    DB_PATH = _DB_PATH_ENV if os.path.isabs(_DB_PATH_ENV) else os.path.join(_BASE_DIR, _DB_PATH_ENV)
else:
    DB_PATH = os.path.join(_BASE_DIR, "stock_data.duckdb")

# 间ュ織
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# 合愭祦墽傛算
TUSHARE_HIGH_FREQ = 500    # 娆?鍒嗛挓 (daily, daily_basic夛?
TUSHARE_MID_FREQ = 200     # 娆?鍒嗛挓 (income, fina_indicator夛?
TUSHARE_STK_FACTOR_FREQ = 30  # 娆?鍒嗛挓 (stk_factor_pro, 5000绉垎合愰)
AKSHARE_FREQ = 60          # 娆?鍒嗛挓 (寤蒋反?

# 鍥級瓙撴村强鍓嶇紑
DIMENSION_PREFIXES = [
    'V_', 'P_', 'G_', 'Q_', 'L_', 'M_', 'S_', 'E_', 'I_', 'C_',
    'TP_', 'EV_', 'MF_', 'VOL_', 'SENT_', 'CAL_', 'GOV_',
    'ANALYST_', 'SECTOR_', 'CONCEPT_', 'TECH_', 'MICRO_',
    'FQ_', 'DIV_', 'CORP_', 'QV_', 'MS_', 'IM_', 'D_', 'T0_'
]

# IC监控阈值
IC_CULL_THRESHOLD = 0.02       # |IC均值| < threshold 则淘汰
IC_IR_CULL_THRESHOLD = 0.3     # IC_IR < threshold 则淘汰
IC_CULL_PERSIST_MONTHS = 3     # 连续N个月满足条件才淘汰

# 下夎偂榛樿墽傛算
DEFAULT_TOP_N = 50
DEFAULT_STOCK_POOL = '000300.SH'  # 娌繁300价愬垎鑲′綔嬭洪粯据ゆ略

# ============================================================
# 屾炴杩琛垚儏数据婧愰厤缃?# ============================================================
# tick 数据婧? 'mootdx' (下氳揪信″崗据? 价?'gm' (鎺橀噾忚忓寲据㈤槄妯″接)
# 注意: gm 使用多进程模式运行，避免主线程椂突
TICK_PROVIDER = os.getenv("TICK_PROVIDER", "mootdx")
# 是否允许数据源失败时回退到 mock 行情悜生产建议关闭）
REALTIME_ALLOW_MOCK_FALLBACK = os.getenv("REALTIME_ALLOW_MOCK_FALLBACK", "0").lower() in ("1", "true", "yes", "on")

# 鎺橀噾忚忓寲閰嶇疆锛圱ICK_PROVIDER='gm' 间控敓上堬栵
GM_TOKEN = os.getenv("GM_TOKEN", "086b66e24238b64b085590bce650f9c6ee755004")          # 鎺橀噾忚忓寲 token
GM_MODE = int(os.getenv("GM_MODE", "1"))      # 1=MODE_LIVE(屾炴杩), 2=MODE_BACKTEST(鍥炴祴)

# ============================================================
# 屾炴杩监控池 UI 閰嶇疆
# ============================================================
REALTIME_UI_CONFIG = {
    "pool1_observe_ui": {
        "observe_poll_sec": int(os.getenv("POOL1_OBSERVE_POLL_SEC", "10")),
        "stale_warn_sec": int(os.getenv("POOL1_OBSERVE_STALE_WARN_SEC", "30")),
        "stale_error_sec": int(os.getenv("POOL1_OBSERVE_STALE_ERROR_SEC", "120")),
        "sample_warn_min": int(os.getenv("POOL1_OBSERVE_SAMPLE_WARN_MIN", "50")),
        "sample_recommend_min": int(os.getenv("POOL1_OBSERVE_SAMPLE_RECOMMEND_MIN", "200")),
    }
}

# ============================================================
# Pool1 闃舵2锛轨有冩壒寰侊栵板樻强閰嶇疆
# ============================================================
POOL1_STAGE2_CONFIG = {
    "enabled": os.getenv("POOL1_STAGE2_ENABLED", "0").lower() in ("1", "true", "yes", "on"),
    "chip": {
        "enabled": os.getenv("POOL1_CHIP_ENABLED", "0").lower() in ("1", "true", "yes", "on"),
        "score_weight": int(os.getenv("POOL1_CHIP_SCORE_WEIGHT", "12")),
        "winner_rate_min": float(os.getenv("POOL1_CHIP_WINNER_RATE_MIN", "0.45")),
        "concentration_band_max_pct": float(os.getenv("POOL1_CHIP_CONCENTRATION_BAND_MAX_PCT", "35")),
    },
}

# ============================================================
# Pool1锛忚时间舵略锛塚2 閰嶇疆
# ============================================================
POOL1_SIGNAL_CONFIG = {
    "enabled_v2": os.getenv("POOL1_SIGNAL_V2_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
    "confirm": {
        "bid_ask_ratio": float(os.getenv("POOL1_CONFIRM_BID_ASK_RATIO", "1.12")),
    },
    "veto": {
        "veto_lure_long": os.getenv("POOL1_VETO_LURE_LONG", "1").lower() in ("1", "true", "yes", "on"),
        "veto_wash_trade": os.getenv("POOL1_VETO_WASH_TRADE", "1").lower() in ("1", "true", "yes", "on"),
    },
    "dynamic": {
        "enabled": os.getenv("POOL1_DYNAMIC_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
    },
    "limit_magnet": {
        "enabled": os.getenv("POOL1_LIMIT_MAGNET_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "threshold_pct": float(os.getenv("POOL1_LIMIT_MAGNET_THRESHOLD_PCT", "0.3")),
    },
    "resonance_60m": {
        "enabled": os.getenv("POOL1_RESONANCE_60M_ENABLED", "0").lower() in ("1", "true", "yes", "on"),
        "refresh_sec": int(os.getenv("POOL1_RESONANCE_60M_REFRESH_SEC", "120")),
    },
    "scoring": {
        "score_executable": float(os.getenv("POOL1_SCORE_EXECUTABLE", "70")),
        "score_observe": float(os.getenv("POOL1_SCORE_OBSERVE", "55")),
    },
    "dedup_sec": {
        "left_side_buy": int(os.getenv("POOL1_DEDUP_LEFT_SEC", "300")),
        "right_side_breakout": int(os.getenv("POOL1_DEDUP_RIGHT_SEC", "300")),
    },
    "decay_half_life_min": {
        "left_side_buy": float(os.getenv("POOL1_HALFLIFE_LEFT_MIN", "4")),
        "right_side_breakout": float(os.getenv("POOL1_HALFLIFE_RIGHT_MIN", "6")),
    },
}

# ============================================================
# T+0 信″类策略閰嶇疆
# ============================================================
T0_SIGNAL_CONFIG = {
    "enabled_v2": True,
    "trigger": {
        "bias_vwap_pct": -1.5,
        "robust_zscore": 2.2,
        "eps_break": 0.003,
        "eps_over": 0.002,
    },
    "confirm": {
        "bid_ask_ratio": 1.15,
        "ask_wall_absorb_ratio": 0.6,
        "gub5_transitions": ("up->flat", "up->down"),
    },
    "veto": {
        "spoofing_vol_ratio": 3.0,
        "wash_trade_bias_floor": 0.05,
    },
    "scoring": {
        "score_executable": 70,
        "score_observe": 55,
        "micro_gate_floor": 0.10,
        "risk_gate_floor": 0.30,
    },
    "hysteresis": {
        "positive_t_pct": 0.5,
        "reverse_t_pct": 0.5,
    },
    "dedup_sec": {
        "positive_t": 120,
        "reverse_t": 90,
    },
    "quality": {
        "horizons_sec": [60, 180, 300],
        "alert_precision_drop": 0.08,
    },
    "drift": {
        "psi_threshold": 0.2,
        "ks_p_threshold": 0.05,
        "window": 120,
    },
    "decay": {
        "open": 0.15,
        "morning": 0.08,
        "afternoon": 0.12,
        "close": 0.20,
        "hard_expiry_min": 12,
        "min_floor": 0.15,
    },
}

# ============================================================
# 鍔銊︹偓噺槇反煎紩鎿庨厤缃术嶈氬洜瀛?/ 嶈氭杩娈?/ 嶉ㄩ摼岀栵
# ============================================================
DYNAMIC_THRESHOLD_CONFIG = {
    "enabled": os.getenv("DYNAMIC_THRESHOLD_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
    "version": os.getenv("DYNAMIC_THRESHOLD_VERSION", "dyn-v1"),
    "volatility_regime": {
        "high": float(os.getenv("DYNAMIC_VOL_REGIME_HIGH", "0.30")),
        "mid": float(os.getenv("DYNAMIC_VOL_REGIME_MID", "0.15")),
    },
    "limit_magnet": {
        "enabled": os.getenv("DYNAMIC_LIMIT_MAGNET_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "distance_pct": float(os.getenv("DYNAMIC_LIMIT_MAGNET_DISTANCE_PCT", "1.5")),
        "actions": {
            "left_side_buy": os.getenv("DYNAMIC_LIMIT_ACTION_LEFT", "blocked"),
            "right_side_breakout": os.getenv("DYNAMIC_LIMIT_ACTION_RIGHT", "observer_only"),
            "positive_t": os.getenv("DYNAMIC_LIMIT_ACTION_POS_T", "none"),
            "reverse_t": os.getenv("DYNAMIC_LIMIT_ACTION_REV_T", "none"),
        },
    },
    "base": {
        "left_side_buy": {
            "near_lower_th": 0.5,
            "rsi_oversold": 30.0,
        },
        "right_side_breakout": {
            "eps_mid": 1.002,
            "eps_upper": 1.001,
            "breakout_max_offset_pct": 3.0,
        },
        "positive_t": {
            "bias_vwap_th": -1.5,
            "eps_break": 0.003,
        },
        "reverse_t": {
            "z_th": 2.2,
            "eps_over": 0.002,
        },
    },
    "phase_overrides": {
        "open": {
            "reverse_t": {"z_th": 2.6},
            "positive_t": {"bias_vwap_th": -1.8},
        },
        "close": {
            "reverse_t": {"z_th": 2.8},
            "positive_t": {"bias_vwap_th": -2.0},
        },
    },
    "regime_overrides": {
        "high_vol": {
            "left_side_buy": {"near_lower_th": 0.8},
            "right_side_breakout": {"eps_mid": 1.001, "eps_upper": 1.0005},
            "positive_t": {"bias_vwap_th": -2.0},
            "reverse_t": {"z_th": 2.6},
        },
        "low_vol": {
            "positive_t": {"bias_vwap_th": -1.2},
            "reverse_t": {"z_th": 2.0},
        },
    },
}

