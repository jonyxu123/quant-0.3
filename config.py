"""StockTradingApp v8.0 全局配置"""
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

# Realtime hot DB (optional split). When unset, fallback to DB_PATH.
_REALTIME_DB_PATH_ENV = os.getenv("REALTIME_DB_PATH")
if _REALTIME_DB_PATH_ENV:
    REALTIME_DB_PATH = (
        _REALTIME_DB_PATH_ENV
        if os.path.isabs(_REALTIME_DB_PATH_ENV)
        else os.path.join(_BASE_DIR, _REALTIME_DB_PATH_ENV)
    )
else:
    REALTIME_DB_PATH = DB_PATH

# 日志
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# 同步频率配置
TUSHARE_HIGH_FREQ = 500    # 每 500 分钟（daily, daily_basic）
TUSHARE_MID_FREQ = 200     # 每 200 分钟（income, fina_indicator）
TUSHARE_STK_FACTOR_FREQ = 30  # 每 30 分钟（stk_factor_pro，高频增量）
AKSHARE_FREQ = 60          # 每 60 分钟（板块等）

# 因子维度与前缀
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

# 选股默认参数
DEFAULT_TOP_N = 50
DEFAULT_STOCK_POOL = '000300.SH'  # 沪深300成分股作为默认策略池

# ============================================================
# 实时行情数据源配置
# ============================================================
# tick 数据源: 'mootdx'（通达信协议）或 'gm'（掘金量化）
# 注意: gm 使用多进程模式运行，避免主线程冲突
TICK_PROVIDER = os.getenv("TICK_PROVIDER", "mootdx")
# 是否允许数据源失败时回退到 mock 行情（生产建议关闭）
REALTIME_ALLOW_MOCK_FALLBACK = os.getenv("REALTIME_ALLOW_MOCK_FALLBACK", "0").lower() in ("1", "true", "yes", "on")

# 掘金量化配置（TICK_PROVIDER='gm' 时生效）
GM_TOKEN = os.getenv("GM_TOKEN", "")          # 掘金量化 token（必须通过环境变量配置）
GM_MODE = int(os.getenv("GM_MODE", "1"))      # 1=MODE_LIVE(实时), 2=MODE_BACKTEST(回测)

# ============================================================
# 实时监控池 UI 配置
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
# 实时监控 DuckDB Writer 配置（第三步：单写线程 + 可选 Redis 缓冲）
# ============================================================
REALTIME_DB_WRITER_CONFIG = {
    "flush_interval_sec": float(os.getenv("REALTIME_DB_WRITER_FLUSH_SEC", "0.5")),
    "max_batch": int(os.getenv("REALTIME_DB_WRITER_MAX_BATCH", "1000")),
    "redis": {
        "enabled": os.getenv("REALTIME_DB_WRITER_REDIS_ENABLED", "0").lower() in ("1", "true", "yes", "on"),
        "url": os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0"),
        "list_key": os.getenv("REALTIME_DB_WRITER_REDIS_LIST_KEY", "quant:realtime:db_write"),
    },
}

# ============================================================
# Runtime state Redis（质量滑窗 + 实时统计持久化）
# ============================================================
REALTIME_RUNTIME_STATE_CONFIG = {
    "redis": {
        "enabled": os.getenv("RUNTIME_STATE_REDIS_ENABLED", "0").lower() in ("1", "true", "yes", "on"),
        "url": os.getenv("RUNTIME_STATE_REDIS_URL", os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0")),
        "key_prefix": os.getenv("RUNTIME_STATE_REDIS_KEY_PREFIX", "quant:runtime"),
        "ttl_days": int(os.getenv("RUNTIME_STATE_REDIS_TTL_DAYS", "7")),
        "t0_quality_window_size": int(os.getenv("RUNTIME_T0_QUALITY_WINDOW_SIZE", "500")),
    },
}

# ============================================================
# Pool1 两阶段观测统计存储配置（Redis 可选，默认关闭）
# ============================================================
POOL1_OBSERVE_STORAGE_CONFIG = {
    "redis": {
        "enabled": os.getenv("POOL1_OBSERVE_REDIS_ENABLED", "0").lower() in ("1", "true", "yes", "on"),
        "url": os.getenv("POOL1_OBSERVE_REDIS_URL", os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0")),
        "key_prefix": os.getenv("POOL1_OBSERVE_REDIS_KEY_PREFIX", "quant:pool1:observe"),
        "ttl_days": int(os.getenv("POOL1_OBSERVE_REDIS_TTL_DAYS", "7")),
    },
}

# ============================================================
# Pool1 阶段2（筹码特征）增强配置
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
# Pool1（择时策略）V2 配置
# ============================================================
POOL1_SIGNAL_CONFIG = {
    "enabled_v2": os.getenv("POOL1_SIGNAL_V2_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
    # 当日信号展示策略：默认当日持续展示，直到出现新信号覆盖或跨日重置。
    "keep_signals_until_eod": os.getenv("POOL1_KEEP_SIGNALS_UNTIL_EOD", "1").lower() in ("1", "true", "yes", "on"),
    "confirm": {
        "bid_ask_ratio": float(os.getenv("POOL1_CONFIRM_BID_ASK_RATIO", "1.12")),
    },
    "left_reclaim": {
        "enabled": os.getenv("POOL1_LEFT_RECLAIM_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "below_lower_min_confirms": int(os.getenv("POOL1_LEFT_RECLAIM_BELOW_MIN_CONFIRMS", "2")),
        "near_lower_min_confirms": int(os.getenv("POOL1_LEFT_RECLAIM_NEAR_MIN_CONFIRMS", "1")),
        "bias_vwap_deep_threshold_pct": float(os.getenv("POOL1_LEFT_RECLAIM_BIAS_VWAP_DEEP_THRESHOLD_PCT", "-1.20")),
        "left_tail_zscore_threshold": float(os.getenv("POOL1_LEFT_RECLAIM_LEFT_TAIL_ZSCORE_THRESHOLD", "-1.60")),
        "bid_ask_ratio_min": float(os.getenv("POOL1_LEFT_RECLAIM_BIDASK_MIN", "1.05")),
        "ask_wall_absorb_ratio_min": float(os.getenv("POOL1_LEFT_RECLAIM_ASK_ABSORB_MIN", "0.55")),
        "super_net_inflow_bps_min": float(os.getenv("POOL1_LEFT_RECLAIM_SUPER_INFLOW_BPS_MIN", "20")),
        "high_position_drop_pct": float(os.getenv("POOL1_LEFT_RECLAIM_HIGH_POSITION_DROP_PCT", "-2.50")),
        "high_position_midline_buffer_pct": float(os.getenv("POOL1_LEFT_RECLAIM_HIGH_POSITION_MIDLINE_BUFFER_PCT", "0.20")),
        "distribution_bid_ask_max": float(os.getenv("POOL1_LEFT_RECLAIM_DISTRIBUTION_BID_ASK_MAX", "0.95")),
        "distribution_ask_absorb_max": float(os.getenv("POOL1_LEFT_RECLAIM_DISTRIBUTION_ASK_ABSORB_MAX", "0.35")),
        "distribution_super_out_bps": float(os.getenv("POOL1_LEFT_RECLAIM_DISTRIBUTION_SUPER_OUT_BPS", "-30")),
        "price_reclaim_buffer_pct": float(os.getenv("POOL1_LEFT_RECLAIM_PRICE_BUFFER_PCT", "0.10")),
        "intraday_avwap_reclaim_buffer_pct": float(os.getenv("POOL1_LEFT_RECLAIM_INTRADAY_AVWAP_RECLAIM_BUFFER_PCT", "0.05")),
        "recent_break_lookback_bars": int(os.getenv("POOL1_LEFT_RECLAIM_LOOKBACK_BARS", "5")),
        "recent_break_eps_pct": float(os.getenv("POOL1_LEFT_RECLAIM_BREAK_EPS_PCT", "0.20")),
        "higher_low_min_rise_pct": float(os.getenv("POOL1_LEFT_RECLAIM_HIGHER_LOW_MIN_RISE_PCT", "0.05")),
        "higher_low_3m_min_rise_pct": float(os.getenv("POOL1_LEFT_RECLAIM_HIGHER_LOW_3M_MIN_RISE_PCT", "0.08")),
        "score_bonus_per_confirm": int(os.getenv("POOL1_LEFT_RECLAIM_SCORE_BONUS_PER_CONFIRM", "4")),
        "max_confirm_bonus": int(os.getenv("POOL1_LEFT_RECLAIM_MAX_CONFIRM_BONUS", "12")),
    },
    "concept_ecology": {
        "enabled": os.getenv("POOL1_CONCEPT_ECOLOGY_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "hard_gate_enabled": os.getenv("POOL1_CONCEPT_ECOLOGY_HARD_GATE_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "block_on_retreat": os.getenv("POOL1_CONCEPT_ECOLOGY_BLOCK_ON_RETREAT", "1").lower() in ("1", "true", "yes", "on"),
        "observe_on_hard_weak": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_HARD_WEAK", "1").lower() in ("1", "true", "yes", "on"),
        "hard_retreat_threshold": float(os.getenv("POOL1_CONCEPT_ECOLOGY_HARD_RETREAT_THRESHOLD", "-20")),
        "hard_weak_threshold": float(os.getenv("POOL1_CONCEPT_ECOLOGY_HARD_WEAK_THRESHOLD", "5")),
        "hard_strong_threshold": float(os.getenv("POOL1_CONCEPT_ECOLOGY_HARD_STRONG_THRESHOLD", "30")),
        "score_board_weight": float(os.getenv("POOL1_CONCEPT_ECOLOGY_SCORE_BOARD_WEIGHT", "30")),
        "score_leader_weight": float(os.getenv("POOL1_CONCEPT_ECOLOGY_SCORE_LEADER_WEIGHT", "25")),
        "score_breadth_weight": float(os.getenv("POOL1_CONCEPT_ECOLOGY_SCORE_BREADTH_WEIGHT", "20")),
        "score_fund_flow_weight": float(os.getenv("POOL1_CONCEPT_ECOLOGY_SCORE_FUND_FLOW_WEIGHT", "15")),
        "score_core_weight": float(os.getenv("POOL1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT", "10")),
        "observe_on_retreat": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_RETREAT", "1").lower() in ("1", "true", "yes", "on"),
        "observe_on_weak": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_WEAK", "0").lower() in ("1", "true", "yes", "on"),
        "observe_on_heat_cliff": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_HEAT_CLIFF", "1").lower() in ("1", "true", "yes", "on"),
        "joint_enabled": os.getenv("POOL1_CONCEPT_ECOLOGY_JOINT_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "industry_context_enabled": os.getenv("POOL1_CONCEPT_ECOLOGY_INDUSTRY_CONTEXT_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "observe_on_joint_retreat": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_JOINT_RETREAT", "1").lower() in ("1", "true", "yes", "on"),
        "observe_on_weak_no_support": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_WEAK_NO_SUPPORT", "1").lower() in ("1", "true", "yes", "on"),
        "retreat_score_max": float(os.getenv("POOL1_CONCEPT_ECOLOGY_RETREAT_SCORE_MAX", "-20")),
        "weak_score_max": float(os.getenv("POOL1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX", "5")),
        "heat_cliff_score_max": float(os.getenv("POOL1_CONCEPT_ECOLOGY_HEAT_CLIFF_SCORE_MAX", "-8")),
        "heat_cliff_breadth_max": float(os.getenv("POOL1_CONCEPT_ECOLOGY_HEAT_CLIFF_BREADTH_MAX", "0.38")),
        "heat_cliff_leader_pct_max": float(os.getenv("POOL1_CONCEPT_ECOLOGY_HEAT_CLIFF_LEADER_PCT_MAX", "1.5")),
        "joint_retreat_min_count": int(os.getenv("POOL1_CONCEPT_ECOLOGY_JOINT_RETREAT_MIN_COUNT", "2")),
        "secondary_support_min_count": int(os.getenv("POOL1_CONCEPT_ECOLOGY_SECONDARY_SUPPORT_MIN_COUNT", "1")),
        "strong_score_min": float(os.getenv("POOL1_CONCEPT_ECOLOGY_STRONG_SCORE_MIN", "30")),
        "expand_score_min": float(os.getenv("POOL1_CONCEPT_ECOLOGY_EXPAND_SCORE_MIN", "45")),
        "strong_bonus": int(os.getenv("POOL1_CONCEPT_ECOLOGY_STRONG_BONUS", "4")),
        "expand_bonus": int(os.getenv("POOL1_CONCEPT_ECOLOGY_EXPAND_BONUS", "6")),
        "secondary_support_bonus": int(os.getenv("POOL1_CONCEPT_ECOLOGY_SECONDARY_SUPPORT_BONUS", "3")),
        "weak_penalty": int(os.getenv("POOL1_CONCEPT_ECOLOGY_WEAK_PENALTY", "5")),
        "retreat_penalty": int(os.getenv("POOL1_CONCEPT_ECOLOGY_RETREAT_PENALTY", "10")),
        "joint_retreat_penalty": int(os.getenv("POOL1_CONCEPT_ECOLOGY_JOINT_RETREAT_PENALTY", "6")),
        "weak_no_support_penalty": int(os.getenv("POOL1_CONCEPT_ECOLOGY_WEAK_NO_SUPPORT_PENALTY", "4")),
        "industry_support_bonus": int(os.getenv("POOL1_CONCEPT_ECOLOGY_INDUSTRY_SUPPORT_BONUS", "3")),
        "industry_joint_weak_penalty": int(os.getenv("POOL1_CONCEPT_ECOLOGY_INDUSTRY_JOINT_WEAK_PENALTY", "5")),
        "observe_on_industry_joint_weak": os.getenv("POOL1_CONCEPT_ECOLOGY_OBSERVE_ON_INDUSTRY_JOINT_WEAK", "1").lower() in ("1", "true", "yes", "on"),
    },
    "left_streak_guard": {
        "enabled": os.getenv("POOL1_LEFT_STREAK_GUARD_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "only_left_side_buy": os.getenv("POOL1_LEFT_STREAK_ONLY_LEFT", "1").lower() in ("1", "true", "yes", "on"),
        "cooldown_hours": float(os.getenv("POOL1_LEFT_STREAK_COOLDOWN_HOURS", "72")),
        "same_day_observe_hours": float(os.getenv("POOL1_LEFT_STREAK_SAME_DAY_OBSERVE_HOURS", "18")),
        "quick_clear_max_hold_days": float(os.getenv("POOL1_LEFT_STREAK_QUICK_CLEAR_MAX_HOLD_DAYS", "3.0")),
        "repeat_streak_threshold": int(os.getenv("POOL1_LEFT_STREAK_REPEAT_THRESHOLD", "2")),
        "recent_clear_penalty": int(os.getenv("POOL1_LEFT_STREAK_RECENT_CLEAR_PENALTY", "4")),
        "weak_penalty": int(os.getenv("POOL1_LEFT_STREAK_WEAK_PENALTY", "4")),
        "retreat_penalty": int(os.getenv("POOL1_LEFT_STREAK_RETREAT_PENALTY", "8")),
        "repeat_weak_penalty": int(os.getenv("POOL1_LEFT_STREAK_REPEAT_WEAK_PENALTY", "6")),
        "repeat_retreat_penalty": int(os.getenv("POOL1_LEFT_STREAK_REPEAT_RETREAT_PENALTY", "12")),
        "observe_on_recent_clear": os.getenv("POOL1_LEFT_STREAK_OBSERVE_ON_RECENT_CLEAR", "0").lower() in ("1", "true", "yes", "on"),
        "observe_on_weak": os.getenv("POOL1_LEFT_STREAK_OBSERVE_ON_WEAK", "0").lower() in ("1", "true", "yes", "on"),
        "observe_on_retreat": os.getenv("POOL1_LEFT_STREAK_OBSERVE_ON_RETREAT", "1").lower() in ("1", "true", "yes", "on"),
        "observe_on_repeat_weak": os.getenv("POOL1_LEFT_STREAK_OBSERVE_ON_REPEAT_WEAK", "0").lower() in ("1", "true", "yes", "on"),
        "observe_on_repeat_retreat": os.getenv("POOL1_LEFT_STREAK_OBSERVE_ON_REPEAT_RETREAT", "1").lower() in ("1", "true", "yes", "on"),
    },
    "rebuild": {
        "enabled": os.getenv("POOL1_REBUILD_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "require_stage1_pass": os.getenv("POOL1_REBUILD_REQUIRE_STAGE1", "1").lower() in ("1", "true", "yes", "on"),
        "min_position_gap": float(os.getenv("POOL1_REBUILD_MIN_POSITION_GAP", "0.12")),
        "min_minutes_after_reduce": float(os.getenv("POOL1_REBUILD_MIN_MINUTES_AFTER_REDUCE", "20")),
        "min_signal_strength": float(os.getenv("POOL1_REBUILD_MIN_SIGNAL_STRENGTH", "78")),
        "add_ratio": float(os.getenv("POOL1_REBUILD_ADD_RATIO", "0.50")),
        "observe_tier_add_ratio_bias": float(os.getenv("POOL1_REBUILD_OBSERVE_TIER_ADD_RATIO_BIAS", "0.15")),
        "full_tier_add_ratio_bias": float(os.getenv("POOL1_REBUILD_FULL_TIER_ADD_RATIO_BIAS", "-0.15")),
        "soft_source_add_ratio_bias": float(os.getenv("POOL1_REBUILD_SOFT_SOURCE_ADD_RATIO_BIAS", "0.10")),
        "core_source_add_ratio_bias": float(os.getenv("POOL1_REBUILD_CORE_SOURCE_ADD_RATIO_BIAS", "-0.15")),
        "full_tier_extra_delay_minutes": float(os.getenv("POOL1_REBUILD_FULL_TIER_EXTRA_DELAY_MINUTES", "20")),
        "core_source_extra_delay_minutes": float(os.getenv("POOL1_REBUILD_CORE_SOURCE_EXTRA_DELAY_MINUTES", "15")),
        "allow_left_side_buy": os.getenv("POOL1_REBUILD_ALLOW_LEFT", "1").lower() in ("1", "true", "yes", "on"),
        "allow_right_side_breakout": os.getenv("POOL1_REBUILD_ALLOW_RIGHT", "1").lower() in ("1", "true", "yes", "on"),
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
    # 择时持仓状态（observe/holding）持久化配置
    "position_state": {
        "enabled": os.getenv("POOL1_POSITION_STATE_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "file": os.getenv("POOL1_POSITION_STATE_FILE", ".pool1_position_state.json"),
        "max_items": int(os.getenv("POOL1_POSITION_STATE_MAX_ITEMS", "5000")),
        "file_fallback": os.getenv("POOL1_POSITION_STATE_FILE_FALLBACK", "1").lower() in ("1", "true", "yes", "on"),
        "redis": {
            "enabled": os.getenv(
                "POOL1_POSITION_REDIS_ENABLED",
                os.getenv("RUNTIME_STATE_REDIS_ENABLED", "0"),
            ).lower() in ("1", "true", "yes", "on"),
            "url": os.getenv(
                "POOL1_POSITION_REDIS_URL",
                os.getenv(
                    "RUNTIME_STATE_REDIS_URL",
                    os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0"),
                ),
            ),
            "key_prefix": os.getenv("POOL1_POSITION_REDIS_KEY_PREFIX", "quant:pool1:position"),
            "ttl_days": int(
                os.getenv(
                    "POOL1_POSITION_REDIS_TTL_DAYS",
                    os.getenv("RUNTIME_STATE_REDIS_TTL_DAYS", "14"),
                )
            ),
        },
    },
    # 择时池清仓信号（持仓态专用）
    "exit": {
        "enabled": os.getenv("POOL1_EXIT_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        # 择时池以中持仓周期为主，默认先持有约两周后再考虑常规清仓。
        "min_hold_days": float(os.getenv("POOL1_EXIT_MIN_HOLD_DAYS", "14.0")),
        # 长持仓下提高清仓确认门槛，避免因短时噪声过早离场。
        "long_hold_days": float(os.getenv("POOL1_EXIT_LONG_HOLD_DAYS", "14.0")),
        "long_hold_min_confirm": int(os.getenv("POOL1_EXIT_LONG_HOLD_MIN_CONFIRM", "3")),
        "allow_early_risk_exit": os.getenv("POOL1_EXIT_ALLOW_EARLY_RISK", "1").lower() in ("1", "true", "yes", "on"),
        "hard_drop_pct": float(os.getenv("POOL1_EXIT_HARD_DROP_PCT", "-5.0")),
        "rsi_weak_th": float(os.getenv("POOL1_EXIT_RSI_WEAK_TH", "42")),
        "rsi_hot_th": float(os.getenv("POOL1_EXIT_RSI_HOT_TH", "72")),
        "take_profit_day_gain_pct": float(os.getenv("POOL1_EXIT_TAKE_PROFIT_DAY_GAIN_PCT", "3.0")),
        "take_profit_near_upper_pct": float(os.getenv("POOL1_EXIT_TAKE_PROFIT_NEAR_UPPER_PCT", "0.20")),
        "partial_reduce_ratio": float(os.getenv("POOL1_EXIT_PARTIAL_REDUCE_RATIO", "0.50")),
        "beta_reduce_ratio": float(os.getenv("POOL1_EXIT_BETA_REDUCE_RATIO", "0.33")),
        "observe_reduce_ratio": float(os.getenv("POOL1_EXIT_OBSERVE_REDUCE_RATIO", "0.25")),
        "atr_low_pct": float(os.getenv("POOL1_EXIT_ATR_LOW_PCT", "0.020")),
        "atr_high_pct": float(os.getenv("POOL1_EXIT_ATR_HIGH_PCT", "0.045")),
        "atr_low_extra_confirm": int(os.getenv("POOL1_EXIT_ATR_LOW_EXTRA_CONFIRM", "1")),
        "atr_high_relax_confirm": int(os.getenv("POOL1_EXIT_ATR_HIGH_RELAX_CONFIRM", "1")),
        "bid_ask_weak_ratio": float(os.getenv("POOL1_EXIT_BIDASK_WEAK_RATIO", "0.95")),
        "big_order_bias_sell_th": float(os.getenv("POOL1_EXIT_BIG_BIAS_SELL_TH", "-0.25")),
        "super_order_bias_sell_th": float(os.getenv("POOL1_EXIT_SUPER_BIAS_SELL_TH", "-0.20")),
        "big_net_outflow_bps_th": float(os.getenv("POOL1_EXIT_BIG_NET_OUT_BPS_TH", "-80")),
        "super_net_outflow_bps_th": float(os.getenv("POOL1_EXIT_SUPER_NET_OUT_BPS_TH", "-120")),
    },
    "exit_anchor": {
        "enabled": os.getenv("POOL1_EXIT_ANCHOR_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "entry_cost_break_pct": float(os.getenv("POOL1_EXIT_ENTRY_COST_BREAK_PCT", "1.20")),
        "entry_avwap_break_pct": float(os.getenv("POOL1_EXIT_ENTRY_AVWAP_BREAK_PCT", "0.35")),
        "entry_avwap_reduce_ratio": float(os.getenv("POOL1_EXIT_ENTRY_AVWAP_REDUCE_RATIO", "1.00")),
        "breakout_avwap_reduce_ratio": float(os.getenv("POOL1_EXIT_BREAKOUT_AVWAP_REDUCE_RATIO", "0.50")),
        "event_avwap_reduce_ratio": float(os.getenv("POOL1_EXIT_EVENT_AVWAP_REDUCE_RATIO", "0.40")),
        "session_avwap_reduce_ratio": float(os.getenv("POOL1_EXIT_SESSION_AVWAP_REDUCE_RATIO", "0.20")),
        "entry_cost_reduce_ratio": float(os.getenv("POOL1_EXIT_ENTRY_COST_REDUCE_RATIO", "0.25")),
        "avwap_soft_weak_reduce_ratio": float(os.getenv("POOL1_EXIT_AVWAP_SOFT_WEAK_REDUCE_RATIO", "0.15")),
        "take_profit_cost_premium_pct": float(os.getenv("POOL1_EXIT_TAKE_PROFIT_COST_PREMIUM_PCT", "6.00")),
        "beta_reduce_cost_premium_pct": float(os.getenv("POOL1_EXIT_BETA_REDUCE_COST_PREMIUM_PCT", "3.50")),
        "intraday_avwap_min_bars": int(os.getenv("POOL1_EXIT_INTRADAY_AVWAP_MIN_BARS", "8")),
        "session_avwap_enabled": os.getenv("POOL1_EXIT_SESSION_AVWAP_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "timing_clear_avwap_flow_required": os.getenv("POOL1_EXIT_TIMING_CLEAR_AVWAP_FLOW_REQUIRED", "1").lower() in ("1", "true", "yes", "on"),
    },
    "breakout_anchor": {
        "enabled": os.getenv("POOL1_BREAKOUT_ANCHOR_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "lookback_bars": int(os.getenv("POOL1_BREAKOUT_ANCHOR_LOOKBACK_BARS", "30")),
        "high_lookback_bars": int(os.getenv("POOL1_BREAKOUT_ANCHOR_HIGH_LOOKBACK_BARS", "5")),
        "volume_expand_ratio": float(os.getenv("POOL1_BREAKOUT_ANCHOR_VOLUME_EXPAND_RATIO", "1.35")),
        "min_anchor_bars": int(os.getenv("POOL1_BREAKOUT_ANCHOR_MIN_BARS", "5")),
        "avwap_break_pct": float(os.getenv("POOL1_BREAKOUT_ANCHOR_AVWAP_BREAK_PCT", "0.30")),
        "beta_reduce_premium_pct": float(os.getenv("POOL1_BREAKOUT_ANCHOR_BETA_REDUCE_PREMIUM_PCT", "2.50")),
        "require_avwap_control": os.getenv("POOL1_BREAKOUT_ANCHOR_REQUIRE_AVWAP_CONTROL", "1").lower() in ("1", "true", "yes", "on"),
        "min_control_count": int(os.getenv("POOL1_BREAKOUT_ANCHOR_MIN_CONTROL_COUNT", "1")),
        "allow_missing_stack": os.getenv("POOL1_BREAKOUT_ANCHOR_ALLOW_MISSING_STACK", "1").lower() in ("1", "true", "yes", "on"),
        "allow_session_avwap_control": os.getenv("POOL1_BREAKOUT_ANCHOR_ALLOW_SESSION_AVWAP_CONTROL", "1").lower() in ("1", "true", "yes", "on"),
    },
    "event_anchor": {
        "enabled": os.getenv("POOL1_EVENT_ANCHOR_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "lookback_bars": int(os.getenv("POOL1_EVENT_ANCHOR_LOOKBACK_BARS", "60")),
        "high_lookback_bars": int(os.getenv("POOL1_EVENT_ANCHOR_HIGH_LOOKBACK_BARS", "3")),
        "volume_expand_ratio": float(os.getenv("POOL1_EVENT_ANCHOR_VOLUME_EXPAND_RATIO", "1.50")),
        "impulse_pct": float(os.getenv("POOL1_EVENT_ANCHOR_IMPULSE_PCT", "1.00")),
        "min_anchor_bars": int(os.getenv("POOL1_EVENT_ANCHOR_MIN_BARS", "4")),
        "avwap_break_pct": float(os.getenv("POOL1_EVENT_ANCHOR_AVWAP_BREAK_PCT", "0.25")),
        "beta_reduce_premium_pct": float(os.getenv("POOL1_EVENT_ANCHOR_BETA_REDUCE_PREMIUM_PCT", "2.00")),
    },
    "scoring": {
        "score_executable": float(os.getenv("POOL1_SCORE_EXECUTABLE", "70")),
        "score_observe": float(os.getenv("POOL1_SCORE_OBSERVE", "55")),
    },
    # 盘中量能进度：当日累计量相对昨量，并按交易时段进度归一化
    "volume_pace": {
        "enabled": os.getenv("POOL1_VOLUME_PACE_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "min_progress": float(os.getenv("POOL1_VOLUME_PACE_MIN_PROGRESS", "0.15")),
        "shrink_th": float(os.getenv("POOL1_VOLUME_PACE_SHRINK_TH", "0.70")),
        "expand_th": float(os.getenv("POOL1_VOLUME_PACE_EXPAND_TH", "1.30")),
        "surge_th": float(os.getenv("POOL1_VOLUME_PACE_SURGE_TH", "2.00")),
        "left_shrink_penalty": int(os.getenv("POOL1_VOLUME_PACE_LEFT_SHRINK_PENALTY", "10")),
        "left_expand_bonus": int(os.getenv("POOL1_VOLUME_PACE_LEFT_EXPAND_BONUS", "4")),
        "left_surge_bonus": int(os.getenv("POOL1_VOLUME_PACE_LEFT_SURGE_BONUS", "6")),
        "right_min_pace": float(os.getenv("POOL1_VOLUME_PACE_RIGHT_MIN_PACE", "0.75")),
        "right_expand_bonus": int(os.getenv("POOL1_VOLUME_PACE_RIGHT_EXPAND_BONUS", "6")),
        "right_surge_bonus": int(os.getenv("POOL1_VOLUME_PACE_RIGHT_SURGE_BONUS", "8")),
    },
    "dedup_sec": {
        "left_side_buy": int(os.getenv("POOL1_DEDUP_LEFT_SEC", "300")),
        "right_side_breakout": int(os.getenv("POOL1_DEDUP_RIGHT_SEC", "300")),
        "timing_clear": int(os.getenv("POOL1_DEDUP_CLEAR_SEC", "900")),
    },
    "decay_half_life_min": {
        "left_side_buy": float(os.getenv("POOL1_HALFLIFE_LEFT_MIN", "4")),
        "right_side_breakout": float(os.getenv("POOL1_HALFLIFE_RIGHT_MIN", "6")),
        "timing_clear": float(os.getenv("POOL1_HALFLIFE_CLEAR_MIN", "8")),
    },
}

# ============================================================
# T+0 信号策略配置
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
        "bid_wall_break_ratio": float(os.getenv("T0_BID_WALL_BREAK_RATIO", "0.75")),
        "gub5_transitions": ("up->flat", "up->down"),
        "super_net_inflow_buy_bps": float(os.getenv("T0_SUPER_NET_INFLOW_BUY_BPS", "120")),
        "super_net_inflow_sell_bps": float(os.getenv("T0_SUPER_NET_INFLOW_SELL_BPS", "120")),
        "super_order_bias_buy_th": float(os.getenv("T0_SUPER_ORDER_BIAS_BUY_TH", "0.20")),
        "super_order_bias_sell_th": float(os.getenv("T0_SUPER_ORDER_BIAS_SELL_TH", "0.20")),
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
        "positive_t_pct": float(os.getenv("T0_HYST_POS_PCT", "0.5")),
        "reverse_t_pct": float(os.getenv("T0_HYST_REV_PCT", "0.5")),
    },
    "dedup_sec": {
        "positive_t": int(os.getenv("T0_DEDUP_POS_SEC", "120")),
        "reverse_t": int(os.getenv("T0_DEDUP_REV_SEC", "90")),
    },
    "anti_churn": {
        "enabled": os.getenv("T0_ANTI_CHURN_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "min_flip_sec": int(os.getenv("T0_MIN_FLIP_SEC", "120")),
        "min_flip_spread_pct": float(os.getenv("T0_MIN_FLIP_SPREAD_PCT", "0.35")),
        "roundtrip_fee_bps": float(os.getenv("T0_ROUNDTRIP_FEE_BPS", "16")),
        "min_net_edge_bps": float(os.getenv("T0_MIN_NET_EDGE_BPS", "6")),
        "min_expected_edge_bps": float(os.getenv("T0_MIN_EXPECTED_EDGE_BPS", "25")),
        "reverse_bidask_block_th": float(os.getenv("T0_REVERSE_BIDASK_BLOCK_TH", "1.22")),
        "reverse_vwap_premium_bps": float(os.getenv("T0_REVERSE_VWAP_PREMIUM_BPS", "18")),
    },
    # 盘中量能进度（pace_ratio = (cum_vol / prev_day_vol) / progress_ratio）
    "volume_pace": {
        "enabled": os.getenv("T0_VOLUME_PACE_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "min_progress": float(os.getenv("T0_VOLUME_PACE_MIN_PROGRESS", "0.12")),
        "shrink_th": float(os.getenv("T0_VOLUME_PACE_SHRINK_TH", "0.70")),
        "expand_th": float(os.getenv("T0_VOLUME_PACE_EXPAND_TH", "1.30")),
        "surge_th": float(os.getenv("T0_VOLUME_PACE_SURGE_TH", "2.00")),
        "positive_shrink_penalty": int(os.getenv("T0_VOLUME_PACE_POS_SHRINK_PENALTY", "8")),
        "positive_expand_bonus": int(os.getenv("T0_VOLUME_PACE_POS_EXPAND_BONUS", "4")),
        "positive_surge_bonus": int(os.getenv("T0_VOLUME_PACE_POS_SURGE_BONUS", "6")),
        "reverse_shrink_bonus": int(os.getenv("T0_VOLUME_PACE_REV_SHRINK_BONUS", "4")),
        "reverse_expand_penalty": int(os.getenv("T0_VOLUME_PACE_REV_EXPAND_PENALTY", "6")),
        "reverse_surge_penalty": int(os.getenv("T0_VOLUME_PACE_REV_SURGE_PENALTY", "10")),
        "reverse_surge_bidask_block_th": float(os.getenv("T0_VOLUME_PACE_REV_SURGE_BIDASK_BLOCK_TH", "1.10")),
    },
    "microstructure": {
        "lure_long_big_bias_min": float(os.getenv("T0_LURE_LONG_BIG_BIAS_MIN", "0.25")),
        "lure_long_bidask_max": float(os.getenv("T0_LURE_LONG_BIDASK_MAX", "0.85")),
        "lure_long_pullup_min_pct": float(os.getenv("T0_LURE_LONG_PULLUP_MIN_PCT", "0.25")),
        "lure_long_retrace_min_pct": float(os.getenv("T0_LURE_LONG_RETRACE_MIN_PCT", "0.12")),
        "lure_long_fade_ratio_max": float(os.getenv("T0_LURE_LONG_FADE_RATIO_MAX", "0.80")),
        "lure_long_net_bps_drop": float(os.getenv("T0_LURE_LONG_NET_BPS_DROP", "30")),
        "divergence_flow_ratio_max": float(os.getenv("T0_DIV_FLOW_RATIO_MAX", "0.85")),
        "divergence_net_bps_drop": float(os.getenv("T0_DIV_NET_BPS_DROP", "25")),
        "divergence_price_eps_pct": float(os.getenv("T0_DIV_PRICE_EPS_PCT", "0.05")),
    },
    "trend_guard": {
        "enabled": os.getenv("T0_TREND_GUARD_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "price_above_vwap_bps_min": float(os.getenv("T0_TREND_GUARD_VWAP_BPS_MIN", "15")),
        "pct_chg_min": float(os.getenv("T0_TREND_GUARD_PCT_CHG_MIN", "1.20")),
        "bid_ask_ratio_min": float(os.getenv("T0_TREND_GUARD_BIDASK_MIN", "1.08")),
        "big_order_bias_min": float(os.getenv("T0_TREND_GUARD_BIG_BIAS_MIN", "0.12")),
        "super_order_bias_min": float(os.getenv("T0_TREND_GUARD_SUPER_BIAS_MIN", "0.10")),
        "super_net_inflow_bps_min": float(os.getenv("T0_TREND_GUARD_SUPER_INFLOW_BPS_MIN", "80")),
        "min_guard_score": int(os.getenv("T0_TREND_GUARD_MIN_SCORE", "3")),
        "min_bearish_confirms": int(os.getenv("T0_TREND_GUARD_MIN_BEARISH", "2")),
        "surge_min_bearish_confirms": int(os.getenv("T0_TREND_GUARD_SURGE_MIN_BEARISH", "3")),
        "surge_absorb_block_enabled": os.getenv("T0_TREND_GUARD_SURGE_BLOCK_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
        "surge_absorb_pct_chg_min": float(os.getenv("T0_TREND_GUARD_SURGE_PCT_CHG_MIN", "1.80")),
        "surge_absorb_bidask_min": float(os.getenv("T0_TREND_GUARD_SURGE_BIDASK_MIN", "1.10")),
        "surge_absorb_big_bias_min": float(os.getenv("T0_TREND_GUARD_SURGE_BIG_BIAS_MIN", "0.10")),
        "surge_absorb_super_inflow_bps_min": float(os.getenv("T0_TREND_GUARD_SURGE_SUPER_INFLOW_BPS_MIN", "100")),
        "surge_absorb_min_bearish_confirms": int(os.getenv("T0_TREND_GUARD_SURGE_MIN_BEARISH_HARD", "4")),
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
# 动态阈值引擎配置（多因子 / 多时段 / 全链路）
# ============================================================
DYNAMIC_THRESHOLD_CONFIG = {
    "enabled": os.getenv("DYNAMIC_THRESHOLD_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
    "version": os.getenv("DYNAMIC_THRESHOLD_VERSION", "dyn-v1"),
    "instrument_model": {
        "ipo_early_days": int(os.getenv("DYNAMIC_IPO_EARLY_DAYS", "60")),
        "ipo_young_days": int(os.getenv("DYNAMIC_IPO_YOUNG_DAYS", "180")),
        "volume_drought": {
            "min_progress": float(os.getenv("DYNAMIC_DROUGHT_MIN_PROGRESS", "0.35")),
            "max_ratio": float(os.getenv("DYNAMIC_DROUGHT_MAX_RATIO", "0.55")),
        },
        "seal_env": {
            "distance_pct": float(os.getenv("DYNAMIC_SEAL_DISTANCE_PCT", "0.35")),
            "one_side_ratio": float(os.getenv("DYNAMIC_SEAL_ONE_SIDE_RATIO", "6.0")),
            "min_opposite_vol": int(os.getenv("DYNAMIC_SEAL_MIN_OPPOSITE_VOL", "1000")),
        },
    },
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
            "timing_clear": os.getenv("DYNAMIC_LIMIT_ACTION_TIMING_CLEAR", "none"),
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
    "session_policies": {
        "open_strict": {
            "left_side_buy": {
                "near_lower_th": 0.35,
                "rsi_oversold": 27.0,
                "session_hint": "open_strict",
            },
            "right_side_breakout": {
                "eps_mid": 1.003,
                "eps_upper": 1.0015,
                "breakout_max_offset_pct": 2.0,
                "session_hint": "open_strict",
            },
            "positive_t": {
                "bias_vwap_th": -1.9,
                "session_hint": "open_strict",
            },
            "reverse_t": {
                "z_th": 2.8,
                "eps_over": 0.0025,
                "session_hint": "open_strict",
            },
        },
        "auction_pause": {
            "left_side_buy": {
                "blocked": True,
                "blocked_reason": "auction_pause",
                "session_hint": "auction_pause",
            },
            "right_side_breakout": {
                "blocked": True,
                "blocked_reason": "auction_pause",
                "session_hint": "auction_pause",
            },
            "timing_clear": {
                "blocked": True,
                "blocked_reason": "auction_pause",
                "session_hint": "auction_pause",
            },
            "positive_t": {
                "blocked": True,
                "blocked_reason": "auction_pause",
                "session_hint": "auction_pause",
            },
            "reverse_t": {
                "blocked": True,
                "blocked_reason": "auction_pause",
                "session_hint": "auction_pause",
            },
        },
        "lunch_pause": {
            "left_side_buy": {
                "blocked": True,
                "blocked_reason": "lunch_pause",
                "session_hint": "lunch_pause",
            },
            "right_side_breakout": {
                "blocked": True,
                "blocked_reason": "lunch_pause",
                "session_hint": "lunch_pause",
            },
            "timing_clear": {
                "blocked": True,
                "blocked_reason": "lunch_pause",
                "session_hint": "lunch_pause",
            },
            "positive_t": {
                "blocked": True,
                "blocked_reason": "lunch_pause",
                "session_hint": "lunch_pause",
            },
            "reverse_t": {
                "blocked": True,
                "blocked_reason": "lunch_pause",
                "session_hint": "lunch_pause",
            },
        },
        "close_reduce": {
            "left_side_buy": {
                "blocked": True,
                "blocked_reason": "close_reduce_no_new_buy",
                "session_hint": "close_reduce",
            },
            "right_side_breakout": {
                "blocked": True,
                "blocked_reason": "close_reduce_no_chase",
                "session_hint": "close_reduce",
            },
            "positive_t": {
                "blocked": True,
                "blocked_reason": "close_reduce_no_new_buy",
                "session_hint": "close_reduce",
            },
            "timing_clear": {
                "session_hint": "close_reduce",
            },
            "reverse_t": {
                "session_hint": "close_reduce",
            },
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
        "drought": {
            "left_side_buy": {"near_lower_th": 0.35, "rsi_oversold": 28.0},
            "right_side_breakout": {"eps_mid": 1.003, "eps_upper": 1.0015, "breakout_max_offset_pct": 2.0},
            "positive_t": {"bias_vwap_th": -1.8},
            "reverse_t": {"z_th": 2.6},
        },
        "seal_env": {
            "left_side_buy": {"blocked": True},
            "right_side_breakout": {"observer_only": True, "eps_mid": 1.003, "eps_upper": 1.0015},
            "positive_t": {"observer_only": True, "bias_vwap_th": -2.0},
            "reverse_t": {"observer_only": True, "z_th": 2.8},
        },
    },
    "profile_overrides": [
        {
            "signal_type": "left_side_buy",
            "board_segment": "gem",
            "params": {"near_lower_th": 0.45, "rsi_oversold": 28.0},
            "reason": "gem_left_side_tighten",
        },
        {
            "signal_type": "right_side_breakout",
            "board_segment": "gem",
            "params": {"eps_mid": 1.0025, "eps_upper": 1.0012, "breakout_max_offset_pct": 2.5},
            "reason": "gem_breakout_tighten",
        },
        {
            "signal_type": "positive_t",
            "board_segment": "gem",
            "params": {"bias_vwap_th": -1.8},
            "reason": "gem_t0_buy_tighten",
        },
        {
            "signal_type": "reverse_t",
            "board_segment": "gem",
            "params": {"z_th": 2.5},
            "reason": "gem_t0_sell_tighten",
        },
        {
            "signal_type": "left_side_buy",
            "board_segment": "star",
            "params": {"near_lower_th": 0.45, "rsi_oversold": 28.0},
            "reason": "star_left_side_tighten",
        },
        {
            "signal_type": "right_side_breakout",
            "board_segment": "star",
            "params": {"eps_mid": 1.0025, "eps_upper": 1.0012, "breakout_max_offset_pct": 2.5},
            "reason": "star_breakout_tighten",
        },
        {
            "signal_type": "positive_t",
            "board_segment": "star",
            "params": {"bias_vwap_th": -1.8},
            "reason": "star_t0_buy_tighten",
        },
        {
            "signal_type": "reverse_t",
            "board_segment": "star",
            "params": {"z_th": 2.5},
            "reason": "star_t0_sell_tighten",
        },
        {
            "signal_type": "left_side_buy",
            "security_type": "etf",
            "params": {"observer_only": True},
            "reason": "etf_pool1_observe_only",
        },
        {
            "signal_type": "right_side_breakout",
            "security_type": "etf",
            "params": {"observer_only": True},
            "reason": "etf_pool1_observe_only",
        },
        {
            "signal_type": "positive_t",
            "security_type": "etf",
            "params": {"bias_vwap_th": -1.1},
            "reason": "etf_t0_buy_smoother",
        },
        {
            "signal_type": "reverse_t",
            "security_type": "etf",
            "params": {"z_th": 1.8},
            "reason": "etf_t0_sell_smoother",
        },
        {
            "signal_type": "left_side_buy",
            "security_type": "risk_warning_stock",
            "params": {"blocked": True},
            "reason": "risk_warning_block_pool1_buy",
        },
        {
            "signal_type": "right_side_breakout",
            "security_type": "risk_warning_stock",
            "params": {"blocked": True},
            "reason": "risk_warning_block_pool1_buy",
        },
        {
            "signal_type": "positive_t",
            "security_type": "risk_warning_stock",
            "params": {"observer_only": True, "bias_vwap_th": -2.2},
            "reason": "risk_warning_t0_observe_only",
        },
        {
            "signal_type": "reverse_t",
            "security_type": "risk_warning_stock",
            "params": {"observer_only": True, "z_th": 3.0},
            "reason": "risk_warning_t0_observe_only",
        },
        {
            "signal_type": "left_side_buy",
            "listing_stage": "ipo_early",
            "params": {"blocked": True},
            "reason": "ipo_early_block_pool1_buy",
        },
        {
            "signal_type": "right_side_breakout",
            "listing_stage": "ipo_early",
            "params": {"blocked": True},
            "reason": "ipo_early_block_pool1_breakout",
        },
        {
            "signal_type": "positive_t",
            "listing_stage": "ipo_early",
            "params": {"observer_only": True, "bias_vwap_th": -2.4},
            "reason": "ipo_early_t0_observe_only",
        },
        {
            "signal_type": "reverse_t",
            "listing_stage": "ipo_early",
            "params": {"observer_only": True, "z_th": 3.2},
            "reason": "ipo_early_t0_observe_only",
        },
        {
            "signal_type": "left_side_buy",
            "listing_stage": "ipo_young",
            "params": {"observer_only": True, "near_lower_th": 0.35, "rsi_oversold": 27.0},
            "reason": "ipo_young_pool1_observe_only",
        },
        {
            "signal_type": "right_side_breakout",
            "listing_stage": "ipo_young",
            "params": {"observer_only": True, "eps_mid": 1.003, "eps_upper": 1.0015},
            "reason": "ipo_young_pool1_observe_only",
        },
        {
            "signal_type": "positive_t",
            "listing_stage": "ipo_young",
            "params": {"bias_vwap_th": -2.0},
            "reason": "ipo_young_t0_tighten",
        },
        {
            "signal_type": "reverse_t",
            "listing_stage": "ipo_young",
            "params": {"z_th": 2.8},
            "reason": "ipo_young_t0_tighten",
        },
    ],
}

# ============================================================
# 动态阈值闭环校准配置（分桶：行业/波动率/时段）
# ============================================================
DYNAMIC_THRESHOLD_CLOSED_LOOP_CONFIG = {
    "enabled": os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_ENABLED", "1").lower() in ("1", "true", "yes", "on"),
    "interval_min": float(os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_INTERVAL_MIN", "15")),
    "window_hours": int(os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_WINDOW_HOURS", "24")),
    "min_samples": int(os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_MIN_SAMPLES", "20")),
    "score_observe_t0": float(os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_SCORE_OBSERVE_T0", "55")),
    "score_observe_pool1": float(os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_SCORE_OBSERVE_POOL1", "55")),
    "targets": {
        # trigger_rate 这里定义为“窗口内每只股票平均触发次数”
        "trigger_per_symbol_low": float(os.getenv("DYNAMIC_THRESHOLD_TARGET_TRIGGER_LOW", "0.3")),
        "trigger_per_symbol_high": float(os.getenv("DYNAMIC_THRESHOLD_TARGET_TRIGGER_HIGH", "3.0")),
        "pass_rate_low": float(os.getenv("DYNAMIC_THRESHOLD_TARGET_PASS_LOW", "0.35")),
        "hit_rate_low": float(os.getenv("DYNAMIC_THRESHOLD_TARGET_HIT_LOW", "0.48")),
        "hit_rate_high": float(os.getenv("DYNAMIC_THRESHOLD_TARGET_HIT_HIGH", "0.62")),
    },
    "steps": {
        "positive_t_bias_vwap": float(os.getenv("DYNAMIC_THRESHOLD_STEP_POS_BIAS", "0.1")),
        "reverse_t_z": float(os.getenv("DYNAMIC_THRESHOLD_STEP_REV_Z", "0.1")),
        "left_near_lower": float(os.getenv("DYNAMIC_THRESHOLD_STEP_LEFT_NEAR", "0.03")),
        "right_eps": float(os.getenv("DYNAMIC_THRESHOLD_STEP_RIGHT_EPS", "0.0003")),
    },
}

