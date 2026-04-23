/**
 * 盯盘页面 - 实时监控池（WebSocket 推送版）
 * 通讯方式：优先 WebSocket，失败后自动降级为 HTTP 轮询
 *
 * WS 协议：客户端->服务端 subscribe/unsubscribe/ping
 * 服务端->客户端 tick/minute/transactions/signals/market_status/pong
 */
import React, { useEffect, useState, useRef, useCallback, useMemo } from 'react'
import axios from 'axios'
import { Search, Plus, Trash2, RefreshCw, Eye, TrendingDown, TrendingUp, Volume2, VolumeX, ChevronDown, ChevronUp, Edit3, Wifi, WifiOff } from 'lucide-react'
import { Link } from 'react-router-dom'
import MiniIntradayChart, { type TickDataPoint } from '../components/MiniIntradayChart'

const API = 'http://localhost:8000'
const WS_URL = 'ws://localhost:8000/api/realtime/ws'

type Pool1ObserveUiThresholds = {
  observePollSec: number
  staleWarnSec: number
  staleErrorSec: number
  sampleWarnMin: number
  sampleRecommendMin: number
}

const POOL1_OBSERVE_UI_DEFAULT: Pool1ObserveUiThresholds = {
  observePollSec: 10,
  staleWarnSec: 30,
  staleErrorSec: 120,
  sampleWarnMin: 50,
  sampleRecommendMin: 200,
}

const UI_CONFIG_CACHE_TTL_MS = 60_000
let _uiConfigCache: RealtimeUiConfigResp | null = null
let _uiConfigCacheAt = 0
const OBSERVE_FAIL_WARN_COUNT = 3
const FAST_SLOW_WARN_RATIO = 0.05
const FAST_SLOW_CRIT_RATIO = 0.10

/* ===== 颜色 ===== */
const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', cyanBg: 'rgba(0,200,200,0.08)',
  red: '#ef4444', green: '#22c55e', yellow: '#eab308', purple: '#a855f7',
}

/* ===== 类型 ===== */
interface PoolInfo { pool_id: number; name: string; strategy: string; desc: string; signal_types: string[]; member_count: number }
interface Member {
  ts_code: string
  name: string
  industry?: string
  added_at?: string
  note?: string
  concept_boards?: string[]
  core_concept_board?: string
}
interface Signal { has_signal: boolean; type: string; direction?: 'buy' | 'sell'; price?: number; strength: number; current_strength?: number; message: string; triggered_at: number; details: Record<string, any>; state?: 'active' | 'decaying' | 'expired'; age_sec?: number; expire_reason?: string; channel?: string; signal_source?: string }
interface SignalRow {
  ts_code: string
  name: string
  price: number
  pct_chg: number
  signals: Signal[]
  pool1_position_status?: 'holding' | 'observe' | string
  pool1_position_ratio?: number
  pool1_holding_days?: number
  pool1_in_holding?: boolean
  pool1_last_reduce_at?: number
  pool1_last_reduce_type?: string
  pool1_last_reduce_ratio?: number
  pool1_decision?: 'build' | 'hold' | 'clear' | 'observe' | string
  pool1_decision_label?: string
  pool1_decision_reason?: string
  pool1_decision_strength?: number
  pool1_decision_mode?: 'actionable' | 'watch' | 'neutral' | string
  pool1_decision_signal_types?: string[]
}
interface SearchResult { ts_code: string; name: string; industry?: string }
interface TickData { price: number; open: number; high: number; low: number; pre_close: number; volume: number; amount: number; pct_chg: number; bids: [number, number][]; asks: [number, number][]; is_mock?: boolean; timestamp?: number; source?: string }
interface Txn { time: string; price: number; volume: number; direction: number }
interface MarketStatus { is_open: boolean; status: string; desc?: string }
type SortKey = 'default' | 'pct_chg' | 'signal_strength' | 'amount'
type Pool1HighRiskMode = 'all' | 'suppressed' | 'repeat'
interface QualityByType {
  count: number
  precision_1m?: number | null
  precision_3m?: number | null
  precision_5m: number | null
  avg_ret_bps: number | null
  avg_mfe_bps: number | null
  avg_mae_bps: number | null
}
interface QualityByPhase {
  count: number
  precision_1m?: number | null
  precision_3m?: number | null
  precision_5m: number | null
  avg_ret_bps: number | null
}
interface HorizonMetric { count: number; precision: number | null; avg_ret_bps: number | null; avg_mfe_bps: number | null; avg_mae_bps: number | null }
interface SignalChurn { flip_count: number; sample_count: number; symbols: number; churn_ratio: number | null; flips_per_hour: number | null }
interface QualitySummary {
  hours: number
  total_signals: number
  total_signals_any?: number
  precision_1m?: number | null
  precision_3m?: number | null
  precision_5m: number | null
  avg_ret_bps: number | null
  avg_mfe_bps: number | null
  avg_mae_bps: number | null
  by_horizon?: Record<string, HorizonMetric>
  by_type: Record<string, QualityByType>
  by_phase: Record<string, QualityByPhase>
  by_channel?: Record<string, { count: number; precision_5m: number | null; avg_ret_bps: number | null }>
  by_source?: Record<string, { count: number; precision_5m: number | null; avg_ret_bps: number | null }>
  by_channel_source?: Record<string, {
    channel: string
    signal_source: string
    count: number
    precision_5m: number | null
    avg_ret_bps: number | null
  }>
  signal_churn?: SignalChurn
  message?: string
}
interface DriftFeature { psi: number | null; ks_stat: number | null; severity: string; drifted: boolean; precision_drop: number | null; alerted: boolean }
interface DriftStatus {
  days: number
  total_checks: number
  has_active_alert: boolean
  latest_checked_at?: string
  latest: Record<string, DriftFeature>
  alerts: number
  quality_split_by_channel_source?: Array<{
    channel: string
    signal_source: string
    recent_count: number
    recent_precision_5m: number | null
    recent_avg_ret_bps: number | null
    baseline_count: number
    baseline_precision_5m: number | null
    baseline_avg_ret_bps: number | null
    precision_drop: number | null
  }>
  message?: string
}
interface Pool1ObserveStats {
  screen_total: number
  screen_pass: number
  stage2_triggered: number
  pass_rate: number
  trigger_rate: number
  summary: string
  reject_counts?: Record<string, number>
  reject_by_signal_type?: Record<string, Record<string, number>>
  reject_by_industry?: Record<string, Record<string, number>>
  reject_by_regime?: Record<string, Record<string, number>>
  reject_by_board_segment?: Record<string, Record<string, number>>
  reject_by_security_type?: Record<string, Record<string, number>>
  reject_by_listing_stage?: Record<string, Record<string, number>>
  threshold_observe?: Record<string, {
    total?: number
    blocked?: number
    observer_only?: number
    blocked_ratio?: number
    observer_ratio?: number
    top_bucket?: string
    by_bucket?: Record<string, number>
    by_version?: Record<string, number>
    by_source?: Record<string, number>
    blocked_reasons?: Record<string, number>
    current?: {
      bucket?: string
      threshold_version?: string
      threshold_source?: string
      blocked?: boolean
      observer_only?: boolean
      blocked_reason?: string
      updated_at_iso?: string
    }
  }>
  reject_windows?: Record<string, {
    seconds?: number
    event_count?: number
    reject_counts?: Record<string, number>
    top_rejects?: Array<{ reason: string; count: number }>
    reject_by_signal_type?: Record<string, Record<string, number>>
    reject_by_industry?: Record<string, Record<string, number>>
    reject_by_regime?: Record<string, Record<string, number>>
    reject_by_board_segment?: Record<string, Record<string, number>>
    reject_by_security_type?: Record<string, Record<string, number>>
    reject_by_listing_stage?: Record<string, Record<string, number>>
  }>
  top_rejects?: Array<{ reason: string; count: number }>
  storage_source?: string
  storage_expected?: string
  storage_verified?: boolean
  storage_degraded?: boolean
  storage_note?: string
  redis_enabled?: boolean
  redis_ready?: boolean
}
interface Pool1ObserveStorageCheck {
  expected?: string
  source?: string
  verified?: boolean
  degraded?: boolean
  note?: string
  redis_enabled?: boolean
  redis_ready?: boolean
}
interface Pool1ObserveResp {
  pool_id: number
  provider: string
  supported: boolean
  trade_date?: string
  updated_at?: number
  updated_at_iso?: string
  storage_check?: Pool1ObserveStorageCheck
  data?: Pool1ObserveStats
  message?: string
}
interface Pool1DecisionSummaryItem {
  ts_code: string
  name: string
  decision: 'build' | 'hold' | 'clear' | 'observe' | string
  decision_label: string
  decision_reason: string
  decision_strength: number
  decision_mode: 'actionable' | 'watch' | 'neutral' | string
  position_status: 'holding' | 'observe' | string
  position_ratio?: number
  clear_level?: string
  clear_family?: string
  reduce_ratio?: number
  reduce_streak?: number
  reduce_ratio_cum?: number
  last_rebuild_at?: number
  last_rebuild_type?: string
  last_rebuild_transition?: string
  last_rebuild_add_ratio?: number
  last_rebuild_from_partial_count?: number
  last_rebuild_from_partial_ratio?: number
  last_exit_after_partial?: boolean
  last_exit_partial_count?: number
  last_exit_reduce_ratio_cum?: number
  holding_days?: number
  signal_types?: string[]
  industry?: string
  board_segment?: string
  concept_boards?: string[]
  core_concept_board?: string
  concept_state?: string
  concept_score?: number
  price?: number
  pct_chg?: number
}
interface Pool1DecisionTransition {
  ts_code: string
  name: string
  transition: 'observe->holding' | 'holding->observe' | string
  label: string
  at: number
  at_iso?: string | null
  signal_type?: string
  signal_price?: number
  reduce_ratio?: number
  reduce_streak?: number
  reduce_ratio_cum?: number
  rebuild_after_partial?: boolean
  rebuild_add_ratio?: number
  rebuild_from_partial_count?: number
  rebuild_from_partial_ratio?: number
  exit_after_partial?: boolean
  exit_partial_count?: number
  exit_reduce_ratio_cum?: number
  current_position_status?: 'holding' | 'observe' | string
  current_position_ratio?: number
  current_decision?: 'build' | 'hold' | 'clear' | 'observe' | string
  current_decision_label?: string
  current_decision_mode?: 'actionable' | 'watch' | 'neutral' | string
  current_decision_strength?: number
  current_signal_types?: string[]
  holding_days?: number
  industry?: string
  board_segment?: string
  concept_boards?: string[]
  core_concept_board?: string
  concept_state?: string
  concept_score?: number
}
interface Pool1DecisionDailyRow {
  trade_date: string
  build_actionable: number
  build_watch: number
  clear_actionable: number
  clear_watch: number
  partial_actionable: number
  partial_watch: number
  partial_reduce_ratio_sum: number
  left_side_buy: number
  right_side_breakout: number
  timing_clear: number
  net_build_actionable: number
  net_exposure_change: number
  actionable_total: number
  watch_total: number
  entry_cost_lost_count?: number
  entry_cost_lost_actionable?: number
  entry_avwap_lost_count?: number
  entry_avwap_lost_actionable?: number
  breakout_anchor_lost_count?: number
  breakout_anchor_lost_actionable?: number
  event_anchor_lost_count?: number
  event_anchor_lost_actionable?: number
  multi_anchor_lost_count?: number
  multi_anchor_lost_actionable?: number
  dominant_clear_reason?: string
  dominant_clear_reason_label?: string
  dominant_clear_reason_count?: number
  dominant_clear_reason_actionable?: number
}
interface Pool1DecisionHistoryEvent {
  trade_date: string
  ts_code: string
  name: string
  industry?: string
  board_segment?: string
  concept_boards?: string[]
  core_concept_board?: string
  concept_state?: string
  concept_score?: number
  signal_type: string
  observe_only: boolean
  actionable: boolean
  clear_level?: string
  clear_family?: string
  reduce_ratio?: number
  entry_cost_lost?: boolean
  entry_avwap_lost?: boolean
  breakout_anchor_lost?: boolean
  event_anchor_lost?: boolean
  multi_anchor_lost?: boolean
  lost_anchor_count?: number
  support_anchor_type?: string
  support_anchor_line?: number | null
  dominant_clear_reason?: string
  dominant_clear_reason_label?: string
  triggered_at?: string
}
interface Pool1ClearReasonStat {
  key: string
  label: string
  count: number
  actionable: number
}
interface Pool1DecisionSummaryResp {
  pool_id: number
  provider: string
  checked_at: string
  checked_at_ts?: number
  summary: {
    member_count: number
    build_count: number
    hold_count: number
    clear_count: number
    observe_count: number
    actionable_count: number
    watch_count: number
    neutral_count: number
    holding_count: number
    observe_position_count: number
    holding_ratio: number
    actionable_ratio: number
    watch_ratio: number
    transition_today: number
    build_today: number
    rebuild_today?: number
    partial_today?: number
    chained_partial_today?: number
    clear_today: number
    clear_after_partial_today?: number
    rebuild_stabilized_today?: number
    rebuild_reduced_again_today?: number
    rebuild_cleared_again_today?: number
    rebuild_success_rate?: number
    rebuild_reduce_again_rate?: number
    net_build_today: number
    left_suppressed_count?: number
    left_repeat_suppressed_count?: number
    concept_retreat_watch_count?: number
  }
  decision_counts?: Record<string, number>
  mode_counts?: Record<string, number>
  position_counts?: Record<string, number>
  transition_summary?: {
    build_today: number
    rebuild_today?: number
    partial_today?: number
    chained_partial_today?: number
    clear_today: number
    clear_after_partial_today?: number
    rebuild_stabilized_today?: number
    rebuild_reduced_again_today?: number
    rebuild_cleared_again_today?: number
    transition_today: number
    net_build_today: number
  }
  recent_transitions?: Pool1DecisionTransition[]
  decision_daily?: Pool1DecisionDailyRow[]
  decision_history_events?: Pool1DecisionHistoryEvent[]
  clear_reason_summary?: Record<string, Pool1ClearReasonStat>
  decision_examples?: Record<string, Pool1DecisionSummaryItem[]>
  mode_examples?: Record<string, Pool1DecisionSummaryItem[]>
}
interface FastSlowDiffRow {
  ts_code: string
  name: string
  fast_types: string[]
  slow_types: string[]
  only_fast: string[]
  only_slow: string[]
  fast_price: number
  slow_price: number
  price_delta: number
  match: boolean
}
interface FastSlowDiffResp {
  ok: boolean
  checked_at: string
  pool_id: number
  provider: string
  summary: {
    members: number
    mismatch: number
    exact_match: number
    mismatch_ratio: number
  }
  rows: FastSlowDiffRow[]
  field_missing: Record<string, number>
}
interface FastSlowTrendPoint {
  pool_id: number
  at: number
  at_iso?: string
  mismatch_ratio: number
  mismatch: number
  members: number
  error?: string | null
}
interface FastSlowTrendResp {
  ok: boolean
  checked_at: string
  pool_id: number
  hours: number
  count: number
  latest?: FastSlowTrendPoint | null
  storage_source?: string
  points: FastSlowTrendPoint[]
}
interface RuntimeHealthItem {
  name: string
  ok: boolean
  level: 'green' | 'yellow' | 'red'
  detail?: Record<string, any> | string | null
}
interface RuntimeHealthResp {
  ok: boolean
  level: 'green' | 'yellow' | 'red'
  checked_at: string
  profile?: string
  summary?: { green: number; yellow: number; red: number }
  items: RuntimeHealthItem[]
}
interface RealtimeUiConfigResp {
  data?: {
    pool1_observe_ui?: {
      observe_poll_sec?: number
      stale_warn_sec?: number
      stale_error_sec?: number
      sample_warn_min?: number
      sample_recommend_min?: number
    }
  }
}

function useGlobalIndices(pageVisible: boolean) {
  const [indices, setIndices] = useState<IndexData[]>([])
  const [marketStatus, setMarketStatus] = useState<MarketStatus>({ is_open: true, status: 'unknown' })

  useEffect(() => {
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | null = null

    const loop = async () => {
      if (cancelled) return
      let isOpen = !!marketStatus.is_open
      try {
        const [m, r] = await Promise.all([
          axios.get(`${API}/api/realtime/market_status`),
          axios.get(`${API}/api/realtime/indices`),
        ])
        if (!cancelled) {
          const ms: MarketStatus = m.data || { is_open: false, status: 'unknown' }
          setMarketStatus(ms)
          setIndices((r.data?.data || []) as IndexData[])
          isOpen = !!ms.is_open
        }
      } catch {
        // keep previous snapshot when request fails
      }

      const delay = isOpen ? (pageVisible ? 10_000 : 20_000) : 120_000
      if (!cancelled) timer = setTimeout(loop, delay)
    }

    loop()
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
  }, [pageVisible, marketStatus.is_open])

  return { indices }
}

function IndicesTopBar({ indices }: { indices: IndexData[] }) {
  if (!indices || indices.length <= 0) return null
  return (
    <div style={{
      display: 'flex', gap: 0, marginBottom: 12,
      background: C.card, border: `1px solid ${C.border}`, borderRadius: 6,
      overflow: 'hidden',
    }}>
      {indices.map((idx, i) => {
        const delta = idx.pre_close > 0 ? (idx.price - idx.pre_close) : 0
        const pct = idx.pre_close > 0 ? (delta / idx.pre_close * 100) : (idx.pct_chg || 0)
        const isUp = pct >= 0
        const color = isUp ? C.red : C.green
        const chgPts = idx.pre_close > 0 ? delta.toFixed(2) : '--'
        const sourceLabelMap: Record<string, string> = {
          mootdx_index: 'mootdx',
          cache_last_good: '缓存兜底',
          index_fetch_error: '指数源异常',
          error_fallback: '异常兜底',
        }
        const src = idx.source ? (sourceLabelMap[idx.source] || idx.source) : (idx.is_mock ? '模拟' : '实时')
        return (
          <div key={idx.ts_code} style={{
            flex: 1, padding: '8px 12px', display: 'flex', flexDirection: 'column', gap: 2,
            borderRight: i < indices.length - 1 ? `1px solid ${C.border}` : 'none',
            minWidth: 0,
          }}>
            <div style={{ fontSize: 10, color: C.dim, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{idx.name}</div>
            <div style={{ fontSize: 14, fontWeight: 700, fontFamily: 'monospace', color, lineHeight: 1.2 }}>
              {idx.price > 0 ? idx.price.toFixed(2) : '--'}
            </div>
            <div style={{ fontSize: 10, fontFamily: 'monospace', color, display: 'flex', gap: 6 }}>
              <span>{isUp ? '+' : ''}{chgPts}</span>
              <span>{isUp ? '+' : ''}{pct.toFixed(2)}%</span>
            </div>
            <div style={{ fontSize: 9, color: C.dim, fontFamily: 'monospace' }}>
              {`源:${src} · 更新:${formatTimeText(idx.updated_at)} (${formatAgeText(idx.updated_at)})`}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function isSameTick(a?: TickData, b?: TickData): boolean {
  if (!a || !b) return false
  return (
    a.price === b.price &&
    a.pct_chg === b.pct_chg &&
    a.volume === b.volume &&
    a.amount === b.amount &&
    a.open === b.open &&
    a.high === b.high &&
    a.low === b.low &&
    a.pre_close === b.pre_close
  )
}

function mergeTickMap(
  prev: Record<string, TickData>,
  incoming: Record<string, TickData>,
  replaceAll: boolean = false,
): Record<string, TickData> {
  const src = incoming || {}
  const srcKeys = Object.keys(src)
  const next: Record<string, TickData> = replaceAll ? {} : { ...prev }
  let changed = replaceAll ? (Object.keys(prev).length !== srcKeys.length) : false
  for (const code of srcKeys) {
    const oldTick = next[code]
    const newTick = src[code]
    if (isSameTick(oldTick, newTick)) {
      if (oldTick) next[code] = oldTick
    } else {
      next[code] = newTick
      changed = true
    }
  }
  return changed ? next : prev
}

function isSameSignal(a?: Signal, b?: Signal): boolean {
  if (!a || !b) return false
  return (
    !!a.has_signal === !!b.has_signal &&
    (a.type || '') === (b.type || '') &&
    (a.direction || '') === (b.direction || '') &&
    (a.strength || 0) === (b.strength || 0) &&
    (a.current_strength || 0) === (b.current_strength || 0) &&
    (a.state || '') === (b.state || '') &&
    (a.age_sec || 0) === (b.age_sec || 0) &&
    (a.expire_reason || '') === (b.expire_reason || '') &&
    (a.triggered_at || 0) === (b.triggered_at || 0) &&
    (a.price || 0) === (b.price || 0) &&
    (a.message || '') === (b.message || '') &&
    (a.channel || '') === (b.channel || '') &&
    (a.signal_source || '') === (b.signal_source || '')
  )
}

function normalizeSignals(signals?: Signal[]): Signal[] {
  const arr = Array.isArray(signals) ? signals : []
  return [...arr].sort((x, y) => `${x.type || ''}:${x.direction || ''}`.localeCompare(`${y.type || ''}:${y.direction || ''}`))
}

function isSameSignalRow(a?: SignalRow, b?: SignalRow): boolean {
  if (!a || !b) return false
  if ((a.ts_code || '') !== (b.ts_code || '')) return false
  if ((a.name || '') !== (b.name || '')) return false
  if ((a.price || 0) !== (b.price || 0)) return false
  if ((a.pct_chg || 0) !== (b.pct_chg || 0)) return false
  if ((a.pool1_position_status || '') !== (b.pool1_position_status || '')) return false
  if (Number(a.pool1_holding_days || 0) !== Number(b.pool1_holding_days || 0)) return false
  if (!!a.pool1_in_holding !== !!b.pool1_in_holding) return false
  if ((a.pool1_decision || '') !== (b.pool1_decision || '')) return false
  if ((a.pool1_decision_label || '') !== (b.pool1_decision_label || '')) return false
  if ((a.pool1_decision_reason || '') !== (b.pool1_decision_reason || '')) return false
  if (Number(a.pool1_decision_strength || 0) !== Number(b.pool1_decision_strength || 0)) return false
  if ((a.pool1_decision_mode || '') !== (b.pool1_decision_mode || '')) return false
  const sa = normalizeSignals(a.signals)
  const sb = normalizeSignals(b.signals)
  if (sa.length !== sb.length) return false
  for (let i = 0; i < sa.length; i++) {
    if (!isSameSignal(sa[i], sb[i])) return false
  }
  return true
}

function mergeSignalRows(prev: SignalRow[], incoming: SignalRow[], replaceAll: boolean = false): SignalRow[] {
  const src = Array.isArray(incoming) ? incoming : []
  const prevArr = Array.isArray(prev) ? prev : []
  if (replaceAll) {
    const prevMap = new Map(prevArr.map(r => [r.ts_code, r]))
    let changed = prevArr.length !== src.length
    const next = src.map(r => {
      const old = prevMap.get(r.ts_code)
      if (old && isSameSignalRow(old, r)) return old
      changed = true
      return r
    })
    return changed ? next : prevArr
  }

  const map = new Map(prevArr.map(r => [r.ts_code, r]))
  const order = prevArr.map(r => r.ts_code)
  let changed = false
  for (const r of src) {
    const old = map.get(r.ts_code)
    if (old && isSameSignalRow(old, r)) {
      map.set(r.ts_code, old)
    } else {
      map.set(r.ts_code, r)
      changed = true
    }
    if (!order.includes(r.ts_code)) {
      order.push(r.ts_code)
      changed = true
    }
  }
  if (!changed) return prevArr
  const next: SignalRow[] = []
  for (const code of order) {
    const row = map.get(code)
    if (row) next.push(row)
  }
  return next
}

function isSameTxnList(a?: Txn[], b?: Txn[]): boolean {
  if (!a || !b) return false
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    const x = a[i]
    const y = b[i]
    if (x.time !== y.time || x.price !== y.price || x.volume !== y.volume || x.direction !== y.direction) {
      return false
    }
  }
  return true
}

function mergeTxnsMap(
  prev: Record<string, Txn[]>,
  incoming: Record<string, Txn[]>,
  replaceAll: boolean = false,
): Record<string, Txn[]> {
  const src = incoming || {}
  const srcKeys = Object.keys(src)
  const next: Record<string, Txn[]> = replaceAll ? {} : { ...prev }
  let changed = replaceAll ? (Object.keys(prev).length !== srcKeys.length) : false
  for (const code of srcKeys) {
    const oldTxns = next[code]
    const newTxns = src[code]
    if (isSameTxnList(oldTxns, newTxns)) {
      if (oldTxns) next[code] = oldTxns
    } else {
      next[code] = newTxns
      changed = true
    }
  }
  return changed ? next : prev
}

function formatAgeText(updatedAtSec?: number): string {
  if (!updatedAtSec) return '--'
  const ageSec = Math.max(0, Math.floor(Date.now() / 1000) - updatedAtSec)
  if (ageSec < 60) return `${ageSec}s前`
  const min = Math.floor(ageSec / 60)
  const sec = ageSec % 60
  return `${min}m${sec}s前`
}

function formatTimeText(updatedAtSec?: number): string {
  if (!updatedAtSec) return '--:--:--'
  try {
    return new Date(updatedAtSec * 1000).toLocaleTimeString()
  } catch {
    return '--:--:--'
  }
}

function usePageVisible(): boolean {
  const [visible, setVisible] = useState(() => {
    try {
      return typeof document === 'undefined' ? true : document.visibilityState !== 'hidden'
    } catch {
      return true
    }
  })
  useEffect(() => {
    const onVisibility = () => {
      try {
        setVisible(document.visibilityState !== 'hidden')
      } catch {
        setVisible(true)
      }
    }
    document.addEventListener('visibilitychange', onVisibility)
    return () => document.removeEventListener('visibilitychange', onVisibility)
  }, [])
  return visible
}

function csvEscape(v: unknown): string {
  const s = String(v ?? '')
  if (s.includes('"') || s.includes(',') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`
  }
  return s
}

function downloadCsv(filename: string, headers: string[], rows: (string | number | boolean | null | undefined)[][]): void {
  const lines = [headers.map(csvEscape).join(',')]
  for (const row of rows) lines.push(row.map(csvEscape).join(','))
  const csvText = '\uFEFF' + lines.join('\n')
  const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

function buildPool1DecisionView(
  summary: Pool1DecisionSummaryResp | null,
  decisionFilter: 'all' | 'build_actionable' | 'clear_actionable' | 'partial_actionable' | 'watch_only' | 'hold_only',
  boardFilter: string,
  industryFilter: string,
  conceptFilter: string,
  riskHotwordFilter: string = 'all',
) {
  const buildList = Array.isArray(summary?.decision_examples?.build) ? (summary?.decision_examples?.build || []) : []
  const clearList = Array.isArray(summary?.decision_examples?.clear) ? (summary?.decision_examples?.clear || []) : []
  const holdList = Array.isArray(summary?.decision_examples?.hold) ? (summary?.decision_examples?.hold || []) : []
  const transitions = Array.isArray(summary?.recent_transitions) ? (summary?.recent_transitions || []) : []
  const dailyRows = Array.isArray(summary?.decision_daily) ? (summary?.decision_daily || []) : []
  const historyEvents = Array.isArray(summary?.decision_history_events) ? (summary?.decision_history_events || []) : []
  const transitionSummary = (summary?.transition_summary || summary?.summary || {}) as Record<string, any>
  const clearReasonMeta: Array<{ key: string; label: string }> = [
    { key: 'entry_cost_lost', label: '成本锚失守' },
    { key: 'entry_avwap_lost', label: '建仓AVWAP失守' },
    { key: 'breakout_anchor_lost', label: '突破锚失守' },
    { key: 'event_anchor_lost', label: '事件锚失守' },
    { key: 'multi_anchor_lost', label: '多锚失守' },
  ]
  const finalizePool1DailyRow = (row: Pool1DecisionDailyRow): Pool1DecisionDailyRow => {
    const base: Pool1DecisionDailyRow = {
      ...row,
      partial_reduce_ratio_sum: Number(row.partial_reduce_ratio_sum || 0),
      actionable_total: Number(row.actionable_total || 0),
      watch_total: Number(row.watch_total || 0),
      net_build_actionable: Number(row.net_build_actionable || 0),
      net_exposure_change: Number(row.net_exposure_change || 0),
    }
    let dominantKey = String(base.dominant_clear_reason || '')
    let dominantLabel = String(base.dominant_clear_reason_label || '')
    let dominantCount = Number(base.dominant_clear_reason_count || 0)
    let dominantActionable = Number(base.dominant_clear_reason_actionable || 0)
    if (!dominantKey) {
      clearReasonMeta.forEach(({ key, label }) => {
        const count = Number((base as Record<string, any>)[`${key}_count`] || 0)
        const actionable = Number((base as Record<string, any>)[`${key}_actionable`] || 0)
        if (count > dominantCount) {
          dominantKey = key
          dominantLabel = label
          dominantCount = count
          dominantActionable = actionable
        }
      })
    }
    return {
      ...base,
      dominant_clear_reason: dominantKey,
      dominant_clear_reason_label: dominantLabel,
      dominant_clear_reason_count: dominantCount,
      dominant_clear_reason_actionable: dominantActionable,
    }
  }

  const candidateSource = [...buildList, ...clearList, ...holdList]
  const boardOptions = Array.from(new Set(
    [
      ...candidateSource.map(x => String(x.board_segment || '').trim()).filter(Boolean),
      ...transitions.map(x => String(x.board_segment || '').trim()).filter(Boolean),
      ...historyEvents.map(x => String(x.board_segment || '').trim()).filter(Boolean),
    ],
  )).sort()
  const industryOptions = Array.from(new Set(
    [
      ...candidateSource.map(x => String(x.industry || '').trim()).filter(Boolean),
      ...transitions.map(x => String(x.industry || '').trim()).filter(Boolean),
      ...historyEvents.map(x => String(x.industry || '').trim()).filter(Boolean),
    ],
  )).sort((a, b) => a.localeCompare(b, 'zh-CN'))
  const conceptOptions = Array.from(new Set(
    [
      ...candidateSource.map(x => String(x.core_concept_board || '').trim()).filter(Boolean),
      ...transitions.map(x => String(x.core_concept_board || '').trim()).filter(Boolean),
      ...historyEvents.map(x => String(x.core_concept_board || '').trim()).filter(Boolean),
    ],
  )).sort((a, b) => a.localeCompare(b, 'zh-CN'))
  const boardMatch = (board?: string | null) => boardFilter === 'all' || String(board || '').trim() === boardFilter
  const industryMatch = (industry?: string | null) => industryFilter === 'all' || String(industry || '').trim() === industryFilter
  const conceptMatch = (concept?: string | null) => conceptFilter === 'all' || String(concept || '').trim() === conceptFilter
  const candidateDecisionMatch = (item: Pool1DecisionSummaryItem) => {
    if (decisionFilter === 'build_actionable') {
      return item.decision === 'build' && item.decision_mode === 'actionable'
    }
    if (decisionFilter === 'clear_actionable') {
      return item.decision === 'clear' && item.decision_mode === 'actionable' && String(item.clear_level || '').toLowerCase() !== 'partial'
    }
    if (decisionFilter === 'partial_actionable') {
      return item.decision === 'clear' && item.decision_mode === 'actionable' && String(item.clear_level || '').toLowerCase() === 'partial'
    }
    if (decisionFilter === 'watch_only') {
      return item.decision_mode === 'watch'
    }
    if (decisionFilter === 'hold_only') {
      return item.decision === 'hold'
    }
    return true
  }
  const historyDecisionMatch = (evt: Pool1DecisionHistoryEvent) => {
    const sig = String(evt.signal_type || '')
    const clearLevel = String(evt.clear_level || '').toLowerCase()
    if (decisionFilter === 'build_actionable') {
      return Boolean(evt.actionable) && (sig === 'left_side_buy' || sig === 'right_side_breakout')
    }
    if (decisionFilter === 'clear_actionable') {
      return Boolean(evt.actionable) && sig === 'timing_clear' && clearLevel !== 'partial'
    }
    if (decisionFilter === 'partial_actionable') {
      return Boolean(evt.actionable) && sig === 'timing_clear' && clearLevel === 'partial'
    }
    if (decisionFilter === 'watch_only') {
      return !Boolean(evt.actionable)
    }
    if (decisionFilter === 'hold_only') {
      return false
    }
    return true
  }

  const baseFilteredCandidates = candidateSource.filter(item => {
    if (!boardMatch(item.board_segment) || !industryMatch(item.industry) || !conceptMatch(item.core_concept_board)) {
      return false
    }
    return candidateDecisionMatch(item)
  })
  const filteredTransitions = transitions.filter(item => {
    if (!boardMatch(item.board_segment) || !industryMatch(item.industry) || !conceptMatch(item.core_concept_board)) return false
    if (decisionFilter === 'partial_actionable') {
      return String(item.transition || '') === 'holding->holding(partial)'
    }
    if (decisionFilter === 'clear_actionable') {
      return String(item.transition || '') === 'holding->observe'
    }
    if (decisionFilter === 'build_actionable') {
      return String(item.transition || '') === 'observe->holding'
    }
    return true
  })
  const filteredHistoryEvents = historyEvents.filter(evt =>
    boardMatch(evt.board_segment) && industryMatch(evt.industry) && conceptMatch(evt.core_concept_board) && historyDecisionMatch(evt)
  )
  const filteredDailyRows = (() => {
    if (boardFilter === 'all' && industryFilter === 'all' && conceptFilter === 'all') {
      return dailyRows.map(finalizePool1DailyRow)
    }
    const map = new Map<string, Pool1DecisionDailyRow>()
    filteredHistoryEvents
      .forEach(evt => {
        const key = String(evt.trade_date || '')
        if (!key) return
        if (!map.has(key)) {
          map.set(key, {
            trade_date: key,
            build_actionable: 0,
            build_watch: 0,
            clear_actionable: 0,
            clear_watch: 0,
            partial_actionable: 0,
            partial_watch: 0,
            partial_reduce_ratio_sum: 0,
            left_side_buy: 0,
            right_side_breakout: 0,
            timing_clear: 0,
            net_build_actionable: 0,
            net_exposure_change: 0,
            actionable_total: 0,
            watch_total: 0,
            entry_cost_lost_count: 0,
            entry_cost_lost_actionable: 0,
            entry_avwap_lost_count: 0,
            entry_avwap_lost_actionable: 0,
            breakout_anchor_lost_count: 0,
            breakout_anchor_lost_actionable: 0,
            event_anchor_lost_count: 0,
            event_anchor_lost_actionable: 0,
            multi_anchor_lost_count: 0,
            multi_anchor_lost_actionable: 0,
          })
        }
        const row = map.get(key)!
        const sig = String(evt.signal_type || '')
        const actionable = Boolean(evt.actionable)
        const clearLevel = String(evt.clear_level || '').trim().toLowerCase()
        const reduceRatio = Number(evt.reduce_ratio || 0)
        if (sig === 'left_side_buy' || sig === 'right_side_breakout') {
          if (actionable) row.build_actionable += 1
          else row.build_watch += 1
        } else if (sig === 'timing_clear') {
          if (actionable) row.clear_actionable += 1
          else row.clear_watch += 1
          if (clearLevel === 'partial') {
            if (actionable) {
              row.partial_actionable += 1
              row.partial_reduce_ratio_sum += Number.isFinite(reduceRatio) ? reduceRatio : 0
            } else {
              row.partial_watch += 1
            }
          }
          clearReasonMeta.forEach(({ key }) => {
            if (!Boolean((evt as Record<string, any>)[key])) return
            ;(row as Record<string, any>)[`${key}_count`] = Number((row as Record<string, any>)[`${key}_count`] || 0) + 1
            if (actionable) {
              ;(row as Record<string, any>)[`${key}_actionable`] = Number((row as Record<string, any>)[`${key}_actionable`] || 0) + 1
            }
          })
        }
        if (sig === 'left_side_buy') row.left_side_buy += 1
        if (sig === 'right_side_breakout') row.right_side_breakout += 1
        if (sig === 'timing_clear') row.timing_clear += 1
      })
    return Array.from(map.values())
      .sort((a, b) => a.trade_date.localeCompare(b.trade_date))
      .map(row => finalizePool1DailyRow({
        ...row,
        net_build_actionable: row.build_actionable - row.clear_actionable,
        partial_reduce_ratio_sum: Number(row.partial_reduce_ratio_sum || 0),
        net_exposure_change: Number(row.build_actionable || 0) - (Number(row.clear_actionable || 0) - Number(row.partial_actionable || 0)) - Number(row.partial_reduce_ratio_sum || 0),
        actionable_total: row.build_actionable + row.clear_actionable,
        watch_total: row.build_watch + row.clear_watch,
      }))
  })()
  const clearReasonSummary = (() => {
    const stats = clearReasonMeta.map(({ key, label }) => ({ key, label, count: 0, actionable: 0 }))
    const byKey = new Map(stats.map(item => [item.key, item]))
    filteredHistoryEvents.forEach((evt) => {
      if (String(evt.signal_type || '') !== 'timing_clear') return
      clearReasonMeta.forEach(({ key }) => {
        if (!Boolean((evt as Record<string, any>)[key])) return
        const item = byKey.get(key)
        if (!item) return
        item.count += 1
        if (evt.actionable) item.actionable += 1
      })
    })
    return stats
      .filter(item => item.count > 0)
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count
        if (b.actionable !== a.actionable) return b.actionable - a.actionable
        return a.label.localeCompare(b.label, 'zh-CN')
      })
  })()
  const historyTradeDates = Array.from(new Set(filteredHistoryEvents.map(evt => String(evt.trade_date || '')).filter(Boolean))).sort()

  const aggregateRanking = (
    items: Pool1DecisionHistoryEvent[],
    pickKey: (evt: Pool1DecisionHistoryEvent) => string,
    pickLabel: (evt: Pool1DecisionHistoryEvent) => string,
  ) => {
    const map = new Map<string, {
      key: string
      label: string
      build_actionable: number
      clear_actionable: number
      build_watch: number
      clear_watch: number
      actionable_total: number
      net_build_actionable: number
      daily_net_map: Record<string, number>
      daily_build_map: Record<string, number>
      daily_clear_map: Record<string, number>
    }>()
    items.forEach(evt => {
      const key = pickKey(evt)
      const label = pickLabel(evt)
      if (!key) return
      if (!map.has(key)) {
        map.set(key, {
          key,
          label,
          build_actionable: 0,
          clear_actionable: 0,
          build_watch: 0,
          clear_watch: 0,
          actionable_total: 0,
          net_build_actionable: 0,
          daily_net_map: {},
          daily_build_map: {},
          daily_clear_map: {},
        })
      }
      const row = map.get(key)!
      const sig = String(evt.signal_type || '')
      const actionable = Boolean(evt.actionable)
      const tradeDate = String(evt.trade_date || '')
      if (sig === 'left_side_buy' || sig === 'right_side_breakout') {
        if (actionable) {
          row.build_actionable += 1
          if (tradeDate) row.daily_net_map[tradeDate] = Number(row.daily_net_map[tradeDate] || 0) + 1
          if (tradeDate) row.daily_build_map[tradeDate] = Number(row.daily_build_map[tradeDate] || 0) + 1
        } else row.build_watch += 1
      } else if (sig === 'timing_clear') {
        if (actionable) {
          row.clear_actionable += 1
          if (tradeDate) row.daily_net_map[tradeDate] = Number(row.daily_net_map[tradeDate] || 0) - 1
          if (tradeDate) row.daily_clear_map[tradeDate] = Number(row.daily_clear_map[tradeDate] || 0) + 1
        } else row.clear_watch += 1
      }
    })
    return Array.from(map.values())
      .map(row => {
        const dailySeries = historyTradeDates.map(date => Number(row.daily_net_map[date] || 0))
        const latestDate = historyTradeDates.length > 0 ? historyTradeDates[historyTradeDates.length - 1] : ''
        const latestBuild = latestDate ? Number(row.daily_build_map[latestDate] || 0) : 0
        const latestClear = latestDate ? Number(row.daily_clear_map[latestDate] || 0) : 0
        const latestActionableTotal = latestBuild + latestClear
        let streakDirection = 0
        let streakCount = 0
        for (let i = dailySeries.length - 1; i >= 0; i -= 1) {
          const v = Number(dailySeries[i] || 0)
          const sign = v > 0 ? 1 : v < 0 ? -1 : 0
          if (sign === 0) break
          if (streakDirection === 0) {
            streakDirection = sign
            streakCount = 1
            continue
          }
          if (sign !== streakDirection) break
          streakCount += 1
        }
        const latestNet = dailySeries.length > 0 ? Number(dailySeries[dailySeries.length - 1] || 0) : 0
        const tailSeries = dailySeries.slice(-2)
        const prevSeries = dailySeries.slice(Math.max(0, dailySeries.length - 4), Math.max(0, dailySeries.length - 2))
        const tailAvg = tailSeries.length > 0 ? (tailSeries.reduce((acc, v) => acc + Number(v || 0), 0) / tailSeries.length) : 0
        const prevAvg = prevSeries.length > 0 ? (prevSeries.reduce((acc, v) => acc + Number(v || 0), 0) / prevSeries.length) : 0
        const accelDelta = tailAvg - prevAvg
        let streakLabel = ''
        if (streakDirection > 0 && streakCount >= 2) streakLabel = `连强${streakCount}`
        else if (streakDirection < 0 && streakCount >= 2) streakLabel = `连弱${streakCount}`
        else if (latestNet > 0) streakLabel = '转强'
        else if (latestNet < 0) streakLabel = '转弱'
        let accelLabel = ''
        let accelColor = C.dim
        if (latestNet > 0 && accelDelta > 0.35) {
          accelLabel = '↑加速转强'
          accelColor = C.green
        } else if (latestNet < 0 && accelDelta < -0.35) {
          accelLabel = '↓加速转弱'
          accelColor = C.red
        } else if (latestNet > 0 && accelDelta < -0.2) {
          accelLabel = '↘转强放缓'
          accelColor = C.yellow
        } else if (latestNet < 0 && accelDelta > 0.2) {
          accelLabel = '↗转弱放缓'
          accelColor = C.yellow
        }
        return {
          ...row,
          actionable_total: row.build_actionable + row.clear_actionable,
          net_build_actionable: row.build_actionable - row.clear_actionable,
          daily_series: dailySeries,
          latest_net: latestNet,
          latest_build_actionable: latestBuild,
          latest_clear_actionable: latestClear,
          latest_actionable_total: latestActionableTotal,
          latest_build_ratio: latestActionableTotal > 0 ? (latestBuild / latestActionableTotal) : 0,
          latest_clear_ratio: latestActionableTotal > 0 ? (latestClear / latestActionableTotal) : 0,
          accel_delta: accelDelta,
          accel_label: accelLabel,
          accel_color: accelColor,
          streak_direction: streakDirection,
          streak_count: streakCount,
          streak_label: streakLabel,
        }
      })
      .sort((a, b) => {
        if (b.net_build_actionable !== a.net_build_actionable) return b.net_build_actionable - a.net_build_actionable
        if (b.actionable_total !== a.actionable_total) return b.actionable_total - a.actionable_total
        return a.label.localeCompare(b.label, 'zh-CN')
      })
  }

  const boardRanking = aggregateRanking(
    filteredHistoryEvents,
    evt => String(evt.board_segment || 'unknown').trim() || 'unknown',
    evt => boardSegmentLabel(evt.board_segment),
  )
  const industryRanking = aggregateRanking(
    filteredHistoryEvents,
    evt => String(evt.industry || '未分类').trim() || '未分类',
    evt => String(evt.industry || '未分类').trim() || '未分类',
  )
  const conceptRanking = aggregateRanking(
    filteredHistoryEvents,
    evt => String(evt.core_concept_board || '未归概念').trim() || '未归概念',
    evt => String(evt.core_concept_board || '未归概念').trim() || '未归概念',
  )

  const trendStrength = (() => {
    if (filteredDailyRows.length <= 0) {
      return null
    }
    const latest = filteredDailyRows[filteredDailyRows.length - 1]
    const totalActionable = filteredDailyRows.reduce((acc, row) => acc + Number(row.actionable_total || 0), 0)
    const totalNet = filteredDailyRows.reduce((acc, row) => acc + Number(row.net_build_actionable || 0), 0)
    const latestActionable = Math.max(1, Number(latest.actionable_total || 0))
    const latestBias = (Number(latest.build_actionable || 0) - Number(latest.clear_actionable || 0)) / latestActionable
    const rollingBias = totalActionable > 0 ? (totalNet / totalActionable) : 0
    const tailRows = filteredDailyRows.slice(-2)
    const headRows = filteredDailyRows.slice(0, Math.max(1, filteredDailyRows.length - tailRows.length))
    const tailActionable = Math.max(1, tailRows.reduce((acc, row) => acc + Number(row.actionable_total || 0), 0))
    const headActionable = Math.max(1, headRows.reduce((acc, row) => acc + Number(row.actionable_total || 0), 0))
    const tailBias = tailRows.reduce((acc, row) => acc + Number(row.net_build_actionable || 0), 0) / tailActionable
    const headBias = headRows.reduce((acc, row) => acc + Number(row.net_build_actionable || 0), 0) / headActionable
    const trendDelta = tailBias - headBias
    const participation = Math.min(1, totalActionable / Math.max(4, filteredDailyRows.length * 2))
    const calcConfirmStreak = (mode: 'expand' | 'defense') => {
      let streak = 0
      for (let i = filteredDailyRows.length - 1; i >= 0; i -= 1) {
        const row = filteredDailyRows[i]
        const actionable = Math.max(1, Number(row.actionable_total || 0))
        const buildRatio = Number(row.build_actionable || 0) / actionable
        const clearRatio = Number(row.clear_actionable || 0) / actionable
        const net = Number(row.net_build_actionable || 0)
        const matched = mode === 'expand'
          ? (net > 0 && buildRatio >= 0.55)
          : (net < 0 && clearRatio >= 0.52)
        if (!matched) break
        streak += 1
      }
      return streak
    }
    const expansionConfirmStreak = calcConfirmStreak('expand')
    const defenseConfirmStreak = calcConfirmStreak('defense')
    const rawScore = 50 + rollingBias * 28 + latestBias * 18 + trendDelta * 16 + (participation - 0.5) * 12
    const score = Math.max(0, Math.min(100, rawScore))
    const label = score >= 70 ? '偏强' : score >= 56 ? '转强' : score >= 44 ? '中性' : score >= 30 ? '转弱' : '偏弱'
    const color = score >= 70 ? C.green : score >= 56 ? C.cyan : score >= 44 ? C.yellow : C.red
    const reasons: string[] = []
    if (rollingBias > 0.12) reasons.push('近5日净建仓占优')
    if (rollingBias < -0.12) reasons.push('近5日清仓占优')
    if (latestBias > 0.2) reasons.push('最新一日建仓动能较强')
    if (latestBias < -0.2) reasons.push('最新一日清仓动能较强')
    if (trendDelta > 0.08) reasons.push('近两日强于前段')
    if (trendDelta < -0.08) reasons.push('近两日弱于前段')
    if (participation < 0.35) reasons.push('样本偏少')
    const stateTag = (() => {
      if (score >= 68 && totalNet > 0 && (Number(latest.build_actionable || 0) / latestActionable) >= 0.55 && participation >= 0.45) {
        return {
          text: expansionConfirmStreak >= 2 ? `主线扩张确认区 · 连续${expansionConfirmStreak}日` : '主线扩张确认区',
          color: C.green,
        }
      }
      if ((score < 40 || (Number(latest.clear_actionable || 0) / latestActionable) >= 0.52) && totalNet <= 0) {
        return {
          text: defenseConfirmStreak >= 2 ? `主线收缩防守区 · 连续${defenseConfirmStreak}日` : '主线收缩防守区',
          color: C.red,
        }
      }
      if (score >= 54 && (Number(latest.build_actionable || 0) / latestActionable) > (Number(latest.clear_actionable || 0) / latestActionable)) {
        return {
          text: '主线转强观察区',
          color: C.cyan,
        }
      }
      return {
        text: '主线均衡拉锯区',
        color: C.yellow,
      }
    })()
    const confirmLevel = (() => {
      if (stateTag.color === C.green) {
        return {
          text: expansionConfirmStreak >= 3 ? '强确认' : expansionConfirmStreak >= 2 ? '已确认' : '初确认',
          color: C.green,
        }
      }
      if (stateTag.color === C.red) {
        return {
          text: defenseConfirmStreak >= 3 ? '强防守' : defenseConfirmStreak >= 2 ? '已防守' : '初防守',
          color: C.red,
        }
      }
      if (stateTag.color === C.cyan) {
        return {
          text: '观察中',
          color: C.cyan,
        }
      }
      return {
        text: '拉锯中',
        color: C.yellow,
      }
    })()
    return {
      score: Math.round(score),
      label,
      color,
      stateTag,
      confirmLevel,
      expansionConfirmStreak,
      defenseConfirmStreak,
      totalActionable,
      totalNet,
      latestBuildRatio: latestActionable > 0 ? (Number(latest.build_actionable || 0) / latestActionable) : 0,
      latestClearRatio: latestActionable > 0 ? (Number(latest.clear_actionable || 0) / latestActionable) : 0,
      reasons,
    }
  })()
  const conceptHeat = (() => {
    const expandCount = baseFilteredCandidates.filter(item => {
      const state = String(item.concept_state || '').toLowerCase()
      return item.decision === 'build' && item.decision_mode === 'actionable' && (state === 'expand' || state === 'strong')
    }).length
    const retreatCount = baseFilteredCandidates.filter(item => {
      const state = String(item.concept_state || '').toLowerCase()
      return (state === 'retreat' || state === 'weak') && (
        item.decision === 'clear' || (item.decision === 'build' && item.decision_mode !== 'actionable')
      )
    }).length
    const weakCount = baseFilteredCandidates.filter(item => {
      const state = String(item.concept_state || '').toLowerCase()
      return state === 'weak' && (item.decision === 'clear' || item.decision_mode === 'watch')
    }).length
    let leadTag = ''
    if (retreatCount >= 2) leadTag = '概念退潮抑制'
    else if (expandCount >= 2) leadTag = '概念扩张共振'
    else if (weakCount >= 2) leadTag = '题材承接转弱'
    return {
      expandCount,
      retreatCount,
      weakCount,
      hasExpand: expandCount >= 2,
      hasRetreat: retreatCount >= 2,
      hasWeak: weakCount >= 2,
      leadTag,
    }
  })()
  const rebuildHeat = (() => {
    const rebuildToday = Number(transitionSummary.rebuild_today || 0)
    const partialToday = Number(transitionSummary.partial_today || 0)
    const chainedPartialToday = Number(transitionSummary.chained_partial_today || 0)
    const clearAfterPartialToday = Number(transitionSummary.clear_after_partial_today || 0)
    const transitionToday = Math.max(1, Number(transitionSummary.transition_today || 0))
    const rebuildRatio = rebuildToday / transitionToday
    const partialRatio = partialToday / transitionToday
    const netRebuildBias = rebuildToday - clearAfterPartialToday
    return {
      rebuildToday,
      partialToday,
      chainedPartialToday,
      clearAfterPartialToday,
      rebuildRatio,
      partialRatio,
      netRebuildBias,
      hasRebuildRepair: rebuildToday >= 1 && netRebuildBias >= 0,
      hasChainReduce: chainedPartialToday >= 1 || partialToday >= 2,
      hasReduceThenClear: clearAfterPartialToday >= 1,
    }
  })()
  const summaryText = (() => {
    if (!trendStrength) {
      return '近5日主线数据不足，先看执行级建仓/清仓样本继续积累。'
    }
    const scopeParts: string[] = []
    if (boardFilter !== 'all') scopeParts.push(`板块=${boardSegmentLabel(boardFilter)}`)
    if (industryFilter !== 'all') scopeParts.push(`行业=${industryFilter}`)
    if (conceptFilter !== 'all') scopeParts.push(`概念=${conceptFilter}`)
    const scope = scopeParts.length > 0 ? `当前筛选(${scopeParts.join(' · ')})` : '全池视角'
    const latestDirection = trendStrength.latestBuildRatio >= trendStrength.latestClearRatio ? '建仓' : '清仓'
    const ratioText = latestDirection === '建仓'
      ? `最新建仓占比 ${(trendStrength.latestBuildRatio * 100).toFixed(1)}%`
      : `最新清仓占比 ${(trendStrength.latestClearRatio * 100).toFixed(1)}%`
    const reason = trendStrength.reasons[0] || '近5日处于均衡震荡'
    const conceptText = conceptHeat.leadTag ? `，题材侧出现「${conceptHeat.leadTag}」` : ''
    const rebuildText = rebuildHeat.hasRebuildRepair
      ? `，盘中出现${rebuildHeat.rebuildToday}次回补修复`
      : rebuildHeat.hasChainReduce
        ? `，盘中减仓链路活跃（连减${rebuildHeat.chainedPartialToday}）`
        : rebuildHeat.hasReduceThenClear
          ? `，存在减仓后清仓${rebuildHeat.clearAfterPartialToday}次`
          : ''
    return `${scope}：主线${trendStrength.label}（${trendStrength.score}分），${ratioText}，${reason}${conceptText}${rebuildText}。`
  })()
  const summaryRisk = (() => {
    if (!trendStrength) {
      return {
        text: '风险提示：样本仍少，先看执行级建仓/清仓是否开始连续化，再决定是否放大仓位。',
        color: C.dim,
      }
    }
    if (conceptHeat.hasRetreat) {
      return {
        text: '风险提示：当前已有东方财富概念退潮压制，左侧抄底即使触发也更适合先看“仅观察”，避免在题材退潮主段连续抄底。',
        color: C.red,
      }
    }
    if (rebuildHeat.hasReduceThenClear && rebuildHeat.clearAfterPartialToday >= rebuildHeat.rebuildToday) {
      return {
        text: '风险提示：减仓后清仓仍多于回补修复，说明主线修复承接还不稳，持仓内回补更适合小步试探而不是一次加满。',
        color: C.red,
      }
    }
    if (rebuildHeat.hasChainReduce && rebuildHeat.partialRatio >= 0.35 && !rebuildHeat.hasRebuildRepair) {
      return {
        text: '风险提示：当前更像连续减仓收缩，而非减仓后的回补修复，主线内部仍在去杠杆，优先看防守而不是急着加回仓位。',
        color: C.yellow,
      }
    }
    if (conceptHeat.hasWeak && trendStrength.latestClearRatio >= 0.42) {
      return {
        text: '风险提示：题材承接转弱与清仓回流开始共振，左侧信号要提高回收确认要求，持仓更适合先去弱留强。',
        color: C.yellow,
      }
    }
    if (rebuildHeat.hasRebuildRepair && rebuildHeat.rebuildToday >= rebuildHeat.clearAfterPartialToday) {
      return {
        text: '动作提示：盘中已出现减仓后的回补修复，说明主线内部承接仍在；更适合优先跟踪回补成功的强票，而不是只盯新开仓。',
        color: C.cyan,
      }
    }
    if (conceptHeat.hasExpand && trendStrength.score >= 60) {
      return {
        text: '动作提示：当前存在概念扩张共振，执行级建仓更值得优先跟踪；但若午后清仓占比回升，仍要防题材一致后分歧。',
        color: C.green,
      }
    }
    if (trendStrength.score >= 70 && trendStrength.latestBuildRatio >= 0.58) {
      return {
        text: '动作提示：主线扩张较明确，可优先跟踪执行级建仓；但若午后清仓占比回升，要防止追高后回落。',
        color: C.green,
      }
    }
    if (trendStrength.score >= 56 && trendStrength.latestBuildRatio >= trendStrength.latestClearRatio) {
      return {
        text: '动作提示：主线处于转强段，建仓方向占优；更适合等盘中确认后的执行级信号，不宜抢跑。',
        color: C.cyan,
      }
    }
    if (trendStrength.latestClearRatio >= 0.5 || trendStrength.score < 44) {
      return {
        text: '风险提示：当前更偏防守，清仓/收缩信号占优；持仓以去弱留强、等待下一轮主线重新扩张为先。',
        color: C.red,
      }
    }
    return {
      text: '动作提示：主线仍在拉锯，先看执行级建仓与清仓哪一侧先连续化，避免在均衡区频繁切换。',
      color: C.yellow,
    }
  })()
  const rhythmSummary = (() => {
    if (filteredDailyRows.length <= 0) {
      return {
        text: '节奏结论：近5日样本不足，先继续观察执行级建仓与清仓是否形成连续主导。',
        color: C.dim,
      }
    }
    const tags = filteredDailyRows.map((row) => {
      const actionableTotal = Math.max(1, Number(row.actionable_total || 0))
      const buildRatioPct = (Number(row.build_actionable || 0) / actionableTotal) * 100
      const clearRatioPct = (Number(row.clear_actionable || 0) / actionableTotal) * 100
      if (Number(row.net_build_actionable || 0) > 0 && buildRatioPct >= 55) return 'build'
      if (Number(row.net_build_actionable || 0) < 0 && clearRatioPct >= 52) return 'clear'
      return 'balance'
    })
    const latestTag = tags[tags.length - 1]
    const prevTag = tags.length >= 2 ? tags[tags.length - 2] : ''
    let latestStreak = 0
    for (let i = tags.length - 1; i >= 0; i -= 1) {
      if (tags[i] !== latestTag) break
      latestStreak += 1
    }
    if (latestTag === 'build' && latestStreak >= 2) {
      return {
        text: `节奏结论：建仓主导连续${latestStreak}日，主线扩张仍在延续。`,
        color: C.green,
      }
    }
    if (latestTag === 'clear' && latestStreak >= 2) {
      return {
        text: `节奏结论：清仓主导连续${latestStreak}日，主线进入收缩防守节奏。`,
        color: C.red,
      }
    }
    if (rebuildHeat.hasRebuildRepair && rebuildHeat.rebuildToday >= 1 && rebuildHeat.netRebuildBias > 0) {
      return {
        text: `节奏结论：盘中出现${rebuildHeat.rebuildToday}次回补修复，减仓后的回流开始增强，主线更像内部修复而非单边收缩。`,
        color: C.cyan,
      }
    }
    if (rebuildHeat.hasChainReduce && rebuildHeat.chainedPartialToday >= 1) {
      return {
        text: `节奏结论：盘中连续减仓${rebuildHeat.chainedPartialToday}次，主线正在做仓位收缩，先看回补能否接续出现。`,
        color: C.yellow,
      }
    }
    if (latestTag === 'build' && prevTag !== 'build') {
      return {
        text: prevTag === 'balance' ? '节奏结论：由均衡转扩张，建仓信号开始占优。' : '节奏结论：由清仓转向建仓，主线有回暖迹象。',
        color: C.cyan,
      }
    }
    if (latestTag === 'clear' && prevTag !== 'clear') {
      return {
        text: prevTag === 'balance' ? '节奏结论：由均衡转收缩，清仓信号开始回升。' : '节奏结论：由建仓转向清仓，主线明显降温。',
        color: C.red,
      }
    }
    return {
      text: '节奏结论：近5日以均衡拉锯为主，主线尚未形成单边扩张或收缩。',
      color: C.yellow,
    }
  })()
  const riskHotwords = (() => {
    const tags: Array<{ text: string; color: string; weight: number }> = []
    if (!trendStrength || filteredDailyRows.length <= 0) {
      return tags
    }
    const latest = filteredDailyRows[filteredDailyRows.length - 1]
    const prev = filteredDailyRows.length >= 2 ? filteredDailyRows[filteredDailyRows.length - 2] : null
    const latestActionable = Math.max(1, Number(latest.actionable_total || 0))
    const latestBuildRatio = Number(latest.build_actionable || 0) / latestActionable
    const latestClearRatio = Number(latest.clear_actionable || 0) / latestActionable
    const prevActionable = Math.max(1, Number(prev?.actionable_total || 0))
    const prevClearRatio = prev ? (Number(prev.clear_actionable || 0) / prevActionable) : 0
    const prevBuildRatio = prev ? (Number(prev.build_actionable || 0) / prevActionable) : 0
    if (trendStrength.score >= 68 && latestBuildRatio >= 0.6 && Number(latest.net_build_actionable || 0) >= 2) {
      tags.push({ text: '建仓扩散', color: C.green, weight: 100 })
    }
    if (latestClearRatio >= 0.45 && latestClearRatio - prevClearRatio >= 0.12) {
      tags.push({ text: '清仓回流', color: C.red, weight: 95 })
    }
    if (trendStrength.score >= 56 && latestBuildRatio > 0.56 && latestClearRatio > 0.28) {
      tags.push({ text: '追高风险回升', color: C.yellow, weight: 88 })
    }
    if (prev && latestBuildRatio - prevBuildRatio >= 0.12 && Number(latest.net_build_actionable || 0) > 0) {
      tags.push({ text: '扩张提速', color: C.cyan, weight: 84 })
    }
    if (trendStrength.score < 44 && latestClearRatio >= 0.52) {
      tags.push({ text: '防守收缩', color: C.red, weight: 90 })
    }
    if (conceptHeat.hasExpand) {
      tags.push({ text: '概念扩张共振', color: C.green, weight: 93 })
    }
    if (conceptHeat.hasRetreat) {
      tags.push({ text: '概念退潮抑制', color: C.red, weight: 97 })
    }
    if (conceptHeat.hasWeak) {
      tags.push({ text: '题材承接转弱', color: C.yellow, weight: 89 })
    }
    if (rebuildHeat.hasRebuildRepair) {
      tags.push({ text: '回补修复', color: C.cyan, weight: 92 })
    }
    if (rebuildHeat.hasChainReduce) {
      tags.push({ text: '连减收缩', color: C.yellow, weight: 91 })
    }
    if (rebuildHeat.hasReduceThenClear) {
      tags.push({ text: '减后清仓', color: C.red, weight: 94 })
    }
    const reasonText = baseFilteredCandidates.map(item => String(item.decision_reason || '')).join(' || ')
    if (reasonText.includes('左侧抑制:repeat_retreat_after_quick_clear')) {
      tags.push({ text: '左侧重复退潮', color: C.red, weight: 99 })
    } else if (reasonText.includes('左侧抑制:retreat_after_quick_clear')) {
      tags.push({ text: '左侧退潮抑制', color: C.red, weight: 94 })
    }
    if (reasonText.includes('左侧抑制:repeat_weak_after_quick_clear')) {
      tags.push({ text: '左侧重复转弱', color: C.yellow, weight: 91 })
    } else if (reasonText.includes('左侧抑制:weak_after_quick_clear')) {
      tags.push({ text: '左侧转弱抑制', color: C.yellow, weight: 87 })
    }
    return tags
  })()
  const candidateRiskTagMap = new Map<string, Array<{ text: string; color: string }>>()
  baseFilteredCandidates.forEach((item) => {
    const tags: Array<{ text: string; color: string }> = []
    const seen = new Set<string>()
    const pushTag = (text: string, color: string) => {
      if (seen.has(text)) return
      seen.add(text)
      tags.push({ text, color })
    }
    const pct = Number(item.pct_chg || 0)
    const conceptState = String(item.concept_state || '').toLowerCase()
    const reasonText = String(item.decision_reason || '')
    if (item.decision === 'build' && item.decision_mode === 'actionable' && trendStrength && trendStrength.score >= 68) {
      pushTag('建仓扩散', C.green)
    }
    if (item.decision === 'clear' || (item.decision === 'hold' && trendStrength && trendStrength.score < 44)) {
      pushTag('清仓回流', C.red)
    }
    if (item.decision === 'build' && item.decision_mode !== 'neutral' && pct >= 2) {
      pushTag('追高风险回升', C.yellow)
    }
    if (item.decision === 'build' && item.decision_mode === 'actionable' && trendStrength && trendStrength.score >= 56 && trendStrength.reasons.some((x: string) => x.includes('近两日强于前段'))) {
      pushTag('扩张提速', C.cyan)
    }
    if (item.decision === 'clear' && item.decision_mode !== 'neutral' && trendStrength && trendStrength.score < 50) {
      pushTag('防守收缩', C.red)
    }
    if ((conceptState === 'expand' || conceptState === 'strong') && item.decision === 'build' && item.decision_mode === 'actionable') {
      pushTag('概念扩张共振', C.green)
    }
    if (conceptState === 'retreat' && (item.decision === 'clear' || (item.decision === 'build' && item.decision_mode !== 'actionable'))) {
      pushTag('概念退潮抑制', C.red)
    } else if (conceptState === 'weak' && (item.decision === 'clear' || item.decision_mode === 'watch')) {
      pushTag('题材承接转弱', C.yellow)
    }
    if (reasonText.includes('左侧抑制:repeat_retreat_after_quick_clear')) {
      pushTag('左侧重复退潮', C.red)
    } else if (reasonText.includes('左侧抑制:retreat_after_quick_clear')) {
      pushTag('左侧退潮抑制', C.red)
    }
    if (reasonText.includes('左侧抑制:repeat_weak_after_quick_clear')) {
      pushTag('左侧重复转弱', C.yellow)
    } else if (reasonText.includes('左侧抑制:weak_after_quick_clear')) {
      pushTag('左侧转弱抑制', C.yellow)
    }
    if (String(item.last_rebuild_transition || '').includes('rebuild')) {
      pushTag('回补修复', C.cyan)
    }
    if (item.position_status === 'holding' && Number(item.reduce_streak || 0) >= 2) {
      pushTag('连减收缩', C.yellow)
    }
    if (Boolean(item.last_exit_after_partial) || Number(item.last_exit_partial_count || 0) > 0) {
      pushTag('减后清仓', C.red)
    }
    candidateRiskTagMap.set(item.ts_code, tags)
  })
  const candidateRiskCounts = new Map<string, number>()
  riskHotwords.forEach((tag) => {
    const count = baseFilteredCandidates.reduce((acc, item) => {
      const tags = candidateRiskTagMap.get(item.ts_code) || []
      return acc + (tags.some(x => x.text === tag.text) ? 1 : 0)
    }, 0)
    candidateRiskCounts.set(tag.text, count)
  })
  const sortedRiskHotwords = riskHotwords
    .map(tag => ({ ...tag, count: Number(candidateRiskCounts.get(tag.text) || 0) }))
    .filter(tag => tag.count > 0)
    .sort((a, b) => {
      if (b.weight !== a.weight) return b.weight - a.weight
      if (b.count !== a.count) return b.count - a.count
      return a.text.localeCompare(b.text, 'zh-CN')
    })
    .slice(0, 4)
  const filteredCandidates = baseFilteredCandidates.filter(item => {
    if (riskHotwordFilter === 'all') return true
    const tags = candidateRiskTagMap.get(item.ts_code) || []
    return tags.some(tag => tag.text === riskHotwordFilter)
  }).sort((a, b) => {
    const tagsA = candidateRiskTagMap.get(a.ts_code) || []
    const tagsB = candidateRiskTagMap.get(b.ts_code) || []
    const scoreA = Math.max(0, ...tagsA.map(tag => {
      const found = sortedRiskHotwords.find(x => x.text === tag.text)
      return Number(found?.weight || 0)
    }))
    const scoreB = Math.max(0, ...tagsB.map(tag => {
      const found = sortedRiskHotwords.find(x => x.text === tag.text)
      return Number(found?.weight || 0)
    }))
    if (scoreB !== scoreA) return scoreB - scoreA
    const actionableA = a.decision_mode === 'actionable' ? 1 : 0
    const actionableB = b.decision_mode === 'actionable' ? 1 : 0
    if (actionableB !== actionableA) return actionableB - actionableA
    if (Number(b.decision_strength || 0) !== Number(a.decision_strength || 0)) {
      return Number(b.decision_strength || 0) - Number(a.decision_strength || 0)
    }
    return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN')
  })
  return {
    buildList,
    clearList,
    holdList,
    transitions,
    dailyRows,
    historyEvents,
    filteredHistoryEvents,
    boardOptions,
    industryOptions,
    filteredCandidates,
    filteredTransitions,
    filteredDailyRows,
    boardRanking,
    industryRanking,
    conceptRanking,
    conceptOptions,
    clearReasonSummary,
    trendStrength,
    summaryText,
    summaryRisk,
    rhythmSummary,
    riskHotwords: sortedRiskHotwords,
    candidateRiskTagMap,
    candidateScopeCount: baseFilteredCandidates.length,
  }
}

function healthDetailText(detail: unknown): string {
  if (detail == null) return '--'
  if (typeof detail === 'string') return detail
  if (typeof detail === 'number' || typeof detail === 'boolean') return String(detail)
  if (typeof detail !== 'object') return String(detail)
  const obj = detail as Record<string, unknown>
  const parts: string[] = []
  for (const [k, v] of Object.entries(obj)) {
    if (v == null) continue
    if (typeof v === 'object') continue
    parts.push(`${k}=${String(v)}`)
  }
  if (parts.length > 0) return parts.join(' · ')
  try {
    return JSON.stringify(obj)
  } catch {
    return '--'
  }
}

function derivePool1GuardTags(signals: Signal[]): Array<{ text: string; color: string }> {
  const out: Array<{ text: string; color: string }> = []
  const seen = new Set<string>()
  const pushTag = (text: string, color: string) => {
    const key = `${text}|${color}`
    if (seen.has(key)) return
    seen.add(key)
    out.push({ text, color })
  }
  for (const sig of signals || []) {
    const details = sig?.details && typeof sig.details === 'object' ? sig.details as Record<string, any> : {}
    const observeReason = String(details.observe_reason || '').trim()
    const conceptEcology = details.concept_ecology && typeof details.concept_ecology === 'object'
      ? details.concept_ecology as Record<string, any>
      : null
    const conceptState = String(conceptEcology?.state || '').trim().toLowerCase()
    if (observeReason === 'left_side_repeat_retreat') pushTag('左侧重复退潮', C.red)
    else if (observeReason === 'left_side_streak_retreat') pushTag('左侧退潮抑制', C.red)
    else if (observeReason === 'left_side_repeat_weak') pushTag('左侧重复转弱', C.yellow)
    else if (observeReason === 'left_side_streak_weak') pushTag('左侧转弱抑制', C.yellow)
    else if (observeReason === 'concept_retreat') pushTag('概念退潮', C.red)
    else if (observeReason === 'concept_weak') pushTag('题材转弱', C.yellow)

    if ((sig.type === 'left_side_buy' || sig.type === 'right_side_breakout') && !details.observe_only) {
      if (conceptState === 'expand') pushTag('概念扩张', C.green)
      else if (conceptState === 'strong') pushTag('概念走强', C.cyan)
    }
  }
  return out
}

function derivePool1CooldownBadge(signals: Signal[]): { text: string; color: string; title?: string } | null {
  let best: { text: string; color: string; title?: string; priority: number } | null = null
  for (const sig of signals || []) {
    const details = sig?.details && typeof sig.details === 'object' ? sig.details as Record<string, any> : {}
    const guard = details.left_streak_guard && typeof details.left_streak_guard === 'object'
      ? details.left_streak_guard as Record<string, any>
      : null
    if (!guard || !guard.active) continue
    const reason = String(guard.observe_reason || guard.reason || '').trim()
    const remaining = Number(guard.remaining_cooldown_hours ?? NaN)
    if (!Number.isFinite(remaining) || remaining <= 0) continue
    const rounded = remaining >= 24 ? `${(remaining / 24).toFixed(1)}天` : `${Math.ceil(remaining)}h`
    const isRepeat = reason === 'left_side_repeat_retreat' || reason === 'left_side_repeat_weak' || reason.includes('repeat_')
    const color = reason.includes('retreat') ? C.red : C.yellow
    const candidate = {
      text: `冷却剩余${rounded}`,
      color,
      title: `左侧抑制冷却剩余 ${rounded}`,
      priority: isRepeat ? 2 : 1,
    }
    if (!best || candidate.priority > best.priority || (candidate.priority === best.priority && color === C.red && best.color !== C.red)) {
      best = candidate
    }
  }
  return best ? { text: best.text, color: best.color, title: best.title } : null
}

function derivePool1RecentFailBadge(signals: Signal[]): { text: string; color: string; title?: string } | null {
  let best: { text: string; color: string; title?: string; hours: number } | null = null
  for (const sig of signals || []) {
    const details = sig?.details && typeof sig.details === 'object' ? sig.details as Record<string, any> : {}
    const guard = details.left_streak_guard && typeof details.left_streak_guard === 'object'
      ? details.left_streak_guard as Record<string, any>
      : null
    if (!guard || !guard.active) continue
    const hours = Number(guard.hours_since_clear ?? NaN)
    if (!Number.isFinite(hours) || hours < 0) continue
    const reason = String(guard.observe_reason || guard.reason || '').trim()
    const color = reason.includes('retreat') ? C.red : C.yellow
    const rounded = hours >= 24 ? `${(hours / 24).toFixed(1)}天前` : `${Math.floor(hours)}h前`
    const candidate = {
      text: `前次失败${rounded}`,
      color,
      title: `最近一次左侧失败发生在 ${rounded}`,
      hours,
    }
    if (!best || candidate.hours < best.hours || (candidate.hours === best.hours && color === C.red && best.color !== C.red)) {
      best = candidate
    }
  }
  return best ? { text: best.text, color: best.color, title: best.title } : null
}

function derivePool1RepeatBadge(signals: Signal[]): { text: string; color: string; title?: string } | null {
  let best: { text: string; color: string; title?: string; streak: number } | null = null
  for (const sig of signals || []) {
    const details = sig?.details && typeof sig.details === 'object' ? sig.details as Record<string, any> : {}
    const guard = details.left_streak_guard && typeof details.left_streak_guard === 'object'
      ? details.left_streak_guard as Record<string, any>
      : null
    if (!guard || !guard.active) continue
    const streak = Number(guard.quick_clear_streak ?? 0)
    if (!Number.isFinite(streak) || streak < 2) continue
    const reason = String(guard.observe_reason || guard.reason || '').trim()
    const color = reason.includes('retreat') ? C.red : C.yellow
    const candidate = {
      text: `连败${Math.floor(streak)}次`,
      color,
      title: `左侧连续失败 ${Math.floor(streak)} 次`,
      streak: Math.floor(streak),
    }
    if (!best || candidate.streak > best.streak || (candidate.streak === best.streak && color === C.red && best.color !== C.red)) {
      best = candidate
    }
  }
  return best ? { text: best.text, color: best.color, title: best.title } : null
}

function MismatchSparkline({
  points,
  width = 220,
  height = 46,
  warn = FAST_SLOW_WARN_RATIO,
  crit = FAST_SLOW_CRIT_RATIO,
}: {
  points: FastSlowTrendPoint[]
  width?: number
  height?: number
  warn?: number
  crit?: number
}) {
  if (!points || points.length < 2) {
    return <div style={{ fontSize: 10, color: C.dim }}>趋势样本不足</div>
  }
  const [hoverIdx, setHoverIdx] = useState<number | null>(null)
  const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v))
  const vals = points.map(p => Number(p.mismatch_ratio || 0))
  const maxV = Math.max(crit * 1.25, ...vals, 0.001)
  const minV = 0
  const xDen = Math.max(1, points.length - 1)
  const ySpan = Math.max(1e-6, maxV - minV)
  const yFor = (v: number) => {
    const vv = clamp(v, minV, maxV)
    return (1 - (vv - minV) / ySpan) * (height - 1)
  }
  const xFor = (idx: number) => (idx / xDen) * (width - 1)
  const poly = points.map((p, i) => {
    const x = xFor(i)
    const y = yFor(Number(p.mismatch_ratio || 0))
    return `${x.toFixed(2)},${y.toFixed(2)}`
  }).join(' ')
  const latest = vals[vals.length - 1]
  const lineColor = latest >= crit ? C.red : latest >= warn ? C.yellow : C.green
  const yWarn = yFor(warn)
  const yCrit = yFor(crit)
  const markerStep = Math.max(1, Math.ceil(points.length / 70))
  const markers = points
    .map((p, i) => ({ p, i, v: Number(p.mismatch_ratio || 0) }))
    .filter(({ i, v }) => v >= warn && (i % markerStep === 0 || i === points.length - 1))
  const hoverPoint = (hoverIdx != null && hoverIdx >= 0 && hoverIdx < points.length) ? points[hoverIdx] : null
  const hx = hoverIdx != null ? xFor(hoverIdx) : 0
  const hy = hoverPoint ? yFor(Number(hoverPoint.mismatch_ratio || 0)) : 0
  const hoverAtLabel = hoverPoint
    ? (hoverPoint.at_iso
      ? new Date(hoverPoint.at_iso).toLocaleTimeString()
      : (hoverPoint.at ? new Date(Number(hoverPoint.at) * 1000).toLocaleTimeString() : '--:--:--'))
    : '--:--:--'
  const tipW = 166
  const tipLeft = clamp(hx + 8, 0, Math.max(0, width - tipW))
  const handleMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect()
    const px = clamp(e.clientX - rect.left, 0, width - 1)
    const idx = Math.round((px / Math.max(1, width - 1)) * xDen)
    setHoverIdx(clamp(idx, 0, points.length - 1))
  }

  return (
    <div style={{ position: 'relative', width }}>
      <svg
        width={width}
        height={height}
        style={{ display: 'block', background: 'rgba(0,0,0,0.2)', borderRadius: 4 }}
        onMouseMove={handleMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {/* risk background bands: red(critical) / yellow(warn) / green(normal) */}
        <rect x={0} y={0} width={width} height={Math.max(0, yCrit)} fill="rgba(239,68,68,0.10)" />
        <rect x={0} y={yCrit} width={width} height={Math.max(0, yWarn - yCrit)} fill="rgba(234,179,8,0.10)" />
        <rect x={0} y={yWarn} width={width} height={Math.max(0, height - yWarn)} fill="rgba(34,197,94,0.08)" />
        <line x1={0} y1={yWarn} x2={width} y2={yWarn} stroke={C.yellow} strokeWidth={1} strokeDasharray="3 2" opacity={0.6} />
        <line x1={0} y1={yCrit} x2={width} y2={yCrit} stroke={C.red} strokeWidth={1} strokeDasharray="3 2" opacity={0.65} />
        <polyline fill="none" stroke={lineColor} strokeWidth={2} points={poly} />
        {markers.map(({ i, v }) => {
          const cx = xFor(i)
          const cy = yFor(v)
          const fill = v >= crit ? C.red : C.yellow
          return <circle key={`m-${i}`} cx={cx} cy={cy} r={2.3} fill={fill} opacity={0.9} />
        })}
        {hoverPoint && (
          <>
            <line x1={hx} y1={0} x2={hx} y2={height} stroke={C.cyan} strokeWidth={1} strokeDasharray="2 2" opacity={0.8} />
            <circle cx={hx} cy={hy} r={3.4} fill={lineColor} stroke="#ffffff" strokeWidth={1} />
          </>
        )}
        <circle
          cx={xFor(points.length - 1)}
          cy={yFor(latest)}
          r={3.1}
          fill={lineColor}
          stroke="#ffffff"
          strokeWidth={1}
        />
      </svg>
      {hoverPoint && (
        <div style={{
          position: 'absolute',
          left: tipLeft,
          top: 4,
          width: tipW,
          background: 'rgba(15,17,23,0.95)',
          border: `1px solid ${C.border}`,
          borderRadius: 4,
          padding: '4px 6px',
          fontSize: 10,
          fontFamily: 'monospace',
          color: C.text,
          pointerEvents: 'none',
        }}>
          <div style={{ color: C.dim }}>{hoverAtLabel}</div>
          <div style={{ color: (Number(hoverPoint.mismatch_ratio || 0) >= crit) ? C.red : (Number(hoverPoint.mismatch_ratio || 0) >= warn ? C.yellow : C.green) }}>
            偏差: {(Number(hoverPoint.mismatch_ratio || 0) * 100).toFixed(2)}%
          </div>
          <div style={{ color: C.dim }}>样本: {hoverPoint.mismatch || 0}/{hoverPoint.members || 0}</div>
        </div>
      )}
    </div>
  )
}

function Pool1NetBuildSparkline({
  points,
  width = 220,
  height = 42,
}: {
  points: Pool1DecisionDailyRow[]
  width?: number
  height?: number
}) {
  if (!points || points.length < 2) {
    return <div style={{ fontSize: 10, color: C.dim }}>趋势样本不足</div>
  }
  const vals = points.map(p => Number(p.net_build_actionable || 0))
  const minV = Math.min(...vals, 0)
  const maxV = Math.max(...vals, 0)
  const span = Math.max(1, maxV - minV)
  const xDen = Math.max(1, points.length - 1)
  const xFor = (idx: number) => (idx / xDen) * (width - 1)
  const yFor = (v: number) => {
    const norm = (v - minV) / span
    return (1 - norm) * (height - 1)
  }
  const poly = points.map((p, i) => `${xFor(i).toFixed(2)},${yFor(Number(p.net_build_actionable || 0)).toFixed(2)}`).join(' ')
  const zeroY = yFor(0)
  const latest = vals[vals.length - 1]
  const lineColor = latest > 0 ? C.green : latest < 0 ? C.red : C.yellow
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ overflow: 'visible' }}>
      <line x1={0} y1={zeroY} x2={width} y2={zeroY} stroke={C.border} strokeDasharray="3 3" />
      <polyline fill="none" stroke={lineColor} strokeWidth={2} points={poly} />
      {points.map((p, i) => {
        const x = xFor(i)
        const y = yFor(Number(p.net_build_actionable || 0))
        return (
          <circle
            key={`${p.trade_date}-${i}`}
            cx={x}
            cy={y}
            r={2.8}
            fill={i === points.length - 1 ? C.cyan : lineColor}
          >
            <title>{`${p.trade_date} · 净建仓 ${p.net_build_actionable > 0 ? '+' : ''}${p.net_build_actionable} · 执行总数 ${p.actionable_total}`}</title>
          </circle>
        )
      })}
    </svg>
  )
}

/* ===== 信号提醒音 ===== */
let _audioCtx: AudioContext | null = null
function playSignalBeep() {
  try {
    if (!_audioCtx) _audioCtx = new AudioContext()
    const ctx = _audioCtx
    const osc = ctx.createOscillator()
    const gain = ctx.createGain()
    osc.connect(gain); gain.connect(ctx.destination)
    osc.frequency.value = 880; osc.type = 'sine'
    gain.gain.setValueAtTime(0.15, ctx.currentTime)
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3)
    osc.start(ctx.currentTime); osc.stop(ctx.currentTime + 0.3)
  } catch {}
}

/* ===== WebSocket Hook ===== */
interface IndexData { ts_code: string; name: string; price: number; pre_close: number; pct_chg: number; amount: number; is_mock?: boolean; source?: string; updated_at?: number }

function useRealtimeWS(poolId: number, soundOn: boolean, pageVisible: boolean = true) {
  const [connected, setConnected] = useState(false)
  const [tickMap, setTickMap] = useState<Record<string, TickData>>({})
  // barsMap已移除，改用tick数据绘制分时图
  const [txnsMap, setTxnsMap] = useState<Record<string, Txn[]>>({})
  const [tickHistoryMap, setTickHistoryMap] = useState<Record<string, TickDataPoint[]>>({})
  const [signalRows, setSignalRows] = useState<SignalRow[]>([])
  const [marketStatus, setMarketStatus] = useState<MarketStatus>({ is_open: true, status: 'unknown' })
  const [lastEvalAt, setLastEvalAt] = useState<number>(0)

  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pingTimer = useRef<ReturnType<typeof setInterval> | null>(null)
  const prevSignalKeys = useRef<Set<string>>(new Set())
  const mountedRef = useRef(false)
  const reconnectAttempt = useRef(0)  // 重连次数，用于指数退避
  // HTTP 降级轮询
  const [usePolling, setUsePolling] = useState(false)
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pollSlowRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const memberCodesRef = useRef<string[]>([])
  const memberCodesAtRef = useRef(0)
  const memberCodesFailRef = useRef(0)
  const fastFailRef = useRef(0)
  const tickFailRef = useRef(0)
  const marketFetchedAtRef = useRef(0)

  const handleSignalNotify = useCallback((data: SignalRow[]) => {
    if (!soundOn) return
    const newKeys = new Set<string>()
    for (const row of data) {
      for (const s of row.signals) {
        const key = `${row.ts_code}:${s.type}`
        newKeys.add(key)
        if (!prevSignalKeys.current.has(key)) {
          playSignalBeep()
          if (s.strength >= 75 && Notification.permission === 'granted') {
            new Notification(`${row.name} ${s.message}`, { body: `强度 ${s.strength}` })
          }
        }
      }
    }
    prevSignalKeys.current = newKeys
  }, [soundOn])

  // WS 消息处理
  const onMessage = useCallback((event: MessageEvent) => {
    try {
      const msg = JSON.parse(event.data)
      switch (msg.type) {
        case 'tick':
          setTickMap(prev => mergeTickMap(prev, msg.data || {}, !!msg.full_snapshot))
          break
        case 'transactions':
          setTxnsMap(prev => mergeTxnsMap(prev, msg.data || {}, !!msg.full_snapshot))
          break
        case 'signals':
          const sigData: SignalRow[] = msg.data || []
          setSignalRows(prev => {
            const merged = mergeSignalRows(prev, sigData, !!msg.full_snapshot)
            handleSignalNotify(merged)
            return merged
          })
          setLastEvalAt(Date.now())
          break
        case 'market_status':
          setMarketStatus({ is_open: msg.is_open, status: msg.status, desc: msg.desc })
          break
        case 'tick_history':
          setTickHistoryMap(msg.data || {})
          break
      }
    } catch {}
  }, [handleSignalNotify])

  // WS 消息处理：用 ref 存储，避免 connectWS 因 onMessage 变化而重建
  const onMessageRef = useRef(onMessage)
  onMessageRef.current = onMessage

  // poolId 也用 ref，避免 connectWS 因 poolId 变化而重建
  const poolIdRef = useRef(poolId)
  poolIdRef.current = poolId

  // 连接 WS（不依赖任何 state，通过 ref 读取最新值）
  const connectWS = useCallback(() => {
    if (!mountedRef.current) return
    // 先关闭已有连接
    const oldWs = wsRef.current
    if (oldWs) {
      oldWs.onopen = null
      oldWs.onclose = null
      oldWs.onerror = null
      oldWs.onmessage = null
      if (oldWs.readyState === WebSocket.OPEN || oldWs.readyState === WebSocket.CONNECTING) {
        oldWs.close()
      }
    }

    try {
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => {
        if (!mountedRef.current) { ws.close(); return }
        setConnected(true)
        setUsePolling(false)
        reconnectAttempt.current = 0
        // 订阅当前池（通过 ref 读取最新 poolId）
        ws.send(JSON.stringify({ action: 'subscribe', pool_id: poolIdRef.current }))
        // 心跳
        if (pingTimer.current) clearInterval(pingTimer.current)
        pingTimer.current = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ action: 'ping' }))
          }
        }, 30000)
      }

      ws.onmessage = (event) => onMessageRef.current(event)

      ws.onclose = () => {
        if (!mountedRef.current) return
        setConnected(false)
        if (pingTimer.current) clearInterval(pingTimer.current)
        // 指数退避重连：1s, 2s, 4s, 8s, ... 最大 30s
        reconnectAttempt.current += 1
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempt.current - 1), 30000)
        reconnectTimer.current = setTimeout(connectWS, delay)
      }

      ws.onerror = (error) => {
        if (!mountedRef.current) return
        console.error('[WS] connection error:', error)
        setConnected(false)
        // onerror 后浏览器会自动触发 onclose，重连由 onclose 接管
      }
    } catch {
      setUsePolling(true)
    }
  }, [])  // 空依赖：所有动态值通过 ref 读取

  // WS 生命周期管理（单一 effect，避免多 effect 竞态）
  useEffect(() => {
    mountedRef.current = true
    if (Notification.permission === 'default') Notification.requestPermission()
    connectWS()
    return () => {
      mountedRef.current = false
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current)
      if (pingTimer.current) clearInterval(pingTimer.current)
      if (pollRef.current) clearTimeout(pollRef.current)
      if (pollSlowRef.current) clearTimeout(pollSlowRef.current)
      // 关闭 WS
      const ws = wsRef.current
      if (ws) {
        ws.onopen = null
        ws.onclose = null
        ws.onerror = null
        ws.onmessage = null
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close()
        }
      }
    }
  }, [connectWS])

  // poolId 变化时重新订阅（不重建 WS 连接）
  useEffect(() => {
    const ws = wsRef.current
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ action: 'subscribe', pool_id: poolId }))
    }
    memberCodesRef.current = []
    memberCodesAtRef.current = 0
    fastFailRef.current = 0
    tickFailRef.current = 0
    memberCodesFailRef.current = 0
    marketFetchedAtRef.current = 0
  }, [poolId])

  // HTTP 降级轮询
  useEffect(() => {
    if (!usePolling) return

    let cancelled = false

    const getMemberCodes = async (isOpen: boolean): Promise<string[]> => {
      const now = Date.now()
      const ttl = isOpen ? (pageVisible ? 20_000 : 45_000) : 180_000
      if ((now - memberCodesAtRef.current) < ttl && memberCodesRef.current.length > 0) {
        return memberCodesRef.current
      }
      try {
        const r = await axios.get(`${API}/api/realtime/pool/${poolId}/members`)
        const members: Member[] = r.data.data || []
        const codes = members.map(m => m.ts_code).filter(Boolean)
        memberCodesRef.current = codes
        memberCodesAtRef.current = now
        memberCodesFailRef.current = 0
        return codes
      } catch {
        memberCodesFailRef.current = Math.min(6, memberCodesFailRef.current + 1)
        return memberCodesRef.current
      }
    }

    const fetchFast = async (): Promise<boolean> => {
      const now = Date.now()
      const isOpen = !!marketStatus.is_open
      let ok = true
      try {
        const r = await axios.get(`${API}/api/realtime/pool/${poolId}/signals_fast`)
        const data: SignalRow[] = r.data.data || []
        setSignalRows(prev => mergeSignalRows(prev, data, true))
        setLastEvalAt(Date.now())
        handleSignalNotify(data)
      } catch {
        ok = false
      }

      const marketEvery = isOpen ? (pageVisible ? 6_000 : 15_000) : 60_000
      if ((now - marketFetchedAtRef.current) >= marketEvery) {
        try {
          const r = await axios.get(`${API}/api/realtime/market_status`)
          setMarketStatus(r.data)
          marketFetchedAtRef.current = now
        } catch {
          ok = false
        }
      }

      return ok
    }

    const fetchTick = async (): Promise<boolean> => {
      const isOpen = !!marketStatus.is_open
      const codes = await getMemberCodes(isOpen)
      if (!codes || codes.length === 0) return true
      let ok = true
      try {
        const r1 = await axios.post(`${API}/api/realtime/batch/tick`, { ts_codes: codes })
        setTickMap(prev => mergeTickMap(prev, r1.data.data || {}, true))
      } catch {
        ok = false
      }
      try {
        const r2 = await axios.post(`${API}/api/realtime/batch/transactions`, { ts_codes: codes }, { params: { n: 15 } })
        setTxnsMap(prev => mergeTxnsMap(prev, r2.data.data || {}, true))
      } catch {
        ok = false
      }
      return ok
    }

    const runFastLoop = async () => {
      if (cancelled) return
      const isOpen = !!marketStatus.is_open
      const status = String(marketStatus.status || '')
      const preAuction = isPreAuctionStatus(status)
      const base = preAuction
        ? (pageVisible ? 20_000 : 40_000)
        : isOpen
        ? (pageVisible ? 3000 : 6000)
        : (status === 'lunch_break'
          ? (pageVisible ? 120_000 : 180_000)
          : (pageVisible ? 180_000 : 300_000))
      const ok = await fetchFast()
      fastFailRef.current = ok ? 0 : Math.min(6, fastFailRef.current + 1)
      const next = Math.min(base * Math.pow(2, fastFailRef.current), preAuction ? 60_000 : (isOpen ? 30_000 : 300_000))
      if (!cancelled) pollRef.current = setTimeout(runFastLoop, next)
    }

    const runTickLoop = async () => {
      if (cancelled) return
      const isOpen = !!marketStatus.is_open
      const status = String(marketStatus.status || '')
      const preAuction = isPreAuctionStatus(status)
      const base = preAuction
        ? (pageVisible ? 12_000 : 24_000)
        : isOpen
        ? (pageVisible ? 4500 : 9000)
        : (status === 'lunch_break'
          ? (pageVisible ? 180_000 : 240_000)
          : (pageVisible ? 240_000 : 360_000))
      const ok = await fetchTick()
      tickFailRef.current = ok ? 0 : Math.min(6, tickFailRef.current + 1)
      const next = Math.min(base * Math.pow(2, tickFailRef.current), preAuction ? 90_000 : (isOpen ? 40_000 : 360_000))
      if (!cancelled) pollSlowRef.current = setTimeout(runTickLoop, next)
    }

    runFastLoop()
    runTickLoop()
    return () => {
      cancelled = true
      if (pollRef.current) clearTimeout(pollRef.current)
      if (pollSlowRef.current) clearTimeout(pollSlowRef.current)
    }
  }, [usePolling, poolId, marketStatus.is_open, handleSignalNotify, pageVisible])

  // 手动刷新信号
  const refreshSignals = useCallback(() => {
    if (connected && wsRef.current?.readyState === WebSocket.OPEN) {
      // WS 模式下信号由服务端推送，这里触发一次 HTTP 请求
    }
    axios.get(`${API}/api/realtime/pool/${poolId}/signals_fast`)
      .then(r => {
        const data: SignalRow[] = r.data.data || []
        setSignalRows(prev => mergeSignalRows(prev, data, true)); setLastEvalAt(Date.now())
        handleSignalNotify(data)
      })
      .catch(() => {})
  }, [poolId, connected, handleSignalNotify])

  return {
    connected, usePolling, tickMap, txnsMap, tickHistoryMap, signalRows, marketStatus, lastEvalAt, refreshSignals,
  }
}

/* ===== 主组件 ===== */
export default function RealtimeMonitor() {
  const pageVisible = usePageVisible()
  const [pools, setPools] = useState<PoolInfo[]>([])
  const [activePoolId, setActivePoolId] = useState<number>(1)
  const [loading, setLoading] = useState(true)
  const { indices } = useGlobalIndices(pageVisible)

  const loadPools = () => {
    axios.get(`${API}/api/realtime/pools`)
      .then(r => setPools(r.data.data || []))
      .catch(err => console.error('加载池列表失败', err))
      .finally(() => setLoading(false))
  }

  useEffect(() => { loadPools() }, [])

  const activePool = pools.find(p => p.pool_id === activePoolId)

  return (
    <div style={{ padding: 20, minHeight: '100%', color: C.text }}>
      <IndicesTopBar indices={indices} />
      {/* 池切换 Tabs */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, borderBottom: `1px solid ${C.border}`, alignItems: 'center' }}>
        {pools.map(p => (
          <button key={p.pool_id} onClick={() => setActivePoolId(p.pool_id)}
            style={{
              padding: '10px 20px', cursor: 'pointer', background: 'none', border: 'none',
              fontSize: 14, fontWeight: 500,
              color: p.pool_id === activePoolId ? C.cyan : C.dim,
              borderBottom: p.pool_id === activePoolId ? `2px solid ${C.cyan}` : '2px solid transparent',
              display: 'flex', alignItems: 'center', gap: 6,
            }}>
            <Eye size={14} />
            {p.pool_id}号池·{p.name}
            <span style={{ padding: '1px 6px', borderRadius: 3, fontSize: 10, background: C.card, color: C.bright }}>{p.member_count}</span>
          </button>
        ))}
      </div>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 40, color: C.dim }}>加载中...</div>
      ) : activePool ? (
        <PoolPanel key={activePool.pool_id} pool={activePool} onChanged={loadPools} />
      ) : (
        <div style={{ textAlign: 'center', padding: 40, color: C.dim }}>暂无池</div>
      )}
    </div>
  )
}

/* ===== 池面板 ===== */
function PoolPanel({ pool, onChanged }: { pool: PoolInfo; onChanged: () => void }) {
  const pageVisible = usePageVisible()
  const [members, setMembers] = useState<Member[]>([])
  const [sortBy, setSortBy] = useState<SortKey>('default')
  const [filterSignal, setFilterSignal] = useState(false)
  const [soundOn, setSoundOn] = useState(true)
  const [compact, setCompact] = useState(false)
  const [quality, setQuality] = useState<QualitySummary | null>(null)
  const [qualityOpen, setQualityOpen] = useState(false)
  const [qualityHours, setQualityHours] = useState(24)
  const [drift, setDrift] = useState<DriftStatus | null>(null)
  const [pool1Observe, setPool1Observe] = useState<Pool1ObserveResp | null>(null)
  const [pool1ObserveUi, setPool1ObserveUi] = useState(POOL1_OBSERVE_UI_DEFAULT)
  const [pool1ObserveFailCount, setPool1ObserveFailCount] = useState(0)
  const [pool1DecisionSummary, setPool1DecisionSummary] = useState<Pool1DecisionSummaryResp | null>(null)
  const [pool1DecisionOpen, setPool1DecisionOpen] = useState(false)
  const [pool1DecisionFilter, setPool1DecisionFilter] = useState<'all' | 'build_actionable' | 'clear_actionable' | 'partial_actionable' | 'watch_only' | 'hold_only'>('all')
  const [pool1DecisionBoardFilter, setPool1DecisionBoardFilter] = useState('all')
  const [pool1DecisionIndustryFilter, setPool1DecisionIndustryFilter] = useState('all')
  const [pool1DecisionConceptFilter, setPool1DecisionConceptFilter] = useState('all')
  const [pool1RiskHotwordFilter, setPool1RiskHotwordFilter] = useState('all')
  const [pool1HighRiskMode, setPool1HighRiskMode] = useState<Pool1HighRiskMode>('all')
  const [pool1CandidateGroupOpen, setPool1CandidateGroupOpen] = useState<Record<string, boolean>>({})
  const [pool1CandidateGroupExpanded, setPool1CandidateGroupExpanded] = useState<Record<string, boolean>>({})
  const [pool1CandidateGroupModeFilter, setPool1CandidateGroupModeFilter] = useState<Record<string, 'all' | 'actionable' | 'watch'>>({})
  const [pool1CandidateGroupSortBy, setPool1CandidateGroupSortBy] = useState<Record<string, 'default' | 'strength' | 'pct_chg' | 'holding_days'>>({})
  const [diffOpen, setDiffOpen] = useState(false)
  const [locateTsCode, setLocateTsCode] = useState('')
  const [fastSlowDiff, setFastSlowDiff] = useState<FastSlowDiffResp | null>(null)
  const [fastSlowLoading, setFastSlowLoading] = useState(false)
  const [diffOnlyMismatch, setDiffOnlyMismatch] = useState(true)
  const [diffSignalType, setDiffSignalType] = useState('all')
  const [diffTrendHours, setDiffTrendHours] = useState(24)
  const [fastSlowTrend, setFastSlowTrend] = useState<FastSlowTrendResp | null>(null)
  const [fastSlowTrendLoading, setFastSlowTrendLoading] = useState(false)
  const [healthOpen, setHealthOpen] = useState(false)
  const [runtimeHealth, setRuntimeHealth] = useState<RuntimeHealthResp | null>(null)
  const [runtimeHealthLoading, setRuntimeHealthLoading] = useState(false)
  const pool1ObserveReqInFlight = useRef(false)
  const pool1ObserveReqSeq = useRef(0)
  const pool1DecisionReqInFlight = useRef(false)
  const pool1DecisionReqSeq = useRef(0)
  const cardRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const locateTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pool1CandidateRef = useRef<HTMLDivElement | null>(null)
  const pool1TrajectoryRef = useRef<HTMLDivElement | null>(null)
  const trajectoryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // WebSocket 实时数据
  const { connected, usePolling, tickMap, txnsMap, tickHistoryMap, signalRows, marketStatus, lastEvalAt, refreshSignals } = useRealtimeWS(pool.pool_id, soundOn, pageVisible)

  const loadQuality = useCallback(() => {
    if (pool.pool_id !== 2) return
    axios.get(`${API}/api/realtime/pool/2/quality_summary`, { params: { hours: qualityHours } })
      .then(r => setQuality(r.data))
      .catch(() => {})
    axios.get(`${API}/api/realtime/pool/2/drift_status`, { params: { days: 3 } })
      .then(r => setDrift(r.data))
      .catch(() => {})
  }, [pool.pool_id, qualityHours])

  useEffect(() => { if (qualityOpen) loadQuality() }, [qualityOpen, loadQuality])

  useEffect(() => {
    const applyUiConfig = (resp: RealtimeUiConfigResp | null) => {
      const cfg = resp?.data?.pool1_observe_ui
      if (!cfg) return
      setPool1ObserveUi({
        observePollSec: Number(cfg.observe_poll_sec ?? POOL1_OBSERVE_UI_DEFAULT.observePollSec),
        staleWarnSec: Number(cfg.stale_warn_sec ?? POOL1_OBSERVE_UI_DEFAULT.staleWarnSec),
        staleErrorSec: Number(cfg.stale_error_sec ?? POOL1_OBSERVE_UI_DEFAULT.staleErrorSec),
        sampleWarnMin: Number(cfg.sample_warn_min ?? POOL1_OBSERVE_UI_DEFAULT.sampleWarnMin),
        sampleRecommendMin: Number(cfg.sample_recommend_min ?? POOL1_OBSERVE_UI_DEFAULT.sampleRecommendMin),
      })
    }

    const now = Date.now()
    if (_uiConfigCache && now - _uiConfigCacheAt <= UI_CONFIG_CACHE_TTL_MS) {
      applyUiConfig(_uiConfigCache)
      return
    }

    axios.get<RealtimeUiConfigResp>(`${API}/api/realtime/ui_config`)
      .then(r => {
        _uiConfigCache = r.data || null
        _uiConfigCacheAt = Date.now()
        applyUiConfig(_uiConfigCache)
      })
      .catch(() => {})
  }, [])

  const loadPool1Observe = useCallback(() => {
    if (pool.pool_id !== 1) {
      setPool1Observe(null)
      setPool1ObserveFailCount(0)
      return
    }
    if (pool1ObserveReqInFlight.current) {
      return
    }
    pool1ObserveReqInFlight.current = true
    pool1ObserveReqSeq.current += 1
    const reqSeq = pool1ObserveReqSeq.current
    axios.get(`${API}/api/realtime/pool/1/observe_stats`)
      .then(r => {
        if (reqSeq !== pool1ObserveReqSeq.current) return
        setPool1Observe(r.data || null)
        setPool1ObserveFailCount(0)
      })
      .catch(() => {
        if (reqSeq !== pool1ObserveReqSeq.current) return
        setPool1ObserveFailCount(c => c + 1)
      })
      .finally(() => {
        if (reqSeq === pool1ObserveReqSeq.current) {
          pool1ObserveReqInFlight.current = false
        }
      })
  }, [pool.pool_id])

  const loadPool1DecisionSummary = useCallback(() => {
    if (pool.pool_id !== 1) {
      setPool1DecisionSummary(null)
      return
    }
    if (pool1DecisionReqInFlight.current) {
      return
    }
    pool1DecisionReqInFlight.current = true
    pool1DecisionReqSeq.current += 1
    const reqSeq = pool1DecisionReqSeq.current
    axios.get<Pool1DecisionSummaryResp>(`${API}/api/realtime/pool/1/decision_summary`)
      .then(r => {
        if (reqSeq !== pool1DecisionReqSeq.current) return
        setPool1DecisionSummary(r.data || null)
      })
      .catch(() => {})
      .finally(() => {
        if (reqSeq === pool1DecisionReqSeq.current) {
          pool1DecisionReqInFlight.current = false
        }
      })
  }, [pool.pool_id])

  useEffect(() => {
    if (pool.pool_id !== 1) return
    loadPool1Observe()
    loadPool1DecisionSummary()
    const baseSec = pool1ObserveUi.observePollSec || POOL1_OBSERVE_UI_DEFAULT.observePollSec
    const failFactor = pool1ObserveFailCount >= OBSERVE_FAIL_WARN_COUNT ? 2 : 1
    const visibleFactor = pageVisible ? 1 : 3
    const status = String(marketStatus.status || '')
    const marketFactor = isPreAuctionStatus(status)
      ? 4
      : (marketStatus.is_open ? 1 : (status === 'lunch_break' ? 8 : 15))
    const effectiveSec = baseSec * failFactor * visibleFactor * marketFactor
    const pollMs = Math.max(1000, effectiveSec * 1000)
    const timer = setInterval(() => {
      loadPool1Observe()
      loadPool1DecisionSummary()
    }, pollMs)
    return () => clearInterval(timer)
  }, [pool.pool_id, loadPool1Observe, loadPool1DecisionSummary, pool1ObserveUi.observePollSec, pool1ObserveFailCount, pageVisible, marketStatus.is_open, marketStatus.status])

  const loadFastSlowDiff = useCallback(() => {
    setFastSlowLoading(true)
    axios.get(`${API}/api/realtime/runtime/fast_slow_diff`, {
      params: { pool_id: pool.pool_id, limit: 200 },
    })
      .then(r => setFastSlowDiff(r.data || null))
      .catch(() => {})
      .finally(() => setFastSlowLoading(false))
  }, [pool.pool_id])

  const loadFastSlowTrend = useCallback(() => {
    setFastSlowTrendLoading(true)
    axios.get(`${API}/api/realtime/runtime/fast_slow_trend`, {
      params: { pool_id: pool.pool_id, hours: diffTrendHours, limit: 1500 },
    })
      .then(r => setFastSlowTrend(r.data || null))
      .catch(() => {})
      .finally(() => setFastSlowTrendLoading(false))
  }, [pool.pool_id, diffTrendHours])

  const loadRuntimeHealth = useCallback(() => {
    setRuntimeHealthLoading(true)
    axios.get(`${API}/api/realtime/runtime/health_summary`)
      .then(r => setRuntimeHealth(r.data || null))
      .catch(() => {})
      .finally(() => setRuntimeHealthLoading(false))
  }, [])

  useEffect(() => {
    if (!diffOpen) return
    loadFastSlowDiff()
    loadFastSlowTrend()
    const baseMs = isPreAuctionStatus(marketStatus.status) ? 90_000 : (marketStatus.is_open ? 30_000 : 90_000)
    const visFactor = pageVisible ? 1 : 2
    const timer = setInterval(() => {
      loadFastSlowDiff()
      loadFastSlowTrend()
    }, Math.max(10_000, baseMs * visFactor))
    return () => clearInterval(timer)
  }, [diffOpen, loadFastSlowDiff, loadFastSlowTrend, marketStatus.is_open, marketStatus.status, pageVisible])

  useEffect(() => {
    loadRuntimeHealth()
    const preAuction = isPreAuctionStatus(marketStatus.status)
    const baseMs = healthOpen
      ? (preAuction ? 90_000 : (marketStatus.is_open ? 30_000 : 90_000))
      : (preAuction ? 180_000 : (marketStatus.is_open ? 90_000 : 180_000))
    const visFactor = pageVisible ? 1 : 2
    const timer = setInterval(loadRuntimeHealth, Math.max(15_000, baseMs * visFactor))
    return () => clearInterval(timer)
  }, [loadRuntimeHealth, healthOpen, marketStatus.is_open, marketStatus.status, pageVisible])

  const diffSignalTypeOptions = useMemo(() => {
    const st = new Set<string>()
    for (const r of (fastSlowDiff?.rows || [])) {
      for (const t of (r.fast_types || [])) if (t) st.add(String(t))
      for (const t of (r.slow_types || [])) if (t) st.add(String(t))
    }
    return ['all', ...Array.from(st).sort()]
  }, [fastSlowDiff])

  const diffRowsFiltered = useMemo(() => {
    let rows = (fastSlowDiff?.rows || []) as FastSlowDiffRow[]
    if (diffOnlyMismatch) rows = rows.filter(r => !r.match)
    if (diffSignalType !== 'all') {
      rows = rows.filter(r =>
        (r.fast_types || []).includes(diffSignalType) ||
        (r.slow_types || []).includes(diffSignalType) ||
        (r.only_fast || []).includes(diffSignalType) ||
        (r.only_slow || []).includes(diffSignalType)
      )
    }
    return rows
  }, [fastSlowDiff, diffOnlyMismatch, diffSignalType])

  const exportFastSlowCsv = useCallback(() => {
    if (!fastSlowDiff || !Array.isArray(fastSlowDiff.rows)) return
    const csvRows = diffRowsFiltered.map(r => [
      r.ts_code,
      r.name,
      (r.fast_types || []).join('|'),
      (r.slow_types || []).join('|'),
      (r.only_fast || []).join('|'),
      (r.only_slow || []).join('|'),
      r.fast_price,
      r.slow_price,
      r.price_delta,
      r.match,
    ])
    const stamp = new Date().toISOString().replace(/[:.]/g, '-')
    downloadCsv(
      `fast_slow_diff_pool${pool.pool_id}_${stamp}.csv`,
      ['ts_code', 'name', 'fast_types', 'slow_types', 'only_fast', 'only_slow', 'fast_price', 'slow_price', 'price_delta', 'match'],
      csvRows,
    )
  }, [fastSlowDiff, diffRowsFiltered, pool.pool_id])

  const exportPool1DecisionCsv = useCallback(() => {
    if (pool.pool_id !== 1 || !pool1DecisionSummary) return
    const view = buildPool1DecisionView(
      pool1DecisionSummary,
      pool1DecisionFilter,
      pool1DecisionBoardFilter,
      pool1DecisionIndustryFilter,
      pool1DecisionConceptFilter,
      pool1RiskHotwordFilter,
    )
    const rows: (string | number | boolean | null | undefined)[][] = []
    view.filteredCandidates.forEach(item => {
      const decisionModeLabel = item.decision_mode === 'actionable' ? '执行级' : item.decision_mode === 'watch' ? '观察级' : '中性'
      rows.push([
        'candidate',
        '',
        item.ts_code,
        item.name,
        boardSegmentLabel(item.board_segment),
        item.industry || '',
        item.core_concept_board || '',
        item.decision,
        item.decision_label,
        item.decision_mode,
        decisionModeLabel,
        item.decision_strength,
        item.position_status,
        item.position_ratio != null ? `${(Number(item.position_ratio) * 100).toFixed(0)}%` : '',
        item.reduce_streak || '',
        item.reduce_ratio_cum != null ? `${(Number(item.reduce_ratio_cum) * 100).toFixed(0)}%` : '',
        (item.signal_types || []).join('|'),
        item.decision_reason,
        item.price,
        item.pct_chg,
        '',
        '',
        '',
        item.last_rebuild_add_ratio != null ? `${(Number(item.last_rebuild_add_ratio) * 100).toFixed(0)}%` : '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
      ])
    })
    view.filteredTransitions.forEach(item => {
      const decisionModeLabel = item.current_decision_mode === 'actionable' ? '执行级' : item.current_decision_mode === 'watch' ? '观察级' : '中性'
      rows.push([
        'transition',
        '',
        item.ts_code,
        item.name,
        boardSegmentLabel(item.board_segment),
        item.industry || '',
        item.core_concept_board || '',
        item.current_decision || '',
        item.current_decision_label || '',
        item.current_decision_mode || '',
        decisionModeLabel,
        item.current_decision_strength || '',
        item.current_position_status || '',
        item.current_position_ratio != null ? `${(Number(item.current_position_ratio) * 100).toFixed(0)}%` : '',
        item.reduce_streak || '',
        item.reduce_ratio_cum != null ? `${(Number(item.reduce_ratio_cum) * 100).toFixed(0)}%` : '',
        (item.current_signal_types || []).join('|') || item.signal_type || '',
        item.label || '',
        item.signal_price || '',
        '',
        item.transition || '',
        item.at_iso || '',
        item.reduce_ratio != null ? `${(Number(item.reduce_ratio) * 100).toFixed(0)}%` : '',
        item.rebuild_add_ratio != null ? `${(Number(item.rebuild_add_ratio) * 100).toFixed(0)}%` : '',
        item.rebuild_after_partial ? 'Y' : '',
        item.rebuild_from_partial_count || '',
        item.exit_after_partial ? 'Y' : '',
        item.exit_partial_count || '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
      ])
    })
    view.filteredDailyRows.forEach(item => {
      const actionableTotal = Number(item.actionable_total || 0)
      const buildRatio = actionableTotal > 0 ? (Number(item.build_actionable || 0) / actionableTotal) : 0
      const clearRatio = actionableTotal > 0 ? (Number(item.clear_actionable || 0) / actionableTotal) : 0
      rows.push([
        'daily',
        item.trade_date,
        '',
        '',
        pool1DecisionBoardFilter === 'all' ? '' : boardSegmentLabel(pool1DecisionBoardFilter),
        pool1DecisionIndustryFilter === 'all' ? '' : pool1DecisionIndustryFilter,
        pool1DecisionConceptFilter === 'all' ? '' : pool1DecisionConceptFilter,
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        item.build_actionable,
        item.build_watch,
        item.clear_actionable,
        item.clear_watch,
        item.partial_actionable,
        item.partial_watch,
        item.partial_reduce_ratio_sum,
        item.net_build_actionable,
        item.net_exposure_change,
        `${(buildRatio * 100).toFixed(1)}%`,
        `${(clearRatio * 100).toFixed(1)}%`,
      ])
    })
    const stamp = new Date().toISOString().replace(/[:.]/g, '-')
    downloadCsv(
      `pool1_decision_summary_${stamp}.csv`,
      [
        'row_type', 'trade_date', 'ts_code', 'name', 'board_segment', 'industry', 'core_concept_board',
        'decision', 'decision_label', 'decision_mode', 'decision_mode_label', 'decision_strength', 'position_status', 'position_ratio',
        'reduce_streak', 'reduce_ratio_cum_pct', 'signal_info', 'reason', 'price', 'pct_chg',
        'transition', 'triggered_at', 'reduce_ratio_pct', 'rebuild_add_ratio_pct', 'rebuild_after_partial', 'rebuild_from_partial_count', 'exit_after_partial', 'exit_partial_count',
        'build_actionable', 'build_watch', 'clear_actionable', 'clear_watch', 'partial_actionable', 'partial_watch', 'partial_reduce_ratio_sum', 'net_build_actionable', 'net_exposure_change', 'build_ratio_pct', 'clear_ratio_pct',
      ],
      rows,
    )
  }, [pool.pool_id, pool1DecisionSummary, pool1DecisionFilter, pool1DecisionBoardFilter, pool1DecisionIndustryFilter, pool1DecisionConceptFilter, pool1RiskHotwordFilter])

  const loadMembers = useCallback(() => {
    axios.get(`${API}/api/realtime/pool/${pool.pool_id}/members`)
      .then(r => setMembers(r.data.data || []))
      .catch(err => console.error('加载成员失败', err))
  }, [pool.pool_id])

  useEffect(() => { loadMembers() }, [loadMembers])

  const removeMember = (ts_code: string) => {
    if (!confirm('从池中移除？')) return
    axios.delete(`${API}/api/realtime/pool/${pool.pool_id}/remove/${ts_code}`)
      .then(() => { loadMembers(); onChanged() })
  }

  const addMember = (stock: SearchResult) => {
    axios.post(`${API}/api/realtime/pool/${pool.pool_id}/add`, {
      ts_code: stock.ts_code, name: stock.name, industry: stock.industry,
    }).then(r => {
      if (r.data.ok) { loadMembers(); onChanged() }
      else alert(r.data.msg)
    })
  }

  const updateNote = (ts_code: string, note: string) => {
    axios.put(`${API}/api/realtime/pool/${pool.pool_id}/note/${ts_code}`, null, { params: { note } })
      .then(() => loadMembers()).catch(() => {})
  }

  const signalMap = new Map(signalRows.map(r => [r.ts_code, r]))
  const pool1CardRiskTagMap = useMemo(() => {
    if (pool.pool_id !== 1 || !pool1DecisionSummary) {
      return new Map<string, Array<{ text: string; color: string }>>()
    }
    const view = buildPool1DecisionView(
      pool1DecisionSummary,
      'all',
      pool1DecisionBoardFilter,
      pool1DecisionIndustryFilter,
      pool1DecisionConceptFilter,
      'all',
    )
    return view.candidateRiskTagMap as Map<string, Array<{ text: string; color: string }>>
  }, [pool.pool_id, pool1DecisionSummary, pool1DecisionBoardFilter, pool1DecisionIndustryFilter, pool1DecisionConceptFilter])

  const pool1HighRiskBuckets = useMemo(() => {
    const repeatCodes = new Set<string>()
    const suppressedCodes = new Set<string>()
    if (pool.pool_id !== 1) {
      return { repeatCodes, suppressedCodes, allCodes: new Set<string>() }
    }
    const adverseTags = new Set([
      '左侧重复退潮',
      '左侧退潮抑制',
      '左侧重复转弱',
      '左侧转弱抑制',
      '概念退潮',
      '题材转弱',
    ])
    const repeatTags = new Set([
      '左侧重复退潮',
      '左侧重复转弱',
    ])
    members.forEach((member) => {
      const signalTags = derivePool1GuardTags(signalMap.get(member.ts_code)?.signals || [])
      const decisionTags = pool1CardRiskTagMap.get(member.ts_code) || []
      const merged = [...signalTags, ...decisionTags]
      if (merged.some(tag => repeatTags.has(tag.text))) repeatCodes.add(member.ts_code)
      if (merged.some(tag => adverseTags.has(tag.text))) suppressedCodes.add(member.ts_code)
    })
    const allCodes = new Set<string>([...suppressedCodes])
    repeatCodes.forEach(code => allCodes.add(code))
    return { repeatCodes, suppressedCodes, allCodes }
  }, [members, pool.pool_id, pool1CardRiskTagMap, signalMap])
  const pool1CooldownStats = useMemo(() => {
    let cooldownCount = 0
    let repeatCooldownCount = 0
    let expiringSoonCount = 0
    members.forEach((member) => {
      const signals = signalMap.get(member.ts_code)?.signals || []
      const cooldownBadge = derivePool1CooldownBadge(signals)
      if (!cooldownBadge) return
      cooldownCount += 1
      const repeatBadge = derivePool1RepeatBadge(signals)
      if (repeatBadge) repeatCooldownCount += 1
      const guard = signals
        .map(sig => (sig?.details && typeof sig.details === 'object' ? (sig.details as Record<string, any>).left_streak_guard : null))
        .find(x => x && typeof x === 'object' && Boolean((x as Record<string, any>).active)) as Record<string, any> | undefined
      const remaining = Number(guard?.remaining_cooldown_hours ?? NaN)
      if (Number.isFinite(remaining) && remaining > 0 && remaining <= 12) {
        expiringSoonCount += 1
      }
    })
    const suppressedTotal = pool1HighRiskBuckets.suppressedCodes.size
    return {
      cooldownCount,
      repeatCooldownCount,
      expiringSoonCount,
      cooldownRatio: suppressedTotal > 0 ? (cooldownCount / suppressedTotal) : 0,
    }
  }, [members, pool1HighRiskBuckets.suppressedCodes, signalMap])
  const locateMemberCard = useCallback((tsCode: string) => {
    if (!tsCode) return
    setFilterSignal(false)
    setPool1HighRiskMode('all')
    setLocateTsCode(tsCode)
    if (locateTimerRef.current) clearTimeout(locateTimerRef.current)
    const tryLocate = (attempt: number) => {
      const el = cardRefs.current[tsCode]
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
        return
      }
      if (attempt < 4) {
        window.setTimeout(() => tryLocate(attempt + 1), 120)
      }
    }
    window.setTimeout(() => tryLocate(0), 40)
    locateTimerRef.current = setTimeout(() => setLocateTsCode(prev => prev === tsCode ? '' : prev), 3200)
  }, [])

  const focusPool1Section = useCallback((target: { board?: string; industry?: string; concept?: string }, anchor: 'trajectory' | 'candidate' = 'trajectory') => {
    if (target.board != null) {
      setPool1DecisionBoardFilter(target.board)
      setPool1DecisionIndustryFilter('all')
      setPool1DecisionConceptFilter('all')
    }
    if (target.industry != null) {
      setPool1DecisionIndustryFilter(target.industry)
      setPool1DecisionBoardFilter('all')
      setPool1DecisionConceptFilter('all')
    }
    if (target.concept != null) {
      setPool1DecisionConceptFilter(target.concept)
      setPool1DecisionBoardFilter('all')
      setPool1DecisionIndustryFilter('all')
    }
    if (!pool1DecisionOpen) {
      setPool1DecisionOpen(true)
    }
    if (trajectoryTimerRef.current) clearTimeout(trajectoryTimerRef.current)
    trajectoryTimerRef.current = window.setTimeout(() => {
      const el = anchor === 'candidate' ? pool1CandidateRef.current : pool1TrajectoryRef.current
      el?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 120)
  }, [pool1DecisionOpen])

  const openPool1RiskHotwordFromCard = useCallback((tag: string) => {
    if (!tag) return
    setPool1DecisionOpen(true)
    setPool1DecisionFilter('all')
    setPool1DecisionBoardFilter('all')
    setPool1DecisionIndustryFilter('all')
    setPool1DecisionConceptFilter('all')
    setPool1HighRiskMode('all')
    setPool1RiskHotwordFilter(tag)
    setPool1CandidateGroupOpen(prev => ({ ...prev, [tag]: true }))
    if (trajectoryTimerRef.current) clearTimeout(trajectoryTimerRef.current)
    trajectoryTimerRef.current = window.setTimeout(() => {
      pool1CandidateRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 120)
  }, [])

  useEffect(() => {
    return () => {
      if (locateTimerRef.current) clearTimeout(locateTimerRef.current)
      if (trajectoryTimerRef.current) clearTimeout(trajectoryTimerRef.current)
    }
  }, [])

  // 排序
  const sortedMembers = [...members].sort((a, b) => {
    if (sortBy === 'pct_chg') {
      return (tickMap[b.ts_code]?.pct_chg ?? signalMap.get(b.ts_code)?.pct_chg ?? 0) -
             (tickMap[a.ts_code]?.pct_chg ?? signalMap.get(a.ts_code)?.pct_chg ?? 0)
    }
    if (sortBy === 'signal_strength') {
      return (signalMap.get(b.ts_code)?.signals?.[0]?.strength ?? 0) -
             (signalMap.get(a.ts_code)?.signals?.[0]?.strength ?? 0)
    }
    if (sortBy === 'amount') {
      return (tickMap[b.ts_code]?.amount ?? 0) - (tickMap[a.ts_code]?.amount ?? 0)
    }
    return 0
  })
  const firstHighRiskCodeByMode = useMemo(() => {
    const firstFor = (mode: Pool1HighRiskMode): string => {
      for (const member of sortedMembers) {
        if (mode === 'repeat' && pool1HighRiskBuckets.repeatCodes.has(member.ts_code)) return member.ts_code
        if (mode === 'suppressed' && pool1HighRiskBuckets.suppressedCodes.has(member.ts_code)) return member.ts_code
      }
      return ''
    }
    return {
      repeat: firstFor('repeat'),
      suppressed: firstFor('suppressed'),
    }
  }, [pool1HighRiskBuckets.repeatCodes, pool1HighRiskBuckets.suppressedCodes, sortedMembers])
  const openPool1HighRiskView = useCallback((mode: Pool1HighRiskMode) => {
    setFilterSignal(false)
    setPool1HighRiskMode(mode)
    setPool1DecisionOpen(true)
    if (trajectoryTimerRef.current) clearTimeout(trajectoryTimerRef.current)
    trajectoryTimerRef.current = window.setTimeout(() => {
      const targetCode = mode === 'repeat' ? firstHighRiskCodeByMode.repeat : firstHighRiskCodeByMode.suppressed
      if (targetCode) {
        const el = cardRefs.current[targetCode]
        if (el) {
          setLocateTsCode(targetCode)
          el.scrollIntoView({ behavior: 'smooth', block: 'center' })
          if (locateTimerRef.current) clearTimeout(locateTimerRef.current)
          locateTimerRef.current = setTimeout(() => setLocateTsCode(prev => prev === targetCode ? '' : prev), 3200)
          return
        }
      }
      pool1CandidateRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 120)
  }, [firstHighRiskCodeByMode.repeat, firstHighRiskCodeByMode.suppressed])

  const filteredMembers = sortedMembers.filter(m => {
    if (filterSignal && (signalMap.get(m.ts_code)?.signals?.length ?? 0) <= 0) return false
    if (pool.pool_id === 1) {
      if (pool1HighRiskMode === 'repeat' && !pool1HighRiskBuckets.repeatCodes.has(m.ts_code)) return false
      if (pool1HighRiskMode === 'suppressed' && !pool1HighRiskBuckets.suppressedCodes.has(m.ts_code)) return false
    }
    return true
  })

  const sortOptions: { key: SortKey; label: string }[] = [
    { key: 'default', label: '默认' }, { key: 'pct_chg', label: '涨跌幅' },
    { key: 'signal_strength', label: '信号强度' }, { key: 'amount', label: '成交额' },
  ]
  const healthLevel = runtimeHealth?.level || 'green'
  const healthColor = healthLevel === 'red' ? C.red : healthLevel === 'yellow' ? C.yellow : C.green
  const fastSlowHealthItem = (runtimeHealth?.items || []).find((x) => typeof x?.name === 'string' && x.name.startsWith('fast_slow_consistency_pool'))
  const fastSlowHealthDetail = (fastSlowHealthItem && typeof fastSlowHealthItem.detail === 'object')
    ? (fastSlowHealthItem.detail as Record<string, any>)
    : null
  const fsMismatch = Number(fastSlowHealthDetail?.mismatch ?? 0)
  const fsMembers = Number(fastSlowHealthDetail?.members ?? 0)
  const fsRatio = Number(fastSlowHealthDetail?.mismatch_ratio ?? 0)
  const fsWarn = Number(fastSlowHealthDetail?.warn_ratio ?? FAST_SLOW_WARN_RATIO)
  const fsCrit = Number(fastSlowHealthDetail?.crit_ratio ?? FAST_SLOW_CRIT_RATIO)
  const fsColor = fsRatio >= fsCrit ? C.red : fsRatio >= fsWarn ? C.yellow : C.green
  const healthItems = runtimeHealth?.items || []
  const healthBadCount = healthItems.filter(it => it.level !== 'green').length
  const trendPts = fastSlowTrend?.points || []
  const trendLatest = trendPts.length > 0 ? trendPts[trendPts.length - 1] : null

  return (
    <div>
      {/* 池说明 + 工具栏 */}
      <div style={{
        background: C.card, border: `1px solid ${C.border}`, borderRadius: 6,
        padding: '12px 16px', marginBottom: 12,
        display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap',
      }}>
        <div>
          <div style={{ color: C.bright, fontSize: 14, fontWeight: 600 }}>
            {pool.name} <span style={{ color: C.cyan, fontSize: 11, marginLeft: 6 }}>{pool.strategy}</span>
          </div>
          <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>{pool.desc}</div>
          {isPreAuctionStatus(marketStatus.status) && (
            <div style={{ color: '#f59e0b', fontSize: 11, marginTop: 6 }}>
              集合竞价阶段仅刷新行情，不评估信号，09:30 开盘后恢复正常处理
            </div>
          )}
        </div>
        <div style={{ flex: 1 }} />
        {/* 连接状态 */}
        <div style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 4, color: connected ? C.green : (usePolling ? C.yellow : C.dim) }}>
          {connected ? <Wifi size={12} /> : <WifiOff size={12} />}
          {connected ? 'WS' : (usePolling ? 'HTTP轮询' : '断开')}
        </div>
        {/* 交易时段 */}
        <div style={{ fontSize: 11, color: marketStatusColor(marketStatus.status, marketStatus.is_open), display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 6, height: 6, borderRadius: 3, background: marketStatusColor(marketStatus.status, marketStatus.is_open), display: 'inline-block' }} />
          {marketStatusLabel(marketStatus.status, marketStatus.desc)}
        </div>
        <div style={{ color: C.dim, fontSize: 11 }}>
          评估: {lastEvalAt ? new Date(lastEvalAt).toLocaleTimeString() : '--'}
        </div>
        {pool.pool_id === 2 && (
          <Link
            to="/realtime-t0-replay"
            style={{
              fontSize: 11,
              color: C.cyan,
              textDecoration: 'none',
              border: `1px solid ${C.cyan}`,
              borderRadius: 4,
              padding: '2px 8px',
              background: C.cyanBg,
            }}
          >
            反T回放页
          </Link>
        )}
        <button
          onClick={() => setHealthOpen(v => !v)}
          title={runtimeHealth ? `profile:${runtimeHealth.profile || '--'} · ${runtimeHealth.checked_at ? new Date(runtimeHealth.checked_at).toLocaleTimeString() : '--:--:--'}` : '运行健康'}
          style={{
            fontSize: 11, display: 'flex', alignItems: 'center', gap: 4,
            color: healthColor, background: C.bg, border: `1px solid ${healthColor}`,
            borderRadius: 4, padding: '2px 6px', cursor: 'pointer',
          }}
        >
          <span style={{ width: 6, height: 6, borderRadius: 3, background: healthColor, display: 'inline-block' }} />
          {runtimeHealthLoading ? '健康检查中' : `健康:${healthLevel.toUpperCase()}${healthBadCount > 0 ? `(${healthBadCount})` : ''}`}
        </button>
        {fastSlowHealthDetail && (
          <div style={{ fontSize: 10, color: fsColor }}>
            快慢偏差 {fsMismatch}/{fsMembers} ({(fsRatio * 100).toFixed(1)}%)
          </div>
        )}
        {pool.pool_id === 1 && pool1Observe?.supported && pool1Observe.data && (
          (() => {
            const ageSec = pool1Observe.updated_at ? Math.max(0, Math.floor(Date.now() / 1000) - pool1Observe.updated_at) : 0
            const freshnessColor = ageSec > pool1ObserveUi.staleErrorSec
              ? C.red
              : ageSec > pool1ObserveUi.staleWarnSec
                ? C.yellow
                : C.green
            const sampleWarn = pool1Observe.data.screen_total < pool1ObserveUi.sampleWarnMin
            const storageCheck = pool1Observe.storage_check || {}
            const redisEnabled = Boolean(storageCheck.redis_enabled ?? pool1Observe.data.redis_enabled ?? false)
            const redisReady = Boolean(storageCheck.redis_ready ?? pool1Observe.data.redis_ready ?? false)
            const storageSource = String(storageCheck.source ?? pool1Observe.data.storage_source ?? 'memory')
            const storageExpected = String(
              storageCheck.expected
                ?? pool1Observe.data.storage_expected
                ?? (redisEnabled ? 'redis' : 'memory')
            )
            const storageVerified = Boolean(
              storageCheck.verified
              ?? pool1Observe.data.storage_verified
              ?? (storageSource === storageExpected)
            )
            const storageDegraded = Boolean(storageCheck.degraded ?? pool1Observe.data.storage_degraded ?? false)
            const storageNote = String(storageCheck.note ?? pool1Observe.data.storage_note ?? '')
            const sourceLabelMap: Record<string, string> = { redis: 'Redis', memory: '内存' }
            const sourceLabel = sourceLabelMap[storageSource] || storageSource
            const expectedLabel = sourceLabelMap[storageExpected] || storageExpected
            const storageWarn = storageDegraded || (redisEnabled && !storageVerified && pool1Observe.data.screen_total > 0)
            const storageColor = storageWarn ? C.yellow : (storageSource === 'redis' ? C.green : C.dim)
            const storageBadgeText = storageWarn
              ? `存储:${sourceLabel}→${expectedLabel}(降级)`
              : `存储:${sourceLabel}`
            const pickTopReason = (m?: Record<string, number>): string => {
              if (!m || typeof m !== 'object') return '--'
              const entries = Object.entries(m).filter(([, v]) => Number(v || 0) > 0)
              if (entries.length <= 0) return '--'
              entries.sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
              return `${entries[0][0]}(${Number(entries[0][1] || 0)})`
            }
            const pickTopBucketReason = (m?: Record<string, Record<string, number>>, labelMap?: Record<string, string>): string => {
              if (!m || typeof m !== 'object') return '--'
              const entries = Object.entries(m)
                .map(([bucket, reasons]) => {
                  const inner = reasons && typeof reasons === 'object' ? Object.entries(reasons).filter(([, v]) => Number(v || 0) > 0) : []
                  if (inner.length <= 0) return null
                  inner.sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
                  const total = inner.reduce((acc, [, v]) => acc + Number(v || 0), 0)
                  const bucketLabel = labelMap?.[bucket] || bucket
                  return {
                    bucket,
                    bucketLabel,
                    total,
                    topReason: `${inner[0][0]}(${Number(inner[0][1] || 0)})`,
                  }
                })
                .filter(Boolean) as Array<{ bucket: string; bucketLabel: string; total: number; topReason: string }>
              if (entries.length <= 0) return '--'
              entries.sort((a, b) => {
                if (b.total !== a.total) return b.total - a.total
                return a.bucketLabel.localeCompare(b.bucketLabel)
              })
              return `${entries[0].bucketLabel}:${entries[0].topReason}`
            }
            const leftTop = pickTopReason(pool1Observe.data.reject_by_signal_type?.left_side_buy)
            const rightTop = pickTopReason(pool1Observe.data.reject_by_signal_type?.right_side_breakout)
            const w5 = pool1Observe.data.reject_windows?.['5m']
            const w15 = pool1Observe.data.reject_windows?.['15m']
            const w60 = pool1Observe.data.reject_windows?.['1h']
            const windowTop = (w?: { top_rejects?: Array<{ reason: string; count: number }> }): string => {
              const topList = Array.isArray(w?.top_rejects) ? (w?.top_rejects || []) : []
              const top = topList.length > 0 ? topList[0] : null
              if (!top || Number(top.count || 0) <= 0) return '--'
              return `${top.reason}(${Number(top.count || 0)})`
            }
            const regimeTop = (() => {
              const rm = pool1Observe.data.reject_by_regime || {}
              const entries = Object.entries(rm).map(([k, v]) => ({ k, top: pickTopReason(v as Record<string, number>) }))
                .filter(x => x.top !== '--')
              if (entries.length <= 0) return '--'
              return `${entries[0].k}:${entries[0].top}`
            })()
            const boardTop = pickTopBucketReason(pool1Observe.data.reject_by_board_segment, {
              main_board: '主板',
              gem: '创业板',
              star: '科创板',
              beijing: '北交所',
              other: '其他',
              unknown: '--',
            })
            const securityTop = pickTopBucketReason(pool1Observe.data.reject_by_security_type, {
              common_stock: '普通股',
              etf: 'ETF',
              risk_warning_stock: '风险警示',
              unknown: '--',
            })
            const listingTop = pickTopBucketReason(pool1Observe.data.reject_by_listing_stage, {
              ipo_early: '上市早期',
              ipo_young: '次新阶段',
              seasoned: '成熟阶段',
              unknown: '--',
            })
            const thObs = pool1Observe.data.threshold_observe || {}
            const thLeft = thObs.left_side_buy || {}
            const thRight = thObs.right_side_breakout || {}
            const thresholdText = (x: any): string => {
              const cur = (x?.current || {}) as Record<string, any>
              const bucket = String(cur.bucket || x?.top_bucket || '--')
              const ver = String(cur.threshold_version || '--')
              const src = String(cur.threshold_source || '--')
              const br = String(cur.blocked_reason || '')
              const blocked = Boolean(cur.blocked)
              const ob = Number(x?.observer_ratio || 0)
              const bd = Number(x?.blocked_ratio || 0)
              const ratio = `B${(bd * 100).toFixed(0)}%/O${(ob * 100).toFixed(0)}%`
              const base = `${bucket}·${ver}·${src}·${ratio}`
              if (blocked && br) return `${base}·${br}`
              return base
            }
            const storageTitle = [
              `期望:${expectedLabel}`,
              `实际:${sourceLabel}`,
              `校验:${storageVerified ? '通过' : '未通过'}`,
              `Redis:${redisEnabled ? (redisReady ? '就绪' : '未就绪') : '未启用'}`,
              storageNote ? `说明:${storageNote}` : '',
            ].filter(Boolean).join(' · ')
            return (
              <div style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '3px 8px', borderRadius: 4,
                background: C.bg,
                border: `1px solid ${sampleWarn || storageWarn ? C.yellow : C.border}`,
                fontSize: 11,
              }} title={`${pool1Observe.data.summary}；建议样本≥${pool1ObserveUi.sampleRecommendMin}后再做稳定判断`}>
                <span style={{ color: C.cyan, fontWeight: 600 }}>Pool1 两阶段</span>
                {sampleWarn && (
                  <span style={{ color: C.yellow, fontSize: 10 }}>样本偏少</span>
                )}
                <span style={{ color: C.dim }}>
                  通过率 <span style={{ color: sampleWarn ? C.yellow : C.bright }}>{(pool1Observe.data.pass_rate * 100).toFixed(1)}%</span>
                </span>
                <span style={{ color: C.dim }}>
                  触发率 <span style={{ color: sampleWarn ? C.yellow : C.bright }}>{(pool1Observe.data.trigger_rate * 100).toFixed(1)}%</span>
                </span>
                <span style={{ color: C.dim }}>
                  样本 <span style={{ color: sampleWarn ? C.yellow : C.bright }}>{pool1Observe.data.screen_total}</span>
                </span>
                <span style={{ color: storageColor, fontSize: 10 }} title={storageTitle}>
                  {storageBadgeText}
                </span>
                {Array.isArray(pool1Observe.data.top_rejects) && pool1Observe.data.top_rejects.length > 0 && (
                  <span style={{ color: C.dim, fontSize: 10 }}>
                    拦截Top: <span style={{ color: C.yellow }}>{pool1Observe.data.top_rejects.slice(0, 2).map(x => `${x.reason}(${x.count})`).join(' / ')}</span>
                  </span>
                )}
                <span style={{ color: C.dim, fontSize: 10 }}>
                  分型: <span style={{ color: C.yellow }}>左{leftTop}</span> / <span style={{ color: C.yellow }}>右{rightTop}</span>
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  近窗: <span style={{ color: C.yellow }}>5m {windowTop(w5)}</span> · <span style={{ color: C.yellow }}>15m {windowTop(w15)}</span> · <span style={{ color: C.yellow }}>1h {windowTop(w60)}</span>
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  Regime: <span style={{ color: C.yellow }}>{regimeTop}</span>
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  制度: <span style={{ color: C.yellow }}>板块 {boardTop}</span> / <span style={{ color: C.yellow }}>类型 {securityTop}</span> / <span style={{ color: C.yellow }}>阶段 {listingTop}</span>
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  阈值: <span style={{ color: C.yellow }}>左{thresholdText(thLeft)}</span> / <span style={{ color: C.yellow }}>右{thresholdText(thRight)}</span>
                </span>
                {(pool1Observe.trade_date || pool1Observe.updated_at) && (
                  <span style={{ color: freshnessColor, fontSize: 10, display: 'flex', alignItems: 'center', gap: 4 }}>
                    <span style={{ width: 6, height: 6, borderRadius: 3, background: freshnessColor, display: 'inline-block' }} />
                    {pool1Observe.trade_date || '--'} {pool1Observe.updated_at ? new Date(pool1Observe.updated_at * 1000).toLocaleTimeString() : '--:--:--'} · {formatAgeText(pool1Observe.updated_at)}
                  </span>
                )}
              </div>
            )
          })()
        )}
        {pool.pool_id === 1 && pool1DecisionSummary?.summary && (
          (() => {
            const s = pool1DecisionSummary.summary
            const checkedAtTs = Number(pool1DecisionSummary.checked_at_ts || 0)
            const actionableExamples = Array.isArray(pool1DecisionSummary.mode_examples?.actionable)
              ? (pool1DecisionSummary.mode_examples?.actionable || [])
              : []
            const buildExamples = Array.isArray(pool1DecisionSummary.decision_examples?.build)
              ? (pool1DecisionSummary.decision_examples?.build || [])
              : []
            const clearExamples = Array.isArray(pool1DecisionSummary.decision_examples?.clear)
              ? (pool1DecisionSummary.decision_examples?.clear || [])
              : []
            const leftSuppressedCount = Number(s.left_suppressed_count || 0)
            const leftRepeatSuppressedCount = Number(s.left_repeat_suppressed_count || 0)
            const conceptRetreatWatchCount = Number(s.concept_retreat_watch_count || 0)
            const leftSuppressedRatio = s.member_count > 0 ? (leftSuppressedCount / s.member_count) : 0
            const leftGeneralSuppressedCount = Math.max(0, leftSuppressedCount - leftRepeatSuppressedCount)
            const cooldownCount = Number(pool1CooldownStats.cooldownCount || 0)
            const repeatCooldownCount = Number(pool1CooldownStats.repeatCooldownCount || 0)
            const expiringSoonCount = Number(pool1CooldownStats.expiringSoonCount || 0)
            const cooldownRatio = Number(pool1CooldownStats.cooldownRatio || 0)
            const partialToday = Number(s.partial_today || 0)
            const rebuildToday = Number(s.rebuild_today || 0)
            const chainedPartialToday = Number(s.chained_partial_today || 0)
            const clearAfterPartialToday = Number(s.clear_after_partial_today || 0)
            const rebuildStabilizedToday = Number(s.rebuild_stabilized_today || 0)
            const rebuildReducedAgainToday = Number(s.rebuild_reduced_again_today || 0)
            const rebuildClearedAgainToday = Number(s.rebuild_cleared_again_today || 0)
            const rebuildSuccessRate = Number(s.rebuild_success_rate || 0)
            const rebuildReduceAgainRate = Number(s.rebuild_reduce_again_rate || 0)
            const titleParts = [
              `成员:${s.member_count}`,
              `建仓:${s.build_count}`,
              `持有:${s.hold_count}`,
              `清仓:${s.clear_count}`,
              partialToday > 0 ? `减仓:${partialToday}` : '',
              rebuildToday > 0 ? `回补:${rebuildToday}` : '',
              rebuildStabilizedToday > 0 ? `回补稳住:${rebuildStabilizedToday}` : '',
              rebuildReducedAgainToday > 0 ? `回补后再减:${rebuildReducedAgainToday}` : '',
              rebuildClearedAgainToday > 0 ? `回补后再清:${rebuildClearedAgainToday}` : '',
              chainedPartialToday > 0 ? `连减:${chainedPartialToday}` : '',
              clearAfterPartialToday > 0 ? `减后清:${clearAfterPartialToday}` : '',
              `观望:${s.observe_count}`,
              `执行级:${s.actionable_count}`,
              `观察级:${s.watch_count}`,
              leftSuppressedCount > 0 ? `左侧抑制:${leftSuppressedCount}(${(leftSuppressedRatio * 100).toFixed(1)}%)` : '',
              leftRepeatSuppressedCount > 0 ? `重复抑制:${leftRepeatSuppressedCount}` : '',
              cooldownCount > 0 ? `冷却中:${cooldownCount}(${(cooldownRatio * 100).toFixed(1)}%)` : '',
              expiringSoonCount > 0 ? `冷却将尽:${expiringSoonCount}` : '',
              conceptRetreatWatchCount > 0 ? `概念退潮观察:${conceptRetreatWatchCount}` : '',
              buildExamples.length > 0 ? `建仓候选:${buildExamples.slice(0, 3).map(x => `${x.name}(${x.decision_strength.toFixed(1)})`).join(' / ')}` : '',
              clearExamples.length > 0 ? `清仓候选:${clearExamples.slice(0, 3).map(x => `${x.name}(${x.decision_strength.toFixed(1)})`).join(' / ')}` : '',
              actionableExamples.length > 0 ? `执行级Top:${actionableExamples.slice(0, 4).map(x => `${x.name}:${x.decision_label}`).join(' / ')}` : '',
            ].filter(Boolean).join(' · ')
            return (
              <div style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '3px 8px', borderRadius: 4,
                background: C.bg,
                border: `1px solid ${C.border}`,
                fontSize: 11,
              }} title={titleParts}>
                <span style={{ color: C.cyan, fontWeight: 600 }}>Pool1 主线决策</span>
                <span style={{ color: C.dim }}>
                  建仓 <span style={{ color: pool1DecisionColor('build') }}>{s.build_count}</span>
                </span>
                <span style={{ color: C.dim }}>
                  持有 <span style={{ color: pool1DecisionColor('hold') }}>{s.hold_count}</span>
                </span>
                <span style={{ color: C.dim }}>
                  清仓 <span style={{ color: pool1DecisionColor('clear') }}>{s.clear_count}</span>
                </span>
                {partialToday > 0 && (
                  <span style={{ color: C.dim }}>
                    减仓 <span style={{ color: C.yellow }}>{partialToday}</span>
                  </span>
                )}
                {rebuildToday > 0 && (
                  <span style={{ color: C.dim }}>
                    回补 <span style={{ color: C.green }}>{rebuildToday}</span>
                  </span>
                )}
                {rebuildToday > 0 && (
                  <span style={{ color: C.dim }}>
                    回补稳住 <span style={{ color: rebuildSuccessRate >= 0.6 ? C.green : rebuildSuccessRate >= 0.35 ? C.yellow : C.red }}>{rebuildStabilizedToday}</span>
                    <span style={{ color: C.dim }}> ({(rebuildSuccessRate * 100).toFixed(0)}%)</span>
                  </span>
                )}
                {rebuildToday > 0 && (
                  <span style={{ color: C.dim }}>
                    回补后再减 <span style={{ color: rebuildReduceAgainRate >= 0.4 ? C.yellow : C.dim }}>{rebuildReducedAgainToday}</span>
                    {rebuildClearedAgainToday > 0 && (
                      <span style={{ color: C.red }}> · 再清 {rebuildClearedAgainToday}</span>
                    )}
                  </span>
                )}
                {chainedPartialToday > 0 && (
                  <span style={{ color: C.dim }}>
                    连减 <span style={{ color: C.yellow }}>{chainedPartialToday}</span>
                  </span>
                )}
                {clearAfterPartialToday > 0 && (
                  <span style={{ color: C.dim }}>
                    减后清 <span style={{ color: C.red }}>{clearAfterPartialToday}</span>
                  </span>
                )}
                <span style={{ color: C.dim }}>
                  观望 <span style={{ color: pool1DecisionColor('observe') }}>{s.observe_count}</span>
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  执行级 <span style={{ color: C.cyan }}>{s.actionable_count}</span> / 观察级 <span style={{ color: C.yellow }}>{s.watch_count}</span>
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  持仓 <span style={{ color: C.bright }}>{s.holding_count}</span> ({(Number(s.holding_ratio || 0) * 100).toFixed(1)}%)
                </span>
                <span style={{ color: C.dim, fontSize: 10 }}>
                  今日切换 <span style={{ color: C.cyan }}>{s.transition_today}</span> · 回补 <span style={{ color: C.green }}>{rebuildToday}</span> · 减仓 <span style={{ color: C.yellow }}>{partialToday}</span> · 净建仓 <span style={{ color: s.net_build_today >= 0 ? C.green : C.red }}>{s.net_build_today}</span>
                </span>
                {(leftSuppressedCount > 0 || leftRepeatSuppressedCount > 0 || conceptRetreatWatchCount > 0) && (
                  <span style={{ color: C.dim, fontSize: 10, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    <button
                      onClick={() => openPool1HighRiskView('suppressed')}
                      style={{
                        padding: '1px 6px',
                        fontSize: 10,
                        cursor: 'pointer',
                        borderRadius: 999,
                        background: pool1HighRiskMode === 'suppressed' ? 'rgba(234,179,8,0.10)' : C.bg,
                        border: `1px solid ${pool1HighRiskMode === 'suppressed' ? C.yellow : C.border}`,
                        color: pool1HighRiskMode === 'suppressed' ? C.yellow : C.dim,
                      }}
                    >
                      左侧抑制 {leftSuppressedCount}
                    </button>
                    <span>占比 <span style={{ color: leftSuppressedRatio >= 0.25 ? C.red : (leftSuppressedRatio >= 0.12 ? C.yellow : C.cyan) }}>{(leftSuppressedRatio * 100).toFixed(1)}%</span></span>
                    {leftRepeatSuppressedCount > 0 && (
                      <button
                        onClick={() => openPool1HighRiskView('repeat')}
                        style={{
                          padding: '1px 6px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 999,
                          background: pool1HighRiskMode === 'repeat' ? 'rgba(239,68,68,0.10)' : C.bg,
                          border: `1px solid ${pool1HighRiskMode === 'repeat' ? C.red : C.border}`,
                          color: pool1HighRiskMode === 'repeat' ? C.red : C.dim,
                        }}
                      >
                        重复 {leftRepeatSuppressedCount}
                      </button>
                    )}
                    {leftGeneralSuppressedCount > 0 && (
                      <span>
                        一般 <span style={{ color: C.yellow }}>{leftGeneralSuppressedCount}</span>
                      </span>
                    )}
                    {conceptRetreatWatchCount > 0 && (
                      <span>
                        退潮观察 <span style={{ color: C.red }}>{conceptRetreatWatchCount}</span>
                      </span>
                    )}
                    {cooldownCount > 0 && (
                      <span>
                        冷却中 <span style={{ color: cooldownRatio >= 0.6 ? C.red : (cooldownRatio >= 0.35 ? C.yellow : C.cyan) }}>{cooldownCount}</span>
                        {' · '}占高风险 <span style={{ color: cooldownRatio >= 0.6 ? C.red : (cooldownRatio >= 0.35 ? C.yellow : C.cyan) }}>{(cooldownRatio * 100).toFixed(1)}%</span>
                        {repeatCooldownCount > 0 && (
                          <>
                            {' · '}重复冷却 <span style={{ color: C.red }}>{repeatCooldownCount}</span>
                          </>
                        )}
                        {expiringSoonCount > 0 && (
                          <>
                            {' · '}冷却将尽 <span style={{ color: expiringSoonCount >= 3 ? C.yellow : C.cyan }}>{expiringSoonCount}</span>
                          </>
                        )}
                      </span>
                    )}
                  </span>
                )}
                {checkedAtTs > 0 && (
                  <span style={{ color: C.dim, fontSize: 10 }}>
                    {new Date(checkedAtTs * 1000).toLocaleTimeString()} · {formatAgeText(checkedAtTs)}
                  </span>
                )}
              </div>
            )
          })()
        )}
        {pool.pool_id === 1 && pool1DecisionSummary?.summary && (
          <button
            onClick={() => setPool1DecisionOpen(v => !v)}
            style={{
              padding: '3px 8px',
              fontSize: 11,
              cursor: 'pointer',
              borderRadius: 4,
              background: pool1DecisionOpen ? C.cyanBg : C.bg,
              border: `1px solid ${pool1DecisionOpen ? C.cyan : C.border}`,
              color: pool1DecisionOpen ? C.cyan : C.dim,
              display: 'flex',
              alignItems: 'center',
              gap: 4,
            }}
          >
            {pool1DecisionOpen ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
            主线决策明细
          </button>
        )}
        {pool.pool_id === 1 && pool1ObserveFailCount >= OBSERVE_FAIL_WARN_COUNT && (
          <span style={{
            fontSize: 10,
            color: C.yellow,
            background: C.bg,
            border: `1px solid ${C.yellow}`,
            borderRadius: 4,
            padding: '2px 6px',
          }} title={`observe_stats 连续失败 ${pool1ObserveFailCount} 次，轮询已降频`}>
            统计暂不可用(降频)
          </span>
        )}
        <select value={sortBy} onChange={e => setSortBy(e.target.value as SortKey)}
          style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, color: C.text, fontSize: 11, padding: '3px 6px' }}>
          {sortOptions.map(o => <option key={o.key} value={o.key}>排序:{o.label}</option>)}
        </select>
        <button onClick={() => setFilterSignal(!filterSignal)}
          style={{ padding: '3px 8px', fontSize: 11, cursor: 'pointer', borderRadius: 4, background: filterSignal ? C.cyanBg : C.bg, border: `1px solid ${filterSignal ? C.cyan : C.border}`, color: filterSignal ? C.cyan : C.dim }}>
          {filterSignal ? '仅信号' : '全部'}
        </button>
        {pool.pool_id === 1 && (
          <>
            <button
              onClick={() => setPool1HighRiskMode(prev => prev === 'repeat' ? 'all' : 'repeat')}
              style={{
                padding: '3px 8px',
                fontSize: 11,
                cursor: 'pointer',
                borderRadius: 4,
                background: pool1HighRiskMode === 'repeat' ? 'rgba(239,68,68,0.10)' : C.bg,
                border: `1px solid ${pool1HighRiskMode === 'repeat' ? C.red : C.border}`,
                color: pool1HighRiskMode === 'repeat' ? C.red : C.dim,
              }}
              title={`只看左侧重复退潮/重复转弱票，当前 ${pool1HighRiskBuckets.repeatCodes.size} 只`}
            >
              重复退潮 {pool1HighRiskBuckets.repeatCodes.size}
            </button>
            <button
              onClick={() => setPool1HighRiskMode(prev => prev === 'suppressed' ? 'all' : 'suppressed')}
              style={{
                padding: '3px 8px',
                fontSize: 11,
                cursor: 'pointer',
                borderRadius: 4,
                background: pool1HighRiskMode === 'suppressed' ? 'rgba(234,179,8,0.10)' : C.bg,
                border: `1px solid ${pool1HighRiskMode === 'suppressed' ? C.yellow : C.border}`,
                color: pool1HighRiskMode === 'suppressed' ? C.yellow : C.dim,
              }}
              title={`只看左侧/题材抑制票，当前 ${pool1HighRiskBuckets.suppressedCodes.size} 只`}
            >
              一般抑制 {pool1HighRiskBuckets.suppressedCodes.size}
            </button>
          </>
        )}
        <button onClick={() => setCompact(!compact)}
          style={{ padding: '3px 8px', fontSize: 11, cursor: 'pointer', borderRadius: 4, background: C.bg, border: `1px solid ${C.border}`, color: compact ? C.cyan : C.dim }}>
          {compact ? '展开' : '紧凑'}
        </button>
        <button onClick={() => setSoundOn(!soundOn)} title={soundOn ? '关闭提醒音' : '开启提醒音'}
          style={{ background: 'none', border: 'none', cursor: 'pointer', color: soundOn ? C.cyan : C.dim, padding: 2 }}>
          {soundOn ? <Volume2 size={14} /> : <VolumeX size={14} />}
        </button>
        <button onClick={refreshSignals}
          style={{ padding: '6px 12px', background: C.cyanBg, border: `1px solid ${C.cyan}`, color: C.cyan, borderRadius: 4, cursor: 'pointer', fontSize: 12, display: 'flex', alignItems: 'center', gap: 4 }}>
          <RefreshCw size={12} /> 刷新信号
        </button>
      </div>

      {pool.pool_id === 1 && pool1DecisionOpen && pool1DecisionSummary?.summary && (
        (() => {
          const view = buildPool1DecisionView(
            pool1DecisionSummary,
            pool1DecisionFilter,
            pool1DecisionBoardFilter,
            pool1DecisionIndustryFilter,
            pool1DecisionConceptFilter,
            pool1RiskHotwordFilter,
          )
          const filteredCandidates = view.filteredCandidates
          const filteredTransitions = view.filteredTransitions
          const filteredDailyRows = view.filteredDailyRows
          const boardOptions = view.boardOptions
          const industryOptions = view.industryOptions
          const conceptOptions = view.conceptOptions
          const boardRanking = view.boardRanking
          const industryRanking = view.industryRanking
          const conceptRanking = view.conceptRanking
          const filteredHistoryEvents = view.filteredHistoryEvents
          const clearReasonSummary = view.clearReasonSummary
          const trendStrength = view.trendStrength
          const summaryText = view.summaryText
          const summaryRisk = view.summaryRisk
          const rhythmSummary = view.rhythmSummary
          const riskHotwords = view.riskHotwords
          const candidateRiskTagMap = view.candidateRiskTagMap
          const candidateScopeCount = Number(view.candidateScopeCount || 0)
          const transitionSummary = pool1DecisionSummary.transition_summary || pool1DecisionSummary.summary
          const filteredDailyMaxActionable = Math.max(1, ...filteredDailyRows.map(x => Number(x.actionable_total || 0)))
          const scopeText = [
            pool1DecisionBoardFilter === 'all' ? '' : `板块=${boardSegmentLabel(pool1DecisionBoardFilter)}`,
            pool1DecisionIndustryFilter === 'all' ? '' : `行业=${pool1DecisionIndustryFilter}`,
            pool1DecisionConceptFilter === 'all' ? '' : `概念=${pool1DecisionConceptFilter}`,
          ].filter(Boolean).join(' · ') || '全池视角'
          const candidateButtons: Array<{ key: 'all' | 'build_actionable' | 'clear_actionable' | 'partial_actionable' | 'watch_only' | 'hold_only'; label: string }> = [
            { key: 'all', label: '全部' },
            { key: 'build_actionable', label: '执行级建仓' },
            { key: 'clear_actionable', label: '执行级清仓' },
            { key: 'partial_actionable', label: '执行级减仓' },
            { key: 'watch_only', label: '观察级' },
            { key: 'hold_only', label: '持有建议' },
          ]
          const candidateGroups = (() => {
            const groups: Array<{
              key: string
              label: string
              color: string
              items: Pool1DecisionSummaryItem[]
              avgStrength: number
              actionableRatio: number
              avgPctChg: number
              avgHoldingDays: number | null
            }> = []
            const buildGroup = (key: string, label: string, color: string, items: Pool1DecisionSummaryItem[]) => {
              const actionableCount = items.filter(item => item.decision_mode === 'actionable').length
              const avgStrength = items.length > 0
                ? (items.reduce((acc, item) => acc + Number(item.decision_strength || 0), 0) / items.length)
                : 0
              const avgPctChg = items.length > 0
                ? (items.reduce((acc, item) => acc + Number(item.pct_chg || 0), 0) / items.length)
                : 0
              const holdingSamples = items
                .map(item => Number(item.holding_days))
                .filter(v => Number.isFinite(v) && v >= 0)
              const avgHoldingDays = holdingSamples.length > 0
                ? (holdingSamples.reduce((acc, v) => acc + v, 0) / holdingSamples.length)
                : null
              return {
                key,
                label,
                color,
                items,
                avgStrength,
                actionableRatio: items.length > 0 ? (actionableCount / items.length) : 0,
                avgPctChg,
                avgHoldingDays,
              }
            }
            if (pool1RiskHotwordFilter !== 'all') {
              groups.push(buildGroup(
                pool1RiskHotwordFilter,
                pool1RiskHotwordFilter,
                riskHotwords.find(tag => tag.text === pool1RiskHotwordFilter)?.color || C.cyan,
                filteredCandidates,
              ))
              return groups
            }
            riskHotwords.forEach((tag) => {
              const items = filteredCandidates.filter(item => (candidateRiskTagMap.get(item.ts_code) || []).some(x => x.text === tag.text))
              if (items.length > 0) {
                groups.push(buildGroup(tag.text, `${tag.text} ${items.length}`, tag.color, items))
              }
            })
            const otherItems = filteredCandidates.filter(item => (candidateRiskTagMap.get(item.ts_code) || []).length <= 0)
            if (otherItems.length > 0) {
              groups.push(buildGroup('_others', `其他 ${otherItems.length}`, C.dim, otherItems))
            }
            return groups
          })()
          const exportCandidateGroupCsv = (groupKey: string, items: Pool1DecisionSummaryItem[]) => {
            if (!items || items.length <= 0) return
            const rows: (string | number | boolean | null | undefined)[][] = items.map((item) => {
              const decisionModeLabel = item.decision_mode === 'actionable' ? '执行级' : item.decision_mode === 'watch' ? '观察级' : '中性'
              const riskTags = (candidateRiskTagMap.get(item.ts_code) || []).map(tag => tag.text).join('|')
              return [
                groupKey,
                item.ts_code,
                item.name,
                boardSegmentLabel(item.board_segment),
                item.industry || '',
                item.core_concept_board || '',
                item.decision,
                item.decision_label,
                item.decision_mode,
                decisionModeLabel,
                item.decision_strength,
                item.position_status,
                item.position_ratio != null ? `${(Number(item.position_ratio) * 100).toFixed(0)}%` : '',
                (item.signal_types || []).join('|'),
                riskTags,
                item.decision_reason,
                item.price,
                item.pct_chg,
              ]
            })
            const stamp = new Date().toISOString().replace(/[:.]/g, '-')
            downloadCsv(
              `pool1_candidate_group_${groupKey}_${stamp}.csv`,
              ['group', 'ts_code', 'name', 'board_segment', 'industry', 'core_concept_board', 'decision', 'decision_label', 'decision_mode', 'decision_mode_label', 'decision_strength', 'position_status', 'position_ratio', 'signal_types', 'risk_tags', 'reason', 'price', 'pct_chg'],
              rows,
            )
          }
          const renderCandidate = (item: Pool1DecisionSummaryItem, idx: number) => (
            <div key={`${item.ts_code}-${idx}`} style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: 10,
              padding: '6px 0',
              borderBottom: idx === 5 ? 'none' : `1px dashed ${C.border}`,
            }}>
              <div style={{ minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{ color: C.bright, fontSize: 12 }}>{item.name} <span style={{ color: C.dim }}>{item.ts_code}</span></span>
                  <span style={{ color: pool1DecisionColor(item.decision), fontSize: 11 }}>{item.decision_label}</span>
                  <span style={{ color: item.decision_mode === 'actionable' ? C.cyan : item.decision_mode === 'watch' ? C.yellow : C.dim, fontSize: 10 }}>
                    {item.decision_mode === 'actionable' ? '执行级' : item.decision_mode === 'watch' ? '观察级' : '中性'}
                  </span>
                  {item.core_concept_board && (
                    <span style={{
                      padding: '1px 6px',
                      borderRadius: 999,
                      border: `1px solid ${C.purple}`,
                      color: C.purple,
                      background: C.bg,
                      fontSize: 9,
                    }}>
                      概念:{item.core_concept_board}
                    </span>
                  )}
                  {(candidateRiskTagMap.get(item.ts_code) || []).map((tag, tagIdx) => (
                    <span key={`${item.ts_code}-${tag.text}-${tagIdx}`} style={{
                      padding: '1px 6px',
                      borderRadius: 999,
                      border: `1px solid ${tag.color}`,
                      color: tag.color,
                      background: C.bg,
                      fontSize: 9,
                    }}>
                      {tag.text}
                    </span>
                  ))}
                </div>
                <div style={{ color: C.dim, fontSize: 10, marginTop: 2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {item.decision_reason || '--'}
                </div>
                <div style={{ color: C.dim, fontSize: 9, marginTop: 2 }}>
                  状态 {item.position_status === 'holding' ? '持仓' : '观望'}
                  {typeof item.position_ratio === 'number' ? ` · 仓位${(Number(item.position_ratio) * 100).toFixed(0)}%` : ''}
                  {typeof item.holding_days === 'number' && item.holding_days > 0 ? ` · 持有${Number(item.holding_days).toFixed(1)}天` : ''}
                </div>
              </div>
              <div style={{ textAlign: 'right', fontSize: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
                <button
                  onClick={() => locateMemberCard(item.ts_code)}
                  style={{
                    padding: '2px 6px',
                    fontSize: 10,
                    cursor: 'pointer',
                    borderRadius: 4,
                    background: C.bg,
                    border: `1px solid ${C.border}`,
                    color: C.cyan,
                  }}
                >
                  定位
                </button>
                <div style={{ color: C.bright }}>{Number(item.decision_strength || 0).toFixed(1)}</div>
                <div style={{ color: C.dim }}>
                  {Number(item.price || 0) > 0 ? Number(item.price || 0).toFixed(2) : '--'} / {Number(item.pct_chg || 0).toFixed(2)}%
                </div>
              </div>
            </div>
          )
          return (
            <>
              <div ref={pool1TrajectoryRef} style={{
                marginBottom: 12,
                background: C.card,
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: '10px 14px',
                display: 'grid',
                gridTemplateColumns: '1.3fr 1.3fr 1fr',
                gap: 14,
              }}>
                <div ref={pool1CandidateRef}>
                  <div style={{ color: C.cyan, fontSize: 11, marginBottom: 8 }}>当前建仓/清仓候选</div>
                <div style={{ color: C.dim, fontSize: 10, marginBottom: 6 }}>
                  建仓 {pool1DecisionSummary.summary.build_count} / 减仓 {Number(pool1DecisionSummary.summary.partial_today || 0)} / 清仓 {pool1DecisionSummary.summary.clear_count} / 持有 {pool1DecisionSummary.summary.hold_count}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                  <button
                    onClick={exportPool1DecisionCsv}
                    style={{
                      padding: '2px 8px',
                      fontSize: 10,
                      cursor: 'pointer',
                      borderRadius: 4,
                      background: C.bg,
                      border: `1px solid ${C.border}`,
                      color: C.cyan,
                    }}
                  >
                    导出CSV
                  </button>
                  <select
                      value={pool1DecisionBoardFilter}
                      onChange={(e) => setPool1DecisionBoardFilter(e.target.value)}
                      style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, color: C.text, padding: '2px 6px' }}
                    >
                      <option value="all">全部板块</option>
                      {boardOptions.map(opt => <option key={opt} value={opt}>{boardSegmentLabel(opt)}</option>)}
                    </select>
                    <select
                      value={pool1DecisionIndustryFilter}
                      onChange={(e) => setPool1DecisionIndustryFilter(e.target.value)}
                      style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, color: C.text, padding: '2px 6px' }}
                    >
                      <option value="all">全部行业</option>
                      {industryOptions.map(opt => <option key={opt} value={opt}>{opt}</option>)}
                    </select>
                    <select
                      value={pool1DecisionConceptFilter}
                      onChange={(e) => setPool1DecisionConceptFilter(e.target.value)}
                      style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, color: C.text, padding: '2px 6px' }}
                    >
                      <option value="all">全部概念</option>
                      {conceptOptions.map(opt => <option key={opt} value={opt}>{opt}</option>)}
                    </select>
                    {(pool1DecisionBoardFilter !== 'all' || pool1DecisionIndustryFilter !== 'all' || pool1DecisionConceptFilter !== 'all') && (
                      <button
                        onClick={() => {
                          setPool1DecisionBoardFilter('all')
                          setPool1DecisionIndustryFilter('all')
                          setPool1DecisionConceptFilter('all')
                          setPool1RiskHotwordFilter('all')
                        }}
                        style={{
                          padding: '2px 8px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.dim,
                        }}
                      >
                        清空筛选
                      </button>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                    {candidateButtons.map(btn => (
                      <button
                        key={btn.key}
                        onClick={() => setPool1DecisionFilter(btn.key)}
                        style={{
                          padding: '2px 8px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: pool1DecisionFilter === btn.key ? C.cyanBg : C.bg,
                          border: `1px solid ${pool1DecisionFilter === btn.key ? C.cyan : C.border}`,
                          color: pool1DecisionFilter === btn.key ? C.cyan : C.dim,
                        }}
                      >
                        {btn.label}
                      </button>
                    ))}
                  </div>
                  {riskHotwords.length > 0 && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                      <button
                        onClick={() => setPool1RiskHotwordFilter('all')}
                        style={{
                          padding: '2px 8px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 999,
                          background: pool1RiskHotwordFilter === 'all' ? C.cyanBg : C.bg,
                          border: `1px solid ${pool1RiskHotwordFilter === 'all' ? C.cyan : C.border}`,
                          color: pool1RiskHotwordFilter === 'all' ? C.cyan : C.dim,
                        }}
                      >
                        全部热词
                      </button>
                      {riskHotwords.map((tag) => (
                        <button
                          key={`risk-filter-${tag.text}`}
                          onClick={() => setPool1RiskHotwordFilter(tag.text)}
                          style={{
                            padding: '2px 8px',
                            fontSize: 10,
                            cursor: 'pointer',
                            borderRadius: 999,
                            background: pool1RiskHotwordFilter === tag.text ? `${tag.color}18` : C.bg,
                            border: `1px solid ${pool1RiskHotwordFilter === tag.text ? tag.color : C.border}`,
                            color: pool1RiskHotwordFilter === tag.text ? tag.color : C.dim,
                          }}
                        >
                          {tag.text} {tag.count}
                        </button>
                      ))}
                    </div>
                  )}
                  {candidateGroups.length > 0 && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                      <button
                        onClick={() => setPool1CandidateGroupOpen(Object.fromEntries(candidateGroups.map(group => [group.key, true])))}
                        style={{
                          padding: '2px 8px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.cyan,
                        }}
                      >
                        展开全部
                      </button>
                      <button
                        onClick={() => setPool1CandidateGroupOpen(Object.fromEntries(candidateGroups.map(group => [group.key, false])))}
                        style={{
                          padding: '2px 8px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.dim,
                        }}
                      >
                        收起全部
                      </button>
                    </div>
                  )}
                  <div style={{ color: C.dim, fontSize: 10, marginBottom: 8 }}>
                    当前视角：{scopeText} · 候选 {filteredCandidates.length}/{candidateScopeCount} 只 · 切换 {filteredTransitions.length} 条
                    {pool1RiskHotwordFilter !== 'all' ? ` · 热词=${pool1RiskHotwordFilter}` : ''}
                  </div>
                  <div>
                    {filteredCandidates.length <= 0 && (
                      <div style={{ color: C.dim, fontSize: 11 }}>当前暂无明确主线候选</div>
                    )}
                    {candidateGroups.map((group) => {
                      const open = pool1CandidateGroupOpen[group.key] ?? true
                      const modeFilter = pool1CandidateGroupModeFilter[group.key] ?? 'all'
                      const sortBy = pool1CandidateGroupSortBy[group.key] ?? 'default'
                      const groupItemsRaw = group.items.filter((item) => {
                        if (modeFilter === 'actionable') return item.decision_mode === 'actionable'
                        if (modeFilter === 'watch') return item.decision_mode === 'watch'
                        return true
                      })
                      const groupItems = [...groupItemsRaw].sort((a, b) => {
                        if (sortBy === 'strength') {
                          return Number(b.decision_strength || 0) - Number(a.decision_strength || 0)
                        }
                        if (sortBy === 'pct_chg') {
                          return Number(b.pct_chg || 0) - Number(a.pct_chg || 0)
                        }
                        if (sortBy === 'holding_days') {
                          return Number(b.holding_days || 0) - Number(a.holding_days || 0)
                        }
                        const actionableA = a.decision_mode === 'actionable' ? 1 : 0
                        const actionableB = b.decision_mode === 'actionable' ? 1 : 0
                        if (actionableB !== actionableA) return actionableB - actionableA
                        if (Number(b.decision_strength || 0) !== Number(a.decision_strength || 0)) {
                          return Number(b.decision_strength || 0) - Number(a.decision_strength || 0)
                        }
                        return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN')
                      })
                      const distTotal = Math.max(1, groupItems.length)
                      const highCount = groupItems.filter(item => Number(item.decision_strength || 0) >= 80).length
                      const midCount = groupItems.filter(item => Number(item.decision_strength || 0) >= 60 && Number(item.decision_strength || 0) < 80).length
                      const lowCount = Math.max(0, groupItems.length - highCount - midCount)
                      const defaultLimit = pool1RiskHotwordFilter === 'all' ? 4 : 8
                      const expanded = pool1CandidateGroupExpanded[group.key] ?? false
                      const visibleItems = expanded ? groupItems : groupItems.slice(0, defaultLimit)
                      return (
                        <div key={`candidate-group-${group.key}`} style={{ marginBottom: 8, border: `1px solid ${C.border}`, borderRadius: 6, background: C.bg }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 8px' }}>
                            <button
                              onClick={() => setPool1CandidateGroupOpen(prev => ({ ...prev, [group.key]: !open }))}
                              style={{
                                flex: 1,
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                gap: 8,
                                background: 'none',
                                border: 'none',
                                cursor: 'pointer',
                                color: group.color,
                                fontSize: 10,
                                padding: 0,
                              }}
                            >
                              <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 2 }}>
                                <span>{group.label}</span>
                                <span style={{ color: C.dim, fontSize: 9 }}>
                                  均分 {group.avgStrength.toFixed(1)} · 执行级占比 {(group.actionableRatio * 100).toFixed(0)}% ·
                                  均涨跌 {group.avgPctChg >= 0 ? '+' : ''}{group.avgPctChg.toFixed(2)}% ·
                                  平均持仓 {group.avgHoldingDays == null ? '--' : `${group.avgHoldingDays.toFixed(1)}天`}
                                </span>
                              </span>
                              <span style={{ color: C.dim, display: 'flex', alignItems: 'center', gap: 4 }}>
                                {groupItems.length}/{group.items.length}只 {open ? <ChevronUp size={10} /> : <ChevronDown size={10} />}
                              </span>
                            </button>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                              {[
                                ['default', '默认'],
                                ['strength', '分数'],
                                ['pct_chg', '涨跌幅'],
                                ['holding_days', '持仓天数'],
                              ].map(([key, label]) => (
                                <button
                                  key={`${group.key}-sort-${key}`}
                                  onClick={() => setPool1CandidateGroupSortBy(prev => ({ ...prev, [group.key]: key as 'default' | 'strength' | 'pct_chg' | 'holding_days' }))}
                                  style={{
                                    padding: '1px 6px',
                                    fontSize: 9,
                                    cursor: 'pointer',
                                    borderRadius: 4,
                                    background: sortBy === key ? C.cyanBg : C.bg,
                                    border: `1px solid ${sortBy === key ? C.cyan : C.border}`,
                                    color: sortBy === key ? C.cyan : C.dim,
                                  }}
                                >
                                  {label}
                                </button>
                              ))}
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                              {[
                                ['all', '全部'],
                                ['actionable', '执行级'],
                                ['watch', '观察级'],
                              ].map(([key, label]) => (
                                <button
                                  key={`${group.key}-${key}`}
                                  onClick={() => setPool1CandidateGroupModeFilter(prev => ({ ...prev, [group.key]: key as 'all' | 'actionable' | 'watch' }))}
                                  style={{
                                    padding: '1px 6px',
                                    fontSize: 9,
                                    cursor: 'pointer',
                                    borderRadius: 4,
                                    background: modeFilter === key ? C.cyanBg : C.bg,
                                    border: `1px solid ${modeFilter === key ? C.cyan : C.border}`,
                                    color: modeFilter === key ? C.cyan : C.dim,
                                  }}
                                >
                                  {label}
                                </button>
                              ))}
                            </div>
                            <button
                              onClick={() => exportCandidateGroupCsv(group.key, groupItems)}
                              style={{
                                padding: '1px 6px',
                                fontSize: 9,
                                cursor: 'pointer',
                                borderRadius: 4,
                                background: C.bg,
                                border: `1px solid ${C.border}`,
                                color: C.cyan,
                              }}
                            >
                              导出
                            </button>
                          </div>
                          {open && (
                            <div style={{ padding: '0 8px 4px' }}>
                              <div style={{ marginBottom: 6 }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                                  <div style={{ flex: 1, height: 8, borderRadius: 999, overflow: 'hidden', display: 'flex', background: 'rgba(255,255,255,0.04)' }}>
                                    <div style={{ width: `${(highCount / distTotal) * 100}%`, background: C.green }} />
                                    <div style={{ width: `${(midCount / distTotal) * 100}%`, background: C.yellow }} />
                                    <div style={{ width: `${(lowCount / distTotal) * 100}%`, background: C.red }} />
                                  </div>
                                  <span style={{ color: C.dim, fontSize: 9 }}>分布</span>
                                </div>
                                <div style={{ color: C.dim, fontSize: 9 }}>
                                  高分 {highCount} · 中分 {midCount} · 低分 {lowCount}
                                </div>
                              </div>
                              {visibleItems.map(renderCandidate)}
                              {groupItems.length > defaultLimit && (
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0 2px', flexWrap: 'wrap' }}>
                                  <button
                                    onClick={() => setPool1CandidateGroupExpanded(prev => ({ ...prev, [group.key]: !expanded }))}
                                    style={{
                                      padding: '1px 6px',
                                      fontSize: 9,
                                      cursor: 'pointer',
                                      borderRadius: 4,
                                      background: C.bg,
                                      border: `1px solid ${C.border}`,
                                      color: expanded ? C.dim : C.cyan,
                                    }}
                                  >
                                    {expanded ? '收起列表' : `查看更多 (${groupItems.length - visibleItems.length})`}
                                  </button>
                                  <span style={{ color: C.dim, fontSize: 10 }}>
                                    当前显示 {visibleItems.length}/{groupItems.length} 只
                                  </span>
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </div>

                <div>
                  <div style={{ color: C.cyan, fontSize: 11, marginBottom: 8 }}>今日状态切换轨迹</div>
                  <div style={{ color: C.dim, fontSize: 10, marginBottom: 6 }}>
                    观望→持仓 {Number(transitionSummary.build_today || 0)} / 减仓后回补 {Number(transitionSummary.rebuild_today || 0)} / 持仓→减仓 {Number(transitionSummary.partial_today || 0)} / 连续减仓 {Number(transitionSummary.chained_partial_today || 0)} / 减仓后清仓 {Number(transitionSummary.clear_after_partial_today || 0)} / 持仓→观望 {Number(transitionSummary.clear_today || 0)} / 净建仓 {Number(transitionSummary.net_build_today || 0)}
                  </div>
                  {filteredTransitions.length <= 0 ? (
                    <div style={{ color: C.dim, fontSize: 11 }}>今日暂未发生主线状态切换</div>
                  ) : filteredTransitions.slice(0, 8).map((item, idx) => (
                    <div key={`${item.ts_code}-${item.transition}-${idx}`} style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 8,
                      padding: '6px 0',
                      borderBottom: idx === 7 ? 'none' : `1px dashed ${C.border}`,
                    }}>
                      <div style={{ minWidth: 0 }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                          <span style={{ color: C.bright, fontSize: 12 }}>{item.name}</span>
                          <span style={{ color: (item.transition === 'observe->holding(rebuild_after_partial)' || item.transition === 'holding->holding(rebuild)') ? C.cyan : item.transition === 'observe->holding' ? C.green : (item.transition === 'holding->holding(partial)' || item.transition === 'holding->holding(partial_chain)') ? C.yellow : C.red, fontSize: 10 }}>{item.label}</span>
                          <span style={{ color: C.dim, fontSize: 10 }}>{item.signal_type || '--'}</span>
                          {typeof item.reduce_ratio === 'number' && item.reduce_ratio > 0 && (
                            <span style={{ color: C.yellow, fontSize: 10 }}>减仓{(Number(item.reduce_ratio) * 100).toFixed(0)}%</span>
                          )}
                          {typeof item.rebuild_add_ratio === 'number' && item.rebuild_add_ratio > 0 && (
                            <span style={{ color: C.cyan, fontSize: 10 }}>回补{(Number(item.rebuild_add_ratio) * 100).toFixed(0)}%</span>
                          )}
                          {Number(item.reduce_streak || 0) >= 2 && (
                            <span style={{ color: C.yellow, fontSize: 10 }}>连减{Number(item.reduce_streak || 0)}次</span>
                          )}
                          {item.rebuild_after_partial && Number(item.rebuild_from_partial_count || 0) > 0 && (
                            <span style={{ color: C.cyan, fontSize: 10 }}>回补前连减{Number(item.rebuild_from_partial_count || 0)}次</span>
                          )}
                          {item.exit_after_partial && Number(item.exit_partial_count || 0) > 0 && (
                            <span style={{ color: C.red, fontSize: 10 }}>清仓前连减{Number(item.exit_partial_count || 0)}次</span>
                          )}
                        </div>
                      <div style={{ color: C.dim, fontSize: 10, marginTop: 2 }}>
                        当前: <span style={{ color: pool1DecisionColor(item.current_decision || '') }}>{item.current_decision_label || '--'}</span>
                        {typeof item.current_position_ratio === 'number' ? ` · 剩余仓位${(Number(item.current_position_ratio) * 100).toFixed(0)}%` : ''}
                        {typeof item.reduce_ratio_cum === 'number' && Number(item.reduce_ratio_cum || 0) > 0 ? ` · 累计减仓${(Number(item.reduce_ratio_cum) * 100).toFixed(0)}%` : ''}
                        {typeof item.rebuild_from_partial_ratio === 'number' && Number(item.rebuild_from_partial_ratio || 0) > 0 ? ` · 回补前累计减仓${(Number(item.rebuild_from_partial_ratio) * 100).toFixed(0)}%` : ''}
                        {typeof item.exit_reduce_ratio_cum === 'number' && Number(item.exit_reduce_ratio_cum || 0) > 0 ? ` · 清仓前累计减仓${(Number(item.exit_reduce_ratio_cum) * 100).toFixed(0)}%` : ''}
                        {typeof item.holding_days === 'number' ? ` · 持仓${Number(item.holding_days).toFixed(1)}天` : ''}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right', fontSize: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
                      <button
                        onClick={() => locateMemberCard(item.ts_code)}
                        style={{
                          padding: '2px 6px',
                          fontSize: 10,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.cyan,
                        }}
                      >
                        定位
                      </button>
                      <div style={{ color: C.bright }}>{item.at ? new Date(item.at * 1000).toLocaleTimeString() : '--:--:--'}</div>
                      <div style={{ color: C.dim }}>{Number(item.signal_price || 0) > 0 ? Number(item.signal_price || 0).toFixed(2) : '--'}</div>
                    </div>
                  </div>
                ))}
                </div>

                <div>
                  <div style={{ color: C.cyan, fontSize: 11, marginBottom: 8 }}>组合态势</div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8, fontSize: 11 }}>
                    <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                      <div style={{ color: C.dim }}>持仓中</div>
                      <div style={{ color: C.bright, fontSize: 16, fontWeight: 600 }}>{pool1DecisionSummary.summary.holding_count}</div>
                      <div style={{ color: C.dim, fontSize: 10 }}>占比 {(Number(pool1DecisionSummary.summary.holding_ratio || 0) * 100).toFixed(1)}%</div>
                    </div>
                    <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                      <div style={{ color: C.dim }}>执行级决策</div>
                      <div style={{ color: C.cyan, fontSize: 16, fontWeight: 600 }}>{pool1DecisionSummary.summary.actionable_count}</div>
                      <div style={{ color: C.dim, fontSize: 10 }}>观察级 {pool1DecisionSummary.summary.watch_count} / 中性 {pool1DecisionSummary.summary.neutral_count}</div>
                    </div>
                    <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                      <div style={{ color: C.dim }}>主线方向</div>
                      <div style={{ color: C.bright, fontSize: 12 }}>
                        建仓 <span style={{ color: pool1DecisionColor('build') }}>{pool1DecisionSummary.summary.build_count}</span> ·
                        持有 <span style={{ color: pool1DecisionColor('hold') }}>{pool1DecisionSummary.summary.hold_count}</span> ·
                        清仓 <span style={{ color: pool1DecisionColor('clear') }}>{pool1DecisionSummary.summary.clear_count}</span>
                      </div>
                      <div style={{ color: C.dim, fontSize: 10, marginTop: 4 }}>观望 {pool1DecisionSummary.summary.observe_count} · 成员 {pool1DecisionSummary.summary.member_count}</div>
                    </div>
                    <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                      <div style={{ color: C.dim }}>近5日主线评分</div>
                      {trendStrength ? (
                        <>
                          <div style={{ color: trendStrength.color, fontSize: 16, fontWeight: 600 }}>{trendStrength.score} / 100</div>
                          <div style={{ color: trendStrength.color, fontSize: 10 }}>{trendStrength.label}</div>
                          <div style={{ color: trendStrength.stateTag.color, fontSize: 10, marginTop: 3 }}>
                            {trendStrength.stateTag.text}
                          </div>
                          <div style={{ color: trendStrength.confirmLevel.color, fontSize: 10, marginTop: 3 }}>
                            确认档位：{trendStrength.confirmLevel.text}
                          </div>
                          <div style={{ color: C.dim, fontSize: 10, marginTop: 4 }}>
                            净建仓 {trendStrength.totalNet > 0 ? '+' : ''}{trendStrength.totalNet} · 最新建仓占比 {(trendStrength.latestBuildRatio * 100).toFixed(1)}%
                          </div>
                          {trendStrength.reasons.length > 0 && (
                            <div style={{ color: C.dim, fontSize: 10, marginTop: 4 }}>
                              {trendStrength.reasons.slice(0, 2).join(' / ')}
                            </div>
                          )}
                        </>
                      ) : (
                        <div style={{ color: C.dim, fontSize: 10 }}>暂无评分</div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
              <div style={{
                marginBottom: 12,
                background: C.card,
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: '10px 14px',
              }}>
              <div style={{ color: C.cyan, fontSize: 11, marginBottom: 8 }}>近5日主线轨迹</div>
              <div style={{ color: C.dim, fontSize: 10, marginBottom: 8 }}>
                口径: 以 Pool1 历史信号中的执行级/观察级建仓与清仓建议为准，便于看主线节奏是否在放大或收缩
              </div>
              <div style={{ color: trendStrength?.color || C.dim, fontSize: 10, marginBottom: 8 }}>
                {summaryText}
              </div>
              {trendStrength?.stateTag && (
                <div style={{ color: trendStrength.stateTag.color, fontSize: 10, marginBottom: 8 }}>
                  状态标记：{trendStrength.stateTag.text}
                </div>
              )}
              {trendStrength?.confirmLevel && (
                <div style={{ color: trendStrength.confirmLevel.color, fontSize: 10, marginBottom: 8 }}>
                  确认档位：{trendStrength.confirmLevel.text}
                </div>
              )}
              <div style={{ color: rhythmSummary?.color || C.dim, fontSize: 10, marginBottom: 8 }}>
                {rhythmSummary?.text}
              </div>
              {(Number(transitionSummary.rebuild_today || 0) > 0 || Number(transitionSummary.partial_today || 0) > 0 || Number(transitionSummary.clear_after_partial_today || 0) > 0) && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 8, color: C.dim, fontSize: 10 }}>
                  <span style={{ color: C.dim }}>连续行为</span>
                  {Number(transitionSummary.rebuild_today || 0) > 0 && (
                    <span style={{ color: C.cyan }}>
                      回补修复 {Number(transitionSummary.rebuild_today || 0)}
                      <span style={{ color: C.dim }}> · 稳住 {Number(pool1DecisionSummary.summary.rebuild_stabilized_today || 0)}</span>
                    </span>
                  )}
                  {Number(transitionSummary.partial_today || 0) > 0 && (
                    <span style={{ color: C.yellow }}>
                      连减收缩 {Number(transitionSummary.chained_partial_today || 0) > 0 ? Number(transitionSummary.chained_partial_today || 0) : Number(transitionSummary.partial_today || 0)}
                    </span>
                  )}
                  {Number(transitionSummary.clear_after_partial_today || 0) > 0 && (
                    <span style={{ color: C.red }}>
                      减后清仓 {Number(transitionSummary.clear_after_partial_today || 0)}
                    </span>
                  )}
                  {Number(transitionSummary.rebuild_today || 0) > 0 && (
                    <span style={{ color: C.dim }}>
                      回补成功率 {(Number(pool1DecisionSummary.summary.rebuild_success_rate || 0) * 100).toFixed(0)}% · 回补后再减率 {(Number(pool1DecisionSummary.summary.rebuild_reduce_again_rate || 0) * 100).toFixed(0)}%
                    </span>
                  )}
                </div>
              )}
              {riskHotwords.length > 0 && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                  <span style={{ color: C.dim, fontSize: 10 }}>风险热词</span>
                  {riskHotwords.map((tag, idx) => (
                    <button
                      key={`${tag.text}-${idx}`}
                      onClick={() => {
                        setPool1RiskHotwordFilter(tag.text)
                        focusPool1Section(
                          {
                            board: pool1DecisionBoardFilter === 'all' ? undefined : pool1DecisionBoardFilter,
                            industry: pool1DecisionIndustryFilter === 'all' ? undefined : pool1DecisionIndustryFilter,
                          },
                          'candidate',
                        )
                      }}
                      style={{
                        padding: '2px 8px',
                        borderRadius: 999,
                        border: `1px solid ${tag.color}`,
                        color: tag.color,
                        background: pool1RiskHotwordFilter === tag.text ? `${tag.color}18` : C.bg,
                        fontSize: 10,
                        cursor: 'pointer',
                      }}
                    >
                      {tag.text} {tag.count}
                    </button>
                  ))}
                </div>
              )}
              <div style={{ color: summaryRisk?.color || C.dim, fontSize: 10, marginBottom: 8 }}>
                {summaryRisk?.text}
              </div>
              {filteredDailyRows.length > 0 && (
                <>
                  <div style={{
                    marginBottom: 10,
                    background: C.bg,
                    border: `1px solid ${C.border}`,
                    borderRadius: 4,
                    padding: '8px 10px',
                  }}>
                    <div style={{ color: C.cyan, fontSize: 11, marginBottom: 6 }}>减仓/清仓原因统计</div>
                    {clearReasonSummary.length <= 0 ? (
                      <div style={{ color: C.dim, fontSize: 10 }}>当前筛选视角下暂无锚点退出样本</div>
                    ) : (
                      <>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 8, marginBottom: 8 }}>
                          {clearReasonSummary.map((item) => {
                            const totalClearBase = Math.max(1, filteredHistoryEvents.filter(evt => String(evt.signal_type || '') === 'timing_clear').length)
                            const ratio = item.count / totalClearBase
                            const color = item.key === 'multi_anchor_lost'
                              ? C.red
                              : item.key === 'event_anchor_lost'
                                ? C.yellow
                                : item.key === 'breakout_anchor_lost'
                                  ? C.cyan
                                  : C.dim
                            return (
                              <div key={item.key} style={{
                                border: `1px solid ${color}`,
                                borderRadius: 4,
                                padding: '7px 8px',
                                background: `${color}10`,
                                fontSize: 10,
                              }}>
                                <div style={{ color, fontWeight: 600, marginBottom: 4 }}>{item.label}</div>
                                <div style={{ color: C.bright }}>总样本 <span style={{ color }}>{item.count}</span></div>
                                <div style={{ color: C.dim }}>执行级 <span style={{ color: item.actionable > 0 ? color : C.text }}>{item.actionable}</span> · 占退出 {(ratio * 100).toFixed(1)}%</div>
                              </div>
                            )
                          })}
                        </div>
                        <div style={{ color: C.dim, fontSize: 10 }}>
                          当前主导退出原因：
                          <span style={{ color: clearReasonSummary[0]?.key === 'multi_anchor_lost' ? C.red : C.cyan, marginLeft: 6 }}>
                            {clearReasonSummary[0]?.label || '--'}
                          </span>
                          {filteredDailyRows.length > 0 && filteredDailyRows[filteredDailyRows.length - 1]?.dominant_clear_reason_label && (
                            <>
                              {' · '}最新一日主导：
                              <span style={{ color: C.yellow, marginLeft: 6 }}>
                                {filteredDailyRows[filteredDailyRows.length - 1]?.dominant_clear_reason_label}
                              </span>
                            </>
                          )}
                        </div>
                      </>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                    <Pool1NetBuildSparkline points={filteredDailyRows} width={220} height={42} />
                    <div style={{ fontSize: 10, color: C.dim, display: 'flex', flexDirection: 'column', gap: 3 }}>
                      <span>净建仓趋势线</span>
                      <span>最新: <span style={{ color: Number(filteredDailyRows[filteredDailyRows.length - 1]?.net_build_actionable || 0) >= 0 ? C.green : C.red }}>
                        {Number(filteredDailyRows[filteredDailyRows.length - 1]?.net_build_actionable || 0) > 0 ? '+' : ''}{Number(filteredDailyRows[filteredDailyRows.length - 1]?.net_build_actionable || 0)}
                      </span></span>
                      <span>
                        建仓占比 {(() => {
                          const latest = filteredDailyRows[filteredDailyRows.length - 1]
                          const total = Number(latest?.actionable_total || 0)
                          const ratio = total > 0 ? (Number(latest?.build_actionable || 0) / total) : 0
                          return `${(ratio * 100).toFixed(1)}%`
                        })()} / 清仓占比 {(() => {
                          const latest = filteredDailyRows[filteredDailyRows.length - 1]
                          const total = Number(latest?.actionable_total || 0)
                          const ratio = total > 0 ? (Number(latest?.clear_actionable || 0) / total) : 0
                          return `${(ratio * 100).toFixed(1)}%`
                        })()}
                      </span>
                      <span>
                        净仓位变化 {(() => {
                          const latest = filteredDailyRows[filteredDailyRows.length - 1]
                          const v = Number(latest?.net_exposure_change || 0)
                          return `${v > 0 ? '+' : ''}${v.toFixed(2)}`
                        })()} / 部分减仓 {(() => {
                          const latest = filteredDailyRows[filteredDailyRows.length - 1]
                          return `${Number(latest?.partial_actionable || 0)}次`
                        })()}
                      </span>
                    </div>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10, flexWrap: 'wrap' }}>
                    <span style={{ color: C.dim, fontSize: 10 }}>净变化热力</span>
                    {filteredDailyRows.map((row, idx) => {
                      const net = Number(row.net_build_actionable || 0)
                      const intensity = Math.min(1, Math.max(0.18, Math.abs(net) / Math.max(1, Number(row.actionable_total || 1))))
                      const bg = net > 0
                        ? `rgba(34,197,94,${intensity.toFixed(2)})`
                        : net < 0
                          ? `rgba(239,68,68,${intensity.toFixed(2)})`
                          : `rgba(234,179,8,0.20)`
                      return (
                        <div key={`heat-${row.trade_date}-${idx}`} title={`${row.trade_date} · 净建仓 ${net > 0 ? '+' : ''}${net} · 执行总数 ${row.actionable_total}`} style={{
                          width: 26,
                          height: 18,
                          borderRadius: 4,
                          background: bg,
                          border: `1px solid ${idx === filteredDailyRows.length - 1 ? C.cyan : C.border}`,
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          color: C.bright,
                          fontSize: 9,
                          fontFamily: 'monospace',
                        }}>
                          {net > 0 ? `+${net}` : String(net)}
                        </div>
                      )
                    })}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, height: 56, marginBottom: 10 }}>
                  {filteredDailyRows.map((row, idx) => {
                    const actionable = Number(row.actionable_total || 0)
                    const net = Number(row.net_build_actionable || 0)
                    const h = Math.max(6, Math.round((actionable / filteredDailyMaxActionable) * 34))
                    const barColor = net > 0 ? C.green : net < 0 ? C.red : C.yellow
                    return (
                      <div key={`spark-${row.trade_date}-${idx}`} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4, minWidth: 48 }}>
                        <div title={`${row.trade_date} · 执行总数 ${actionable} · 净建仓 ${net > 0 ? '+' : ''}${net}`} style={{
                          width: 18,
                          height: h,
                          borderRadius: 4,
                          background: barColor,
                          boxShadow: idx === filteredDailyRows.length - 1 ? `0 0 0 1px ${C.cyan}` : 'none',
                          opacity: idx === filteredDailyRows.length - 1 ? 1 : 0.85,
                        }} />
                        <div style={{ color: C.dim, fontSize: 9 }}>{row.trade_date.slice(5)}</div>
                      </div>
                    )
                  })}
                  </div>
                </>
              )}
              {filteredDailyRows.length <= 0 ? (
                <div style={{ color: C.dim, fontSize: 11 }}>暂无近5日主线轨迹数据</div>
              ) : (
                <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 6 }}>
                  {filteredDailyRows.map((row, idx) => {
                    const actionableTotal = Math.max(1, Number(row.actionable_total || 0))
                    const buildRatioPct = (Number(row.build_actionable || 0) / actionableTotal) * 100
                    const clearRatioPct = (Number(row.clear_actionable || 0) / actionableTotal) * 100
                    const dayDominance = (() => {
                      if (Number(row.net_build_actionable || 0) > 0 && buildRatioPct >= 55) {
                        return { text: '建仓主导', color: C.green }
                      }
                      if (Number(row.net_build_actionable || 0) < 0 && clearRatioPct >= 52) {
                        return { text: '清仓主导', color: C.red }
                      }
                      if (Number(row.partial_actionable || 0) > 0 && Number(row.partial_reduce_ratio_sum || 0) >= 0.3) {
                        return { text: '减仓主导', color: C.yellow }
                      }
                      return { text: '均衡', color: C.yellow }
                    })()
                    return (
                      <div key={`${row.trade_date}-${idx}`} style={{
                        display: 'grid',
                        gridTemplateColumns: '100px 1fr 1fr 1fr 1fr 1fr 1fr',
                        gap: 10,
                        alignItems: 'center',
                        padding: '6px 8px',
                        borderRadius: 4,
                        background: idx === filteredDailyRows.length - 1 ? 'rgba(0,200,200,0.06)' : C.bg,
                        border: `1px solid ${idx === filteredDailyRows.length - 1 ? C.cyan : C.border}`,
                        fontSize: 10,
                      }}>
                        <div style={{ color: C.bright }}>
                          <div>{row.trade_date}</div>
                          <div style={{ color: dayDominance.color, fontSize: 9, marginTop: 3 }}>{dayDominance.text}</div>
                        </div>
                        <div style={{ color: C.dim }}>
                          执行建仓 <span style={{ color: C.green }}>{row.build_actionable}</span>
                          <div style={{ color: C.dim }}>观察建仓 {row.build_watch} · 占比 {buildRatioPct.toFixed(1)}%</div>
                        </div>
                        <div style={{ color: C.dim }}>
                          执行清仓 <span style={{ color: C.red }}>{row.clear_actionable}</span>
                          <div style={{ color: C.dim }}>观察清仓 {row.clear_watch} · 占比 {clearRatioPct.toFixed(1)}%</div>
                          <div style={{ color: row.dominant_clear_reason_label ? C.yellow : C.dim }}>
                            主导原因 {row.dominant_clear_reason_label || '--'}
                            {Number(row.dominant_clear_reason_count || 0) > 0 && (
                              <span style={{ color: C.dim }}>
                                {' · '}执行 {Number(row.dominant_clear_reason_actionable || 0)} / 总 {Number(row.dominant_clear_reason_count || 0)}
                              </span>
                            )}
                          </div>
                        </div>
                        <div style={{ color: C.dim }}>
                          部分减仓 <span style={{ color: C.yellow }}>{row.partial_actionable}</span>
                          <div style={{ color: C.dim }}>观察减仓 {row.partial_watch} · 累计减仓 {Number(row.partial_reduce_ratio_sum || 0).toFixed(2)}</div>
                        </div>
                        <div style={{ color: C.dim }}>
                          左侧 <span style={{ color: C.yellow }}>{row.left_side_buy}</span>
                          <div style={{ color: C.dim }}>右侧 {row.right_side_breakout}</div>
                        </div>
                        <div style={{ color: C.dim }}>
                          清仓信号 <span style={{ color: C.yellow }}>{row.timing_clear}</span>
                          <div style={{ color: C.dim }}>观察总数 {row.watch_total}</div>
                        </div>
                        <div style={{ color: C.dim }}>
                          净建仓 <span style={{ color: row.net_build_actionable >= 0 ? C.green : C.red }}>{row.net_build_actionable}</span>
                          <div style={{ color: C.dim }}>净仓位 {Number(row.net_exposure_change || 0) > 0 ? '+' : ''}{Number(row.net_exposure_change || 0).toFixed(2)} · 执行总数 {row.actionable_total}</div>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
              <div style={{ marginTop: 10, display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 10 }}>
                <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                  <div style={{ color: C.cyan, fontSize: 11, marginBottom: 6 }}>板块净建仓排行</div>
                  {boardRanking.length <= 0 ? (
                    <div style={{ color: C.dim, fontSize: 10 }}>暂无板块排行</div>
                  ) : boardRanking.slice(0, 6).map((row, idx) => (
                    <div key={`${row.key}-${idx}`} style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 8,
                      padding: '5px 0',
                      borderBottom: idx === 5 ? 'none' : `1px dashed ${C.border}`,
                    }}>
                      <button
                        onClick={() => focusPool1Section({ board: row.key }, 'trajectory')}
                        style={{
                          background: 'none',
                          border: 'none',
                          padding: 0,
                          margin: 0,
                          cursor: 'pointer',
                          color: pool1DecisionBoardFilter === row.key ? C.cyan : C.bright,
                          fontSize: 10,
                          textAlign: 'left',
                        }}
                      >
                        {idx + 1}. {row.label}
                        {row.streak_label ? (
                          <span style={{
                            marginLeft: 6,
                            color: row.streak_direction > 0 ? C.green : row.streak_direction < 0 ? C.red : C.yellow,
                            fontSize: 9,
                          }}>
                            {row.streak_label}
                          </span>
                        ) : null}
                        {row.accel_label ? (
                          <span style={{
                            marginLeft: 6,
                            color: row.accel_color || C.dim,
                            fontSize: 9,
                          }}>
                            {row.accel_label}
                          </span>
                        ) : null}
                      </button>
                      <button
                        onClick={() => focusPool1Section({ board: row.key }, 'candidate')}
                        style={{
                          padding: '1px 6px',
                          fontSize: 9,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.cyan,
                        }}
                      >
                        看候选
                      </button>
                      <div style={{ textAlign: 'right', fontSize: 10, color: C.dim }}>
                        <div>
                          今 <span style={{ color: row.latest_net >= 0 ? C.green : C.red }}>
                            {row.latest_net > 0 ? '+' : ''}{row.latest_net}
                          </span>
                        </div>
                        <div>
                          5日 <span style={{ color: row.net_build_actionable >= 0 ? C.green : C.red }}>
                            {row.net_build_actionable > 0 ? '+' : ''}{row.net_build_actionable}
                          </span>
                        </div>
                        <div>今建仓占比 {(Number(row.latest_build_ratio || 0) * 100).toFixed(0)}%</div>
                        <div>建{row.build_actionable}/清{row.clear_actionable}</div>
                      </div>
                    </div>
                  ))}
                </div>
                <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                  <div style={{ color: C.cyan, fontSize: 11, marginBottom: 6 }}>行业净建仓排行</div>
                  {industryRanking.length <= 0 ? (
                    <div style={{ color: C.dim, fontSize: 10 }}>暂无行业排行</div>
                  ) : industryRanking.slice(0, 6).map((row, idx) => (
                    <div key={`${row.key}-${idx}`} style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 8,
                      padding: '5px 0',
                      borderBottom: idx === 5 ? 'none' : `1px dashed ${C.border}`,
                    }}>
                      <button
                        onClick={() => focusPool1Section({ industry: row.key }, 'trajectory')}
                        style={{
                          background: 'none',
                          border: 'none',
                          padding: 0,
                          margin: 0,
                          cursor: 'pointer',
                          color: pool1DecisionIndustryFilter === row.key ? C.cyan : C.bright,
                          fontSize: 10,
                          textAlign: 'left',
                        }}
                      >
                        {idx + 1}. {row.label}
                        {row.streak_label ? (
                          <span style={{
                            marginLeft: 6,
                            color: row.streak_direction > 0 ? C.green : row.streak_direction < 0 ? C.red : C.yellow,
                            fontSize: 9,
                          }}>
                            {row.streak_label}
                          </span>
                        ) : null}
                        {row.accel_label ? (
                          <span style={{
                            marginLeft: 6,
                            color: row.accel_color || C.dim,
                            fontSize: 9,
                          }}>
                            {row.accel_label}
                          </span>
                        ) : null}
                      </button>
                      <button
                        onClick={() => focusPool1Section({ industry: row.key }, 'candidate')}
                        style={{
                          padding: '1px 6px',
                          fontSize: 9,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.cyan,
                        }}
                      >
                        看候选
                      </button>
                      <div style={{ textAlign: 'right', fontSize: 10, color: C.dim }}>
                        <div>
                          今 <span style={{ color: row.latest_net >= 0 ? C.green : C.red }}>
                            {row.latest_net > 0 ? '+' : ''}{row.latest_net}
                          </span>
                        </div>
                        <div>
                          5日 <span style={{ color: row.net_build_actionable >= 0 ? C.green : C.red }}>
                            {row.net_build_actionable > 0 ? '+' : ''}{row.net_build_actionable}
                          </span>
                        </div>
                        <div>今建仓占比 {(Number(row.latest_build_ratio || 0) * 100).toFixed(0)}%</div>
                        <div>建{row.build_actionable}/清{row.clear_actionable}</div>
                      </div>
                    </div>
                  ))}
                </div>
                <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: '8px 10px' }}>
                  <div style={{ color: C.cyan, fontSize: 11, marginBottom: 6 }}>东方财富概念净建仓排行</div>
                  {conceptRanking.length <= 0 ? (
                    <div style={{ color: C.dim, fontSize: 10 }}>暂无概念排行</div>
                  ) : conceptRanking.slice(0, 6).map((row, idx) => (
                    <div key={`${row.key}-${idx}`} style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 8,
                      padding: '5px 0',
                      borderBottom: idx === 5 ? 'none' : `1px dashed ${C.border}`,
                    }}>
                      <button
                        onClick={() => focusPool1Section({ concept: row.key }, 'trajectory')}
                        style={{
                          background: 'none',
                          border: 'none',
                          padding: 0,
                          margin: 0,
                          cursor: 'pointer',
                          color: pool1DecisionConceptFilter === row.key ? C.cyan : C.bright,
                          fontSize: 10,
                          textAlign: 'left',
                        }}
                      >
                        {idx + 1}. {row.label}
                        {row.streak_label ? (
                          <span style={{
                            marginLeft: 6,
                            color: row.streak_direction > 0 ? C.green : row.streak_direction < 0 ? C.red : C.yellow,
                            fontSize: 9,
                          }}>
                            {row.streak_label}
                          </span>
                        ) : null}
                        {row.accel_label ? (
                          <span style={{
                            marginLeft: 6,
                            color: row.accel_color || C.dim,
                            fontSize: 9,
                          }}>
                            {row.accel_label}
                          </span>
                        ) : null}
                      </button>
                      <button
                        onClick={() => focusPool1Section({ concept: row.key }, 'candidate')}
                        style={{
                          padding: '1px 6px',
                          fontSize: 9,
                          cursor: 'pointer',
                          borderRadius: 4,
                          background: C.bg,
                          border: `1px solid ${C.border}`,
                          color: C.cyan,
                        }}
                      >
                        看候选
                      </button>
                      <div style={{ textAlign: 'right', fontSize: 10, color: C.dim }}>
                        <div>
                          今 <span style={{ color: row.latest_net >= 0 ? C.green : C.red }}>
                            {row.latest_net > 0 ? '+' : ''}{row.latest_net}
                          </span>
                        </div>
                        <div>
                          5日 <span style={{ color: row.net_build_actionable >= 0 ? C.green : C.red }}>
                            {row.net_build_actionable > 0 ? '+' : ''}{row.net_build_actionable}
                          </span>
                        </div>
                        <div>今建仓占比 {(Number(row.latest_build_ratio || 0) * 100).toFixed(0)}%</div>
                        <div>建{row.build_actionable}/清{row.clear_actionable}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              </div>
            </>
          )
        })()
      )}

      {healthOpen && (
        <div style={{ marginBottom: 12, background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, overflow: 'hidden' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 14px', borderBottom: `1px solid ${C.border}` }}>
            <span style={{ fontSize: 11, color: C.cyan, fontWeight: 600 }}>运行健康明细</span>
            <span style={{ fontSize: 10, color: C.dim }}>
              {runtimeHealth?.checked_at ? new Date(runtimeHealth.checked_at).toLocaleTimeString() : '--:--:--'}
            </span>
            <div style={{ flex: 1 }} />
            <button
              onClick={loadRuntimeHealth}
              style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, padding: '2px 6px', cursor: 'pointer' }}
            >
              刷新
            </button>
          </div>
          <div style={{ padding: '8px 14px', display: 'flex', flexDirection: 'column', gap: 6 }}>
            {runtimeHealthLoading && <div style={{ color: C.dim, fontSize: 11 }}>加载中...</div>}
            {!runtimeHealthLoading && !runtimeHealth && <div style={{ color: C.dim, fontSize: 11 }}>暂无健康数据</div>}
            {!runtimeHealthLoading && runtimeHealth && (runtimeHealth.items || []).map((it, idx) => {
              const lvl = it.level
              const lvlColor = lvl === 'red' ? C.red : lvl === 'yellow' ? C.yellow : C.green
              return (
                <div key={`${it.name}-${idx}`} style={{
                  display: 'flex', alignItems: 'center', gap: 8, fontSize: 10, fontFamily: 'monospace',
                  background: 'rgba(0,0,0,0.2)', border: `1px solid ${C.border}`, borderRadius: 4, padding: '5px 8px',
                }}>
                  <span style={{ width: 8, height: 8, borderRadius: 4, background: lvlColor, display: 'inline-block' }} />
                  <span style={{ color: C.bright, minWidth: 180 }}>{it.name}</span>
                  <span style={{ color: lvlColor, fontWeight: 700, minWidth: 48 }}>{String(lvl).toUpperCase()}</span>
                  <span style={{ color: C.dim, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {healthDetailText(it.detail)}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* 快慢路径一致性面板 */}
      <div style={{ marginBottom: 12, background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, overflow: 'hidden' }}>
        <div onClick={() => setDiffOpen(o => !o)}
          style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 14px', cursor: 'pointer', userSelect: 'none' }}>
          <span style={{ fontSize: 11, color: C.cyan, fontWeight: 600 }}>快慢路径一致性</span>
          {fastSlowDiff && (
            <>
              <span style={{ fontSize: 10, color: C.dim }}>样本 {fastSlowDiff.summary.members}</span>
              <span style={{
                fontSize: 10,
                color: fastSlowDiff.summary.mismatch_ratio >= FAST_SLOW_CRIT_RATIO
                  ? C.red
                  : fastSlowDiff.summary.mismatch_ratio >= FAST_SLOW_WARN_RATIO
                    ? C.yellow
                    : C.green,
              }}>
                偏差 {fastSlowDiff.summary.mismatch} ({(fastSlowDiff.summary.mismatch_ratio * 100).toFixed(1)}%)
              </span>
              <span style={{ fontSize: 9, color: C.dim }}>
                {fastSlowDiff.provider} · {fastSlowDiff.checked_at ? new Date(fastSlowDiff.checked_at).toLocaleTimeString() : '--:--:--'}
              </span>
            </>
          )}
          <div style={{ flex: 1 }} />
          {diffOpen && (
            <>
              <select
                value={diffTrendHours}
                onChange={(e) => setDiffTrendHours(Number(e.target.value))}
                onClick={(e) => e.stopPropagation()}
                style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, padding: '2px 4px' }}
              >
                {[1, 6, 24].map(h => <option key={h} value={h}>{h}h趋势</option>)}
              </select>
              <button
                onClick={(e) => { e.stopPropagation(); exportFastSlowCsv() }}
                style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, padding: '2px 6px', cursor: 'pointer' }}
              >
                导出CSV
              </button>
              <button
                onClick={(e) => { e.stopPropagation(); loadFastSlowDiff(); loadFastSlowTrend() }}
                style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, padding: '2px 6px', cursor: 'pointer' }}
              >
                刷新
              </button>
            </>
          )}
          <span style={{ color: C.dim, fontSize: 10 }}>{diffOpen ? '▼' : '▶'}</span>
        </div>
        {diffOpen && (
          <div style={{ padding: '0 14px 12px', borderTop: `1px solid ${C.border}` }}>
            {fastSlowLoading && <div style={{ color: C.dim, fontSize: 11, padding: '8px 0' }}>加载中...</div>}
            {!fastSlowLoading && !fastSlowDiff && <div style={{ color: C.dim, fontSize: 11, padding: '8px 0' }}>暂无数据</div>}
            {!fastSlowLoading && fastSlowDiff && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, paddingTop: 8 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                  <MismatchSparkline points={trendPts} warn={FAST_SLOW_WARN_RATIO} crit={FAST_SLOW_CRIT_RATIO} />
                  <div style={{ fontSize: 10, color: C.dim, display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <span>趋势窗口: {diffTrendHours}h · 样本: {fastSlowTrend?.count ?? 0}</span>
                    <span>存储: {fastSlowTrend?.storage_source || '--'} {fastSlowTrendLoading ? '· 加载中...' : ''}</span>
                    <span style={{ color: trendLatest ? ((trendLatest.mismatch_ratio || 0) >= FAST_SLOW_CRIT_RATIO ? C.red : (trendLatest.mismatch_ratio || 0) >= FAST_SLOW_WARN_RATIO ? C.yellow : C.green) : C.dim }}>
                      最新偏差: {trendLatest ? `${((trendLatest.mismatch_ratio || 0) * 100).toFixed(2)}% (${trendLatest.mismatch || 0}/${trendLatest.members || 0})` : '--'}
                    </span>
                  </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10, color: C.dim, cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={diffOnlyMismatch}
                      onChange={(e) => setDiffOnlyMismatch(e.target.checked)}
                    />
                    仅不一致
                  </label>
                  <span style={{ fontSize: 10, color: C.dim }}>
                    阈值: ≥{(FAST_SLOW_WARN_RATIO * 100).toFixed(0)}% 警告，≥{(FAST_SLOW_CRIT_RATIO * 100).toFixed(0)}% 严重
                  </span>
                  <select
                    value={diffSignalType}
                    onChange={(e) => setDiffSignalType(e.target.value)}
                    style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, padding: '2px 4px' }}
                  >
                    {diffSignalTypeOptions.map(t => (
                      <option key={t} value={t}>{t === 'all' ? '全部信号类型' : t}</option>
                    ))}
                  </select>
                </div>
                {Object.keys(fastSlowDiff.field_missing || {}).length > 0 && (
                  <div style={{ fontSize: 10, color: C.dim }}>
                    慢路径字段缺失:
                    {Object.entries(fastSlowDiff.field_missing).map(([k, v]) => ` ${k}=${v}`).join(' · ')}
                  </div>
                )}
                {diffRowsFiltered.slice(0, 30).map((r) => (
                  <div key={r.ts_code} style={{ fontSize: 10, fontFamily: 'monospace', background: 'rgba(0,0,0,0.25)', border: `1px solid ${C.border}`, borderRadius: 4, padding: '5px 8px' }}>
                    <span style={{ color: C.cyan }}>{r.ts_code}</span>
                    <span style={{ color: C.dim }}> {r.name} </span>
                    <span style={{ color: C.red }}>fast[{r.fast_types.join(',') || '--'}]</span>
                    <span style={{ color: C.dim }}> vs </span>
                    <span style={{ color: C.green }}>slow[{r.slow_types.join(',') || '--'}]</span>
                    <span style={{ color: C.dim }}> ΔP={r.price_delta?.toFixed(4) ?? '0.0000'}</span>
                  </div>
                ))}
                {diffRowsFiltered.length === 0 && (
                  <div style={{ fontSize: 10, color: C.dim }}>当前筛选条件下无差异样本</div>
                )}
                {fastSlowDiff.summary.mismatch === 0 && (
                  <div style={{ fontSize: 11, color: C.green, padding: '4px 0' }}>当前无快慢路径偏差</div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* T+0 质量监控面板（仅池2） */}
      {pool.pool_id === 2 && (
        <div style={{ marginBottom: 12, background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, overflow: 'hidden' }}>
          <div onClick={() => setQualityOpen(o => !o)}
            style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 14px', cursor: 'pointer', userSelect: 'none' }}>
            <span style={{ fontSize: 11, color: C.cyan, fontWeight: 600 }}>信号质量监控</span>
            <span style={{ fontSize: 10, color: C.dim, marginLeft: 4 }}>1/3/5min胜率 · MFE/MAE · churn</span>
            <div style={{ flex: 1 }} />
            {qualityOpen && (
              <select value={qualityHours} onChange={e => setQualityHours(Number(e.target.value))}
                onClick={e => e.stopPropagation()}
                style={{ fontSize: 10, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, padding: '2px 4px' }}>
                {[6, 12, 24, 48, 72].map(h => <option key={h} value={h}>{h}h</option>)}
              </select>
            )}
            <span style={{ color: C.dim, fontSize: 10 }}>{qualityOpen ? '▼' : '▶'}</span>
          </div>
          {qualityOpen && (
            <div style={{ padding: '0 14px 12px', borderTop: `1px solid ${C.border}` }}>
              {quality === null ? (
                <div style={{ color: C.dim, fontSize: 11, padding: '8px 0' }}>加载中...</div>
              ) : ((quality.total_signals_any ?? quality.total_signals) === 0) ? (
                <div style={{ color: C.dim, fontSize: 11, padding: '8px 0' }}>{quality.message || '暂无质量数据，需信号触发后等待5分钟'}</div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10, paddingTop: 10 }}>
                  <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                    <QStat label="样本(任意)" value={String(quality.total_signals_any ?? quality.total_signals)} />
                    <QStat label="样本(5m)" value={String(quality.total_signals)} />
                    <QStat label="1m胜率" value={quality.precision_1m != null ? `${(quality.precision_1m * 100).toFixed(1)}%` : '--'}
                      color={quality.precision_1m != null ? (quality.precision_1m >= 0.55 ? C.green : quality.precision_1m >= 0.45 ? C.yellow : C.red) : C.dim} />
                    <QStat label="3m胜率" value={quality.precision_3m != null ? `${(quality.precision_3m * 100).toFixed(1)}%` : '--'}
                      color={quality.precision_3m != null ? (quality.precision_3m >= 0.55 ? C.green : quality.precision_3m >= 0.45 ? C.yellow : C.red) : C.dim} />
                    <QStat label="5m胜率" value={quality.precision_5m != null ? `${(quality.precision_5m * 100).toFixed(1)}%` : '--'}
                      color={quality.precision_5m != null ? (quality.precision_5m >= 0.55 ? C.green : quality.precision_5m >= 0.45 ? C.yellow : C.red) : C.dim} />
                    <QStat label="均收益" value={quality.avg_ret_bps != null ? `${quality.avg_ret_bps > 0 ? '+' : ''}${quality.avg_ret_bps.toFixed(1)}bps` : '--'}
                      color={quality.avg_ret_bps != null ? (quality.avg_ret_bps > 0 ? C.green : C.red) : C.dim} />
                    <QStat label="MFE" value={quality.avg_mfe_bps != null ? `${quality.avg_mfe_bps.toFixed(1)}bps` : '--'} color={C.cyan} />
                    <QStat label="MAE" value={quality.avg_mae_bps != null ? `${quality.avg_mae_bps.toFixed(1)}bps` : '--'} color={C.red} />
                    <QStat
                      label="翻转率(churn)"
                      value={quality.signal_churn?.churn_ratio != null ? `${(quality.signal_churn.churn_ratio * 100).toFixed(1)}%` : '--'}
                      color={quality.signal_churn?.churn_ratio != null ? (quality.signal_churn.churn_ratio <= 0.15 ? C.green : quality.signal_churn.churn_ratio <= 0.30 ? C.yellow : C.red) : C.dim}
                    />
                  </div>
                  {quality.by_horizon && Object.keys(quality.by_horizon).length > 0 && (
                    <div>
                      <div style={{ fontSize: 10, color: C.dim, marginBottom: 4 }}>按评估窗口</div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(quality.by_horizon).map(([h, v]) => (
                          <div key={h} style={{ background: 'rgba(0,0,0,0.3)', borderRadius: 4, padding: '4px 8px', fontSize: 10, minWidth: 130 }}>
                            <div style={{ color: C.bright, fontWeight: 600, marginBottom: 3 }}>{h === '60' ? '1m' : h === '180' ? '3m' : h === '300' ? '5m' : `${h}s`}</div>
                            <div style={{ display: 'flex', gap: 8, color: C.dim }}>
                              <span>n={v.count}</span>
                              <span style={{ color: v.precision != null ? (v.precision >= 0.55 ? C.green : v.precision >= 0.45 ? C.yellow : C.red) : C.dim }}>
                                {v.precision != null ? `${(v.precision * 100).toFixed(1)}%` : '--'}
                              </span>
                              <span style={{ color: v.avg_ret_bps != null ? (v.avg_ret_bps > 0 ? C.green : C.red) : C.dim }}>
                                {v.avg_ret_bps != null ? `${v.avg_ret_bps > 0 ? '+' : ''}${v.avg_ret_bps.toFixed(1)}bps` : '--'}
                              </span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  {Object.keys(quality.by_type).length > 0 && (
                    <div>
                      <div style={{ fontSize: 10, color: C.dim, marginBottom: 4 }}>按信号类型</div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(quality.by_type).map(([t, v]) => (
                          <div key={t} style={{ background: 'rgba(0,0,0,0.3)', borderRadius: 4, padding: '4px 8px', fontSize: 10, minWidth: 120 }}>
                            <div style={{ color: C.bright, fontWeight: 600, marginBottom: 3 }}>{t === 'positive_t' ? '正T' : t === 'reverse_t' ? '倒T' : t}</div>
                            <div style={{ display: 'flex', gap: 8, color: C.dim }}>
                              <span>n={v.count}</span>
                              <span>{v.precision_1m != null ? `1m ${(v.precision_1m * 100).toFixed(0)}%` : '1m --'}</span>
                              <span>{v.precision_3m != null ? `3m ${(v.precision_3m * 100).toFixed(0)}%` : '3m --'}</span>
                              <span style={{ color: v.precision_5m != null ? (v.precision_5m >= 0.55 ? C.green : v.precision_5m >= 0.45 ? C.yellow : C.red) : C.dim }}>
                                {v.precision_5m != null ? `5m ${(v.precision_5m * 100).toFixed(1)}%` : '5m --'}
                              </span>
                              <span style={{ color: v.avg_ret_bps != null ? (v.avg_ret_bps > 0 ? C.green : C.red) : C.dim }}>
                                {v.avg_ret_bps != null ? `${v.avg_ret_bps > 0 ? '+' : ''}${v.avg_ret_bps.toFixed(1)}bps` : '--'}
                              </span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  {Object.keys(quality.by_phase).length > 0 && (
                    <div>
                      <div style={{ fontSize: 10, color: C.dim, marginBottom: 4 }}>按交易时段</div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(quality.by_phase).map(([phase, v]) => {
                          const phaseLabel: Record<string, string> = { open: '开盘', morning: '早盘', afternoon: '午后', close: '尾盘' }
                          return (
                            <div key={phase} style={{ background: 'rgba(0,0,0,0.3)', borderRadius: 4, padding: '4px 8px', fontSize: 10, minWidth: 90 }}>
                              <div style={{ color: C.bright, fontWeight: 600, marginBottom: 3 }}>{phaseLabel[phase] || phase}</div>
                              <div style={{ display: 'flex', gap: 8, color: C.dim }}>
                                <span>n={v.count}</span>
                                <span>{v.precision_1m != null ? `1m ${(v.precision_1m * 100).toFixed(0)}%` : '1m --'}</span>
                                <span>{v.precision_3m != null ? `3m ${(v.precision_3m * 100).toFixed(0)}%` : '3m --'}</span>
                                <span style={{ color: v.precision_5m != null ? (v.precision_5m >= 0.55 ? C.green : v.precision_5m >= 0.45 ? C.yellow : C.red) : C.dim }}>
                                  {v.precision_5m != null ? `5m ${(v.precision_5m * 100).toFixed(1)}%` : '5m --'}
                                </span>
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  )}
                  {quality.by_channel && Object.keys(quality.by_channel).length > 0 && (
                    <div>
                      <div style={{ fontSize: 10, color: C.dim, marginBottom: 4 }}>按通道(channel)</div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(quality.by_channel).slice(0, 6).map(([channel, v]) => (
                          <div key={channel} style={{ background: 'rgba(0,0,0,0.3)', borderRadius: 4, padding: '4px 8px', fontSize: 10 }}>
                            <span style={{ color: C.bright, marginRight: 6 }}>{channel}</span>
                            <span style={{ color: C.dim, marginRight: 6 }}>n={v.count}</span>
                            <span style={{ color: v.precision_5m != null ? (v.precision_5m >= 0.55 ? C.green : v.precision_5m >= 0.45 ? C.yellow : C.red) : C.dim }}>
                              {v.precision_5m != null ? `${(v.precision_5m * 100).toFixed(1)}%` : '--'}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  {quality.by_source && Object.keys(quality.by_source).length > 0 && (
                    <div>
                      <div style={{ fontSize: 10, color: C.dim, marginBottom: 4 }}>按来源(signal_source)</div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(quality.by_source).slice(0, 6).map(([source, v]) => (
                          <div key={source} style={{ background: 'rgba(0,0,0,0.3)', borderRadius: 4, padding: '4px 8px', fontSize: 10 }}>
                            <span style={{ color: C.bright, marginRight: 6 }}>{source}</span>
                            <span style={{ color: C.dim, marginRight: 6 }}>n={v.count}</span>
                            <span style={{ color: v.precision_5m != null ? (v.precision_5m >= 0.55 ? C.green : v.precision_5m >= 0.45 ? C.yellow : C.red) : C.dim }}>
                              {v.precision_5m != null ? `${(v.precision_5m * 100).toFixed(1)}%` : '--'}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  <div style={{ fontSize: 9, color: C.dim }}>
                    最近{quality.hours}h · 1/3/5min方向正确率 · 5min收益口径 · bps=万分之一
                    {quality.signal_churn && quality.signal_churn.sample_count > 0
                      ? ` · churn=${((quality.signal_churn.churn_ratio ?? 0) * 100).toFixed(1)}%`
                      : ''}
                  </div>
                  {drift && drift.total_checks > 0 && (
                    <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 8, marginTop: 4 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                        <span style={{ fontSize: 10, color: drift.has_active_alert ? C.red : C.dim, fontWeight: 600 }}>
                          {drift.has_active_alert ? '特征漂移告警' : '特征分布稳定'}
                        </span>
                        {drift.alerts > 0 && <span style={{ fontSize: 9, color: C.red }}>近7日 {drift.alerts} 次告警</span>}
                        {drift.latest_checked_at && <span style={{ fontSize: 9, color: C.dim, marginLeft: 'auto' }}>上次检测 {new Date(drift.latest_checked_at).toLocaleTimeString()}</span>}
                      </div>
                      {Object.entries(drift.latest).length > 0 && (
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          {Object.entries(drift.latest).map(([fname, fv]) => (
                            <div key={fname} style={{
                              fontSize: 9, padding: '2px 6px', borderRadius: 3,
                              background: fv.drifted ? 'rgba(239,68,68,0.15)' : 'rgba(0,0,0,0.2)',
                              border: `1px solid ${fv.drifted ? C.red : C.border}`,
                              color: fv.drifted ? C.red : C.dim,
                            }}>
                              {fname} PSI={fv.psi?.toFixed(2) ?? '--'}
                              {fv.drifted && <span style={{ marginLeft: 3, color: C.red }}>[{fv.severity}]</span>}
                            </div>
                          ))}
                        </div>
                      )}
                      {Array.isArray(drift.quality_split_by_channel_source) && drift.quality_split_by_channel_source.length > 0 && (
                        <div style={{ marginTop: 6, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          {drift.quality_split_by_channel_source.slice(0, 6).map((it, idx) => (
                            <div key={`${it.channel}-${it.signal_source}-${idx}`} style={{
                              fontSize: 9, padding: '2px 6px', borderRadius: 3,
                              background: 'rgba(0,0,0,0.2)', border: `1px solid ${C.border}`, color: C.dim,
                            }}>
                              {it.channel}/{it.signal_source} n={it.recent_count}
                              <span style={{ marginLeft: 4, color: it.recent_precision_5m != null ? (it.recent_precision_5m >= 0.55 ? C.green : it.recent_precision_5m >= 0.45 ? C.yellow : C.red) : C.dim }}>
                                {it.recent_precision_5m != null ? `${(it.recent_precision_5m * 100).toFixed(1)}%` : '--'}
                              </span>
                              {it.precision_drop != null && (
                                <span style={{ marginLeft: 4, color: it.precision_drop > 0 ? C.red : C.green }}>
                                  Δ{it.precision_drop > 0 ? '+' : ''}{(it.precision_drop * 100).toFixed(1)}%
                                </span>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                  {drift && drift.total_checks === 0 && (
                    <div style={{ fontSize: 9, color: C.dim, borderTop: `1px solid ${C.border}`, paddingTop: 6, marginTop: 4 }}>
                      {drift.message || '漂移检测需积累1天基线数据'}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      <AddStockBox onAdd={addMember} />

      {filteredMembers.length === 0 ? (
        <div style={{ textAlign: 'center', padding: 60, color: C.dim, background: C.card, borderRadius: 6, border: `1px dashed ${C.border}` }}>
          <Eye size={32} style={{ opacity: 0.3, marginBottom: 12 }} />
          <div>{members.length === 0 ? '池内暂无股票，请搜索添加' : '无匹配股票'}</div>
        </div>
      ) : (
        <div style={{ display: 'grid', gap: 12, gridTemplateColumns: `repeat(auto-fill, minmax(${compact ? 300 : 380}px, 1fr))` }}>
          {filteredMembers.map(m => (
            <div key={m.ts_code} ref={el => { cardRefs.current[m.ts_code] = el }} data-ts-code={m.ts_code}>
              <StockCard member={m} signalRow={signalMap.get(m.ts_code)}
                tick={tickMap[m.ts_code]} txns={txnsMap[m.ts_code] || []} tickHistory={tickHistoryMap[m.ts_code] || []}
                compact={compact} onRemove={() => removeMember(m.ts_code)} onUpdateNote={(note) => updateNote(m.ts_code, note)} poolId={pool.pool_id}
                highlighted={locateTsCode === m.ts_code}
                pool1RiskTags={Array.from(pool1CardRiskTagMap.get(m.ts_code) || [])}
                activePool1RiskHotword={pool1RiskHotwordFilter}
                onSelectPool1RiskHotword={openPool1RiskHotwordFromCard} />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ===== 添加股票搜索框 ===== */
function AddStockBox({ onAdd }: { onAdd: (s: SearchResult) => void }) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[]>([])
  const [show, setShow] = useState(false)
  const debounce = useRef<ReturnType<typeof setTimeout> | null>(null)
  const boxRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (debounce.current) clearTimeout(debounce.current)
    if (!query.trim()) { setResults([]); return }
    debounce.current = setTimeout(() => {
      axios.get(`${API}/api/stock/search`, { params: { q: query, limit: 10 } })
        .then(r => setResults(r.data.data || [])).catch(() => setResults([]))
    }, 250)
    return () => { if (debounce.current) clearTimeout(debounce.current) }
  }, [query])

  useEffect(() => {
    const handler = (e: MouseEvent) => { if (boxRef.current && !boxRef.current.contains(e.target as Node)) setShow(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const handleAdd = (s: SearchResult) => { onAdd(s); setQuery(''); setResults([]); setShow(false) }

  return (
    <div style={{ position: 'relative', marginBottom: 12 }} ref={boxRef}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, padding: '8px 12px' }}>
        <Search size={14} color={C.dim} />
        <input type="text" value={query} onChange={e => { setQuery(e.target.value); setShow(true) }} onFocus={() => setShow(true)}
          placeholder="搜索股票代码或名称（例：600519 / 茅台）"
          style={{ flex: 1, background: 'none', border: 'none', outline: 'none', color: C.bright, fontSize: 13 }} />
      </div>
      {show && results.length > 0 && (
        <div style={{ position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 10, background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, marginTop: 4, maxHeight: 280, overflowY: 'auto' }}>
          {results.map(r => (
            <div key={r.ts_code} onClick={() => handleAdd(r)}
              style={{ padding: '8px 12px', cursor: 'pointer', borderBottom: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', gap: 10 }}
              onMouseEnter={e => (e.currentTarget.style.background = C.cyanBg)} onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}>
              <Plus size={12} color={C.cyan} />
              <span style={{ color: C.cyan, fontFamily: 'monospace', fontSize: 12 }}>{r.ts_code}</span>
              <span style={{ color: C.bright, fontSize: 13 }}>{r.name}</span>
              {r.industry && <span style={{ color: C.dim, fontSize: 11, marginLeft: 'auto' }}>{r.industry}</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ===== 股票监控卡片 ===== */
function StockCard({ member, signalRow, tick, txns, tickHistory: initialTickHistory, compact, onRemove, onUpdateNote, poolId, highlighted, pool1RiskTags = [], activePool1RiskHotword = 'all', onSelectPool1RiskHotword }: {
  member: Member; signalRow?: SignalRow; tick?: TickData; txns: Txn[]; tickHistory?: TickDataPoint[]
  compact: boolean; onRemove: () => void; onUpdateNote: (note: string) => void; poolId: number; highlighted?: boolean
  pool1RiskTags?: Array<{ text: string; color: string }>; activePool1RiskHotword?: string
  onSelectPool1RiskHotword?: (tag: string) => void
}) {
  const signals = signalRow?.signals || []
  const [showDetail, setShowDetail] = useState(!compact)
  const [selectedSignal, setSelectedSignal] = useState<Signal | null>(null)
  const [editingNote, setEditingNote] = useState(false)
  const [noteInput, setNoteInput] = useState(member.note || '')
  const [showAllConceptTags, setShowAllConceptTags] = useState(false)
  // 缓存 tick 历史数据用于绘制分时图
  // 用 ref 保存真实数据，state 仅在跨分钟时同步（触发重绘）
  const tickHistoryRef = useRef<TickDataPoint[]>(initialTickHistory || [])
  const [tickHistory, setTickHistory] = useState<TickDataPoint[]>(initialTickHistory || [])
  const lastRenderedMinute = useRef<number>(0)

  useEffect(() => {
    if (highlighted) {
      setShowDetail(true)
    }
  }, [highlighted])

  // 有新 tick 时写入 ref；首次或跨分钟时再同步到 state 触发渲染
  useEffect(() => {
    if (tick && tick.price > 0) {
      const now = Math.floor(Date.now() / 1000)
      const currentMinute = Math.floor(now / 60)
      const newTick: TickDataPoint = {
        time: now,
        price: tick.price,
        volume: tick.volume || 0,
        amount: tick.amount || 0,
      }
      // 更新 ref
      const arr = tickHistoryRef.current
      arr.push(newTick)
      if (arr.length > 240) arr.shift()
      tickHistoryRef.current = arr
      // 首次数据到达或跨分钟时同步到 state（触发分时图重绘）
      const isFirstRender = lastRenderedMinute.current === 0 && arr.length > 0
      if (isFirstRender || currentMinute !== lastRenderedMinute.current) {
        lastRenderedMinute.current = currentMinute
        setTickHistory([...arr])
      }
    }
  }, [tick])

  // 收到历史tick数据时，初始化
  useEffect(() => {
    if (initialTickHistory && initialTickHistory.length > 0) {
      tickHistoryRef.current = initialTickHistory
      setTickHistory(initialTickHistory)
      if (initialTickHistory.length > 0) {
        lastRenderedMinute.current = Math.floor(initialTickHistory[initialTickHistory.length - 1].time / 60)
      }
    }
  }, [initialTickHistory])

  const price = tick?.price || signalRow?.price || 0
  const rawPct = Number(tick?.pct_chg ?? signalRow?.pct_chg ?? 0)
  const preClose = (tick?.pre_close && tick.pre_close > 0)
    ? tick.pre_close
    : (price > 0 && Number.isFinite(rawPct) && Math.abs(rawPct) < 90
      ? (price / (1 + rawPct / 100))
      : 0)
  const pctChg = preClose > 0
    ? ((price - preClose) / preClose * 100)
    : rawPct
  const isUp = pctChg >= 0
  const priceColor = (p: number) => preClose > 0 ? (p >= preClose ? C.red : C.green) : (isUp ? C.red : C.green)
  const tickSource = tick?.is_mock ? '模拟' : (tick?.source || '实时')
  const tickUpdatedAt = tick?.timestamp
  const pool1PositionStatus = String(signalRow?.pool1_position_status || 'observe')
  const pool1PositionRatio = Number(signalRow?.pool1_position_ratio ?? (pool1PositionStatus === 'holding' ? 1 : 0))
  const pool1HoldingDays = Number(signalRow?.pool1_holding_days ?? 0)
  const pool1InHolding = Boolean(signalRow?.pool1_in_holding || pool1PositionStatus === 'holding')
  const pool1PositionLabel = pool1InHolding ? '持仓' : '观望'
  const pool1PositionColor = pool1InHolding ? C.red : C.dim
  const pool1Decision = String(signalRow?.pool1_decision || (pool1InHolding ? 'hold' : 'observe'))
  const pool1DecisionLabel = String(signalRow?.pool1_decision_label || (pool1InHolding ? '建议持有' : '建议观望'))
  const pool1DecisionReason = String(signalRow?.pool1_decision_reason || '')
  const pool1DecisionMode = String(signalRow?.pool1_decision_mode || 'neutral')
  const pool1DecisionStrength = Number(signalRow?.pool1_decision_strength ?? 0)
  const pool1DecisionColorValue = pool1DecisionColor(pool1Decision)
  const pool1DecisionModeLabel = pool1DecisionMode === 'actionable'
    ? '执行级'
    : (pool1DecisionMode === 'watch' ? '观察级' : '中性')
  const pool1GuardTags = poolId === 1 ? derivePool1GuardTags(signals) : []
  const pool1RecentFailBadge = poolId === 1 ? derivePool1RecentFailBadge(signals) : null
  const pool1RepeatBadge = poolId === 1 ? derivePool1RepeatBadge(signals) : null
  const pool1CooldownBadge = poolId === 1 ? derivePool1CooldownBadge(signals) : null
  const hasSeverePool1Guard = pool1GuardTags.some(tag => tag.text === '左侧重复退潮')
  const hasWarnPool1Guard = pool1GuardTags.some(tag => (
    tag.text === '左侧重复转弱'
    || tag.text === '左侧退潮抑制'
    || tag.text === '左侧转弱抑制'
    || tag.text === '概念退潮'
    || tag.text === '题材转弱'
  ))
  const pool1GuardDot = hasSeverePool1Guard
    ? { color: C.red, title: '左侧重复退潮抑制中' }
    : (hasWarnPool1Guard ? { color: C.yellow, title: '左侧/题材抑制中' } : null)
  const conceptTags = (() => {
    const seen = new Set<string>()
    const tags: string[] = []
    const pushTag = (value: unknown) => {
      const text = String(value || '').trim()
      if (!text || seen.has(text)) return
      seen.add(text)
      tags.push(text)
    }
    pushTag(member.core_concept_board)
    if (Array.isArray(member.concept_boards)) {
      for (const item of member.concept_boards) pushTag(item)
    }
    return tags
  })()
  const visibleConceptTags = showAllConceptTags ? conceptTags : conceptTags.slice(0, 3)
  const hiddenConceptTagCount = Math.max(0, conceptTags.length - visibleConceptTags.length)
  const maxBidVol = Math.max(1, ...(tick?.bids || []).map(([, v]) => v))
  const maxAskVol = Math.max(1, ...(tick?.asks || []).map(([, v]) => v))

  return (
    <div style={{
      background: C.card,
      border: `1px solid ${
        highlighted
          ? C.yellow
          : (hasSeverePool1Guard ? C.red : (hasWarnPool1Guard ? C.yellow : (signals.length > 0 ? C.cyan : C.border)))
      }`,
      borderRadius: 6,
      padding: 10,
      position: 'relative',
      boxShadow: highlighted
        ? '0 0 0 2px rgba(234,179,8,0.35)'
        : (hasSeverePool1Guard
          ? '0 0 0 1px rgba(255,90,90,0.28)'
          : (hasWarnPool1Guard
            ? '0 0 0 1px rgba(234,179,8,0.22)'
            : (signals.length > 0 ? '0 0 0 1px rgba(0,200,200,0.2)' : 'none'))),
      display: 'flex',
      flexDirection: 'column',
      gap: 8,
      transition: 'box-shadow 0.2s ease, border-color 0.2s ease',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Link to={`/stock/${member.ts_code}`} style={{ color: C.cyan, fontSize: 12, fontFamily: 'monospace', textDecoration: 'none', display: 'block' }}>{member.ts_code}</Link>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 1, minWidth: 0 }}>
            <div style={{ color: C.bright, fontSize: 14, fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{member.name}</div>
            {pool1GuardDot && (
              <span
                title={pool1GuardDot.title}
                style={{
                  width: 7,
                  height: 7,
                  minWidth: 7,
                  borderRadius: '50%',
                  background: pool1GuardDot.color,
                  boxShadow: `0 0 0 2px ${pool1GuardDot.color}22`,
                }}
              />
            )}
          </div>
          {conceptTags.length > 0 && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap', marginTop: 3 }}>
              {visibleConceptTags.map((tag, idx) => (
                <span
                  key={`${member.ts_code}-concept-${tag}`}
                  title={tag}
                  style={{
                    padding: '1px 6px',
                    borderRadius: 999,
                    border: `1px solid ${idx === 0 ? C.cyan : C.border}`,
                    color: idx === 0 ? C.cyan : C.text,
                    background: idx === 0 ? `${C.cyan}12` : 'rgba(255,255,255,0.03)',
                    fontSize: 9,
                    lineHeight: 1.4,
                    whiteSpace: 'nowrap',
                  }}
                >
                  {tag}
                </span>
              ))}
              {hiddenConceptTagCount > 0 && !showAllConceptTags && (
                <span
                  title={`还有 ${hiddenConceptTagCount} 个概念未展开`}
                  style={{
                    padding: '1px 6px',
                    borderRadius: 999,
                    border: `1px dashed ${C.border}`,
                    color: C.dim,
                    background: 'rgba(255,255,255,0.02)',
                    fontSize: 9,
                    lineHeight: 1.4,
                    whiteSpace: 'nowrap',
                  }}
                >
                  +{hiddenConceptTagCount}
                </span>
              )}
              {conceptTags.length > 3 && (
                <button
                  type="button"
                  onClick={() => setShowAllConceptTags(v => !v)}
                  style={{
                    padding: '1px 6px',
                    borderRadius: 999,
                    border: `1px solid ${C.border}`,
                    color: C.cyan,
                    background: 'transparent',
                    fontSize: 9,
                    lineHeight: 1.4,
                    whiteSpace: 'nowrap',
                    cursor: 'pointer',
                  }}
                >
                  {showAllConceptTags ? '收起' : '展开全部'}
                </button>
              )}
            </div>
          )}
          <div style={{ color: C.dim, fontSize: 9, fontFamily: 'monospace', marginTop: 2 }}>
            {`源:${tickSource} · 更新:${formatTimeText(tickUpdatedAt)} (${formatAgeText(tickUpdatedAt)})`}
          </div>
          {poolId === 1 && (
            <>
              <div style={{ color: C.dim, fontSize: 9, fontFamily: 'monospace', marginTop: 1 }}>
                状态:<span style={{ color: pool1PositionColor, fontWeight: 700 }}>{pool1PositionLabel}</span>
                {pool1InHolding ? ` · 仓位:${(Math.max(0, Math.min(1, pool1PositionRatio)) * 100).toFixed(0)}%` : ''}
                {pool1InHolding ? ` · 持有:${pool1HoldingDays.toFixed(1)}天` : ''}
                {' · '}决策:<span style={{ color: pool1DecisionColorValue, fontWeight: 700 }}>{pool1DecisionLabel}</span>
                {pool1DecisionStrength > 0 ? ` · 分数:${pool1DecisionStrength.toFixed(0)}` : ''}
                {' · '}<span style={{ color: pool1DecisionMode === 'actionable' ? C.red : (pool1DecisionMode === 'watch' ? C.yellow : C.dim) }}>{pool1DecisionModeLabel}</span>
              </div>
              {pool1DecisionReason && (
                <div style={{ color: C.dim, fontSize: 9, fontFamily: 'monospace', marginTop: 1, whiteSpace: 'normal', wordBreak: 'break-word' }}>
                  {pool1DecisionReason}
                </div>
              )}
              {pool1GuardTags.length > 0 && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap', marginTop: 4 }}>
                  {pool1RecentFailBadge && (
                    <span
                      title={pool1RecentFailBadge.title}
                      style={{
                        padding: '1px 6px',
                        borderRadius: 999,
                        border: `1px dashed ${pool1RecentFailBadge.color}`,
                        color: pool1RecentFailBadge.color,
                        background: `${pool1RecentFailBadge.color}10`,
                        fontSize: 9,
                        lineHeight: 1.4,
                      }}
                    >
                      {pool1RecentFailBadge.text}
                    </span>
                  )}
                  {pool1RepeatBadge && (
                    <span
                      title={pool1RepeatBadge.title}
                      style={{
                        padding: '1px 6px',
                        borderRadius: 999,
                        border: `1px solid ${pool1RepeatBadge.color}`,
                        color: pool1RepeatBadge.color,
                        background: `${pool1RepeatBadge.color}14`,
                        fontSize: 9,
                        lineHeight: 1.4,
                        fontWeight: 700,
                      }}
                    >
                      {pool1RepeatBadge.text}
                    </span>
                  )}
                  {pool1CooldownBadge && (
                    <span
                      title={pool1CooldownBadge.title}
                      style={{
                        padding: '1px 6px',
                        borderRadius: 999,
                        border: `1px dashed ${pool1CooldownBadge.color}`,
                        color: pool1CooldownBadge.color,
                        background: `${pool1CooldownBadge.color}10`,
                        fontSize: 9,
                        lineHeight: 1.4,
                      }}
                    >
                      {pool1CooldownBadge.text}
                    </span>
                  )}
                  {pool1GuardTags.map((tag, idx) => (
                    <button
                      key={`${member.ts_code}-guard-${tag.text}-${idx}`}
                      onClick={() => onSelectPool1RiskHotword?.(tag.text)}
                      style={{
                        padding: '1px 6px',
                        borderRadius: 999,
                        border: `1px solid ${tag.color}`,
                        color: tag.color,
                        background: `${tag.color}12`,
                        fontSize: 9,
                        lineHeight: 1.4,
                        cursor: 'pointer',
                        fontFamily: 'inherit',
                      }}
                    >
                      {tag.text}
                    </button>
                  ))}
                </div>
              )}
              {pool1RiskTags.length > 0 && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap', marginTop: 4 }}>
                  {pool1RiskTags.map((tag, idx) => (
                    <button
                      key={`${member.ts_code}-${tag.text}-${idx}`}
                      onClick={() => onSelectPool1RiskHotword?.(tag.text)}
                      style={{
                        padding: '1px 6px',
                        borderRadius: 999,
                        border: `1px solid ${tag.color}`,
                        color: tag.color,
                        background: activePool1RiskHotword === tag.text ? `${tag.color}18` : C.bg,
                        fontSize: 9,
                        lineHeight: 1.4,
                        cursor: 'pointer',
                      }}
                    >
                      {tag.text}
                    </button>
                  ))}
                </div>
              )}
            </>
          )}
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ color: isUp ? C.red : C.green, fontSize: 18, fontWeight: 700, fontFamily: 'monospace', lineHeight: 1.1 }}>{price > 0 ? price.toFixed(2) : '--'}</div>
          <div style={{ color: isUp ? C.red : C.green, fontSize: 11, fontFamily: 'monospace' }}>{pctChg >= 0 ? '+' : ''}{pctChg.toFixed(2)}%</div>
        </div>
        <button onClick={onRemove} title="移除" style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.dim, padding: 2 }}><Trash2 size={12} /></button>
      </div>

      {tick && showDetail && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '3px 8px', fontSize: 10, fontFamily: 'monospace', padding: '4px 6px', background: 'rgba(0,0,0,0.2)', borderRadius: 3 }}>
          <Stat label="开" value={tick.open.toFixed(2)} color={priceColor(tick.open)} />
          <Stat label="高" value={tick.high.toFixed(2)} color={C.red} />
          <Stat label="低" value={tick.low.toFixed(2)} color={C.green} />
          <Stat label="昨" value={preClose > 0 ? preClose.toFixed(2) : '--'} />
          <Stat label="量" value={fmtVol(tick.volume)} />
          <Stat label="额" value={fmtAmt(tick.amount)} />
        </div>
      )}

      {tick?.is_mock && <div style={{ padding: '3px 6px', background: 'rgba(234,179,8,0.08)', color: C.yellow, fontSize: 9, textAlign: 'center', borderRadius: 3 }}>mootdx 未安装，以下为模拟数据</div>}

      <MiniIntradayChart
        tickData={tickHistory}
        preClose={preClose || signalRow?.price || 0}
        width={compact ? 256 : 316}
        height={compact ? 80 : 120}
        signals={signals
          .filter(s => s && typeof s.price === 'number' && s.price > 0)
          .map(s => ({
            type: s.type,
            direction: s.direction || ((s.type === 'reverse_t' || s.type === 'timing_clear') ? 'sell' : 'buy'),
            price: s.price as number,
            triggered_at: s.triggered_at,
            message: s.message,
            strength: s.strength,
          }))}
      />

      {signals.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {signals.map((s, i) => (
            <SignalBadge
              key={`${s.type || 'sig'}-${s.triggered_at || 0}-${i}`}
              signal={s}
              onOpenDetail={(x) => setSelectedSignal(x)}
            />
          ))}
        </div>
      )}

      {compact && (
        <button onClick={() => setShowDetail(!showDetail)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.dim, fontSize: 10, display: 'flex', alignItems: 'center', gap: 4, justifyContent: 'center' }}>
          {showDetail ? <ChevronUp size={10} /> : <ChevronDown size={10} />}{showDetail ? '收起' : '详情'}
        </button>
      )}

      {tick && showDetail && poolId === 2 && (
        <div>
          <SubTitle>五档盘口</SubTitle>
          <div>
            {[...(tick.asks || [])].reverse().map(([p, v], i) => (
              <OrderRow key={`a${5-i}`} label={`卖${5-i}`} price={p} volume={v} trackColor={C.green} maxVol={maxAskVol} priceColor={priceColor(p)} />
            ))}
            <div style={{ padding: '3px 6px', textAlign: 'center', borderTop: `1px solid ${C.border}`, borderBottom: `1px solid ${C.border}`, color: priceColor(price), fontSize: 11, fontWeight: 700, fontFamily: 'monospace', background: 'rgba(0,0,0,0.3)' }}>{price > 0 ? price.toFixed(2) : '--'}</div>
            {(tick.bids || []).map(([p, v], i) => (
              <OrderRow key={`b${i+1}`} label={`买${i+1}`} price={p} volume={v} trackColor={C.red} maxVol={maxBidVol} priceColor={priceColor(p)} />
            ))}
          </div>
        </div>
      )}

      {tick && showDetail && poolId === 2 && (
        <div>
          <SubTitle>逐笔成交</SubTitle>
          <div style={{ fontSize: 10, fontFamily: 'monospace', display: 'grid', gridTemplateColumns: '60px 1fr 1fr 24px', rowGap: 2, columnGap: 6 }}>
            {txns.length === 0 ? (
              <div style={{ gridColumn: '1 / -1', color: C.dim, textAlign: 'center', padding: 6 }}>暂无</div>
            ) : txns.map((t, i) => {
              const isBuy = t.direction === 0
              return (
                <React.Fragment key={i}>
                  <span style={{ color: C.dim }}>{(t.time || '').slice(-8)}</span>
                  <span style={{ color: priceColor(t.price), textAlign: 'right' }}>{t.price.toFixed(2)}</span>
                  <span style={{ color: C.text, textAlign: 'right' }}>{t.volume}</span>
                  <span style={{ color: isBuy ? C.red : C.green, textAlign: 'center', fontWeight: 600 }}>{isBuy ? 'B' : 'S'}</span>
                </React.Fragment>
              )
            })}
          </div>
        </div>
      )}

      <div style={{ paddingTop: 6, borderTop: `1px solid ${C.border}` }}>
        {editingNote ? (
          <div style={{ display: 'flex', gap: 4 }}>
            <input value={noteInput} onChange={e => setNoteInput(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') { onUpdateNote(noteInput); setEditingNote(false) } if (e.key === 'Escape') setEditingNote(false) }}
              placeholder="添加备注..." autoFocus
              style={{ flex: 1, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, fontSize: 10, padding: '2px 6px', outline: 'none' }} />
            <button onClick={() => { onUpdateNote(noteInput); setEditingNote(false) }} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.cyan, fontSize: 10 }}>OK</button>
          </div>
        ) : (
          <div onClick={() => { setNoteInput(member.note || ''); setEditingNote(true) }} style={{ fontSize: 10, color: member.note ? C.dim : C.border, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
            <Edit3 size={8} />{member.note || '添加备注'}
          </div>
        )}
      </div>
      {selectedSignal && (
        <SignalDetailModal
          signal={selectedSignal}
          stockCode={member.ts_code}
          stockName={member.name}
          onClose={() => setSelectedSignal(null)}
        />
      )}
    </div>
  )
}

/* ===== 小组件 ===== */
function Stat({ label, value, color }: { label: string; value: string; color?: string }) {
  return <div style={{ display: 'flex', gap: 4, alignItems: 'baseline' }}><span style={{ color: C.dim }}>{label}</span><span style={{ color: color || C.text }}>{value}</span></div>
}
function QStat({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 60 }}>
      <span style={{ fontSize: 9, color: C.dim, letterSpacing: 0.5 }}>{label}</span>
      <span style={{ fontSize: 13, fontWeight: 700, fontFamily: 'monospace', color: color || C.bright }}>{value}</span>
    </div>
  )
}
function SubTitle({ children }: { children: React.ReactNode }) {
  return <div style={{ color: C.dim, fontSize: 9, letterSpacing: 1, marginBottom: 4, textTransform: 'uppercase' as const }}>{children}</div>
}
function OrderRow({ label, price, volume, trackColor, maxVol, priceColor }: { label: string; price: number; volume: number; trackColor: string; maxVol: number; priceColor: string }) {
  const ratio = Math.min(1, volume / maxVol)
  return (
    <div style={{ position: 'relative', padding: '2px 6px', display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, fontFamily: 'monospace' }}>
      <div style={{ position: 'absolute', right: 0, top: 0, bottom: 0, width: `${ratio * 100}%`, background: trackColor, opacity: 0.12 }} />
      <span style={{ color: C.dim, width: 26, zIndex: 1 }}>{label}</span>
      <span style={{ color: priceColor, flex: 1, textAlign: 'right', zIndex: 1 }}>{price > 0 ? price.toFixed(2) : '--'}</span>
      <span style={{ color: C.text, width: 50, textAlign: 'right', zIndex: 1 }}>{volume}</span>
    </div>
  )
}
function fmtVol(v: number): string { if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'; if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'; return String(v) }
function fmtAmt(v: number): string { if (v >= 1e8) return (v / 1e8).toFixed(2) + '亿'; if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'; return v.toFixed(0) }

function SignalBadge({ signal, onOpenDetail }: { signal: Signal; onOpenDetail?: (signal: Signal) => void }) {
  const direction: 'buy' | 'sell' = signal.direction || ((signal.type === 'reverse_t' || signal.type === 'timing_clear') ? 'sell' : 'buy')
  const isBuy = direction === 'buy'
  const state = signal.state || 'active'
  const isExpired = state === 'expired'
  const isDecaying = state === 'decaying'
  const opacity = isExpired ? 0.4 : isDecaying ? 0.75 : 1.0
  const color = isBuy ? C.red : C.green
  const bg = isBuy ? 'rgba(239,68,68,0.12)' : 'rgba(34,197,94,0.12)'
  const Icon = isBuy ? TrendingUp : TrendingDown
  const tagText = isBuy ? '买入' : '卖出'
  const stateLabel = isExpired ? (signal.expire_reason === 'reversed' ? '已反转' : signal.expire_reason === 'timeout' ? '已超时' : '已失效')
    : isDecaying ? '衰减中' : ''
  const displayStrength = typeof signal.current_strength === 'number' ? signal.current_strength : signal.strength
  const ageSec = signal.age_sec
  const ageLabel = ageSec != null ? (ageSec < 60 ? `${ageSec}s` : `${Math.floor(ageSec / 60)}m${ageSec % 60}s`) : ''
  const [expanded, setExpanded] = useState(false)

  const details = signal.details && typeof signal.details === 'object' ? signal.details as Record<string, any> : {}
  const observeOnly = Boolean(details.observe_only) || String(signal.message || '').includes('仅观察')
  const actionable = !observeOnly
  const actionLabel = actionable ? '可执行' : '待观察'
  const actionColor = actionable ? C.red : C.yellow
  const pool1StageRaw = details.pool1_stage
  const pool1Stage = pool1StageRaw && typeof pool1StageRaw === 'object' ? pool1StageRaw as Record<string, any> : null
  const resonanceInfoRaw = pool1Stage?.resonance_60m_info ?? details.resonance_60m_info
  const resonanceInfo = resonanceInfoRaw && typeof resonanceInfoRaw === 'object' ? resonanceInfoRaw as Record<string, any> : null
  const resonanceValueRaw = pool1Stage?.resonance_60m ?? details.resonance_60m
  const resonanceValue = typeof resonanceValueRaw === 'boolean' ? resonanceValueRaw : null
  const resonanceSource = typeof resonanceInfo?.source === 'string' ? resonanceInfo.source : ''
  const sourceLabelMap: Record<string, string> = {
    cache: '缓存',
    '1m_aggregate': '真实计算',
    proxy_fallback: '代理兜底',
    precomputed: '预计算',
    disabled: '关闭',
  }
  const sourceLabel = resonanceSource ? (sourceLabelMap[resonanceSource] || resonanceSource) : ''
  const resonanceParts: string[] = []
  if (sourceLabel) resonanceParts.push(`来源:${sourceLabel}`)
  if (typeof resonanceInfo?.cache_age_sec === 'number') resonanceParts.push(`cache_age:${resonanceInfo.cache_age_sec.toFixed(1)}s`)
  if (typeof resonanceInfo?.bars_1m === 'number') resonanceParts.push(`1m:${resonanceInfo.bars_1m}`)
  if (typeof resonanceInfo?.bars_60m === 'number') resonanceParts.push(`60m:${resonanceInfo.bars_60m}`)
  if (typeof resonanceInfo?.reason === 'string' && resonanceInfo.reason) resonanceParts.push(`reason:${resonanceInfo.reason}`)
  const isPool1Signal = signal.type === 'left_side_buy' || signal.type === 'right_side_breakout' || signal.type === 'timing_clear'
  const showResonanceLine = isPool1Signal && (resonanceValue !== null || resonanceParts.length > 0)
  const resonanceText = `60m共振:${resonanceValue === null ? '--' : (resonanceValue ? 'ON' : 'OFF')}`
  const channelRaw = typeof signal.channel === 'string'
    ? signal.channel
    : (typeof details.channel === 'string' ? details.channel : '')
  const signalSourceRaw = typeof signal.signal_source === 'string'
    ? signal.signal_source
    : (typeof details.signal_source === 'string' ? details.signal_source : '')
  const channelLabelMap: Record<string, string> = {
    pool1_timing: 'Pool1择时',
    pool2_t0: 'Pool2做T',
  }
  const channelLabel = channelRaw ? (channelLabelMap[channelRaw] || channelRaw) : ''
  const showChannelLine = !!(channelLabel || signalSourceRaw)
  const gateReason = typeof details.gate_reason === 'string' ? details.gate_reason : ''
  const antiChurnRaw = details.anti_churn
  const antiChurn = antiChurnRaw && typeof antiChurnRaw === 'object' ? antiChurnRaw as Record<string, any> : null
  const antiChurnReject = typeof antiChurn?.reject_reason === 'string' ? antiChurn.reject_reason : ''
  const marketStructureRaw = details.market_structure
  const marketStructure = marketStructureRaw && typeof marketStructureRaw === 'object' ? marketStructureRaw as Record<string, any> : null
  const msTag = typeof marketStructure?.tag === 'string' ? marketStructure.tag : ''
  const msTagLabelMap: Record<string, string> = {
    normal: '中性',
    lure_long: '诱多',
    lure_short: '诱空',
    wash: '对倒',
    real_buy: '主力净流入',
    real_sell: '主力净流出',
  }
  const msTagLabel = msTag ? (msTagLabelMap[msTag] || msTag) : ''
  const msTagColor = msTag === 'real_buy'
    ? C.red
    : (msTag === 'real_sell' || msTag === 'lure_long' || msTag === 'wash')
      ? C.green
      : (msTag === 'lure_short' ? C.yellow : C.dim)
  const msBidAsk = Number(marketStructure?.bid_ask_ratio ?? NaN)
  const msBigBias = Number(marketStructure?.big_order_bias ?? NaN)
  const msSuperBias = Number(marketStructure?.super_order_bias ?? NaN)
  const msBigFlow = Number(marketStructure?.big_net_flow_bps ?? NaN)
  const msSuperFlow = Number(marketStructure?.super_net_flow_bps ?? NaN)
  const msDelta = Number(marketStructure?.strength_delta ?? NaN)
  const msParts: string[] = []
  if (Number.isFinite(msBidAsk)) msParts.push(`买卖比:${msBidAsk.toFixed(2)}`)
  if (Number.isFinite(msBigBias)) msParts.push(`大单偏置:${msBigBias.toFixed(2)}`)
  if (Number.isFinite(msSuperBias)) msParts.push(`超大偏置:${msSuperBias.toFixed(2)}`)
  if (Number.isFinite(msBigFlow)) msParts.push(`大单净流:${msBigFlow.toFixed(0)}bps`)
  if (Number.isFinite(msSuperFlow)) msParts.push(`超大净流:${msSuperFlow.toFixed(0)}bps`)
  if (Number.isFinite(msDelta) && Math.abs(msDelta) > 0) msParts.push(`微观分:${msDelta > 0 ? '+' : ''}${msDelta.toFixed(0)}`)
  const showMicroLine = !!(msTagLabel || msParts.length > 0)
  const thresholdRaw = details.threshold
  const threshold = thresholdRaw && typeof thresholdRaw === 'object' ? thresholdRaw as Record<string, any> : null
  const veto = typeof details.veto === 'string' ? details.veto : ''
  const vetoLabel = veto ? guardVetoLabel(veto) : ''
  const vetoColor = veto ? guardVetoColor(veto) : C.dim
  const trendGuardRaw = details.trend_guard
  const trendGuard = trendGuardRaw && typeof trendGuardRaw === 'object' ? trendGuardRaw as Record<string, any> : null
  const trendGuardActive = Boolean(trendGuard?.active ?? trendGuard?.guard)
  const trendGuardOverride = Boolean(details.trend_guard_override)
  const surgeAbsorbGuard = Boolean(trendGuard?.surge_absorb_guard)
  const trendGuardParts: string[] = []
  if (vetoLabel) trendGuardParts.push(`保护:${vetoLabel}`)
  else if (surgeAbsorbGuard) trendGuardParts.push('保护:强延续监控')
  else if (trendGuardActive) trendGuardParts.push('保护:主升浪监控')
  if (trendGuardOverride) trendGuardParts.push('已突破保护')
  if (typeof trendGuard?.required_bearish_confirms === 'number') trendGuardParts.push(`需偏空确认:${trendGuard.required_bearish_confirms}`)
  if (Array.isArray(trendGuard?.bearish_confirms) && trendGuard.bearish_confirms.length > 0) {
    trendGuardParts.push(`已确认:${trendGuard.bearish_confirms.length}`)
  }
  const thresholdVersion = typeof threshold?.threshold_version === 'string' ? threshold.threshold_version : ''
  const phaseDetail = typeof threshold?.market_phase_detail === 'string' ? threshold.market_phase_detail : ''
  const sessionPolicy = typeof threshold?.session_policy === 'string' ? threshold.session_policy : ''
  const sessionLabel = sessionPolicyLabel(sessionPolicy, phaseDetail)
  const sessionColor = sessionPolicyColor(sessionPolicy, phaseDetail)
  const metaParts: string[] = []
  if (sessionLabel) metaParts.push(`时段:${sessionLabel}`)
  if (trendGuardParts.length > 0) metaParts.push(...trendGuardParts)
  if (gateReason) metaParts.push(`门控:${gateReason}`)
  if (antiChurnReject) metaParts.push(`防抖:${antiChurnReject}`)
  if (thresholdVersion) metaParts.push(`阈值:${thresholdVersion}`)
  const longMsg = (signal.message || '').length > 46
  const longResonance = resonanceParts.join(' · ').length > 72
  const longChannel = signalSourceRaw.length > 56
  const longMicro = (`${msTagLabel} ${msParts.join(' · ')}`).length > 72
  const longMeta = metaParts.join(' · ').length > 56
  const longGuard = trendGuardParts.join(' · ').length > 56
  const showExpandToggle = longMsg || longResonance || longChannel || longMicro || longMeta || longGuard

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 2,
      padding: '4px 8px', borderRadius: 4, background: bg, fontSize: 11,
      borderLeft: `3px solid ${color}`, opacity,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{
          background: color, color: '#fff', fontSize: 10, fontWeight: 700,
          padding: '1px 5px', borderRadius: 3, fontFamily: 'monospace', letterSpacing: 0.5,
        }}>{tagText}</span>
        <span style={{
          background: actionColor,
          color: '#111',
          fontSize: 9,
          fontWeight: 700,
          padding: '1px 5px',
          borderRadius: 3,
          letterSpacing: 0.3,
        }}>{actionLabel}</span>
        {sessionLabel && (
          <span style={{
            background: 'rgba(255,255,255,0.06)',
            color: sessionColor,
            fontSize: 9,
            fontWeight: 700,
            padding: '1px 5px',
            borderRadius: 3,
            letterSpacing: 0.3,
            border: `1px solid ${sessionColor}`,
          }}>{sessionLabel}</span>
        )}
        <Icon size={11} color={color} />
        <div style={{ flex: 1 }} />
        {typeof signal.price === 'number' && signal.price > 0 && (
          <span style={{
            color: '#fff', background: color, fontFamily: 'monospace',
            fontSize: 10, fontWeight: 700, padding: '1px 4px', borderRadius: 2,
          }}>@{signal.price.toFixed(2)}</span>
        )}
        {stateLabel && (
          <span style={{ color: C.dim, fontSize: 9, fontFamily: 'monospace', whiteSpace: 'nowrap' }}>{stateLabel}</span>
        )}
        <span style={{ color, fontFamily: 'monospace', fontSize: 10, opacity: 0.8 }}>
          {displayStrength.toFixed(0)}{ageLabel ? `·${ageLabel}` : ''}
        </span>
      </div>
      <div
        title={signal.message || ''}
        style={{
          paddingLeft: 22,
          color,
          fontWeight: 500,
          lineHeight: 1.35,
          whiteSpace: 'normal',
          wordBreak: 'break-word',
          maxHeight: expanded ? 'none' : '2.8em',
          overflow: 'hidden',
        }}
      >
        {signal.message}
      </div>
      {showResonanceLine && (
        <div style={{
          paddingLeft: 22, color: C.dim, fontSize: 9, fontFamily: 'monospace',
          lineHeight: 1.35, whiteSpace: 'normal', wordBreak: 'break-word',
          maxHeight: expanded ? 'none' : '2.7em', overflow: 'hidden',
        }}>
          <span style={{ color: resonanceValue ? C.red : C.green }}>{resonanceText}</span>
          {resonanceParts.length > 0 ? ` · ${resonanceParts.join(' · ')}` : ''}
        </div>
      )}
      {showChannelLine && (
        <div style={{
          paddingLeft: 22, color: C.dim, fontSize: 9, fontFamily: 'monospace',
          lineHeight: 1.35, whiteSpace: 'normal', wordBreak: 'break-word',
          maxHeight: expanded ? 'none' : '2.7em', overflow: 'hidden',
        }}>
          {channelLabel ? `通道:${channelLabel}` : '通道:--'}
          {signalSourceRaw ? ` · 来源:${signalSourceRaw}` : ''}
        </div>
      )}
      {showMicroLine && (
        <div style={{
          paddingLeft: 22, color: C.dim, fontSize: 9, fontFamily: 'monospace',
          lineHeight: 1.35, whiteSpace: 'normal', wordBreak: 'break-word',
          maxHeight: expanded ? 'none' : '2.7em', overflow: 'hidden',
        }}>
          {msTagLabel ? <span style={{ color: msTagColor }}>{`盘口:${msTagLabel}`}</span> : '盘口:--'}
          {msParts.length > 0 ? ` · ${msParts.join(' · ')}` : ''}
        </div>
      )}
      {trendGuardParts.length > 0 && (
        <div style={{
          paddingLeft: 22, color: C.dim, fontSize: 9, fontFamily: 'monospace',
          lineHeight: 1.35, whiteSpace: 'normal', wordBreak: 'break-word',
          maxHeight: expanded ? 'none' : '2.7em', overflow: 'hidden',
        }}>
          {vetoLabel
            ? <span style={{ color: vetoColor }}>{vetoLabel}</span>
            : <span style={{ color: surgeAbsorbGuard ? C.red : C.yellow }}>{surgeAbsorbGuard ? '强延续保护' : '主升浪保护'}</span>}
          {trendGuardParts.length > 1 ? ` · ${trendGuardParts.slice(1).join(' · ')}` : ''}
        </div>
      )}
      {metaParts.length > 0 && (
        <div style={{
          paddingLeft: 22, color: C.dim, fontSize: 9, fontFamily: 'monospace',
          lineHeight: 1.35, whiteSpace: 'normal', wordBreak: 'break-word',
          maxHeight: expanded ? 'none' : '2.7em', overflow: 'hidden',
        }}>
          {metaParts.join(' · ')}
        </div>
      )}
      {showExpandToggle && (
        <button
          onClick={() => setExpanded(v => !v)}
          style={{
            marginLeft: 22,
            marginTop: 1,
            alignSelf: 'flex-start',
            background: 'none',
            border: `1px solid ${C.border}`,
            borderRadius: 3,
            color: C.dim,
            fontSize: 9,
            padding: '1px 5px',
            cursor: 'pointer',
          }}
        >
          {expanded ? '收起' : '展开'}
        </button>
      )}
      <button
        onClick={() => onOpenDetail?.(signal)}
        style={{
          marginLeft: 22,
          marginTop: 1,
          alignSelf: 'flex-start',
          background: 'none',
          border: `1px solid ${C.border}`,
          borderRadius: 3,
          color: C.cyan,
          fontSize: 9,
          padding: '1px 5px',
          cursor: 'pointer',
        }}
      >
        详情
      </button>
    </div>
  )
}

function signalTypeLabel(type: string): string {
  const m: Record<string, string> = {
    left_side_buy: '左侧买入',
    right_side_breakout: '右侧突破',
    timing_clear: '择时清仓',
    positive_t: '正T买入',
    reverse_t: '反T卖出',
  }
  return m[type] || type || '--'
}

function signalDirectionLabel(sig: Signal): string {
  const d = sig.direction || ((sig.type === 'reverse_t' || sig.type === 'timing_clear') ? 'sell' : 'buy')
  return d === 'sell' ? '卖出' : '买入'
}

function formatSignalTime(ts?: number): string {
  if (!ts || ts <= 0) return '--'
  try {
    return new Date(ts * 1000).toLocaleString()
  } catch {
    return '--'
  }
}

function safeJson(v: any): string {
  try {
    return JSON.stringify(v ?? {}, null, 2)
  } catch {
    return '{}'
  }
}

function guardVetoLabel(veto?: string | null): string {
  const key = String(veto || '').trim()
  const m: Record<string, string> = {
    trend_surge_absorb_guard: '强延续保护拦截',
    trend_extension_guard: '主升浪保护拦截',
    off_session_skip: '非交易时段跳过',
    volume_pace_surge_block: '放量强攻拦截',
  }
  return m[key] || key
}

function guardVetoColor(veto?: string | null): string {
  const key = String(veto || '').trim()
  if (key === 'trend_surge_absorb_guard') return C.red
  if (key === 'trend_extension_guard') return '#fb7185'
  if (key === 'off_session_skip') return C.dim
  if (key === 'volume_pace_surge_block') return C.yellow
  return C.dim
}

function sessionPolicyLabel(policy?: string | null, phaseDetail?: string | null): string {
  const p = String(policy || '').trim()
  const d = String(phaseDetail || '').trim()
  if (p === 'auction_pause' || d === 'pre_auction') return '集合竞价暂停'
  if (p === 'open_strict' || d === 'open_strict') return '开盘严格窗'
  if (p === 'lunch_pause' || d === 'lunch_break') return '午休暂停'
  if (p === 'close_reduce' || d === 'close_reduce') return '尾盘收敛窗'
  return ''
}

function sessionPolicyColor(policy?: string | null, phaseDetail?: string | null): string {
  const p = String(policy || '').trim()
  const d = String(phaseDetail || '').trim()
  if (p === 'auction_pause' || d === 'pre_auction') return '#f59e0b'
  if (p === 'open_strict' || d === 'open_strict') return C.yellow
  if (p === 'lunch_pause' || d === 'lunch_break') return C.dim
  if (p === 'close_reduce' || d === 'close_reduce') return '#fb7185'
  return C.dim
}

function marketStatusLabel(status?: string | null, desc?: string | null): string {
  const s = String(status || '').trim()
  const map: Record<string, string> = {
    pre_auction: '集合竞价中，信号暂停',
    morning_session: '上午交易',
    afternoon_session: '下午交易',
    lunch_break: '午休暂停',
    before_open: '开盘前',
    after_close: '收盘后',
  }
  return map[s] || String(desc || '').trim() || '未知状态'
}

function marketStatusColor(status?: string | null, isOpen?: boolean): string {
  const s = String(status || '').trim()
  if (s === 'pre_auction') return '#f59e0b'
  if (s === 'lunch_break') return C.dim
  return isOpen ? C.green : C.dim
}

function isPreAuctionStatus(status?: string | null): boolean {
  return String(status || '').trim() === 'pre_auction'
}

function pool1DecisionColor(decision?: string | null): string {
  const d = String(decision || '').trim()
  if (d === 'build') return C.red
  if (d === 'hold') return C.cyan
  if (d === 'clear') return C.green
  return C.dim
}

function boardSegmentLabel(board?: string | null): string {
  const b = String(board || '').trim()
  if (b === 'main_board') return '主板'
  if (b === 'gem') return '创业板'
  if (b === 'star') return '科创板'
  if (b === 'beijing') return '北交所'
  if (b === 'other') return '其他'
  return b || '--'
}

function SignalDetailModal({ signal, stockCode, stockName, onClose }: {
  signal: Signal
  stockCode: string
  stockName: string
  onClose: () => void
}) {
  const details = signal.details && typeof signal.details === 'object' ? signal.details as Record<string, any> : {}
  const threshold = details.threshold && typeof details.threshold === 'object' ? details.threshold as Record<string, any> : null
  const instrumentProfile = details.instrument_profile && typeof details.instrument_profile === 'object'
    ? details.instrument_profile as Record<string, any>
    : (threshold?.instrument_profile && typeof threshold.instrument_profile === 'object' ? threshold.instrument_profile as Record<string, any> : null)
  const microEnvironment = threshold?.micro_environment && typeof threshold.micro_environment === 'object'
    ? threshold.micro_environment as Record<string, any>
    : null
  const conceptEcology = details.concept_ecology && typeof details.concept_ecology === 'object'
    ? details.concept_ecology as Record<string, any>
    : (threshold?.concept_ecology && typeof threshold.concept_ecology === 'object' ? threshold.concept_ecology as Record<string, any> : null)
  const leftStreakGuard = details.left_streak_guard && typeof details.left_streak_guard === 'object'
    ? details.left_streak_guard as Record<string, any>
    : null
  const timingClearDetails = signal.type === 'timing_clear' ? details : null
  const antiChurn = details.anti_churn && typeof details.anti_churn === 'object' ? details.anti_churn as Record<string, any> : null
  const marketStructure = details.market_structure && typeof details.market_structure === 'object'
    ? details.market_structure as Record<string, any>
    : null
  const trendGuard = details.trend_guard && typeof details.trend_guard === 'object'
    ? details.trend_guard as Record<string, any>
    : null
  const pool1Decision = details.pool1_decision && typeof details.pool1_decision === 'object'
    ? details.pool1_decision as Record<string, any>
    : null
  const pool1Position = details.pool1_position && typeof details.pool1_position === 'object'
    ? details.pool1_position as Record<string, any>
    : null
  const veto = typeof details.veto === 'string' ? details.veto : ''
  const vetoLabel = veto ? guardVetoLabel(veto) : ''
  const vetoColor = veto ? guardVetoColor(veto) : C.dim
  const currentStrength = typeof signal.current_strength === 'number' ? signal.current_strength : signal.strength
  const rawScore = Number(details.raw_score ?? NaN)
  const gateValueRaw = details.micro_gate ?? details.risk_gate
  const gateValue = Number(gateValueRaw ?? NaN)
  const observeTh = Number(details.score_observe_th ?? NaN)
  const execTh = Number(details.score_exec_th ?? NaN)
  const observeOnly = Boolean(details.observe_only) || String(signal.message || '').includes('仅观察')
  const scoreStatus = observeOnly ? '待观察' : '可执行'
  const scoreStatusColor = observeOnly ? C.yellow : C.red
  const panelBorder = signalDirectionLabel(signal) === '买入' ? C.red : C.green
  const trendGuardActive = Boolean(trendGuard?.active ?? trendGuard?.guard)
  const trendGuardOverride = Boolean(details.trend_guard_override)
  const surgeAbsorbGuard = Boolean(trendGuard?.surge_absorb_guard)
  const bearishConfirms = Array.isArray(trendGuard?.bearish_confirms) ? trendGuard.bearish_confirms as any[] : []
  const guardReasons = Array.isArray(trendGuard?.guard_reasons) ? trendGuard.guard_reasons as any[] : []
  const structureReasons = Array.isArray(trendGuard?.structure_reasons) ? trendGuard.structure_reasons as any[] : []
  const negativeFlags = Array.isArray(trendGuard?.negative_flags) ? trendGuard.negative_flags as any[] : []
  const sessionLabel = sessionPolicyLabel(
    typeof threshold?.session_policy === 'string' ? threshold.session_policy : '',
    typeof threshold?.market_phase_detail === 'string' ? threshold.market_phase_detail : '',
  )
  const sessionColor = sessionPolicyColor(
    typeof threshold?.session_policy === 'string' ? threshold.session_policy : '',
    typeof threshold?.market_phase_detail === 'string' ? threshold.market_phase_detail : '',
  )
  const clearFamilyRaw = timingClearDetails ? String(timingClearDetails.clear_family || '--') : '--'
  const clearLevelRaw = timingClearDetails ? String(timingClearDetails.clear_level || timingClearDetails.clear_level_hint || '--') : '--'
  const clearReduceRatio = timingClearDetails ? Number(timingClearDetails.suggest_reduce_ratio ?? NaN) : NaN
  const clearAtrBucket = timingClearDetails ? String(timingClearDetails.atr_bucket || '--') : '--'
  const timingClearAnchor = timingClearDetails && timingClearDetails.entry_anchor && typeof timingClearDetails.entry_anchor === 'object'
    ? timingClearDetails.entry_anchor as Record<string, any>
    : null
  const clearEntryCostAnchor = timingClearAnchor ? Number(timingClearAnchor.entry_cost_anchor ?? NaN) : NaN
  const clearEntryAvwap = timingClearAnchor ? Number(timingClearAnchor.entry_avwap ?? NaN) : NaN
  const clearEntryPremiumPct = timingClearAnchor ? Number(timingClearAnchor.entry_premium_pct ?? NaN) : NaN
  const clearEntryCostLost = Boolean(timingClearAnchor?.entry_cost_lost)
  const clearEntryAvwapLost = Boolean(timingClearAnchor?.entry_avwap_lost)
  const clearBreakoutAnchorAvwap = timingClearAnchor ? Number(timingClearAnchor.breakout_anchor_avwap ?? NaN) : NaN
  const clearBreakoutAnchorPremiumPct = timingClearAnchor ? Number(timingClearAnchor.breakout_anchor_premium_pct ?? NaN) : NaN
  const clearBreakoutAnchorLost = Boolean(timingClearAnchor?.breakout_anchor_lost)
  const clearBreakoutAnchorReason = timingClearAnchor ? String(timingClearAnchor.breakout_anchor_reason || '--') : '--'
  const clearEventAnchorAvwap = timingClearAnchor ? Number(timingClearAnchor.event_anchor_avwap ?? NaN) : NaN
  const clearEventAnchorPremiumPct = timingClearAnchor ? Number(timingClearAnchor.event_anchor_premium_pct ?? NaN) : NaN
  const clearEventAnchorLost = Boolean(timingClearAnchor?.event_anchor_lost)
  const clearEventAnchorReason = timingClearAnchor ? String(timingClearAnchor.event_anchor_reason || '--') : '--'
  const clearAnchorReason = timingClearAnchor ? String(timingClearAnchor.reason || '--') : '--'
  const clearSupportBandLow = timingClearAnchor ? Number(timingClearAnchor.support_band_low ?? NaN) : NaN
  const clearSupportBandHigh = timingClearAnchor ? Number(timingClearAnchor.support_band_high ?? NaN) : NaN
  const clearSupportAnchorType = timingClearAnchor ? String(timingClearAnchor.support_anchor_type || '--') : '--'
  const clearSupportAnchorLine = timingClearAnchor ? Number(timingClearAnchor.support_anchor_line ?? NaN) : NaN
  const clearSupportAnchorLost = Boolean(timingClearAnchor?.support_anchor_lost)
  const clearLostAnchorCount = timingClearAnchor ? Number(timingClearAnchor.lost_anchor_count ?? NaN) : NaN
  const clearTriggerItems = timingClearDetails && Array.isArray(timingClearDetails.trigger_items)
    ? timingClearDetails.trigger_items as any[]
    : []
  const clearConfirmItems = timingClearDetails && Array.isArray(timingClearDetails.confirm_items)
    ? timingClearDetails.confirm_items as any[]
    : []
  const clearFamilyLabelMap: Record<string, string> = {
    defense: '防守退出',
    take_profit: '分批止盈',
    beta_reduce: '被动减仓',
  }
  const clearLevelLabelMap: Record<string, string> = {
    full: '全清',
    partial: '部分减仓',
    observe: '观察减仓',
  }
  const clearFamilyLabel = clearFamilyLabelMap[clearFamilyRaw] || clearFamilyRaw
  const clearLevelLabel = clearLevelLabelMap[clearLevelRaw] || clearLevelRaw
  const clearAtrColor = clearAtrBucket === 'high' ? C.red : clearAtrBucket === 'low' ? C.cyan : C.text
  const msTagRaw = typeof marketStructure?.tag === 'string' ? marketStructure.tag : ''
  const msTagLabelMap: Record<string, string> = {
    normal: '中性',
    lure_long: '诱多',
    lure_short: '诱空',
    wash: '对倒',
    real_buy: '主力净流入',
    real_sell: '主力净流出',
  }
  const msTagLabel = msTagRaw ? (msTagLabelMap[msTagRaw] || msTagRaw) : '--'

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div
      onClick={onClose}
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 2000,
        background: 'rgba(0,0,0,0.55)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 12,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: 'min(860px, calc(100vw - 24px))',
          maxHeight: '84vh',
          overflowY: 'auto',
          background: C.card,
          border: `1px solid ${panelBorder}`,
          borderRadius: 8,
          boxShadow: '0 8px 30px rgba(0,0,0,0.35)',
          padding: 12,
          display: 'flex',
          flexDirection: 'column',
          gap: 10,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ color: C.cyan, fontFamily: 'monospace', fontSize: 12 }}>{stockCode}</span>
          <span style={{ color: C.bright, fontSize: 14, fontWeight: 600 }}>{stockName || '--'}</span>
          <span style={{
            fontSize: 10,
            color: '#fff',
            background: panelBorder,
            borderRadius: 3,
            padding: '1px 6px',
            fontWeight: 700,
          }}>
            {signalTypeLabel(signal.type)} · {signalDirectionLabel(signal)}
          </span>
          <span style={{
            fontSize: 10,
            color: '#111',
            background: scoreStatusColor,
            borderRadius: 3,
            padding: '1px 6px',
            fontWeight: 700,
          }}>
            {scoreStatus}
          </span>
          {sessionLabel && (
            <span style={{
              fontSize: 10,
              color: sessionColor,
              background: 'rgba(255,255,255,0.04)',
              border: `1px solid ${sessionColor}`,
              borderRadius: 3,
              padding: '1px 6px',
              fontWeight: 700,
            }}>
              {sessionLabel}
            </span>
          )}
          <div style={{ flex: 1 }} />
          <button
            onClick={onClose}
            style={{
              background: 'none',
              border: `1px solid ${C.border}`,
              color: C.dim,
              borderRadius: 4,
              padding: '2px 8px',
              cursor: 'pointer',
              fontSize: 11,
            }}
          >
            关闭
          </button>
        </div>

        <div style={{ color: C.bright, fontSize: 12, lineHeight: 1.45, whiteSpace: 'normal', wordBreak: 'break-word' }}>
          {signal.message || '--'}
        </div>

        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
          gap: 6,
          fontSize: 11,
          fontFamily: 'monospace',
          background: 'rgba(0,0,0,0.22)',
          border: `1px solid ${C.border}`,
          borderRadius: 6,
          padding: 8,
        }}>
          <div style={{ color: C.dim }}>触发时间: <span style={{ color: C.text }}>{formatSignalTime(signal.triggered_at)}</span></div>
          <div style={{ color: C.dim }}>触发价: <span style={{ color: C.text }}>{(signal.price && signal.price > 0) ? signal.price.toFixed(3) : '--'}</span></div>
          <div style={{ color: C.dim }}>最终分数: <span style={{ color: C.text }}>{signal.strength ?? '--'}</span></div>
          <div style={{ color: C.dim }}>当前分数: <span style={{ color: C.text }}>{Number(currentStrength).toFixed(0)}</span></div>
          <div style={{ color: C.dim }}>原始分: <span style={{ color: C.text }}>{Number.isFinite(rawScore) ? rawScore.toFixed(1) : '--'}</span></div>
          <div style={{ color: C.dim }}>门控系数: <span style={{ color: C.text }}>{Number.isFinite(gateValue) ? gateValue.toFixed(2) : '--'}</span></div>
          <div style={{ color: C.dim }}>观察阈值: <span style={{ color: C.text }}>{Number.isFinite(observeTh) ? observeTh.toFixed(0) : '--'}</span></div>
          <div style={{ color: C.dim }}>执行阈值: <span style={{ color: C.text }}>{Number.isFinite(execTh) ? execTh.toFixed(0) : '--'}</span></div>
          <div style={{ color: C.dim }}>执行结论: <span style={{ color: scoreStatusColor, fontWeight: 700 }}>{scoreStatus}</span></div>
          <div style={{ color: C.dim }}>保护状态: <span style={{ color: vetoLabel ? vetoColor : (surgeAbsorbGuard ? C.red : trendGuardActive ? C.yellow : C.text) }}>
            {vetoLabel || (surgeAbsorbGuard ? '强延续保护' : trendGuardActive ? '主升浪保护' : '--')}
          </span></div>
          <div style={{ color: C.dim }}>状态: <span style={{ color: C.text }}>{signal.state || 'active'}</span></div>
          <div style={{ color: C.dim }}>时效: <span style={{ color: C.text }}>{signal.age_sec != null ? `${signal.age_sec}s` : '--'}</span></div>
          <div style={{ color: C.dim }}>时段策略: <span style={{ color: sessionLabel ? sessionColor : C.text }}>{sessionLabel || '--'}</span></div>
          <div style={{ color: C.dim }}>通道: <span style={{ color: C.text }}>{signal.channel || '--'}</span></div>
          <div style={{ color: C.dim }}>来源: <span style={{ color: C.text }}>{signal.signal_source || '--'}</span></div>
          {timingClearDetails && (
            <>
              <div style={{ color: C.dim }}>退出语义: <span style={{ color: C.text }}>{clearFamilyLabel}</span></div>
              <div style={{ color: C.dim }}>退出级别: <span style={{ color: clearLevelRaw === 'full' ? C.red : clearLevelRaw === 'partial' ? C.yellow : C.cyan }}>{clearLevelLabel}</span></div>
              <div style={{ color: C.dim }}>建议减仓: <span style={{ color: C.text }}>{Number.isFinite(clearReduceRatio) ? `${(clearReduceRatio * 100).toFixed(0)}%` : '--'}</span></div>
              <div style={{ color: C.dim }}>ATR分桶: <span style={{ color: clearAtrColor }}>{clearAtrBucket}</span></div>
              <div style={{ color: C.dim }}>建仓成本: <span style={{ color: C.text }}>{Number.isFinite(clearEntryCostAnchor) ? clearEntryCostAnchor.toFixed(3) : '--'}</span></div>
              <div style={{ color: C.dim }}>相对成本: <span style={{ color: Number.isFinite(clearEntryPremiumPct) ? (clearEntryPremiumPct >= 0 ? C.red : C.green) : C.text }}>{Number.isFinite(clearEntryPremiumPct) ? `${clearEntryPremiumPct.toFixed(2)}%` : '--'}</span></div>
              <div style={{ color: C.dim }}>建仓AVWAP: <span style={{ color: C.text }}>{Number.isFinite(clearEntryAvwap) ? clearEntryAvwap.toFixed(3) : '--'}</span></div>
              <div style={{ color: C.dim }}>突破AVWAP: <span style={{ color: C.text }}>{Number.isFinite(clearBreakoutAnchorAvwap) ? clearBreakoutAnchorAvwap.toFixed(3) : '--'}</span></div>
              <div style={{ color: C.dim }}>事件AVWAP: <span style={{ color: C.text }}>{Number.isFinite(clearEventAnchorAvwap) ? clearEventAnchorAvwap.toFixed(3) : '--'}</span></div>
              <div style={{ color: C.dim }}>支撑带: <span style={{ color: C.text }}>{Number.isFinite(clearSupportBandLow) && Number.isFinite(clearSupportBandHigh) ? `${clearSupportBandLow.toFixed(3)} ~ ${clearSupportBandHigh.toFixed(3)}` : '--'}</span></div>
              <div style={{ color: C.dim }}>主支撑锚: <span style={{ color: clearSupportAnchorLost ? C.red : C.text }}>{Number.isFinite(clearSupportAnchorLine) ? `${clearSupportAnchorType} @ ${clearSupportAnchorLine.toFixed(3)}` : '--'}</span></div>
              <div style={{ color: C.dim }}>失守锚数: <span style={{ color: Number.isFinite(clearLostAnchorCount) && clearLostAnchorCount >= 2 ? C.red : C.text }}>{Number.isFinite(clearLostAnchorCount) ? `${clearLostAnchorCount}` : '--'}</span></div>
              <div style={{ color: C.dim }}>锚点状态: <span style={{ color: clearEntryCostLost || clearEntryAvwapLost || clearBreakoutAnchorLost || clearEventAnchorLost ? C.red : C.text }}>{clearEntryCostLost || clearEntryAvwapLost || clearBreakoutAnchorLost || clearEventAnchorLost ? '已失守' : '--'}</span></div>
            </>
          )}
          {pool1Decision && (
            <>
              <div style={{ color: C.dim }}>Pool1决策: <span style={{ color: pool1DecisionColor(String(pool1Decision.decision || '')) }}>{String(pool1Decision.label || '--')}</span></div>
              <div style={{ color: C.dim }}>决策模式: <span style={{ color: C.text }}>{String(pool1Decision.mode || '--')}</span></div>
              <div style={{ color: C.dim }}>持仓状态: <span style={{ color: C.text }}>{String(pool1Decision.position_status || '--')}</span></div>
            </>
          )}
          {pool1Position && (
            <>
              <div style={{ color: C.dim }}>仓位前: <span style={{ color: C.text }}>{pool1Position.position_ratio_before != null ? `${(Number(pool1Position.position_ratio_before) * 100).toFixed(0)}%` : '--'}</span></div>
              <div style={{ color: C.dim }}>仓位后: <span style={{ color: C.text }}>{pool1Position.position_ratio_after != null ? `${(Number(pool1Position.position_ratio_after) * 100).toFixed(0)}%` : '--'}</span></div>
            </>
          )}
        </div>

        {(threshold || antiChurn || marketStructure || trendGuard || vetoLabel || leftStreakGuard || timingClearDetails) && (
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
            gap: 8,
          }}>
            {timingClearDetails && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>退出模板</div>
                <div>clear_family: <span style={{ color: C.text }}>{clearFamilyLabel}</span></div>
                <div>clear_level: <span style={{ color: clearLevelRaw === 'full' ? C.red : clearLevelRaw === 'partial' ? C.yellow : C.cyan }}>{clearLevelLabel}</span></div>
                <div>suggest_reduce_ratio: <span style={{ color: C.text }}>{Number.isFinite(clearReduceRatio) ? `${(clearReduceRatio * 100).toFixed(0)}%` : '--'}</span></div>
                <div>atr_14: <span style={{ color: C.text }}>{String(timingClearDetails.atr_14 ?? '--')}</span></div>
                <div>atr_pct: <span style={{ color: C.text }}>{String(timingClearDetails.atr_pct ?? '--')}</span></div>
                <div>atr_bucket: <span style={{ color: clearAtrColor }}>{clearAtrBucket}</span></div>
                <div>entry_anchor.reason: <span style={{ color: C.text }}>{clearAnchorReason}</span></div>
                <div>entry_cost_anchor: <span style={{ color: C.text }}>{Number.isFinite(clearEntryCostAnchor) ? clearEntryCostAnchor.toFixed(3) : '--'}</span></div>
                <div>entry_premium_pct: <span style={{ color: Number.isFinite(clearEntryPremiumPct) ? (clearEntryPremiumPct >= 0 ? C.red : C.green) : C.text }}>{Number.isFinite(clearEntryPremiumPct) ? `${clearEntryPremiumPct.toFixed(2)}%` : '--'}</span></div>
                <div>entry_cost_lost: <span style={{ color: clearEntryCostLost ? C.red : C.text }}>{String(clearEntryCostLost)}</span></div>
                <div>entry_avwap: <span style={{ color: C.text }}>{Number.isFinite(clearEntryAvwap) ? clearEntryAvwap.toFixed(3) : '--'}</span></div>
                <div>entry_avwap_lost: <span style={{ color: clearEntryAvwapLost ? C.red : C.text }}>{String(clearEntryAvwapLost)}</span></div>
                <div>entry_avwap_bars: <span style={{ color: C.text }}>{String(timingClearAnchor?.entry_avwap_bars ?? '--')}</span></div>
                <div>breakout_anchor.reason: <span style={{ color: C.text }}>{clearBreakoutAnchorReason}</span></div>
                <div>breakout_anchor_avwap: <span style={{ color: C.text }}>{Number.isFinite(clearBreakoutAnchorAvwap) ? clearBreakoutAnchorAvwap.toFixed(3) : '--'}</span></div>
                <div>breakout_anchor_premium_pct: <span style={{ color: Number.isFinite(clearBreakoutAnchorPremiumPct) ? (clearBreakoutAnchorPremiumPct >= 0 ? C.red : C.green) : C.text }}>{Number.isFinite(clearBreakoutAnchorPremiumPct) ? `${clearBreakoutAnchorPremiumPct.toFixed(2)}%` : '--'}</span></div>
                <div>breakout_anchor_lost: <span style={{ color: clearBreakoutAnchorLost ? C.red : C.text }}>{String(clearBreakoutAnchorLost)}</span></div>
                <div>breakout_anchor_bars: <span style={{ color: C.text }}>{String(timingClearAnchor?.breakout_anchor_bars ?? '--')}</span></div>
                <div>event_anchor.reason: <span style={{ color: C.text }}>{clearEventAnchorReason}</span></div>
                <div>event_anchor_avwap: <span style={{ color: C.text }}>{Number.isFinite(clearEventAnchorAvwap) ? clearEventAnchorAvwap.toFixed(3) : '--'}</span></div>
                <div>event_anchor_premium_pct: <span style={{ color: Number.isFinite(clearEventAnchorPremiumPct) ? (clearEventAnchorPremiumPct >= 0 ? C.red : C.green) : C.text }}>{Number.isFinite(clearEventAnchorPremiumPct) ? `${clearEventAnchorPremiumPct.toFixed(2)}%` : '--'}</span></div>
                <div>event_anchor_lost: <span style={{ color: clearEventAnchorLost ? C.red : C.text }}>{String(clearEventAnchorLost)}</span></div>
                <div>event_anchor_bars: <span style={{ color: C.text }}>{String(timingClearAnchor?.event_anchor_bars ?? '--')}</span></div>
                <div>support_band: <span style={{ color: C.text }}>{Number.isFinite(clearSupportBandLow) && Number.isFinite(clearSupportBandHigh) ? `${clearSupportBandLow.toFixed(3)} ~ ${clearSupportBandHigh.toFixed(3)}` : '--'}</span></div>
                <div>support_anchor: <span style={{ color: clearSupportAnchorLost ? C.red : C.text }}>{Number.isFinite(clearSupportAnchorLine) ? `${clearSupportAnchorType} @ ${clearSupportAnchorLine.toFixed(3)}` : '--'}</span></div>
                <div>lost_anchor_count: <span style={{ color: Number.isFinite(clearLostAnchorCount) && clearLostAnchorCount >= 2 ? C.red : C.text }}>{Number.isFinite(clearLostAnchorCount) ? String(clearLostAnchorCount) : '--'}</span></div>
                <div>min_confirm: <span style={{ color: C.text }}>{String(timingClearDetails.min_confirm ?? '--')}</span></div>
                <div>trigger_items: <span style={{ color: C.text }}>{clearTriggerItems.length > 0 ? clearTriggerItems.join(', ') : '--'}</span></div>
                <div>confirm_items: <span style={{ color: C.text }}>{clearConfirmItems.length > 0 ? clearConfirmItems.join(', ') : '--'}</span></div>
              </div>
            )}
            {leftStreakGuard && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>左侧抑制</div>
                <div>active: <span style={{ color: C.text }}>{String(Boolean(leftStreakGuard.active))}</span></div>
                <div>reason: <span style={{ color: C.text }}>{String(leftStreakGuard.reason || '--')}</span></div>
                <div>observe_only: <span style={{ color: Boolean(leftStreakGuard.observe_only) ? C.yellow : C.text }}>{String(Boolean(leftStreakGuard.observe_only))}</span></div>
                <div>observe_reason: <span style={{ color: C.text }}>{String(leftStreakGuard.observe_reason || '--')}</span></div>
                <div>quick_clear_streak: <span style={{ color: C.text }}>{String(leftStreakGuard.quick_clear_streak ?? '--')}</span></div>
                <div>hours_since_clear: <span style={{ color: C.text }}>{String(leftStreakGuard.hours_since_clear ?? '--')}</span></div>
                <div>remaining_cooldown_hours: <span style={{ color: C.text }}>{String(leftStreakGuard.remaining_cooldown_hours ?? '--')}</span></div>
                <div>hold_days_before_clear: <span style={{ color: C.text }}>{String(leftStreakGuard.hold_days_before_clear ?? '--')}</span></div>
                <div>concept_state: <span style={{ color: C.text }}>{String(leftStreakGuard.concept_state || '--')}</span></div>
                <div>score_adj: <span style={{ color: C.text }}>{String(leftStreakGuard.score_adj ?? '--')}</span></div>
              </div>
            )}
            {(trendGuard || vetoLabel) && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>趋势保护</div>
                <div>veto: <span style={{ color: vetoLabel ? vetoColor : C.text }}>{vetoLabel || '--'}</span></div>
                <div>guard_active: <span style={{ color: C.text }}>{String(trendGuardActive)}</span></div>
                <div>surge_absorb_guard: <span style={{ color: surgeAbsorbGuard ? C.red : C.text }}>{String(surgeAbsorbGuard)}</span></div>
                <div>override: <span style={{ color: trendGuardOverride ? C.yellow : C.text }}>{String(trendGuardOverride)}</span></div>
                <div>guard_score: <span style={{ color: C.text }}>{String(trendGuard?.guard_score ?? '--')}</span></div>
                <div>required_bearish: <span style={{ color: C.text }}>{String(trendGuard?.required_bearish_confirms ?? '--')}</span></div>
                <div>surge_required: <span style={{ color: C.text }}>{String(trendGuard?.surge_absorb_required_bearish_confirms ?? '--')}</span></div>
                <div>bearish_confirms: <span style={{ color: C.text }}>{bearishConfirms.length > 0 ? bearishConfirms.join(', ') : '--'}</span></div>
                <div>guard_reasons: <span style={{ color: C.text }}>{guardReasons.length > 0 ? guardReasons.join(', ') : '--'}</span></div>
                <div>structure_reasons: <span style={{ color: C.text }}>{structureReasons.length > 0 ? structureReasons.join(', ') : '--'}</span></div>
                <div>negative_flags: <span style={{ color: C.text }}>{negativeFlags.length > 0 ? negativeFlags.join(', ') : '--'}</span></div>
              </div>
            )}
            {threshold && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>阈值快照</div>
                <div>version: <span style={{ color: C.text }}>{String(threshold.threshold_version || '--')}</span></div>
                <div>phase: <span style={{ color: C.text }}>{String(threshold.market_phase || '--')}</span></div>
                <div>phase_detail: <span style={{ color: C.text }}>{String(threshold.market_phase_detail || '--')}</span></div>
                <div>session_policy: <span style={{ color: C.text }}>{String(threshold.session_policy || '--')}</span></div>
                <div>regime: <span style={{ color: C.text }}>{String(threshold.regime || '--')}</span></div>
                <div>board_segment: <span style={{ color: C.text }}>{String(threshold.board_segment || instrumentProfile?.board_segment || '--')}</span></div>
                <div>security_type: <span style={{ color: C.text }}>{String(threshold.security_type || instrumentProfile?.security_type || '--')}</span></div>
                <div>listing_stage: <span style={{ color: C.text }}>{String(threshold.listing_stage || instrumentProfile?.listing_stage || '--')}</span></div>
                <div>listing_days: <span style={{ color: C.text }}>{String(instrumentProfile?.listing_days ?? '--')}</span></div>
                <div>price_limit_pct: <span style={{ color: C.text }}>{String(threshold.price_limit_pct ?? instrumentProfile?.price_limit_pct ?? '--')}</span></div>
                <div>core_concept_board: <span style={{ color: C.text }}>{String(threshold.core_concept_board || details.concept_board || '--')}</span></div>
                <div>concept_boards: <span style={{ color: C.text }}>{Array.isArray(threshold.concept_boards) && threshold.concept_boards.length > 0 ? threshold.concept_boards.join(', ') : (Array.isArray(details.concept_boards) && details.concept_boards.length > 0 ? details.concept_boards.join(', ') : '--')}</span></div>
                <div>concept_state: <span style={{ color: String(conceptEcology?.state || '') === 'retreat' ? C.red : String(conceptEcology?.state || '') === 'expand' ? C.green : String(conceptEcology?.state || '') === 'strong' ? C.cyan : C.text }}>{String(conceptEcology?.state || '--')}</span></div>
                <div>concept_score: <span style={{ color: C.text }}>{String(conceptEcology?.score ?? '--')}</span></div>
                <div>risk_warning: <span style={{ color: threshold.risk_warning ? C.red : C.text }}>{String(Boolean(threshold.risk_warning ?? instrumentProfile?.risk_warning))}</span></div>
                <div>market_name: <span style={{ color: C.text }}>{String(instrumentProfile?.market_name || '--')}</span></div>
                <div>volume_drought: <span style={{ color: threshold.volume_drought ? C.yellow : C.text }}>{String(Boolean(threshold.volume_drought ?? microEnvironment?.volume_drought))}</span></div>
                <div>board_seal_env: <span style={{ color: threshold.board_seal_env ? C.red : C.text }}>{String(Boolean(threshold.board_seal_env ?? microEnvironment?.board_seal_env))}</span></div>
                <div>seal_side: <span style={{ color: C.text }}>{String(threshold.seal_side || microEnvironment?.seal_side || '--')}</span></div>
                <div>profile_rule: <span style={{ color: C.text }}>{Array.isArray(threshold.profile_override?.rules) && threshold.profile_override.rules.length > 0 ? threshold.profile_override.rules.map((x: any) => String(x?.reason || '--')).join(', ') : '--'}</span></div>
                <div>session_rule: <span style={{ color: C.text }}>{String(threshold.session_override?.policy || '--')}</span></div>
                <div>limit_dist_pct: <span style={{ color: C.text }}>{String(threshold.limit_filter?.distance_pct ?? '--')}</span></div>
                <div>magnet_th_pct: <span style={{ color: C.text }}>{String(threshold.limit_filter?.threshold_pct ?? '--')}</span></div>
                <div>blocked: <span style={{ color: C.text }}>{String(Boolean(threshold.blocked))}</span></div>
                <div>blocked_reason: <span style={{ color: C.text }}>{String(threshold.blocked_reason || '--')}</span></div>
                <div>observer_only: <span style={{ color: C.text }}>{String(Boolean(threshold.observer_only))}</span></div>
                <div>observer_reason: <span style={{ color: C.text }}>{String(threshold.observer_reason || '--')}</span></div>
                <div>concept_observe_reason: <span style={{ color: C.text }}>{String(conceptEcology?.observe_reason || details.observe_reason || '--')}</span></div>
              </div>
            )}
            {pool1Decision && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>Pool1 状态决策</div>
                <div>decision: <span style={{ color: pool1DecisionColor(String(pool1Decision.decision || '')) }}>{String(pool1Decision.label || pool1Decision.decision || '--')}</span></div>
                <div>mode: <span style={{ color: C.text }}>{String(pool1Decision.mode || '--')}</span></div>
                <div>position_status: <span style={{ color: C.text }}>{String(pool1Decision.position_status || '--')}</span></div>
                <div>strength: <span style={{ color: C.text }}>{String(pool1Decision.strength ?? '--')}</span></div>
                <div>signal_types: <span style={{ color: C.text }}>{Array.isArray(pool1Decision.signal_types) && pool1Decision.signal_types.length > 0 ? pool1Decision.signal_types.join(', ') : '--'}</span></div>
                <div>reason: <span style={{ color: C.text }}>{String(pool1Decision.reason || '--')}</span></div>
              </div>
            )}
            {pool1Position && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>Pool1 仓位状态</div>
                <div>status_before: <span style={{ color: C.text }}>{String(pool1Position.status_before || '--')}</span></div>
                <div>status_after: <span style={{ color: C.text }}>{String(pool1Position.status_after || '--')}</span></div>
                <div>position_ratio_before: <span style={{ color: C.text }}>{pool1Position.position_ratio_before != null ? `${(Number(pool1Position.position_ratio_before) * 100).toFixed(0)}%` : '--'}</span></div>
                <div>position_ratio_after: <span style={{ color: C.text }}>{pool1Position.position_ratio_after != null ? `${(Number(pool1Position.position_ratio_after) * 100).toFixed(0)}%` : '--'}</span></div>
                <div>holding_days_before: <span style={{ color: C.text }}>{String(pool1Position.holding_days_before ?? '--')}</span></div>
                <div>holding_days_after: <span style={{ color: C.text }}>{String(pool1Position.holding_days_after ?? '--')}</span></div>
                <div>transition: <span style={{ color: C.text }}>{String(pool1Position.transition || '--')}</span></div>
              </div>
            )}
            {antiChurn && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>防抖快照</div>
                <div>enabled: <span style={{ color: C.text }}>{String(Boolean(antiChurn.enabled))}</span></div>
                <div>reject_reason: <span style={{ color: antiChurn.reject_reason ? C.yellow : C.text }}>{String(antiChurn.reject_reason || '--')}</span></div>
                <div>expected_edge_bps: <span style={{ color: C.text }}>{String(antiChurn.expected_edge_bps ?? '--')}</span></div>
                <div>seconds_since_last: <span style={{ color: C.text }}>{String(antiChurn.seconds_since_last ?? '--')}</span></div>
                <div>flip_realized_bps: <span style={{ color: C.text }}>{String(antiChurn.flip_realized_bps ?? '--')}</span></div>
              </div>
            )}
            {marketStructure && (
              <div style={{
                background: 'rgba(0,0,0,0.22)',
                border: `1px solid ${C.border}`,
                borderRadius: 6,
                padding: 8,
                fontSize: 10,
                fontFamily: 'monospace',
                color: C.dim,
                lineHeight: 1.5,
              }}>
                <div style={{ color: C.cyan, fontSize: 11, marginBottom: 4 }}>盘口微观结构</div>
                <div>tag: <span style={{ color: C.text }}>{msTagLabel}</span></div>
                <div>bid_ask_ratio: <span style={{ color: C.text }}>{String(marketStructure.bid_ask_ratio ?? '--')}</span></div>
                <div>big_order_bias: <span style={{ color: C.text }}>{String(marketStructure.big_order_bias ?? '--')}</span></div>
                <div>big_net_flow_bps: <span style={{ color: C.text }}>{String(marketStructure.big_net_flow_bps ?? '--')}</span></div>
                <div>super_order_bias: <span style={{ color: C.text }}>{String(marketStructure.super_order_bias ?? '--')}</span></div>
                <div>super_net_flow_bps: <span style={{ color: C.text }}>{String(marketStructure.super_net_flow_bps ?? '--')}</span></div>
                <div>strength_delta: <span style={{ color: C.text }}>{String(marketStructure.strength_delta ?? '--')}</span></div>
                <div>channel_adjust: <span style={{ color: C.text }}>{String(marketStructure.channel_adjust ?? '--')}</span></div>
              </div>
            )}
          </div>
        )}

        <div style={{
          background: 'rgba(0,0,0,0.28)',
          border: `1px solid ${C.border}`,
          borderRadius: 6,
          padding: 8,
          color: C.dim,
          fontSize: 10,
          fontFamily: 'monospace',
        }}>
          <div style={{ color: C.cyan, fontSize: 11, marginBottom: 6 }}>Details JSON</div>
          <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word', color: C.text }}>
            {safeJson(details)}
          </pre>
        </div>
      </div>
    </div>
  )
}
