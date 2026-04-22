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
interface Member { ts_code: string; name: string; industry?: string; added_at?: string; note?: string }
interface Signal { has_signal: boolean; type: string; direction?: 'buy' | 'sell'; price?: number; strength: number; current_strength?: number; message: string; triggered_at: number; details: Record<string, any>; state?: 'active' | 'decaying' | 'expired'; age_sec?: number; expire_reason?: string; channel?: string; signal_source?: string }
interface SignalRow {
  ts_code: string
  name: string
  price: number
  pct_chg: number
  signals: Signal[]
  pool1_position_status?: 'holding' | 'observe' | string
  pool1_holding_days?: number
  pool1_in_holding?: boolean
}
interface SearchResult { ts_code: string; name: string; industry?: string }
interface TickData { price: number; open: number; high: number; low: number; pre_close: number; volume: number; amount: number; pct_chg: number; bids: [number, number][]; asks: [number, number][]; is_mock?: boolean; timestamp?: number; source?: string }
interface Txn { time: string; price: number; volume: number; direction: number }
interface MarketStatus { is_open: boolean; status: string; desc?: string }
type SortKey = 'default' | 'pct_chg' | 'signal_strength' | 'amount'
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
      const base = isOpen
        ? (pageVisible ? 3000 : 6000)
        : (status === 'lunch_break'
          ? (pageVisible ? 120_000 : 180_000)
          : (pageVisible ? 180_000 : 300_000))
      const ok = await fetchFast()
      fastFailRef.current = ok ? 0 : Math.min(6, fastFailRef.current + 1)
      const next = Math.min(base * Math.pow(2, fastFailRef.current), isOpen ? 30_000 : 300_000)
      if (!cancelled) pollRef.current = setTimeout(runFastLoop, next)
    }

    const runTickLoop = async () => {
      if (cancelled) return
      const isOpen = !!marketStatus.is_open
      const status = String(marketStatus.status || '')
      const base = isOpen
        ? (pageVisible ? 4500 : 9000)
        : (status === 'lunch_break'
          ? (pageVisible ? 180_000 : 240_000)
          : (pageVisible ? 240_000 : 360_000))
      const ok = await fetchTick()
      tickFailRef.current = ok ? 0 : Math.min(6, tickFailRef.current + 1)
      const next = Math.min(base * Math.pow(2, tickFailRef.current), isOpen ? 40_000 : 360_000)
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
  const [diffOpen, setDiffOpen] = useState(false)
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

  useEffect(() => {
    if (pool.pool_id !== 1) return
    loadPool1Observe()
    const baseSec = pool1ObserveUi.observePollSec || POOL1_OBSERVE_UI_DEFAULT.observePollSec
    const failFactor = pool1ObserveFailCount >= OBSERVE_FAIL_WARN_COUNT ? 2 : 1
    const visibleFactor = pageVisible ? 1 : 3
    const status = String(marketStatus.status || '')
    const marketFactor = marketStatus.is_open ? 1 : (status === 'lunch_break' ? 8 : 15)
    const effectiveSec = baseSec * failFactor * visibleFactor * marketFactor
    const pollMs = Math.max(1000, effectiveSec * 1000)
    const timer = setInterval(loadPool1Observe, pollMs)
    return () => clearInterval(timer)
  }, [pool.pool_id, loadPool1Observe, pool1ObserveUi.observePollSec, pool1ObserveFailCount, pageVisible, marketStatus.is_open])

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
    const baseMs = marketStatus.is_open ? 30_000 : 90_000
    const visFactor = pageVisible ? 1 : 2
    const timer = setInterval(() => {
      loadFastSlowDiff()
      loadFastSlowTrend()
    }, Math.max(10_000, baseMs * visFactor))
    return () => clearInterval(timer)
  }, [diffOpen, loadFastSlowDiff, loadFastSlowTrend, marketStatus.is_open, pageVisible])

  useEffect(() => {
    loadRuntimeHealth()
    const baseMs = healthOpen
      ? (marketStatus.is_open ? 30_000 : 90_000)
      : (marketStatus.is_open ? 90_000 : 180_000)
    const visFactor = pageVisible ? 1 : 2
    const timer = setInterval(loadRuntimeHealth, Math.max(15_000, baseMs * visFactor))
    return () => clearInterval(timer)
  }, [loadRuntimeHealth, healthOpen, marketStatus.is_open, pageVisible])

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

  const filteredMembers = filterSignal
    ? sortedMembers.filter(m => (signalMap.get(m.ts_code)?.signals?.length ?? 0) > 0)
    : sortedMembers

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
        </div>
        <div style={{ flex: 1 }} />
        {/* 连接状态 */}
        <div style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 4, color: connected ? C.green : (usePolling ? C.yellow : C.dim) }}>
          {connected ? <Wifi size={12} /> : <WifiOff size={12} />}
          {connected ? 'WS' : (usePolling ? 'HTTP轮询' : '断开')}
        </div>
        {/* 交易时段 */}
        <div style={{ fontSize: 11, color: marketStatus.is_open ? C.green : C.dim, display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 6, height: 6, borderRadius: 3, background: marketStatus.is_open ? C.green : C.dim, display: 'inline-block' }} />
          {marketStatus.desc || (marketStatus.is_open ? '交易中' : '休市')}
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
            <StockCard key={m.ts_code} member={m} signalRow={signalMap.get(m.ts_code)}
              tick={tickMap[m.ts_code]} txns={txnsMap[m.ts_code] || []} tickHistory={tickHistoryMap[m.ts_code] || []}
              compact={compact} onRemove={() => removeMember(m.ts_code)} onUpdateNote={(note) => updateNote(m.ts_code, note)} poolId={pool.pool_id} />
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
function StockCard({ member, signalRow, tick, txns, tickHistory: initialTickHistory, compact, onRemove, onUpdateNote, poolId }: {
  member: Member; signalRow?: SignalRow; tick?: TickData; txns: Txn[]; tickHistory?: TickDataPoint[]
  compact: boolean; onRemove: () => void; onUpdateNote: (note: string) => void; poolId: number
}) {
  const signals = signalRow?.signals || []
  const [showDetail, setShowDetail] = useState(!compact)
  const [selectedSignal, setSelectedSignal] = useState<Signal | null>(null)
  const [editingNote, setEditingNote] = useState(false)
  const [noteInput, setNoteInput] = useState(member.note || '')
  // 缓存 tick 历史数据用于绘制分时图
  // 用 ref 保存真实数据，state 仅在跨分钟时同步（触发重绘）
  const tickHistoryRef = useRef<TickDataPoint[]>(initialTickHistory || [])
  const [tickHistory, setTickHistory] = useState<TickDataPoint[]>(initialTickHistory || [])
  const lastRenderedMinute = useRef<number>(0)

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
  const pool1HoldingDays = Number(signalRow?.pool1_holding_days ?? 0)
  const pool1InHolding = Boolean(signalRow?.pool1_in_holding || pool1PositionStatus === 'holding')
  const pool1PositionLabel = pool1InHolding ? '持仓' : '观望'
  const pool1PositionColor = pool1InHolding ? C.red : C.dim
  const maxBidVol = Math.max(1, ...(tick?.bids || []).map(([, v]) => v))
  const maxAskVol = Math.max(1, ...(tick?.asks || []).map(([, v]) => v))

  return (
    <div style={{ background: C.card, border: `1px solid ${signals.length > 0 ? C.cyan : C.border}`, borderRadius: 6, padding: 10, position: 'relative', boxShadow: signals.length > 0 ? '0 0 0 1px rgba(0,200,200,0.2)' : 'none', display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Link to={`/stock/${member.ts_code}`} style={{ color: C.cyan, fontSize: 12, fontFamily: 'monospace', textDecoration: 'none', display: 'block' }}>{member.ts_code}</Link>
          <div style={{ color: C.bright, fontSize: 14, fontWeight: 600, marginTop: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{member.name}</div>
          <div style={{ color: C.dim, fontSize: 9, fontFamily: 'monospace', marginTop: 2 }}>
            {`源:${tickSource} · 更新:${formatTimeText(tickUpdatedAt)} (${formatAgeText(tickUpdatedAt)})`}
          </div>
          {poolId === 1 && (
            <div style={{ color: C.dim, fontSize: 9, fontFamily: 'monospace', marginTop: 1 }}>
              状态:<span style={{ color: pool1PositionColor, fontWeight: 700 }}>{pool1PositionLabel}</span>
              {pool1InHolding ? ` · 持有:${pool1HoldingDays.toFixed(1)}天` : ''}
            </div>
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
  const metaParts: string[] = []
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
  const antiChurn = details.anti_churn && typeof details.anti_churn === 'object' ? details.anti_churn as Record<string, any> : null
  const marketStructure = details.market_structure && typeof details.market_structure === 'object'
    ? details.market_structure as Record<string, any>
    : null
  const trendGuard = details.trend_guard && typeof details.trend_guard === 'object'
    ? details.trend_guard as Record<string, any>
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
          <div style={{ color: C.dim }}>通道: <span style={{ color: C.text }}>{signal.channel || '--'}</span></div>
          <div style={{ color: C.dim }}>来源: <span style={{ color: C.text }}>{signal.signal_source || '--'}</span></div>
        </div>

        {(threshold || antiChurn || marketStructure || trendGuard || vetoLabel) && (
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
            gap: 8,
          }}>
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
                <div>regime: <span style={{ color: C.text }}>{String(threshold.regime || '--')}</span></div>
                <div>board_segment: <span style={{ color: C.text }}>{String(threshold.board_segment || instrumentProfile?.board_segment || '--')}</span></div>
                <div>security_type: <span style={{ color: C.text }}>{String(threshold.security_type || instrumentProfile?.security_type || '--')}</span></div>
                <div>listing_stage: <span style={{ color: C.text }}>{String(threshold.listing_stage || instrumentProfile?.listing_stage || '--')}</span></div>
                <div>listing_days: <span style={{ color: C.text }}>{String(instrumentProfile?.listing_days ?? '--')}</span></div>
                <div>price_limit_pct: <span style={{ color: C.text }}>{String(threshold.price_limit_pct ?? instrumentProfile?.price_limit_pct ?? '--')}</span></div>
                <div>risk_warning: <span style={{ color: threshold.risk_warning ? C.red : C.text }}>{String(Boolean(threshold.risk_warning ?? instrumentProfile?.risk_warning))}</span></div>
                <div>market_name: <span style={{ color: C.text }}>{String(instrumentProfile?.market_name || '--')}</span></div>
                <div>volume_drought: <span style={{ color: threshold.volume_drought ? C.yellow : C.text }}>{String(Boolean(threshold.volume_drought ?? microEnvironment?.volume_drought))}</span></div>
                <div>board_seal_env: <span style={{ color: threshold.board_seal_env ? C.red : C.text }}>{String(Boolean(threshold.board_seal_env ?? microEnvironment?.board_seal_env))}</span></div>
                <div>seal_side: <span style={{ color: C.text }}>{String(threshold.seal_side || microEnvironment?.seal_side || '--')}</span></div>
                <div>profile_rule: <span style={{ color: C.text }}>{Array.isArray(threshold.profile_override?.rules) && threshold.profile_override.rules.length > 0 ? threshold.profile_override.rules.map((x: any) => String(x?.reason || '--')).join(', ') : '--'}</span></div>
                <div>limit_dist_pct: <span style={{ color: C.text }}>{String(threshold.limit_filter?.distance_pct ?? '--')}</span></div>
                <div>magnet_th_pct: <span style={{ color: C.text }}>{String(threshold.limit_filter?.threshold_pct ?? '--')}</span></div>
                <div>blocked: <span style={{ color: C.text }}>{String(Boolean(threshold.blocked))}</span></div>
                <div>observer_only: <span style={{ color: C.text }}>{String(Boolean(threshold.observer_only))}</span></div>
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
