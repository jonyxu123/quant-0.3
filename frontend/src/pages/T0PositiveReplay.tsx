import { useCallback, useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import { Link } from 'react-router-dom'
import { Download, RefreshCw } from 'lucide-react'

const API = 'http://localhost:8000'

const C = {
  bg: '#0f1117',
  card: '#181d2a',
  border: '#1e2233',
  text: '#c8cdd8',
  dim: '#4a5068',
  bright: '#e8eaf0',
  cyan: '#00c8c8',
  cyanBg: 'rgba(0,200,200,0.08)',
  red: '#ef4444',
  green: '#22c55e',
  yellow: '#eab308',
}

interface MarketStatus {
  is_open?: boolean
  status?: string
  desc?: string
}

interface ReasonCount {
  reason: string
  count: number
}

interface PositiveReplayEval {
  trigger_time: number
  trigger_time_iso: string
  trigger_price: number
  eval_price: number
  ret_bps: number
  mfe_bps: number
  mae_bps: number
  direction_correct: boolean
  market_phase: string
  channel: string
  signal_source: string
  created_at: string
  time_diff_sec: number
  net_ret_bps_after_fee: number
}

interface PositiveRebuildQuality {
  enabled?: boolean
  observe_only?: boolean
  quality_pass?: boolean
  observe_reasons?: string[]
  recover_after_rebuild_score?: number | null
  recover_after_rebuild_min_score?: number | null
  expected_edge_bps?: number | null
  fee_bps?: number | null
  slippage_bps?: number | null
  impact_bps?: number | null
  net_executable_edge_bps?: number | null
  min_net_executable_edge_bps?: number | null
  recovery_confirms?: string[]
  continuation_risks?: string[]
  inventory_warnings?: string[]
  inventory_ok?: boolean
  temp_inventory_ratio?: number | null
  reverse_room_ratio_after?: number | null
  anchor_cost_raise_bps_est?: number | null
}

interface T0InventorySnapshot {
  observe_only?: boolean
  observe_reason?: string | null
  blocked_reason?: string | null
  action_path?: string | null
  action_role?: string | null
  overnight_base_qty?: number | null
  tradable_t_qty?: number | null
  reserve_qty?: number | null
  suggested_action_qty?: number | null
  applied_action_qty?: number | null
  cash_available_for_t?: number | null
  inventory_anchor_cost?: number | null
  state_ready?: boolean
  executed?: boolean
}

interface PositiveReplayRow {
  id: number | string
  ts_code: string
  name: string
  signal_type: string
  channel: string
  signal_source: string
  strength: number
  message: string
  price: number
  pct_chg: number
  triggered_at: string
  history_snapshot?: Record<string, any> | null
  positive_rebuild_quality?: PositiveRebuildQuality | null
  t0_inventory?: T0InventorySnapshot | null
  quality?: PositiveReplayEval | null
  classification: string
  classification_reason: string
  fee_bps: number
}

interface PositiveReplayQualitySummary {
  sample_count?: number
  quality_pass_count?: number
  observe_only_count?: number
  quality_pass_rate?: number | null
  observe_only_rate?: number | null
  avg_recovery_score?: number | null
  avg_net_executable_edge_bps?: number | null
  avg_temp_inventory_ratio?: number | null
  avg_reverse_room_ratio_after?: number | null
  top_observe_reasons?: ReasonCount[]
  top_recovery_confirms?: ReasonCount[]
  top_continuation_risks?: ReasonCount[]
  top_inventory_warnings?: ReasonCount[]
  matched_quality_count?: number
  avg_ret_bps_5m?: number | null
  avg_net_ret_bps_5m?: number | null
  direction_correct_rate?: number | null
  cost_covered_count?: number
  cost_covered_rate?: number | null
}

interface PositiveReplayResp {
  ok: boolean
  checked_at: string
  hours: number
  limit: number
  fee_bps: number
  summary: Record<string, number>
  quality_summary?: PositiveReplayQualitySummary | null
  count: number
  rows: PositiveReplayRow[]
}

interface PositiveDiagRow {
  ts_code: string
  name: string
  price: number
  pct_chg: number
  status: string
  has_signal: boolean
  observe_only: boolean
  strength: number
  message: string
  veto?: string | null
  trigger_items?: string[]
  confirm_items?: string[]
  candidate_reasons?: string[]
  threshold?: Record<string, any>
  positive_rebuild_quality?: PositiveRebuildQuality | null
  t0_inventory?: T0InventorySnapshot | null
  market_structure?: Record<string, any>
  feature_snapshot?: Record<string, any>
}

interface PositiveDiagResp {
  ok: boolean
  checked_at: string
  interesting_only: boolean
  summary: Record<string, number>
  rows_total: number
  count: number
  rows: PositiveDiagRow[]
}

function fmtDateTime(v?: string | null): string {
  if (!v) return '--'
  const dt = new Date(v)
  if (Number.isNaN(dt.getTime())) return String(v)
  return dt.toLocaleString('zh-CN', { hour12: false })
}

function fmtPct(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '--'
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
}

function fmtBps(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '--'
  return `${v > 0 ? '+' : ''}${v.toFixed(1)}bps`
}

function fmtRatePct(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '--'
  return `${(v * 100).toFixed(1)}%`
}

function fmtNum(v?: number | null, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '--'
  return v.toFixed(digits)
}

function fmtQty(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '--'
  return `${Math.round(v)}股`
}

function csvCell(v: unknown): string {
  if (v === null || v === undefined) return ''
  const s = String(v)
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`
  return s
}

function downloadCsv(filename: string, headers: string[], rows: Array<Array<unknown>>) {
  const content = [headers.map(csvCell).join(','), ...rows.map(row => row.map(csvCell).join(','))].join('\r\n')
  const blob = new Blob([`\uFEFF${content}`], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

function marketStatusLabel(status?: string, desc?: string): string {
  if (desc) return desc
  const s = String(status || '')
  if (s === 'open') return '交易中'
  if (s === 'lunch_break') return '午休暂停'
  if (s === 'closed') return '已收盘'
  if (s === 'pre_open') return '待开盘'
  return '状态未知'
}

function marketStatusColor(status?: string, isOpen?: boolean): string {
  if (isOpen) return C.green
  const s = String(status || '')
  if (s === 'lunch_break') return C.yellow
  if (s === 'closed') return C.dim
  return C.yellow
}

function replayLabel(v: string): string {
  const m: Record<string, string> = {
    good_rebuild: '回补有效',
    weak_rebuild: '边际偏薄',
    false_rebuild: '回补失败',
    observe_filtered: '观察拦截有效',
    observe_missed: '观察偏严错过',
    pending: '待评估',
  }
  return m[v] || v
}

function replayColor(v: string): string {
  if (v === 'good_rebuild' || v === 'observe_filtered') return C.green
  if (v === 'weak_rebuild') return C.yellow
  if (v === 'false_rebuild' || v === 'observe_missed') return C.red
  return C.dim
}

function replayPriority(v: string): number {
  const m: Record<string, number> = {
    false_rebuild: 1,
    observe_missed: 2,
    weak_rebuild: 3,
    pending: 4,
    observe_filtered: 5,
    good_rebuild: 6,
  }
  return m[v] ?? 99
}

function positiveDiagStatusLabel(status: string): string {
  const m: Record<string, string> = {
    triggered: '已触发',
    trigger_observe_only: '触发但仅观察',
    veto: 'veto 拦截',
    trigger_no_confirm: '触发未确认',
    trigger_score_filtered: '评分/质量门过滤',
    candidate: '候选',
    off_session_skip: '非交易时段跳过',
    idle: '未成形',
    error: '异常',
  }
  return m[status] || status
}

function positiveDiagStatusColor(status: string): string {
  if (status === 'triggered') return C.green
  if (status === 'trigger_observe_only' || status === 'trigger_score_filtered' || status === 'candidate') return C.yellow
  if (status === 'veto' || status === 'error') return C.red
  return C.dim
}

function buildPositiveReplayCompare(row: PositiveReplayRow, diag?: PositiveDiagRow | null): { label: string; detail: string; color: string } {
  if (!diag) {
    return {
      label: '缺少当前诊断',
      detail: '当前时点没有可对照的正T实时诊断，只能先看历史回放。',
      color: C.dim,
    }
  }
  const status = String(diag.status || '')
  if (row.classification === 'false_rebuild') {
    if (status === 'triggered') {
      return {
        label: '当前仍会放行',
        detail: '这类历史回补失败样本在当前链路下仍可能继续进入可执行，优先继续收紧。',
        color: C.red,
      }
    }
    if (status === 'trigger_observe_only' || status === 'veto' || status === 'trigger_score_filtered') {
      return {
        label: '当前已更谨慎',
        detail: `当前状态是 ${positiveDiagStatusLabel(status)}，比历史失败样本出现时更保守。`,
        color: C.green,
      }
    }
    return {
      label: '当前暂未成形',
      detail: `当前状态是 ${positiveDiagStatusLabel(status)}，还没走到可执行回补。`,
      color: C.yellow,
    }
  }
  if (row.classification === 'observe_missed') {
    if (status === 'triggered') {
      return {
        label: '当前已放开执行',
        detail: '历史上被观察错过，但当前链路已经会放行同类回补，说明口径有放宽。',
        color: C.green,
      }
    }
    if (status === 'trigger_observe_only' || status === 'veto' || status === 'trigger_score_filtered') {
      return {
        label: '当前仍会压住',
        detail: `当前状态仍是 ${positiveDiagStatusLabel(status)}，需要继续关注是否在误伤好回补。`,
        color: C.red,
      }
    }
    return {
      label: '当前未到触发层',
      detail: `当前状态是 ${positiveDiagStatusLabel(status)}，暂时还不构成同类放行/拦截判断。`,
      color: C.yellow,
    }
  }
  if (row.classification === 'observe_filtered') {
    if (status === 'triggered') {
      return {
        label: '当前可能偏松',
        detail: '历史上观察拦截是有效的，但当前已经会触发，需要关注是否变松了。',
        color: C.yellow,
      }
    }
    return {
      label: '当前仍谨慎',
      detail: `当前状态是 ${positiveDiagStatusLabel(status)}，和历史的观察拦截方向一致。`,
      color: C.green,
    }
  }
  if (row.classification === 'good_rebuild') {
    if (status === 'triggered') {
      return {
        label: '当前与历史一致',
        detail: '历史上是有效回补，当前链路没有明显把它误伤掉。',
        color: C.green,
      }
    }
    return {
      label: '当前可能偏保守',
      detail: `历史上这是有效回补，但当前状态变成 ${positiveDiagStatusLabel(status)}，要关注是否误杀。`,
      color: C.yellow,
    }
  }
  if (row.classification === 'weak_rebuild') {
    if (status === 'triggered') {
      return {
        label: '当前仍会触发',
        detail: '边际偏薄的回补样本当前仍会放行，手续费后门槛仍值得继续优化。',
        color: C.yellow,
      }
    }
    return {
      label: '当前更谨慎',
      detail: `当前状态是 ${positiveDiagStatusLabel(status)}，比历史边际偏薄样本更稳。`,
      color: C.green,
    }
  }
  return {
    label: '等待后续观察',
    detail: `当前状态是 ${positiveDiagStatusLabel(status)}，先把实时诊断留作对照。`,
    color: C.dim,
  }
}

function buildPositiveReplayDiff(row: PositiveReplayRow, diag?: PositiveDiagRow | null): Array<{ label: string; detail: string; color: string }> {
  const histQ = row.positive_rebuild_quality && typeof row.positive_rebuild_quality === 'object' ? row.positive_rebuild_quality : null
  const nowQ = diag?.positive_rebuild_quality && typeof diag.positive_rebuild_quality === 'object' ? diag.positive_rebuild_quality : null
  const items: Array<{ label: string; detail: string; color: string }> = []
  if (!diag) {
    return [{
      label: '缺少当前诊断',
      detail: '没有当前正T实时诊断，暂时无法比较口径变化。',
      color: C.dim,
    }]
  }

  const histObserve = !!histQ?.observe_only
  const nowObserve = !!nowQ?.observe_only || diag.status === 'trigger_observe_only'
  if (!histObserve && nowObserve) {
    items.push({
      label: '执行等级收紧',
      detail: '历史更接近可执行，当前已经降到仅观察或被质量门压住。',
      color: C.green,
    })
  } else if (histObserve && diag.status === 'triggered' && !diag.observe_only) {
    items.push({
      label: '执行等级放宽',
      detail: '历史是仅观察样本，当前已经能进可执行，注意是否放宽过头。',
      color: C.yellow,
    })
  }

  const histScore = histQ?.recover_after_rebuild_score
  const nowScore = nowQ?.recover_after_rebuild_score
  if (typeof histScore === 'number' && typeof nowScore === 'number' && Math.abs(nowScore - histScore) >= 10) {
    items.push({
      label: '恢复分变化',
      detail: `历史 ${histScore.toFixed(0)} -> 当前 ${nowScore.toFixed(0)}`,
      color: nowScore > histScore ? C.green : C.yellow,
    })
  }

  const histEdge = histQ?.net_executable_edge_bps
  const nowEdge = nowQ?.net_executable_edge_bps
  if (typeof histEdge === 'number' && typeof nowEdge === 'number' && Math.abs(nowEdge - histEdge) >= 8) {
    items.push({
      label: '净边际变化',
      detail: `历史 ${fmtBps(histEdge)} -> 当前 ${fmtBps(nowEdge)}`,
      color: nowEdge > histEdge ? C.green : C.yellow,
    })
  }

  const histReasons = new Set((histQ?.observe_reasons || []).map(x => String(x)))
  const nowReasons = new Set((nowQ?.observe_reasons || []).map(x => String(x)))
  if (!histReasons.has('inventory_quality_degraded') && nowReasons.has('inventory_quality_degraded')) {
    items.push({
      label: '新增库存约束',
      detail: '当前回补质量门新增了库存质量恶化约束。',
      color: C.green,
    })
  }
  if (!histReasons.has('net_executable_edge_low') && nowReasons.has('net_executable_edge_low')) {
    items.push({
      label: '新增净边际约束',
      detail: '当前会更强调手续费后净边际，不再只看方向。',
      color: C.green,
    })
  }

  if (items.length <= 0) {
    items.push({
      label: '当前口径接近',
      detail: '历史样本和当前实时诊断在核心质量门上没有明显新增差异。',
      color: C.dim,
    })
  }
  return items.slice(0, 4)
}

function coverageLabel(row: PositiveReplayRow): string {
  const net = row.quality?.net_ret_bps_after_fee
  if (typeof net !== 'number' || !Number.isFinite(net)) return '待评估'
  return net >= 0 ? '覆盖成本' : '未覆盖成本'
}

function coverageColor(row: PositiveReplayRow): string {
  const net = row.quality?.net_ret_bps_after_fee
  if (typeof net !== 'number' || !Number.isFinite(net)) return C.dim
  return net >= 0 ? C.green : C.red
}

function marketPhaseLabel(v?: string | null): string {
  const m: Record<string, string> = {
    open: '开盘',
    morning: '上午',
    afternoon: '下午',
    close: '尾盘',
    off_session: '非交易时段',
    pending: '待评估',
    unknown: '未知',
  }
  return m[String(v || '')] || String(v || '未知')
}

function reasonLabel(v?: string | null): string {
  const m: Record<string, string> = {
    recovery_quality_low: '恢复质量偏低',
    net_executable_edge_low: '净边际不足',
    inventory_quality_degraded: '库存质量变差',
    avwap_reclaim_space: 'AVWAP 回收空间',
    gub5_turning: 'GUB5 转向',
    bid_support: '买盘承接修复',
    ask_wall_absorb: '卖墙被吃',
    washout_or_lure_short: '洗盘/诱空确认',
    super_order_bias: '超大单偏多',
    super_net_inflow: '超大单净流入',
    volume_pace_expand: '成交扩张',
    volume_pace_surge: '成交脉冲',
    already_above_or_near_avwap: '已接近 AVWAP',
    gub5_still_down: 'GUB5 仍下行',
    bid_support_weak: '承接偏弱',
    wash_trade_noise: '洗盘噪音偏大',
    super_order_sell_bias: '超大单偏空',
    super_net_outflow: '超大单净流出',
    volume_pace_shrink: '成交量枯竭',
    temp_inventory_too_heavy: '临时库存过重',
    reverse_t_room_low_after_rebuild: '回补后反T空间不足',
    anchor_cost_raise_too_high: '库存锚成本抬高过多',
    base_qty_missing: '底仓基数缺失',
    no_cash_available_for_positive_t: '正T 资金空间不足',
    no_tradable_t_inventory: '可做T底仓不足',
    t0_inventory_missing: '底仓状态缺失',
    t_count_exhausted: '当日T次数已满',
    action_qty_below_lot: '建议股数不足一手',
  }
  const key = String(v || '')
  return m[key] || key || '--'
}

function reasonListText(items?: string[] | null, limit = 3): string {
  if (!Array.isArray(items) || items.length <= 0) return '--'
  return items.slice(0, limit).map(reasonLabel).join(' / ')
}

function topReasonText(items?: ReasonCount[] | null, limit = 3): string {
  if (!Array.isArray(items) || items.length <= 0) return '--'
  return items
    .filter(it => it && typeof it.reason === 'string' && it.reason)
    .slice(0, limit)
    .map(it => `${reasonLabel(it.reason)}(${Number(it.count || 0)})`)
    .join(' / ')
}

function SummaryCard({ label, value, color = C.bright }: { label: string; value: string; color?: string }) {
  return (
    <div style={{
      minWidth: 140,
      flex: '1 1 140px',
      background: C.card,
      border: `1px solid ${C.border}`,
      borderRadius: 12,
      padding: '14px 16px',
    }}>
      <div style={{ color: C.dim, fontSize: 11, marginBottom: 6 }}>{label}</div>
      <div style={{ color, fontSize: 22, fontWeight: 700, fontFamily: 'monospace' }}>{value}</div>
    </div>
  )
}

function KV({ label, value, color = C.text }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <span style={{ color: C.dim, fontSize: 11 }}>{label}</span>
      <span style={{ color, fontSize: 13, fontFamily: 'monospace' }}>{value}</span>
    </div>
  )
}

export default function T0PositiveReplay() {
  const [hours, setHours] = useState(24)
  const [limit, setLimit] = useState(120)
  const [symbolFilter, setSymbolFilter] = useState('')
  const [classificationFilter, setClassificationFilter] = useState('all')
  const [gateFilter, setGateFilter] = useState('all')
  const [coverageFilter, setCoverageFilter] = useState('all')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [market, setMarket] = useState<MarketStatus>({})
  const [replay, setReplay] = useState<PositiveReplayResp | null>(null)
  const [positiveDiag, setPositiveDiag] = useState<PositiveDiagResp | null>(null)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [m, r] = await Promise.all([
        axios.get(`${API}/api/realtime/market_status`),
        axios.get(`${API}/api/realtime/runtime/t0_positive_replay`, {
          params: { hours, limit },
        }),
      ])
      const replayData = (r.data || null) as PositiveReplayResp | null
      const replayCodes = Array.from(new Set(((replayData?.rows || []).map(row => String(row.ts_code || '').trim()).filter(Boolean))))
      let diagData: PositiveDiagResp | null = null
      if (replayCodes.length > 0) {
        const d = await axios.get(`${API}/api/realtime/runtime/t0_positive_diagnostics`, {
          params: {
            interesting_only: false,
            limit: Math.max(200, replayCodes.length),
            ts_codes: replayCodes.join(','),
          },
        })
        diagData = (d.data || null) as PositiveDiagResp | null
      }
      setMarket((m.data || {}) as MarketStatus)
      setReplay(replayData)
      setPositiveDiag(diagData)
    } catch (e: any) {
      setError(String(e?.message || e || '加载失败'))
    } finally {
      setLoading(false)
    }
  }, [hours, limit])

  useEffect(() => {
    load()
  }, [load])

  const rows = replay?.rows || []
  const qualitySummary = replay?.quality_summary || null
  const currentDiagSummary = positiveDiag?.summary || {}
  const symbolKeyword = symbolFilter.trim().toUpperCase()
  const positiveDiagByCode = useMemo(() => {
    const map = new Map<string, PositiveDiagRow>()
    for (const row of positiveDiag?.rows || []) {
      const code = String(row.ts_code || '').trim()
      if (!code) continue
      map.set(code, row)
    }
    return map
  }, [positiveDiag])

  const filteredRows = useMemo(() => {
    return rows
      .filter(row => {
        const rowText = `${row.ts_code} ${row.name}`.toUpperCase()
        const gateState = row.positive_rebuild_quality?.observe_only ? 'observe' : (row.positive_rebuild_quality?.quality_pass ? 'pass' : 'unknown')
        const coverage = coverageLabel(row)
        if (symbolKeyword && !rowText.includes(symbolKeyword)) return false
        if (classificationFilter !== 'all' && row.classification !== classificationFilter) return false
        if (gateFilter === 'pass' && gateState !== 'pass') return false
        if (gateFilter === 'observe' && gateState !== 'observe') return false
        if (gateFilter === 'inventory' && !(row.positive_rebuild_quality?.observe_reasons || []).includes('inventory_quality_degraded')) return false
        if (coverageFilter === 'covered' && coverage !== '覆盖成本') return false
        if (coverageFilter === 'not_covered' && coverage !== '未覆盖成本') return false
        if (coverageFilter === 'pending' && coverage !== '待评估') return false
        return true
      })
      .sort((a, b) => {
        const pa = replayPriority(a.classification)
        const pb = replayPriority(b.classification)
        if (pa !== pb) return pa - pb
        const ta = new Date(a.triggered_at || '').getTime()
        const tb = new Date(b.triggered_at || '').getTime()
        if (Number.isFinite(ta) && Number.isFinite(tb) && ta !== tb) return tb - ta
        return Number(b.strength || 0) - Number(a.strength || 0)
      })
  }, [rows, symbolKeyword, classificationFilter, gateFilter, coverageFilter])

  useEffect(() => {
    if (rows.length <= 0) {
      setExpanded({})
      return
    }
    const next: Record<string, boolean> = {}
    let autoExpandedCount = 0
    for (const row of rows) {
      if (row.classification === 'false_rebuild' || row.classification === 'observe_missed') {
        next[String(row.id)] = true
        autoExpandedCount += 1
        if (autoExpandedCount >= 12) break
      }
    }
    setExpanded(next)
  }, [rows])

  const filteredSummary = useMemo(() => {
    const out: Record<string, number> = {
      total: filteredRows.length,
      good_rebuild: 0,
      weak_rebuild: 0,
      false_rebuild: 0,
      observe_filtered: 0,
      observe_missed: 0,
      pending: 0,
      pass: 0,
      observe: 0,
      covered: 0,
    }
    for (const row of filteredRows) {
      out[row.classification] = (out[row.classification] || 0) + 1
      if (row.positive_rebuild_quality?.quality_pass) out.pass += 1
      if (row.positive_rebuild_quality?.observe_only) out.observe += 1
      if (coverageLabel(row) === '覆盖成本') out.covered += 1
    }
    return out
  }, [filteredRows])

  const exportCsvFile = useCallback(() => {
    downloadCsv(
      `t0_positive_replay_${hours}h_${filteredRows.length}rows.csv`,
      [
        '触发时间',
        '代码',
        '名称',
        '分类',
        '分类说明',
        '质量门',
        '恢复分',
        '净可执行边际bps',
        '观察原因',
        '恢复确认',
        '继续走弱风险',
        '库存风险',
        '价格',
        '涨跌幅',
        '信号强度',
        '5分钟收益bps',
        '5分钟净收益bps',
        'MFE bps',
        'MAE bps',
        '成本覆盖',
        '市场阶段',
        '底仓建议股数',
        '可做T底仓',
        '现金空间',
        '消息',
      ],
      filteredRows.map(row => [
        row.triggered_at,
        row.ts_code,
        row.name,
        replayLabel(row.classification),
        row.classification_reason,
        row.positive_rebuild_quality?.observe_only ? '仅观察' : (row.positive_rebuild_quality?.quality_pass ? '可执行' : '--'),
        row.positive_rebuild_quality?.recover_after_rebuild_score ?? '',
        row.positive_rebuild_quality?.net_executable_edge_bps ?? '',
        (row.positive_rebuild_quality?.observe_reasons || []).map(reasonLabel).join(' | '),
        (row.positive_rebuild_quality?.recovery_confirms || []).map(reasonLabel).join(' | '),
        (row.positive_rebuild_quality?.continuation_risks || []).map(reasonLabel).join(' | '),
        (row.positive_rebuild_quality?.inventory_warnings || []).map(reasonLabel).join(' | '),
        row.price,
        row.pct_chg,
        row.strength,
        row.quality?.ret_bps ?? '',
        row.quality?.net_ret_bps_after_fee ?? '',
        row.quality?.mfe_bps ?? '',
        row.quality?.mae_bps ?? '',
        coverageLabel(row),
        marketPhaseLabel(row.quality?.market_phase || 'pending'),
        row.t0_inventory?.suggested_action_qty ?? '',
        row.t0_inventory?.tradable_t_qty ?? '',
        row.t0_inventory?.cash_available_for_t ?? '',
        row.message,
      ]),
    )
  }, [filteredRows, hours])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, color: C.text }}>
      <section style={{
        background: C.card,
        border: `1px solid ${C.border}`,
        borderRadius: 16,
        padding: '18px 20px',
        display: 'flex',
        flexDirection: 'column',
        gap: 14,
      }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <div style={{ color: C.bright, fontSize: 24, fontWeight: 800 }}>正T 回放</div>
            <div style={{ color: C.dim, fontSize: 12 }}>
              把回补质量门和 5 分钟结果放到一张表里，专门看“这次回补值不值得做”。
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', color: C.dim, fontSize: 12 }}>
              <span>更新时间: {replay?.checked_at ? fmtDateTime(replay.checked_at) : '--'}</span>
              <span style={{ color: marketStatusColor(market.status, market.is_open) }}>
                交易状态: {marketStatusLabel(market.status, market.desc)}
              </span>
              <span>样本: {replay?.count ?? 0}</span>
              <span>过滤后: {filteredRows.length}</span>
              <span>默认展开: 回补失败 / 观察偏严错过</span>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <button
              onClick={load}
              disabled={loading}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 6,
                background: C.cyanBg,
                color: C.cyan,
                border: `1px solid ${C.cyan}`,
                borderRadius: 10,
                padding: '8px 12px',
                cursor: 'pointer',
              }}
            >
              <RefreshCw size={14} />
              {loading ? '刷新中' : '刷新'}
            </button>
            <button
              onClick={exportCsvFile}
              disabled={filteredRows.length <= 0}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 6,
                background: C.bg,
                color: filteredRows.length > 0 ? C.green : C.dim,
                border: `1px solid ${filteredRows.length > 0 ? C.green : C.border}`,
                borderRadius: 10,
                padding: '8px 12px',
                cursor: filteredRows.length > 0 ? 'pointer' : 'not-allowed',
              }}
            >
              <Download size={14} />
              导出 CSV
            </button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 10 }}>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: C.dim }}>回放窗口</span>
            <select value={hours} onChange={e => setHours(Number(e.target.value))} style={{ background: C.bg, color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '8px 10px' }}>
              <option value={6}>近 6 小时</option>
              <option value={12}>近 12 小时</option>
              <option value={24}>近 24 小时</option>
              <option value={48}>近 48 小时</option>
              <option value={72}>近 72 小时</option>
            </select>
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: C.dim }}>样本上限</span>
            <select value={limit} onChange={e => setLimit(Number(e.target.value))} style={{ background: C.bg, color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '8px 10px' }}>
              <option value={80}>80</option>
              <option value={120}>120</option>
              <option value={200}>200</option>
              <option value={300}>300</option>
            </select>
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: C.dim }}>分类筛选</span>
            <select value={classificationFilter} onChange={e => setClassificationFilter(e.target.value)} style={{ background: C.bg, color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '8px 10px' }}>
              <option value="all">全部分类</option>
              <option value="good_rebuild">回补有效</option>
              <option value="weak_rebuild">边际偏薄</option>
              <option value="false_rebuild">回补失败</option>
              <option value="observe_filtered">观察拦截有效</option>
              <option value="observe_missed">观察偏严错过</option>
              <option value="pending">待评估</option>
            </select>
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: C.dim }}>质量门筛选</span>
            <select value={gateFilter} onChange={e => setGateFilter(e.target.value)} style={{ background: C.bg, color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '8px 10px' }}>
              <option value="all">全部</option>
              <option value="pass">仅可执行</option>
              <option value="observe">仅观察</option>
              <option value="inventory">只看库存拦截</option>
            </select>
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: C.dim }}>成本覆盖</span>
            <select value={coverageFilter} onChange={e => setCoverageFilter(e.target.value)} style={{ background: C.bg, color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '8px 10px' }}>
              <option value="all">全部</option>
              <option value="covered">覆盖成本</option>
              <option value="not_covered">未覆盖成本</option>
              <option value="pending">待评估</option>
            </select>
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: C.dim }}>代码 / 名称</span>
            <input
              value={symbolFilter}
              onChange={e => setSymbolFilter(e.target.value)}
              placeholder="如 300750 / 宁德时代"
              style={{ background: C.bg, color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '8px 10px' }}
            />
          </label>
        </div>
      </section>

      {error && (
        <div style={{
          background: 'rgba(239,68,68,0.08)',
          border: `1px solid rgba(239,68,68,0.35)`,
          color: C.red,
          borderRadius: 12,
          padding: '12px 14px',
          fontSize: 13,
        }}>
          {error}
        </div>
      )}

      <section style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        <SummaryCard label="过滤后样本" value={String(filteredSummary.total)} color={C.bright} />
        <SummaryCard label="回补有效" value={String(filteredSummary.good_rebuild)} color={C.green} />
        <SummaryCard label="回补失败" value={String(filteredSummary.false_rebuild)} color={C.red} />
        <SummaryCard label="观察拦截有效" value={String(filteredSummary.observe_filtered)} color={C.cyan} />
        <SummaryCard label="观察偏严错过" value={String(filteredSummary.observe_missed)} color={C.red} />
        <SummaryCard label="成本覆盖数" value={String(filteredSummary.covered)} color={C.green} />
      </section>

      <section style={{
        background: C.card,
        border: `1px solid ${C.border}`,
        borderRadius: 16,
        padding: '16px 18px',
        display: 'flex',
        flexDirection: 'column',
        gap: 12,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>当前实时诊断</div>
          <div style={{ color: C.dim, fontSize: 12 }}>
            当前诊断时间 {positiveDiag?.checked_at ? fmtDateTime(positiveDiag.checked_at) : '--'}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <KV label="已触发" value={String(currentDiagSummary.triggered ?? 0)} color={C.green} />
          <KV label="仅观察" value={String(currentDiagSummary.observe_only ?? 0)} color={C.yellow} />
          <KV label="veto" value={String(currentDiagSummary.veto ?? 0)} color={C.red} />
          <KV label="未确认" value={String(currentDiagSummary.trigger_no_confirm ?? 0)} color={C.yellow} />
          <KV label="评分/质量门过滤" value={String(currentDiagSummary.trigger_score_filtered ?? 0)} color={C.yellow} />
          <KV label="恢复分不足" value={String(currentDiagSummary.recovery_quality_low ?? 0)} color={C.yellow} />
          <KV label="净边际不足" value={String(currentDiagSummary.net_executable_edge_low ?? 0)} color={C.yellow} />
          <KV label="库存约束" value={String(currentDiagSummary.inventory_degraded ?? 0)} color={C.yellow} />
        </div>
      </section>

      <section style={{
        background: C.card,
        border: `1px solid ${C.border}`,
        borderRadius: 16,
        padding: '16px 18px',
        display: 'flex',
        flexDirection: 'column',
        gap: 14,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>回补质量总览</div>
          <div style={{ color: C.dim, fontSize: 12 }}>
            历史快照样本 {qualitySummary?.sample_count ?? 0} / 5 分钟评估 {qualitySummary?.matched_quality_count ?? 0}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <KV label="可执行率" value={fmtRatePct(qualitySummary?.quality_pass_rate)} color={(qualitySummary?.quality_pass_rate ?? 0) >= 0.5 ? C.green : C.yellow} />
          <KV label="仅观察率" value={fmtRatePct(qualitySummary?.observe_only_rate)} color={C.yellow} />
          <KV label="平均恢复分" value={fmtNum(qualitySummary?.avg_recovery_score, 1)} color={C.bright} />
          <KV label="平均净可执行边际" value={fmtBps(qualitySummary?.avg_net_executable_edge_bps)} color={(qualitySummary?.avg_net_executable_edge_bps ?? 0) > 0 ? C.green : C.yellow} />
          <KV label="平均 5 分钟收益" value={fmtBps(qualitySummary?.avg_ret_bps_5m)} color={(qualitySummary?.avg_ret_bps_5m ?? 0) > 0 ? C.green : C.yellow} />
          <KV label="平均 5 分钟净收益" value={fmtBps(qualitySummary?.avg_net_ret_bps_5m)} color={(qualitySummary?.avg_net_ret_bps_5m ?? 0) > 0 ? C.green : C.yellow} />
          <KV label="方向正确率" value={fmtRatePct(qualitySummary?.direction_correct_rate)} color={(qualitySummary?.direction_correct_rate ?? 0) >= 0.5 ? C.green : C.yellow} />
          <KV label="成本覆盖率" value={fmtRatePct(qualitySummary?.cost_covered_rate)} color={(qualitySummary?.cost_covered_rate ?? 0) >= 0.5 ? C.green : C.yellow} />
          <KV label="平均临时库存" value={fmtRatePct(qualitySummary?.avg_temp_inventory_ratio)} color={C.text} />
          <KV label="平均回补后反T空间" value={fmtRatePct(qualitySummary?.avg_reverse_room_ratio_after)} color={C.text} />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 10 }}>
          <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px' }}>
            <div style={{ color: C.dim, fontSize: 11, marginBottom: 6 }}>观察原因 Top</div>
            <div style={{ color: C.yellow, fontSize: 13 }}>{topReasonText(qualitySummary?.top_observe_reasons)}</div>
          </div>
          <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px' }}>
            <div style={{ color: C.dim, fontSize: 11, marginBottom: 6 }}>恢复确认 Top</div>
            <div style={{ color: C.text, fontSize: 13 }}>{topReasonText(qualitySummary?.top_recovery_confirms)}</div>
          </div>
          <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px' }}>
            <div style={{ color: C.dim, fontSize: 11, marginBottom: 6 }}>继续走弱风险 Top</div>
            <div style={{ color: C.yellow, fontSize: 13 }}>{topReasonText(qualitySummary?.top_continuation_risks)}</div>
          </div>
          <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px' }}>
            <div style={{ color: C.dim, fontSize: 11, marginBottom: 6 }}>库存风险 Top</div>
            <div style={{ color: C.yellow, fontSize: 13 }}>{topReasonText(qualitySummary?.top_inventory_warnings)}</div>
          </div>
        </div>
      </section>

      <section style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {filteredRows.length <= 0 && (
          <div style={{
            background: C.card,
            border: `1px solid ${C.border}`,
            borderRadius: 16,
            padding: '28px 20px',
            textAlign: 'center',
            color: C.dim,
          }}>
            当前筛选条件下没有正T回放样本。
          </div>
        )}

        {filteredRows.map(row => {
          const key = String(row.id)
          const open = !!expanded[key]
          const quality = row.positive_rebuild_quality || null
          const inventory = row.t0_inventory || null
          const currentDiag = positiveDiagByCode.get(row.ts_code) || null
          const compare = buildPositiveReplayCompare(row, currentDiag)
          const diffItems = buildPositiveReplayDiff(row, currentDiag)
          return (
            <article
              key={key}
              style={{
                background: C.card,
                border: `1px solid ${C.border}`,
                borderRadius: 16,
                padding: '14px 16px',
                display: 'flex',
                flexDirection: 'column',
                gap: 12,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    <Link to={`/stock/${row.ts_code}`} style={{ color: C.cyan, textDecoration: 'none', fontFamily: 'monospace', fontSize: 14 }}>
                      {row.ts_code}
                    </Link>
                    <span style={{ color: C.bright, fontSize: 16, fontWeight: 700 }}>{row.name}</span>
                    <span style={{
                      fontSize: 11,
                      color: replayColor(row.classification),
                      border: `1px solid ${replayColor(row.classification)}`,
                      background: 'rgba(255,255,255,0.02)',
                      borderRadius: 999,
                      padding: '2px 8px',
                    }}>
                      {replayLabel(row.classification)}
                    </span>
                    <span style={{
                      fontSize: 11,
                      color: quality?.observe_only ? C.yellow : (quality?.quality_pass ? C.green : C.dim),
                      border: `1px solid ${quality?.observe_only ? C.yellow : (quality?.quality_pass ? C.green : C.border)}`,
                      borderRadius: 999,
                      padding: '2px 8px',
                    }}>
                      {quality?.observe_only ? '仅观察' : (quality?.quality_pass ? '可执行' : '无质门快照')}
                    </span>
                    <span style={{
                      fontSize: 11,
                      color: coverageColor(row),
                      border: `1px solid ${coverageColor(row)}`,
                      borderRadius: 999,
                      padding: '2px 8px',
                    }}>
                      {coverageLabel(row)}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', color: C.dim, fontSize: 12 }}>
                    <span>{fmtDateTime(row.triggered_at)}</span>
                    <span>价格 {fmtNum(row.price)}</span>
                    <span style={{ color: row.pct_chg >= 0 ? C.red : C.green }}>涨跌幅 {fmtPct(row.pct_chg)}</span>
                    <span>强度 {fmtNum(row.strength, 1)}</span>
                    <span>恢复分 {fmtNum(quality?.recover_after_rebuild_score, 0)}</span>
                    <span style={{ color: (quality?.net_executable_edge_bps ?? 0) > 0 ? C.green : C.yellow }}>净边际 {fmtBps(quality?.net_executable_edge_bps)}</span>
                    <span style={{ color: (row.quality?.net_ret_bps_after_fee ?? 0) >= 0 ? C.green : C.yellow }}>5分钟净收益 {fmtBps(row.quality?.net_ret_bps_after_fee)}</span>
                  </div>
                  <div style={{ color: C.text, fontSize: 13 }}>{row.classification_reason}</div>
                  <div style={{ color: compare.color, fontSize: 13, fontWeight: 600 }}>{compare.label}</div>
                  <div style={{ color: C.dim, fontSize: 12, lineHeight: 1.5 }}>{compare.detail}</div>
                  <div style={{ color: C.dim, fontSize: 12, lineHeight: 1.5 }}>{row.message}</div>
                </div>
                <button
                  onClick={() => setExpanded(prev => ({ ...prev, [key]: !prev[key] }))}
                  style={{
                    alignSelf: 'center',
                    background: C.bg,
                    color: C.cyan,
                    border: `1px solid ${C.cyan}`,
                    borderRadius: 8,
                    padding: '6px 12px',
                    cursor: 'pointer',
                    minWidth: 90,
                  }}
                >
                  {open ? '收起详情' : '展开详情'}
                </button>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10 }}>
                <KV label="观察原因" value={reasonListText(quality?.observe_reasons)} color={quality?.observe_only ? C.yellow : C.dim} />
                <KV label="恢复确认" value={reasonListText(quality?.recovery_confirms)} color={C.text} />
                <KV label="走弱风险" value={reasonListText(quality?.continuation_risks)} color={C.yellow} />
                <KV label="库存风险" value={reasonListText(quality?.inventory_warnings)} color={C.yellow} />
                <KV label="建议回补" value={fmtQty(inventory?.suggested_action_qty)} color={C.text} />
                <KV label="回补后反T空间" value={fmtRatePct(quality?.reverse_room_ratio_after)} color={(quality?.reverse_room_ratio_after ?? 0) >= 0.35 ? C.green : C.yellow} />
              </div>

              {open && (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 12 }}>
                  <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 10 }}>
                    <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>当前实时对照</div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                      <span style={{ fontSize: 11, color: compare.color, border: `1px solid ${compare.color}`, borderRadius: 999, padding: '2px 8px' }}>{compare.label}</span>
                      {currentDiag && (
                        <span style={{ fontSize: 11, color: positiveDiagStatusColor(currentDiag.status), border: `1px solid ${positiveDiagStatusColor(currentDiag.status)}`, borderRadius: 999, padding: '2px 8px' }}>
                          {positiveDiagStatusLabel(currentDiag.status)}
                        </span>
                      )}
                    </div>
                    <div style={{ color: C.dim, fontSize: 12, lineHeight: 1.5 }}>{compare.detail}</div>
                    {currentDiag ? (
                      <>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(100px, 1fr))', gap: 10 }}>
                          <KV label="当前价格" value={fmtNum(currentDiag.price)} />
                          <KV label="当前涨跌幅" value={fmtPct(currentDiag.pct_chg)} color={(currentDiag.pct_chg ?? 0) >= 0 ? C.red : C.green} />
                          <KV label="当前强度" value={fmtNum(currentDiag.strength, 1)} color={positiveDiagStatusColor(currentDiag.status)} />
                          <KV label="当前恢复分" value={fmtNum(currentDiag.positive_rebuild_quality?.recover_after_rebuild_score, 0)} />
                          <KV label="当前净边际" value={fmtBps(currentDiag.positive_rebuild_quality?.net_executable_edge_bps)} color={(currentDiag.positive_rebuild_quality?.net_executable_edge_bps ?? 0) > 0 ? C.green : C.yellow} />
                          <KV label="当前观察原因" value={reasonListText(currentDiag.positive_rebuild_quality?.observe_reasons)} color={currentDiag.observe_only ? C.yellow : C.text} />
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                          {diffItems.map((item, idx) => (
                            <div key={`${key}-diff-${idx}`} style={{ color: item.color, fontSize: 12, lineHeight: 1.5 }}>
                              {item.label}: <span style={{ color: C.text }}>{item.detail}</span>
                            </div>
                          ))}
                        </div>
                        <div style={{ color: C.dim, fontSize: 12, lineHeight: 1.5 }}>{currentDiag.message || '当前没有额外信号文案。'}</div>
                      </>
                    ) : (
                      <div style={{ color: C.dim, fontSize: 12 }}>当前没有可对应的实时正T诊断。</div>
                    )}
                  </div>
                  <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 10 }}>
                    <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>回补质量门</div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(100px, 1fr))', gap: 10 }}>
                      <KV label="恢复分阈值" value={fmtNum(quality?.recover_after_rebuild_min_score, 0)} />
                      <KV label="净边际阈值" value={fmtBps(quality?.min_net_executable_edge_bps)} />
                      <KV label="预期边际" value={fmtBps(quality?.expected_edge_bps)} color={(quality?.expected_edge_bps ?? 0) > 0 ? C.green : C.yellow} />
                      <KV label="净可执行边际" value={fmtBps(quality?.net_executable_edge_bps)} color={(quality?.net_executable_edge_bps ?? 0) > 0 ? C.green : C.yellow} />
                      <KV label="手续费" value={fmtBps(quality?.fee_bps)} />
                      <KV label="滑点" value={fmtBps(quality?.slippage_bps)} />
                      <KV label="冲击成本" value={fmtBps(quality?.impact_bps)} />
                      <KV label="库存锚抬升" value={fmtBps(quality?.anchor_cost_raise_bps_est)} color={(quality?.anchor_cost_raise_bps_est ?? 0) > 0 ? C.yellow : C.text} />
                      <KV label="临时库存占比" value={fmtRatePct(quality?.temp_inventory_ratio)} />
                      <KV label="库存状态" value={quality?.inventory_ok ? '库存可接受' : '库存需谨慎'} color={quality?.inventory_ok ? C.green : C.yellow} />
                    </div>
                  </div>

                  <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 10 }}>
                    <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>5 分钟结果</div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(100px, 1fr))', gap: 10 }}>
                      <KV label="触发价" value={fmtNum(row.quality?.trigger_price)} />
                      <KV label="评估价" value={fmtNum(row.quality?.eval_price)} />
                      <KV label="收益" value={fmtBps(row.quality?.ret_bps)} color={(row.quality?.ret_bps ?? 0) >= 0 ? C.green : C.red} />
                      <KV label="净收益" value={fmtBps(row.quality?.net_ret_bps_after_fee)} color={(row.quality?.net_ret_bps_after_fee ?? 0) >= 0 ? C.green : C.red} />
                      <KV label="MFE" value={fmtBps(row.quality?.mfe_bps)} color={C.green} />
                      <KV label="MAE" value={fmtBps(row.quality?.mae_bps)} color={C.red} />
                      <KV label="方向判断" value={row.quality ? (row.quality.direction_correct ? '方向正确' : '方向错误') : '--'} color={row.quality?.direction_correct ? C.green : C.yellow} />
                      <KV label="市场阶段" value={marketPhaseLabel(row.quality?.market_phase || 'pending')} />
                      <KV label="质量匹配误差" value={typeof row.quality?.time_diff_sec === 'number' ? `${row.quality.time_diff_sec.toFixed(1)}s` : '--'} />
                      <KV label="质量记录时间" value={fmtDateTime(row.quality?.created_at)} />
                    </div>
                  </div>

                  <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 10 }}>
                    <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>底仓状态</div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(100px, 1fr))', gap: 10 }}>
                      <KV label="隔夜底仓" value={fmtQty(inventory?.overnight_base_qty)} />
                      <KV label="可做T底仓" value={fmtQty(inventory?.tradable_t_qty)} />
                      <KV label="保留底仓" value={fmtQty(inventory?.reserve_qty)} />
                      <KV label="建议回补股数" value={fmtQty(inventory?.suggested_action_qty)} />
                      <KV label="已应用股数" value={fmtQty(inventory?.applied_action_qty)} />
                      <KV label="现金空间" value={fmtNum(inventory?.cash_available_for_t)} />
                      <KV label="库存锚成本" value={fmtNum(inventory?.inventory_anchor_cost)} />
                      <KV label="底仓状态" value={inventory?.state_ready ? '已就绪' : '未就绪'} color={inventory?.state_ready ? C.green : C.yellow} />
                      <KV label="底仓观察原因" value={reasonLabel(inventory?.observe_reason)} color={inventory?.observe_only ? C.yellow : C.text} />
                      <KV label="底仓阻断原因" value={reasonLabel(inventory?.blocked_reason)} color={inventory?.blocked_reason ? C.yellow : C.dim} />
                    </div>
                  </div>
                </div>
              )}
            </article>
          )
        })}
      </section>
    </div>
  )
}
