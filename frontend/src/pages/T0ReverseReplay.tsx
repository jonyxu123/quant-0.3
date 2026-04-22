import React, { useCallback, useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import { Link } from 'react-router-dom'
import { RefreshCw } from 'lucide-react'

const API = 'http://localhost:8000'

const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', cyanBg: 'rgba(0,200,200,0.08)',
  red: '#ef4444', green: '#22c55e', yellow: '#eab308',
}

interface MarketStatus {
  is_open?: boolean
  status?: string
  desc?: string
}

interface ReverseDiagRow {
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
  main_rally_guard?: boolean
  trend_guard?: Record<string, any>
  trigger_items?: string[]
  confirm_items?: string[]
  bearish_confirm_count?: number
  threshold?: Record<string, any>
  momentum_divergence?: Record<string, any>
  market_structure?: Record<string, any>
  feature_snapshot?: Record<string, any>
}

interface ReverseDiagResp {
  ok: boolean
  checked_at: string
  interesting_only: boolean
  summary: Record<string, number>
  rows_total: number
  count: number
  rows: ReverseDiagRow[]
}

interface ReverseReplayQuality {
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
}

interface ReverseReplayRow {
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
  quality?: ReverseReplayQuality | null
  history_snapshot?: Record<string, any> | null
  classification: string
  classification_reason: string
  fee_bps: number
}

interface ReverseReplayResp {
  ok: boolean
  checked_at: string
  hours: number
  limit: number
  fee_bps: number
  summary: Record<string, number>
  count: number
  rows: ReverseReplayRow[]
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

function fmtNum(v?: number | null, n = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '--'
  return v.toFixed(n)
}

function fmtDateTime(v?: string | null): string {
  if (!v) return '--'
  const dt = new Date(v)
  if (Number.isNaN(dt.getTime())) return String(v)
  return dt.toLocaleString('zh-CN', { hour12: false })
}

function tradeDayText(v?: string | null): string {
  if (!v) return '--'
  if (typeof v === 'string' && v.length >= 10) return v.slice(0, 10)
  const dt = new Date(v)
  if (Number.isNaN(dt.getTime())) return String(v)
  return dt.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' }).replace(/\//g, '-')
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

function diagPriority(status: string): number {
  const priority: Record<string, number> = {
    surge_guard_veto: 1,
    guard_veto: 2,
    trigger_observe_only: 3,
    trigger_no_confirm: 4,
    trigger_score_filtered: 5,
    candidate: 6,
    triggered: 7,
    off_session_skip: 8,
    error: 9,
  }
  return priority[status] ?? 99
}

function statusLabel(status: string): string {
  const m: Record<string, string> = {
    triggered: '已触发',
    trigger_observe_only: '仅观察',
    surge_guard_veto: '强延续保护拦截',
    guard_veto: '主升浪保护拦截',
    trigger_no_confirm: '触发未确认',
    trigger_score_filtered: '评分过滤',
    candidate: '候选',
    off_session_skip: '非交易时段跳过',
    error: '异常',
  }
  return m[status] || status
}

function statusColor(status: string): string {
  if (status === 'triggered') return C.green
  if (status === 'trigger_observe_only') return C.yellow
  if (status === 'surge_guard_veto') return C.red
  if (status === 'guard_veto') return '#fb7185'
  if (status === 'candidate') return C.cyan
  if (status === 'off_session_skip') return C.dim
  if (status === 'error') return C.red
  return C.yellow
}

function replayLabel(c: string): string {
  const m: Record<string, string> = {
    bad_sell: '疑似误卖/漏杀',
    weak_edge: '边际不足',
    good_hit: '有效命中',
    pending: '待评估',
  }
  return m[c] || c
}

function replayColor(c: string): string {
  if (c === 'good_hit') return C.green
  if (c === 'weak_edge') return C.yellow
  if (c === 'bad_sell') return C.red
  return C.dim
}

function marketPhaseLabel(phase: string): string {
  const m: Record<string, string> = {
    open: '开盘',
    morning: '上午',
    afternoon: '下午',
    close: '尾盘',
    off_session: '非交易时段',
    pending: '待评估',
    unknown: '未知',
  }
  return m[phase] || phase || '未知'
}

function calcNetRetBps(row: ReverseReplayRow): number | null {
  const ret = row.quality?.ret_bps
  if (typeof ret !== 'number' || !Number.isFinite(ret)) return null
  return ret - (Number.isFinite(row.fee_bps) ? row.fee_bps : 0)
}

function feeCoverageLabel(row: ReverseReplayRow): string {
  const net = calcNetRetBps(row)
  if (net === null) return '待评估'
  return net >= 0 ? '覆盖成本' : '未覆盖成本'
}

function feeCoverageColor(row: ReverseReplayRow): string {
  const net = calcNetRetBps(row)
  if (net === null) return C.dim
  return net >= 0 ? C.green : C.red
}

function issueRateColor(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return C.dim
  if (v >= 0.30) return C.red
  if (v >= 0.15) return C.yellow
  return C.green
}

function buildReplayCompare(row: ReverseReplayRow, diag?: ReverseDiagRow | null): { label: string; detail: string; color: string } {
  if (!diag) {
    return {
      label: '缺少当前诊断',
      detail: '只有历史回放结果，当前时点没有可对照的保护状态。',
      color: C.dim,
    }
  }
  const status = diag.status
  if (row.classification === 'bad_sell') {
    if (status === 'surge_guard_veto' || status === 'guard_veto') {
      return {
        label: '当前已补上保护',
        detail: `这类历史误卖在当前链路下会被 ${statusLabel(status)} 拦住。`,
        color: C.green,
      }
    }
    if (status === 'triggered') {
      return {
        label: '当前仍会放行',
        detail: '历史上属于误卖/漏杀，但当前链路仍可能继续发出反T，需要继续收紧门槛。',
        color: C.red,
      }
    }
    return {
      label: '当前已降级未放行',
      detail: `当前状态是 ${statusLabel(status)}，虽然未必硬拦截，但已经比当时更谨慎。`,
      color: C.yellow,
    }
  }
  if (row.classification === 'weak_edge') {
    if (status === 'triggered') {
      return {
        label: '当前仍会触发',
        detail: '方向可能没问题，但边际收益仍偏薄，适合继续优化手续费后门槛。',
        color: C.yellow,
      }
    }
    return {
      label: '当前更谨慎',
      detail: `当前状态是 ${statusLabel(status)}，比历史发出时更保守。`,
      color: C.green,
    }
  }
  if (row.classification === 'good_hit') {
    if (status === 'surge_guard_veto' || status === 'guard_veto') {
      return {
        label: '当前可能偏保守',
        detail: `历史上是有效命中，但当前会被 ${statusLabel(status)} 拦住，需要关注是否误杀。`,
        color: C.yellow,
      }
    }
    return {
      label: '当前与历史一致',
      detail: '历史样本有效，当前链路没有明显把它误伤掉。',
      color: C.green,
    }
  }
  return {
    label: '等待质量评估',
    detail: '5分钟质量结果还没齐，暂时只看当前诊断状态。',
    color: C.dim,
  }
}

function buildHistoryCurrentDiff(row: ReverseReplayRow, currentDiag?: ReverseDiagRow | null): Array<{ label: string; detail: string; color: string }> {
  const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
  if (!hs) {
    if (currentDiag) {
      return [{
        label: '历史无快照',
        detail: `这条旧样本触发时还没持久化保护快照，只能参考当前的 ${statusLabel(currentDiag.status)}。`,
        color: C.dim,
      }]
    }
    return [{
      label: '历史无快照',
      detail: '这条旧样本既没有历史保护快照，也没有当前诊断可对照。',
      color: C.dim,
    }]
  }

  const items: Array<{ label: string; detail: string; color: string }> = []
  const hsThreshold = hs.threshold && typeof hs.threshold === 'object' ? hs.threshold : {}
  const hsTrendGuard = hs.trend_guard && typeof hs.trend_guard === 'object' ? hs.trend_guard : {}
  const nowTrendGuard = currentDiag?.trend_guard && typeof currentDiag.trend_guard === 'object' ? currentDiag.trend_guard : {}
  const histObserve = hs.observe_only === true
  const histVeto = String(hs.veto || '')
  const nowVeto = String(currentDiag?.veto || '')
  const histConfirms = Array.isArray(hs.confirm_items) ? hs.confirm_items : []
  const nowConfirms = Array.isArray(currentDiag?.confirm_items) ? (currentDiag?.confirm_items as string[]) : []
  const histThresholdVersion = String(hsThreshold.threshold_version || '')
  const nowThresholdVersion = String((currentDiag?.threshold || {}).threshold_version || '')

  if (!histObserve && currentDiag) {
    if (currentDiag.status === 'surge_guard_veto' || currentDiag.status === 'guard_veto') {
      items.push({
        label: '执行等级收紧',
        detail: `历史快照里不是仅观察，现在这类形态会被 ${statusLabel(currentDiag.status)}。`,
        color: C.green,
      })
    } else if (currentDiag.observe_only) {
      items.push({
        label: '降为仅观察',
        detail: '历史快照里更接近可执行，现在同类形态会先降为仅观察。',
        color: C.green,
      })
    }
  }

  if (!Boolean(hsTrendGuard.active) && Boolean(currentDiag?.main_rally_guard || nowTrendGuard.active)) {
    items.push({
      label: '新增主升浪保护',
      detail: '历史快照里未见趋势保护，当前已经纳入主升浪保护链路。',
      color: C.green,
    })
  }

  if (!Boolean(hsTrendGuard.surge_absorb_guard) && Boolean(currentDiag?.status === 'surge_guard_veto' || nowTrendGuard.surge_absorb_guard)) {
    items.push({
      label: '新增强延续保护',
      detail: '现在对持续放量、强承接、吃压盘的上涨段会更严格拦截反T。',
      color: C.green,
    })
  }

  if (!histVeto && nowVeto) {
    items.push({
      label: '新增 veto',
      detail: `历史快照没有 veto，当前诊断出现了 ${nowVeto}。`,
      color: C.yellow,
    })
  }

  if ((histThresholdVersion || nowThresholdVersion) && histThresholdVersion !== nowThresholdVersion) {
    items.push({
      label: '阈值版本变化',
      detail: `${histThresholdVersion || '--'} -> ${nowThresholdVersion || '--'}`,
      color: C.cyan,
    })
  }

  if (histConfirms.length !== nowConfirms.length) {
    items.push({
      label: '确认结构变化',
      detail: `历史确认 ${histConfirms.length} 项，当前确认 ${nowConfirms.length} 项。`,
      color: C.yellow,
    })
  }

  if (items.length <= 0) {
    items.push({
      label: '差异不明显',
      detail: '历史快照与当前诊断在主要保护链路上没有明显新增变化。',
      color: C.dim,
    })
  }
  return items.slice(0, 4)
}

function summarizeHistoryCurrentDiff(
  row: ReverseReplayRow,
  currentDiag: ReverseDiagRow | null | undefined,
  compare: { label: string; detail: string; color: string },
  diffItems: Array<{ label: string; detail: string; color: string }>,
): { label: string; detail: string; color: string } {
  const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
  const backfillApprox = String((hs && hs.snapshot_mode) || '') === 'backfill'
  const labels = new Set(diffItems.map(x => x.label))
  const hasImprovement =
    labels.has('执行等级收紧') ||
    labels.has('降为仅观察') ||
    labels.has('新增主升浪保护') ||
    labels.has('新增强延续保护')
  const hasPartial =
    labels.has('新增 veto') ||
    labels.has('确认结构变化') ||
    labels.has('阈值版本变化')

  if (compare.label === '当前仍会放行') {
    return {
      label: '当前仍不足',
      detail: `${backfillApprox ? '基于回补快照近似判断，' : ''}这条历史样本在当前链路下仍可能继续发出反T，优先考虑继续收紧门槛。`,
      color: C.red,
    }
  }
  if (compare.label === '当前可能偏保守') {
    return {
      label: '需关注误杀',
      detail: `${backfillApprox ? '基于回补快照近似判断，' : ''}历史上这是有效命中，但当前保护已经更强，后续要关注是否把好样本拦掉了。`,
      color: C.yellow,
    }
  }
  if (labels.has('历史无快照')) {
    return {
      label: '历史待补',
      detail: currentDiag ? '旧样本缺少历史快照，只能基于当前诊断做近似判断。' : '旧样本既缺历史快照，也缺当前诊断，结论可信度有限。',
      color: C.dim,
    }
  }
  if (hasImprovement && (compare.label === '当前已补上保护' || compare.label === '当前更谨慎' || compare.label === '当前已降级未放行')) {
    return {
      label: '当前已补强',
      detail: `${backfillApprox ? '基于回补快照近似判断，' : ''}这条历史问题样本在当前链路下已经出现明确的保护增强，复发概率理论上应下降。`,
      color: C.green,
    }
  }
  if (hasImprovement || hasPartial) {
    return {
      label: '部分补强',
      detail: `${backfillApprox ? '基于回补快照近似判断，' : ''}当前链路相比历史已有部分增强，但还没有强到可以完全放心，需要继续观察。`,
      color: C.yellow,
    }
  }
  if (compare.label === '当前与历史一致') {
    return {
      label: '与历史一致',
      detail: '当前链路和历史样本表现大体一致，没有明显新增保护或明显退化。',
      color: C.cyan,
    }
  }
  return {
    label: '变化有限',
    detail: row.classification === 'pending' ? '样本还在等待质量评估，目前只能看到有限差异。' : '历史与当前的主要保护链路变化不明显，后续可考虑继续增强。',
    color: C.dim,
  }
}

function buildSnapshotCoverageStats(rows: ReverseReplayRow[]) {
  let nativeCount = 0
  let backfillCount = 0
  let missingCount = 0
  for (const row of rows) {
    const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
    if (!hs) {
      missingCount += 1
      continue
    }
    if (String(hs.snapshot_mode || '') === 'backfill') backfillCount += 1
    else nativeCount += 1
  }
  const total = rows.length
  const coveredCount = nativeCount + backfillCount
  const coverageRate = total > 0 ? coveredCount / total : 0
  const nativeRate = total > 0 ? nativeCount / total : 0
  const backfillRate = total > 0 ? backfillCount / total : 0
  const missingRate = total > 0 ? missingCount / total : 0
  return {
    total,
    nativeCount,
    backfillCount,
    missingCount,
    coveredCount,
    coverageRate,
    nativeRate,
    backfillRate,
    missingRate,
  }
}

function CardTitle({ children }: { children: React.ReactNode }) {
  return <div style={{ color: C.bright, fontSize: 13, fontWeight: 700, marginBottom: 8 }}>{children}</div>
}

function StatCard({ label, value, color = C.bright }: { label: string; value: React.ReactNode; color?: string }) {
  return (
    <div style={{
      background: 'rgba(0,0,0,0.2)',
      border: `1px solid ${C.border}`,
      borderRadius: 8,
      padding: '10px 12px',
      minWidth: 120,
    }}>
      <div style={{ color: C.dim, fontSize: 11, marginBottom: 4 }}>{label}</div>
      <div style={{ color, fontSize: 18, fontWeight: 700, fontFamily: 'monospace' }}>{value}</div>
    </div>
  )
}

export default function T0ReverseReplay() {
  const [hours, setHours] = useState(24)
  const [limit, setLimit] = useState(80)
  const [interestingOnly, setInterestingOnly] = useState(true)
  const [symbolFilter, setSymbolFilter] = useState('')
  const [classificationFilter, setClassificationFilter] = useState('all')
  const [phaseFilter, setPhaseFilter] = useState('all')
  const [diagStatusFilter, setDiagStatusFilter] = useState('all')
  const [costCoverageFilter, setCostCoverageFilter] = useState('all')
  const [diffSummaryFilter, setDiffSummaryFilter] = useState('all')
  const [rankingSort, setRankingSort] = useState('bad_sell')
  const [loading, setLoading] = useState(false)
  const [market, setMarket] = useState<MarketStatus>({})
  const [diag, setDiag] = useState<ReverseDiagResp | null>(null)
  const [replay, setReplay] = useState<ReverseReplayResp | null>(null)
  const [error, setError] = useState('')
  const [expandedReplayIds, setExpandedReplayIds] = useState<Record<string, boolean>>({})
  const replaySectionRef = React.useRef<HTMLElement | null>(null)
  const timelineSectionRef = React.useRef<HTMLElement | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [m, d, r] = await Promise.all([
        axios.get(`${API}/api/realtime/market_status`),
        axios.get(`${API}/api/realtime/runtime/t0_reverse_diagnostics`, {
          params: { limit, interesting_only: interestingOnly },
        }),
        axios.get(`${API}/api/realtime/runtime/t0_reverse_replay`, {
          params: { hours, limit },
        }),
      ])
      setMarket((m.data || {}) as MarketStatus)
      setDiag((d.data || null) as ReverseDiagResp | null)
      setReplay((r.data || null) as ReverseReplayResp | null)
    } catch (e: any) {
      setError(String(e?.message || e || '加载失败'))
    } finally {
      setLoading(false)
    }
  }, [hours, limit, interestingOnly])

  useEffect(() => {
    load()
  }, [load])

  const symbolKeyword = symbolFilter.trim().toUpperCase()
  const diagRows = diag?.rows || []
  const replayRows = replay?.rows || []

  const diagByCode = useMemo(() => {
    const map = new Map<string, ReverseDiagRow>()
    for (const row of diagRows) {
      const prev = map.get(row.ts_code)
      if (!prev) {
        map.set(row.ts_code, row)
        continue
      }
      const prevPriority = diagPriority(prev.status)
      const nextPriority = diagPriority(row.status)
      if (nextPriority < prevPriority || (nextPriority === prevPriority && (row.strength || 0) > (prev.strength || 0))) {
        map.set(row.ts_code, row)
      }
    }
    return map
  }, [diagRows])

  const diagPhaseOptions = useMemo(() => {
    const set = new Set<string>()
    for (const row of diagRows) {
      const phase = String((row.threshold || {}).market_phase || (row.trend_guard || {}).market_phase || 'unknown')
      if (phase) set.add(phase)
    }
    return Array.from(set)
  }, [diagRows])

  const replayPhaseOptions = useMemo(() => {
    const set = new Set<string>()
    for (const row of replayRows) {
      const phase = String(row.quality?.market_phase || 'pending')
      if (phase) set.add(phase)
    }
    return Array.from(set)
  }, [replayRows])

  const phaseOptions = useMemo(() => {
    const set = new Set<string>([...diagPhaseOptions, ...replayPhaseOptions])
    return Array.from(set)
  }, [diagPhaseOptions, replayPhaseOptions])

  const filteredDiagRows = useMemo(() => {
    return diagRows.filter(row => {
      const rowText = `${row.ts_code} ${row.name}`.toUpperCase()
      const rowPhase = String((row.threshold || {}).market_phase || (row.trend_guard || {}).market_phase || 'unknown')
      if (symbolKeyword && !rowText.includes(symbolKeyword)) return false
      if (diagStatusFilter !== 'all' && row.status !== diagStatusFilter) return false
      if (phaseFilter !== 'all' && rowPhase !== phaseFilter) return false
      return true
    })
  }, [diagRows, symbolKeyword, diagStatusFilter, phaseFilter])

  const filteredReplayRows = useMemo(() => {
    return replayRows.filter(row => {
      const rowText = `${row.ts_code} ${row.name}`.toUpperCase()
      const rowPhase = String(row.quality?.market_phase || 'pending')
      const diag = diagByCode.get(row.ts_code)
      const compare = buildReplayCompare(row, diag)
      const diffItems = buildHistoryCurrentDiff(row, diag)
      const diffSummary = summarizeHistoryCurrentDiff(row, diag, compare, diffItems)
      if (symbolKeyword && !rowText.includes(symbolKeyword)) return false
      if (classificationFilter !== 'all' && row.classification !== classificationFilter) return false
      if (phaseFilter !== 'all' && rowPhase !== phaseFilter) return false
      if (costCoverageFilter === 'covered' && feeCoverageLabel(row) !== '覆盖成本') return false
      if (costCoverageFilter === 'not_covered' && feeCoverageLabel(row) !== '未覆盖成本') return false
      if (diffSummaryFilter === 'strengthened' && diffSummary.label !== '当前已补强') return false
      if (diffSummaryFilter === 'insufficient' && diffSummary.label !== '当前仍不足') return false
      if (diffSummaryFilter === 'misblock' && diffSummary.label !== '需关注误杀') return false
      if (diffSummaryFilter === 'pending' && diffSummary.label !== '历史待补') return false
      if (diffSummaryFilter === 'partial' && diffSummary.label !== '部分补强') return false
      return true
    })
  }, [replayRows, symbolKeyword, classificationFilter, phaseFilter, costCoverageFilter, diffSummaryFilter, diagByCode])

  const replaySnapshotStats = useMemo(() => buildSnapshotCoverageStats(replayRows), [replayRows])
  const filteredReplaySnapshotStats = useMemo(() => buildSnapshotCoverageStats(filteredReplayRows), [filteredReplayRows])

  const replayBuckets = useMemo(() => {
    const out: Record<string, ReverseReplayRow[]> = {
      bad_sell: [],
      weak_edge: [],
      good_hit: [],
      pending: [],
    }
    for (const row of filteredReplayRows) {
      const key = out[row.classification] ? row.classification : 'pending'
      out[key].push(row)
    }
    return out
  }, [filteredReplayRows])

  const diffSummaryBuckets = useMemo(() => {
    const out: Record<string, number> = {
      当前已补强: 0,
      当前仍不足: 0,
      需关注误杀: 0,
      历史待补: 0,
      部分补强: 0,
      与历史一致: 0,
      变化有限: 0,
    }
    for (const row of filteredReplayRows) {
      const diag = diagByCode.get(row.ts_code)
      const compare = buildReplayCompare(row, diag)
      const diffItems = buildHistoryCurrentDiff(row, diag)
      const diffSummary = summarizeHistoryCurrentDiff(row, diag, compare, diffItems)
      out[diffSummary.label] = (out[diffSummary.label] || 0) + 1
    }
    return out
  }, [filteredReplayRows, diagByCode])

  const problemStockBase = useMemo(() => {
    const map = new Map<string, {
      ts_code: string
      name: string
      bad_sell_count: number
      weak_edge_count: number
      not_covered_count: number
      total_count: number
      avg_net_ret_bps: number | null
      diag_status: string
      compare_label: string
    }>()
    for (const row of filteredReplayRows) {
      const key = row.ts_code
      if (!key) continue
      const prev = map.get(key)
      const netRet = calcNetRetBps(row)
      const diag = diagByCode.get(key)
      const compare = buildReplayCompare(row, diag)
      if (!prev) {
        map.set(key, {
          ts_code: row.ts_code,
          name: row.name,
          bad_sell_count: row.classification === 'bad_sell' ? 1 : 0,
          weak_edge_count: row.classification === 'weak_edge' ? 1 : 0,
          not_covered_count: feeCoverageLabel(row) === '未覆盖成本' ? 1 : 0,
          total_count: 1,
          avg_net_ret_bps: netRet,
          diag_status: diag?.status || '',
          compare_label: compare.label,
        })
        continue
      }
      prev.bad_sell_count += row.classification === 'bad_sell' ? 1 : 0
      prev.weak_edge_count += row.classification === 'weak_edge' ? 1 : 0
      prev.not_covered_count += feeCoverageLabel(row) === '未覆盖成本' ? 1 : 0
      prev.total_count += 1
      if (netRet !== null) {
        prev.avg_net_ret_bps = prev.avg_net_ret_bps === null ? netRet : ((prev.avg_net_ret_bps * (prev.total_count - 1)) + netRet) / prev.total_count
      }
      if (!prev.diag_status && diag?.status) prev.diag_status = diag.status
      if (prev.compare_label === '缺少当前诊断' && compare.label) prev.compare_label = compare.label
    }
    return Array.from(map.values())
  }, [filteredReplayRows, diagByCode])

  const problemStockRanking = useMemo(() => {
    const rows = [...problemStockBase]
    rows.sort((a, b) => {
      if (rankingSort === 'not_covered') {
        if (b.not_covered_count !== a.not_covered_count) return b.not_covered_count - a.not_covered_count
        if (b.bad_sell_count !== a.bad_sell_count) return b.bad_sell_count - a.bad_sell_count
        if (b.weak_edge_count !== a.weak_edge_count) return b.weak_edge_count - a.weak_edge_count
      } else if (rankingSort === 'weak_edge') {
        if (b.weak_edge_count !== a.weak_edge_count) return b.weak_edge_count - a.weak_edge_count
        if (b.not_covered_count !== a.not_covered_count) return b.not_covered_count - a.not_covered_count
        if (b.bad_sell_count !== a.bad_sell_count) return b.bad_sell_count - a.bad_sell_count
      } else if (rankingSort === 'net_ret') {
        const an = a.avg_net_ret_bps ?? Number.POSITIVE_INFINITY
        const bn = b.avg_net_ret_bps ?? Number.POSITIVE_INFINITY
        if (an !== bn) return an - bn
        if (b.not_covered_count !== a.not_covered_count) return b.not_covered_count - a.not_covered_count
        if (b.bad_sell_count !== a.bad_sell_count) return b.bad_sell_count - a.bad_sell_count
      } else {
        if (b.bad_sell_count !== a.bad_sell_count) return b.bad_sell_count - a.bad_sell_count
        if (b.not_covered_count !== a.not_covered_count) return b.not_covered_count - a.not_covered_count
        if (b.weak_edge_count !== a.weak_edge_count) return b.weak_edge_count - a.weak_edge_count
      }
      return b.total_count - a.total_count
    })
    return rows.slice(0, 12)
  }, [problemStockBase, rankingSort])

  const dailyTrendRows = useMemo(() => {
    const map = new Map<string, {
      trade_day: string
      total_count: number
      bad_sell_count: number
      weak_edge_count: number
      not_covered_count: number
      good_hit_count: number
      net_ret_sum_bps: number
      net_ret_count: number
    }>()
    for (const row of filteredReplayRows) {
      const key = tradeDayText(row.triggered_at)
      const prev = map.get(key)
      const netRet = calcNetRetBps(row)
      if (!prev) {
        map.set(key, {
          trade_day: key,
          total_count: 1,
          bad_sell_count: row.classification === 'bad_sell' ? 1 : 0,
          weak_edge_count: row.classification === 'weak_edge' ? 1 : 0,
          not_covered_count: feeCoverageLabel(row) === '未覆盖成本' ? 1 : 0,
          good_hit_count: row.classification === 'good_hit' ? 1 : 0,
          net_ret_sum_bps: netRet ?? 0,
          net_ret_count: netRet === null ? 0 : 1,
        })
        continue
      }
      prev.total_count += 1
      prev.bad_sell_count += row.classification === 'bad_sell' ? 1 : 0
      prev.weak_edge_count += row.classification === 'weak_edge' ? 1 : 0
      prev.not_covered_count += feeCoverageLabel(row) === '未覆盖成本' ? 1 : 0
      prev.good_hit_count += row.classification === 'good_hit' ? 1 : 0
      if (netRet !== null) {
        prev.net_ret_sum_bps += netRet
        prev.net_ret_count += 1
      }
    }

    const baseRows = Array.from(map.values())
      .sort((a, b) => String(a.trade_day).localeCompare(String(b.trade_day)))
      .map(row => ({
        ...row,
        avg_net_ret_bps: row.net_ret_count > 0 ? row.net_ret_sum_bps / row.net_ret_count : null,
        bad_sell_rate: row.total_count > 0 ? row.bad_sell_count / row.total_count : null,
        not_covered_rate: row.total_count > 0 ? row.not_covered_count / row.total_count : null,
        weak_edge_rate: row.total_count > 0 ? row.weak_edge_count / row.total_count : null,
        good_hit_rate: row.total_count > 0 ? row.good_hit_count / row.total_count : null,
      }))

    const withRolling = baseRows.map((row, idx) => {
      const roll = (days: number) => {
        const windowRows = baseRows.slice(Math.max(0, idx - days + 1), idx + 1)
        const total = windowRows.reduce((s, x) => s + x.total_count, 0)
        const bad = windowRows.reduce((s, x) => s + x.bad_sell_count, 0)
        const notCovered = windowRows.reduce((s, x) => s + x.not_covered_count, 0)
        const netSum = windowRows.reduce((s, x) => s + x.net_ret_sum_bps, 0)
        const netCount = windowRows.reduce((s, x) => s + x.net_ret_count, 0)
        return {
          total,
          bad_sell_rate: total > 0 ? bad / total : null,
          not_covered_rate: total > 0 ? notCovered / total : null,
          avg_net_ret_bps: netCount > 0 ? netSum / netCount : null,
        }
      }
      const roll5 = roll(5)
      const roll10 = roll(10)
      return {
        ...row,
        roll5_bad_sell_rate: roll5.bad_sell_rate,
        roll5_not_covered_rate: roll5.not_covered_rate,
        roll5_avg_net_ret_bps: roll5.avg_net_ret_bps,
        roll10_bad_sell_rate: roll10.bad_sell_rate,
        roll10_not_covered_rate: roll10.not_covered_rate,
        roll10_avg_net_ret_bps: roll10.avg_net_ret_bps,
      }
    })

    return withRolling
      .sort((a, b) => String(b.trade_day).localeCompare(String(a.trade_day)))
      .slice(0, 12)
  }, [filteredReplayRows])

  const timelineFocusCode = useMemo(() => {
    const codes = Array.from(new Set(filteredReplayRows.map(row => row.ts_code).filter(Boolean)))
    if (codes.length === 1) return codes[0]
    if (symbolKeyword) {
      const exact = filteredReplayRows.find(row => String(row.ts_code || '').toUpperCase() === symbolKeyword)
      if (exact) return exact.ts_code
    }
    return ''
  }, [filteredReplayRows, symbolKeyword])

  const timelineRows = useMemo(() => {
    if (!timelineFocusCode) return []
    return filteredReplayRows
      .filter(row => row.ts_code === timelineFocusCode)
      .sort((a, b) => new Date(b.triggered_at || '').getTime() - new Date(a.triggered_at || '').getTime())
      .slice(0, 16)
  }, [filteredReplayRows, timelineFocusCode])

  const timelineMeta = useMemo(() => {
    if (!timelineFocusCode || timelineRows.length <= 0) return null
    const first = timelineRows[0]
    const diag = diagByCode.get(timelineFocusCode)
    return {
      ts_code: timelineFocusCode,
      name: first?.name || '',
      sample_count: timelineRows.length,
      diag_status: diag?.status || '',
      compare_label: buildReplayCompare(first, diag).label,
    }
  }, [diagByCode, timelineFocusCode, timelineRows])

  const clearFilters = () => {
    setSymbolFilter('')
    setClassificationFilter('all')
    setPhaseFilter('all')
    setDiagStatusFilter('all')
    setCostCoverageFilter('all')
    setDiffSummaryFilter('all')
  }

  const exportReplayCsv = useCallback(() => {
    const rows = filteredReplayRows.map(row => {
      const q = row.quality || null
      const rel = diagByCode.get(row.ts_code)
      const hs = row.history_snapshot || null
      const netRetBps = calcNetRetBps(row)
      const compare = buildReplayCompare(row, rel)
      return [
        row.ts_code,
        row.name,
        row.signal_type,
        row.channel,
        row.signal_source,
        row.classification,
        replayLabel(row.classification),
        row.classification_reason,
        fmtDateTime(row.triggered_at),
        q?.trigger_time_iso || '',
        q?.market_phase || '',
        row.strength,
        row.price,
        row.pct_chg,
        q?.ret_bps ?? '',
        netRetBps ?? '',
        feeCoverageLabel(row),
        q?.mfe_bps ?? '',
        q?.mae_bps ?? '',
        q ? (q.direction_correct ? '方向正确' : '方向错误') : '待评估',
        hs?.score_status || '',
        hs?.observe_only === true ? 'true' : hs?.observe_only === false ? 'false' : '',
        Array.isArray(hs?.trigger_items) ? hs.trigger_items.join(' | ') : '',
        Array.isArray(hs?.confirm_items) ? hs.confirm_items.join(' | ') : '',
        hs?.veto || '',
        rel?.status || '',
        rel ? statusLabel(rel.status) : '',
        rel?.veto || '',
        Array.isArray(rel?.trigger_items) ? rel?.trigger_items?.join(' | ') : '',
        Array.isArray(rel?.confirm_items) ? rel?.confirm_items?.join(' | ') : '',
        Array.isArray(rel?.trend_guard?.guard_reasons) ? rel?.trend_guard?.guard_reasons?.join(' | ') : '',
        Array.isArray(rel?.trend_guard?.bearish_confirms) ? rel?.trend_guard?.bearish_confirms?.join(' | ') : '',
        compare.label,
        compare.detail,
        row.message || '',
      ]
    })
    downloadCsv(
      `t0_reverse_replay_${hours}h_${filteredReplayRows.length}rows.csv`,
      [
        'ts_code', 'name', 'signal_type', 'channel', 'signal_source', 'classification_code', 'classification_label',
        'classification_reason', 'triggered_at', 'quality_trigger_time_iso', 'market_phase', 'strength', 'price',
        'pct_chg', 'ret_bps', 'net_ret_after_fee_bps', 'fee_coverage', 'mfe_bps', 'mae_bps', 'direction_result',
        'history_score_status', 'history_observe_only', 'history_trigger_items', 'history_confirm_items', 'history_veto',
        'current_diag_status', 'current_diag_status_label',
        'current_diag_veto', 'current_trigger_items', 'current_confirm_items', 'current_guard_reasons',
        'current_bearish_confirms', 'compare_label', 'compare_detail', 'message',
      ],
      rows,
    )
  }, [diagByCode, filteredReplayRows, hours])

  const exportDiagCsv = useCallback(() => {
    const rows = filteredDiagRows.map(row => {
      const tg = row.trend_guard || {}
      const ms = row.market_structure || {}
      return [
        row.ts_code,
        row.name,
        row.status,
        statusLabel(row.status),
        row.veto || '',
        row.price,
        row.pct_chg,
        row.strength,
        Boolean(row.main_rally_guard),
        Array.isArray(row.trigger_items) ? row.trigger_items.join(' | ') : '',
        Array.isArray(row.confirm_items) ? row.confirm_items.join(' | ') : '',
        tg.guard_score ?? '',
        Array.isArray(tg.guard_reasons) ? tg.guard_reasons.join(' | ') : '',
        Array.isArray(tg.structure_reasons) ? tg.structure_reasons.join(' | ') : '',
        Array.isArray(tg.negative_flags) ? tg.negative_flags.join(' | ') : '',
        Array.isArray(tg.bearish_confirms) ? tg.bearish_confirms.join(' | ') : '',
        ms.bid_ask_ratio ?? '',
        Boolean(ms.ask_wall_absorbed),
        Boolean(ms.bid_wall_broken),
        Boolean(ms.active_buy_fading),
        row.message || '',
      ]
    })
    downloadCsv(
      `t0_reverse_diag_${filteredDiagRows.length}rows.csv`,
      [
        'ts_code', 'name', 'status_code', 'status_label', 'veto', 'price', 'pct_chg', 'strength',
        'main_rally_guard', 'trigger_items', 'confirm_items', 'guard_score', 'guard_reasons',
        'structure_reasons', 'negative_flags', 'bearish_confirms', 'bid_ask_ratio',
        'ask_wall_absorbed', 'bid_wall_broken', 'active_buy_fading', 'message',
      ],
      rows,
    )
  }, [filteredDiagRows])

  const toggleReplayExpand = useCallback((id: string, defaultExpanded: boolean) => {
    setExpandedReplayIds(prev => {
      const current = Object.prototype.hasOwnProperty.call(prev, id) ? prev[id] : defaultExpanded
      return { ...prev, [id]: !current }
    })
  }, [])

  const jumpToReplaySamples = useCallback((tsCode: string) => {
    setSymbolFilter(tsCode)
    setClassificationFilter('all')
    setPhaseFilter('all')
    setCostCoverageFilter('all')
    window.setTimeout(() => {
      replaySectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
  }, [])

  const jumpToStockTimeline = useCallback((tsCode: string) => {
    setSymbolFilter(tsCode)
    setClassificationFilter('all')
    setPhaseFilter('all')
    setCostCoverageFilter('all')
    window.setTimeout(() => {
      timelineSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
  }, [])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, color: C.text }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 12,
        flexWrap: 'wrap',
      }}>
        <div>
          <div style={{ color: C.bright, fontSize: 20, fontWeight: 700 }}>反T误杀/漏杀回放</div>
          <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
            当前保护拦截样本 + 历史 5 分钟质量回放，专门用来复盘 `reverse_t`
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <select value={hours} onChange={e => setHours(Number(e.target.value))} style={selectStyle}>
            {[6, 12, 24, 48, 72].map(v => <option key={v} value={v}>{v}h</option>)}
          </select>
          <select value={limit} onChange={e => setLimit(Number(e.target.value))} style={selectStyle}>
            {[40, 80, 120, 200].map(v => <option key={v} value={v}>{v}条</option>)}
          </select>
          <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: C.dim }}>
            <input type="checkbox" checked={interestingOnly} onChange={e => setInterestingOnly(e.target.checked)} />
            仅看非空样本
          </label>
          <button onClick={load} style={buttonStyle}>
            <RefreshCw size={14} />
            刷新
          </button>
          <button onClick={exportReplayCsv} style={ghostButtonStyle}>导出回放 CSV</button>
          <button onClick={exportDiagCsv} style={ghostButtonStyle}>导出诊断 CSV</button>
        </div>
      </div>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        <StatCard label="市场状态" value={market.is_open ? '盘中' : (market.desc || '收盘后')} color={market.is_open ? C.green : C.yellow} />
        <StatCard label="保护拦截" value={(diag?.summary?.guard_veto || 0) + (diag?.summary?.surge_guard_veto || 0)} color={C.red} />
        <StatCard label="强延续拦截" value={diag?.summary?.surge_guard_veto || 0} color="#fb7185" />
        <StatCard label="主升浪保护" value={diag?.summary?.main_rally_guard || 0} color={C.yellow} />
        <StatCard label="疑似误卖/漏杀" value={replay?.summary?.bad_sell || 0} color={C.red} />
        <StatCard label="边际不足" value={replay?.summary?.weak_edge || 0} color={C.yellow} />
        <StatCard label="有效命中" value={replay?.summary?.good_hit || 0} color={C.green} />
        <StatCard label="快照覆盖率" value={fmtRatePct(replaySnapshotStats.coverageRate)} color={replaySnapshotStats.coverageRate >= 0.8 ? C.green : replaySnapshotStats.coverageRate >= 0.5 ? C.yellow : C.red} />
        <StatCard label="原生持久化" value={replaySnapshotStats.nativeCount} color={C.green} />
        <StatCard label="回补近似" value={replaySnapshotStats.backfillCount} color={C.yellow} />
        <StatCard label="仍缺快照" value={replaySnapshotStats.missingCount} color={replaySnapshotStats.missingCount > 0 ? C.red : C.green} />
      </div>

      <section style={panelStyle}>
        <CardTitle>筛选器</CardTitle>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            value={symbolFilter}
            onChange={e => setSymbolFilter(e.target.value)}
            placeholder="筛股票代码/名称"
            style={{ ...inputStyle, minWidth: 180 }}
          />
          <select value={classificationFilter} onChange={e => setClassificationFilter(e.target.value)} style={selectStyle}>
            <option value="all">全部回放分类</option>
            <option value="bad_sell">疑似误卖/漏杀</option>
            <option value="weak_edge">边际不足</option>
            <option value="good_hit">有效命中</option>
            <option value="pending">待评估</option>
          </select>
          <select value={diagStatusFilter} onChange={e => setDiagStatusFilter(e.target.value)} style={selectStyle}>
            <option value="all">全部诊断状态</option>
            <option value="surge_guard_veto">强延续拦截</option>
            <option value="guard_veto">主升浪拦截</option>
            <option value="triggered">已触发</option>
            <option value="trigger_observe_only">仅观察</option>
            <option value="trigger_no_confirm">触发未确认</option>
            <option value="trigger_score_filtered">评分过滤</option>
            <option value="candidate">候选</option>
          </select>
          <select value={phaseFilter} onChange={e => setPhaseFilter(e.target.value)} style={selectStyle}>
            <option value="all">全部交易时段</option>
            {phaseOptions.map(phase => (
              <option key={phase} value={phase}>{marketPhaseLabel(phase)}</option>
            ))}
          </select>
          <select value={costCoverageFilter} onChange={e => setCostCoverageFilter(e.target.value)} style={selectStyle}>
            <option value="all">全部成本覆盖</option>
            <option value="not_covered">只看未覆盖成本</option>
            <option value="covered">只看覆盖成本</option>
          </select>
          <select value={diffSummaryFilter} onChange={e => setDiffSummaryFilter(e.target.value)} style={selectStyle}>
            <option value="all">全部补强结论</option>
            <option value="insufficient">只看当前仍不足</option>
            <option value="strengthened">只看当前已补强</option>
            <option value="partial">只看部分补强</option>
            <option value="misblock">只看需关注误杀</option>
            <option value="pending">只看历史待补</option>
          </select>
          <button onClick={clearFilters} style={ghostButtonStyle}>清空筛选</button>
          <div style={{ color: C.dim, fontSize: 12 }}>
            当前筛选: 诊断 {filteredDiagRows.length} 条 / 回放 {filteredReplayRows.length} 条
          </div>
        </div>
      </section>

      {error && (
        <div style={{
          background: 'rgba(239,68,68,0.12)',
          border: `1px solid rgba(239,68,68,0.45)`,
          color: '#fecaca',
          borderRadius: 8,
          padding: '10px 12px',
          fontSize: 12,
        }}>
          {error}
        </div>
      )}

      <section style={panelStyle}>
        <CardTitle>当前保护拦截样本</CardTitle>
        <div style={{ color: C.dim, fontSize: 11, marginBottom: 10 }}>
          这里看的是当前时刻 `reverse_t` 为什么被拦、为什么只到候选、为什么仍然触发。
        </div>
        {loading && !diag ? (
          <div style={{ color: C.dim, fontSize: 12 }}>加载中...</div>
        ) : filteredDiagRows.length <= 0 ? (
          <div style={{ color: C.dim, fontSize: 12 }}>当前没有可展示的保护/候选样本。</div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: 10 }}>
            {filteredDiagRows.map(row => {
              const tg = row.trend_guard || {}
              const ms = row.market_structure || {}
              const rowPhase = String((row.threshold || {}).market_phase || tg.market_phase || 'unknown')
              return (
                <div key={`${row.ts_code}-${row.status}`} style={subPanelStyle}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                    <Link to={`/stock/${row.ts_code}`} style={{ color: C.cyan, textDecoration: 'none', fontFamily: 'monospace', fontSize: 12 }}>
                      {row.ts_code}
                    </Link>
                    <span style={{ color: C.bright, fontWeight: 600 }}>{row.name || '--'}</span>
                    <span style={{
                      marginLeft: 'auto',
                      background: 'rgba(0,0,0,0.25)',
                      border: `1px solid ${statusColor(row.status)}`,
                      color: statusColor(row.status),
                      borderRadius: 999,
                      padding: '2px 8px',
                      fontSize: 10,
                      fontWeight: 700,
                    }}>
                      {statusLabel(row.status)}
                    </span>
                  </div>
                  <div style={actionRowStyle}>
                    <button onClick={() => setSymbolFilter(row.ts_code)} style={miniButtonStyle}>只看这只票</button>
                    <Link to={`/stock/${row.ts_code}`} target="_blank" rel="noreferrer" style={miniLinkStyle}>看K线</Link>
                    <span style={{ marginLeft: 'auto', color: C.dim, fontSize: 11 }}>时段 {marketPhaseLabel(rowPhase)}</span>
                  </div>
                  <div style={kvGridStyle}>
                    <div>价格 <span style={valueStyle}>{row.price > 0 ? row.price.toFixed(2) : '--'}</span></div>
                    <div>涨跌幅 <span style={{ ...valueStyle, color: row.pct_chg >= 0 ? C.red : C.green }}>{fmtPct(row.pct_chg)}</span></div>
                    <div>强度 <span style={valueStyle}>{fmtNum(row.strength, 0)}</span></div>
                    <div>主升浪保护 <span style={{ ...valueStyle, color: row.main_rally_guard ? C.yellow : C.dim }}>{String(Boolean(row.main_rally_guard))}</span></div>
                  </div>
                  <div style={lineStyle}>触发项: <span style={valueStyle}>{(row.trigger_items || []).join(', ') || '--'}</span></div>
                  <div style={lineStyle}>确认项: <span style={valueStyle}>{(row.confirm_items || []).join(', ') || '--'}</span></div>
                  <div style={lineStyle}>保护原因: <span style={{ ...valueStyle, color: statusColor(row.status) }}>{row.veto || String(tg.reason || '--')}</span></div>
                  <div style={lineStyle}>guard_score: <span style={valueStyle}>{String(tg.guard_score ?? '--')}</span></div>
                  <div style={lineStyle}>guard_reasons: <span style={valueStyle}>{Array.isArray(tg.guard_reasons) && tg.guard_reasons.length > 0 ? tg.guard_reasons.join(', ') : '--'}</span></div>
                  <div style={lineStyle}>bearish_confirms: <span style={valueStyle}>{Array.isArray(tg.bearish_confirms) && tg.bearish_confirms.length > 0 ? tg.bearish_confirms.join(', ') : '--'}</span></div>
                  <div style={lineStyle}>盘口结构: <span style={valueStyle}>
                    {[
                      typeof ms.bid_ask_ratio === 'number' ? `买卖比 ${fmtNum(ms.bid_ask_ratio, 2)}` : '',
                      ms.ask_wall_absorbed ? '卖墙被吃' : '',
                      ms.bid_wall_broken ? '托盘击穿' : '',
                      ms.active_buy_fading ? '买入力衰减' : '',
                    ].filter(Boolean).join(' · ') || '--'}
                  </span></div>
                  {row.message && <div style={{ ...lineStyle, marginTop: 6 }}>消息: <span style={valueStyle}>{row.message}</span></div>}
                </div>
              )
            })}
          </div>
        )}
      </section>

      <section ref={replaySectionRef} style={panelStyle}>
        <CardTitle>历史已触发样本回放</CardTitle>
        <div style={{ color: C.dim, fontSize: 11, marginBottom: 10 }}>
          `疑似误卖/漏杀` = 反T卖出后 5 分钟价格继续上行；`边际不足` = 方向虽对但收益不足以覆盖成本。
          对于 `bad_sell` 样本，页面会自动展开当前保护诊断快照，方便直接比对“当下为何还没拦住”。
        </div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
          <div style={{
            background: 'rgba(0,0,0,0.18)',
            border: `1px solid ${C.border}`,
            borderRadius: 999,
            padding: '4px 10px',
            fontSize: 11,
            color: filteredReplaySnapshotStats.coverageRate >= 0.8 ? C.green : filteredReplaySnapshotStats.coverageRate >= 0.5 ? C.yellow : C.red,
          }}>
            当前筛选快照覆盖 {fmtRatePct(filteredReplaySnapshotStats.coverageRate)}
          </div>
          <div style={{
            background: 'rgba(0,0,0,0.18)',
            border: `1px solid ${C.border}`,
            borderRadius: 999,
            padding: '4px 10px',
            fontSize: 11,
            color: C.green,
          }}>
            原生持久化 {filteredReplaySnapshotStats.nativeCount} ({fmtRatePct(filteredReplaySnapshotStats.nativeRate)})
          </div>
          <div style={{
            background: 'rgba(0,0,0,0.18)',
            border: `1px solid ${C.border}`,
            borderRadius: 999,
            padding: '4px 10px',
            fontSize: 11,
            color: C.yellow,
          }}>
            回补近似 {filteredReplaySnapshotStats.backfillCount} ({fmtRatePct(filteredReplaySnapshotStats.backfillRate)})
          </div>
          <div style={{
            background: 'rgba(0,0,0,0.18)',
            border: `1px solid ${C.border}`,
            borderRadius: 999,
            padding: '4px 10px',
            fontSize: 11,
            color: filteredReplaySnapshotStats.missingCount > 0 ? C.red : C.green,
          }}>
            仍缺快照 {filteredReplaySnapshotStats.missingCount} ({fmtRatePct(filteredReplaySnapshotStats.missingRate)})
          </div>
        </div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
          {Object.entries(replayBuckets).map(([k, rows]) => (
            <div key={k} style={{
              background: 'rgba(0,0,0,0.18)',
              border: `1px solid ${C.border}`,
              borderRadius: 999,
              padding: '4px 10px',
              fontSize: 11,
              color: replayColor(k),
            }}>
              {replayLabel(k)} {rows.length}
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
          {Object.entries(diffSummaryBuckets)
            .filter(([, count]) => count > 0)
            .map(([label, count]) => {
              const color =
                label === '当前已补强' ? C.green :
                label === '当前仍不足' ? C.red :
                label === '需关注误杀' ? C.yellow :
                label === '部分补强' ? '#f59e0b' :
                label === '与历史一致' ? C.cyan :
                C.dim
              return (
                <button
                  key={label}
                  onClick={() => {
                    if (label === '当前已补强') setDiffSummaryFilter('strengthened')
                    else if (label === '当前仍不足') setDiffSummaryFilter('insufficient')
                    else if (label === '需关注误杀') setDiffSummaryFilter('misblock')
                    else if (label === '部分补强') setDiffSummaryFilter('partial')
                    else if (label === '历史待补') setDiffSummaryFilter('pending')
                    else setDiffSummaryFilter('all')
                  }}
                  style={{
                    background: diffSummaryFilter !== 'all' && (
                      (diffSummaryFilter === 'strengthened' && label === '当前已补强') ||
                      (diffSummaryFilter === 'insufficient' && label === '当前仍不足') ||
                      (diffSummaryFilter === 'misblock' && label === '需关注误杀') ||
                      (diffSummaryFilter === 'partial' && label === '部分补强') ||
                      (diffSummaryFilter === 'pending' && label === '历史待补')
                    ) ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.18)',
                    border: `1px solid ${color}`,
                    borderRadius: 999,
                    padding: '4px 10px',
                    fontSize: 11,
                    color,
                    cursor: 'pointer',
                  }}
                >
                  {label} {count}
                </button>
              )
            })}
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 1540 }}>
            <thead>
              <tr>
                {['时间', '股票', '分类', '时段', '强度', '触发价', '涨跌幅', '5m收益', '净收益', '成本覆盖', 'MFE', 'MAE', '判定', '对照', '动作', '说明'].map(h => (
                  <th key={h} style={thStyle}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredReplayRows.length <= 0 ? (
                <tr>
                  <td colSpan={16} style={{ ...tdStyle, color: C.dim, textAlign: 'center' }}>暂无回放样本</td>
                </tr>
              ) : filteredReplayRows.map(row => {
                const q = row.quality || null
                const ret = q?.ret_bps
                const retColor = typeof ret === 'number' ? (ret > row.fee_bps ? C.green : ret >= 0 ? C.yellow : C.red) : C.dim
                const netRet = calcNetRetBps(row)
                const netRetColor = netRet === null ? C.dim : netRet >= 0 ? C.green : C.red
                const phase = String(q?.market_phase || 'pending')
                const rowKey = String(row.id)
                const relatedDiag = diagByCode.get(row.ts_code)
                const compare = buildReplayCompare(row, relatedDiag)
                const autoExpanded = row.classification === 'bad_sell' && Boolean(relatedDiag)
                const isExpanded = Object.prototype.hasOwnProperty.call(expandedReplayIds, rowKey)
                  ? Boolean(expandedReplayIds[rowKey])
                  : autoExpanded
                const tg = relatedDiag?.trend_guard || {}
                const ms = relatedDiag?.market_structure || {}
                return (
                  <React.Fragment key={rowKey}>
                    <tr>
                      <td style={tdStyle}>
                        <div>{fmtDateTime(row.triggered_at)}</div>
                        {q?.trigger_time_iso && (
                          <div style={{ color: C.dim, fontSize: 11, marginTop: 4 }}>
                            质量时点 {fmtDateTime(q.trigger_time_iso)}
                          </div>
                        )}
                      </td>
                      <td style={tdStyle}>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                          <Link to={`/stock/${row.ts_code}`} style={{ color: C.cyan, textDecoration: 'none', fontFamily: 'monospace' }}>{row.ts_code}</Link>
                          <span style={{ color: C.bright }}>{row.name || '--'}</span>
                        </div>
                      </td>
                      <td style={tdStyle}>
                        <span style={{
                          color: replayColor(row.classification),
                          border: `1px solid ${replayColor(row.classification)}`,
                          borderRadius: 999,
                          padding: '2px 8px',
                          fontSize: 10,
                          fontWeight: 700,
                        }}>
                          {replayLabel(row.classification)}
                        </span>
                      </td>
                      <td style={tdStyle}>{marketPhaseLabel(phase)}</td>
                      <td style={tdStyle}>{fmtNum(row.strength, 0)}</td>
                      <td style={tdStyle}>{row.price > 0 ? row.price.toFixed(2) : '--'}</td>
                      <td style={{ ...tdStyle, color: row.pct_chg >= 0 ? C.red : C.green }}>{fmtPct(row.pct_chg)}</td>
                      <td style={{ ...tdStyle, color: retColor }}>{fmtBps(ret)}</td>
                      <td style={{ ...tdStyle, color: netRetColor }}>{fmtBps(netRet)}</td>
                      <td style={{ ...tdStyle, color: feeCoverageColor(row) }}>{feeCoverageLabel(row)}</td>
                      <td style={tdStyle}>{fmtBps(q?.mfe_bps)}</td>
                      <td style={tdStyle}>{fmtBps(q?.mae_bps)}</td>
                      <td style={tdStyle}>{q ? (q.direction_correct ? '方向正确' : '方向错误') : '待评估'}</td>
                      <td style={{ ...tdStyle, maxWidth: 220, whiteSpace: 'normal', lineHeight: 1.4 }}>
                        <div style={{ color: compare.color, fontWeight: 700 }}>{compare.label}</div>
                        <div style={{ color: C.dim, fontSize: 11, marginTop: 4 }}>{compare.detail}</div>
                      </td>
                      <td style={tdStyle}>
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          <button onClick={() => setSymbolFilter(row.ts_code)} style={miniButtonStyle}>只看这只票</button>
                          <Link to={`/stock/${row.ts_code}`} target="_blank" rel="noreferrer" style={miniLinkStyle}>看K线</Link>
                          {relatedDiag && (
                            <button onClick={() => toggleReplayExpand(rowKey, autoExpanded)} style={miniButtonStyle}>
                              {isExpanded ? '收起诊断' : '展开诊断'}
                            </button>
                          )}
                        </div>
                      </td>
                      <td style={{ ...tdStyle, maxWidth: 340, whiteSpace: 'normal', lineHeight: 1.4 }}>
                        <div style={{ color: C.text }}>{row.classification_reason}</div>
                        <div style={{ color: C.dim, fontSize: 11, marginTop: 4 }}>{row.message || '--'}</div>
                      </td>
                    </tr>
                    {relatedDiag && isExpanded && (
                      <tr>
                        <td colSpan={16} style={{ ...tdStyle, background: 'rgba(0,0,0,0.14)' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 8 }}>
                            <span style={{ color: C.bright, fontWeight: 700 }}>当前保护诊断快照</span>
                            {autoExpanded && (
                              <span style={{
                                color: C.red,
                                border: `1px solid ${C.red}`,
                                borderRadius: 999,
                                padding: '2px 8px',
                                fontSize: 10,
                                fontWeight: 700,
                              }}>
                                bad_sell 自动展开
                              </span>
                            )}
                            <span style={{
                              color: statusColor(relatedDiag.status),
                              border: `1px solid ${statusColor(relatedDiag.status)}`,
                              borderRadius: 999,
                              padding: '2px 8px',
                              fontSize: 10,
                              fontWeight: 700,
                            }}>
                              {statusLabel(relatedDiag.status)}
                            </span>
                            <span style={{
                              color: compare.color,
                              border: `1px solid ${compare.color}`,
                              borderRadius: 999,
                              padding: '2px 8px',
                              fontSize: 10,
                              fontWeight: 700,
                            }}>
                              {compare.label}
                            </span>
                            <span style={{ color: C.dim, fontSize: 11 }}>
                              这是同股票的当前时点诊断，不是历史触发当刻的快照。
                            </span>
                          </div>
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10 }}>
                            <div style={diagInlineCardStyle}>
                              <div style={diagInlineTitleStyle}>历史结果 vs 当前状态</div>
                              <div style={lineStyle}>历史分类: <span style={valueStyle}>{replayLabel(row.classification)}</span></div>
                              <div style={lineStyle}>5m收益: <span style={valueStyle}>{fmtBps(ret)}</span></div>
                              <div style={lineStyle}>手续费后净收益: <span style={{ ...valueStyle, color: netRetColor }}>{fmtBps(netRet)}</span></div>
                              <div style={lineStyle}>成本覆盖: <span style={{ ...valueStyle, color: feeCoverageColor(row) }}>{feeCoverageLabel(row)}</span></div>
                              <div style={lineStyle}>对照结论: <span style={{ ...valueStyle, color: compare.color }}>{compare.label}</span></div>
                              <div style={{ ...lineStyle, marginTop: 6 }}>{compare.detail}</div>
                            </div>
                            <div style={diagInlineCardStyle}>
                              <div style={diagInlineTitleStyle}>保护链路</div>
                              <div style={lineStyle}>veto: <span style={valueStyle}>{relatedDiag.veto || '--'}</span></div>
                              <div style={lineStyle}>guard_score: <span style={valueStyle}>{String(tg.guard_score ?? '--')}</span></div>
                              <div style={lineStyle}>主升浪保护: <span style={valueStyle}>{String(Boolean(relatedDiag.main_rally_guard))}</span></div>
                              <div style={lineStyle}>guard_reasons: <span style={valueStyle}>{Array.isArray(tg.guard_reasons) && tg.guard_reasons.length > 0 ? tg.guard_reasons.join(', ') : '--'}</span></div>
                              <div style={lineStyle}>structure_reasons: <span style={valueStyle}>{Array.isArray(tg.structure_reasons) && tg.structure_reasons.length > 0 ? tg.structure_reasons.join(', ') : '--'}</span></div>
                              <div style={lineStyle}>negative_flags: <span style={valueStyle}>{Array.isArray(tg.negative_flags) && tg.negative_flags.length > 0 ? tg.negative_flags.join(', ') : '--'}</span></div>
                            </div>
                            <div style={diagInlineCardStyle}>
                              <div style={diagInlineTitleStyle}>触发与确认</div>
                              <div style={lineStyle}>触发项: <span style={valueStyle}>{Array.isArray(relatedDiag.trigger_items) && relatedDiag.trigger_items.length > 0 ? relatedDiag.trigger_items.join(', ') : '--'}</span></div>
                              <div style={lineStyle}>确认项: <span style={valueStyle}>{Array.isArray(relatedDiag.confirm_items) && relatedDiag.confirm_items.length > 0 ? relatedDiag.confirm_items.join(', ') : '--'}</span></div>
                              <div style={lineStyle}>bearish_confirms: <span style={valueStyle}>{Array.isArray(tg.bearish_confirms) && tg.bearish_confirms.length > 0 ? tg.bearish_confirms.join(', ') : '--'}</span></div>
                              <div style={lineStyle}>强度: <span style={valueStyle}>{fmtNum(relatedDiag.strength, 0)}</span></div>
                              <div style={lineStyle}>消息: <span style={valueStyle}>{relatedDiag.message || '--'}</span></div>
                            </div>
                            <div style={diagInlineCardStyle}>
                              <div style={diagInlineTitleStyle}>盘口微观结构</div>
                              <div style={lineStyle}>买卖比: <span style={valueStyle}>{fmtNum(ms.bid_ask_ratio, 2)}</span></div>
                              <div style={lineStyle}>卖墙吸收: <span style={valueStyle}>{String(Boolean(ms.ask_wall_absorbed))}</span></div>
                              <div style={lineStyle}>托盘击穿: <span style={valueStyle}>{String(Boolean(ms.bid_wall_broken))}</span></div>
                              <div style={lineStyle}>买入力衰减: <span style={valueStyle}>{String(Boolean(ms.active_buy_fading))}</span></div>
                              <div style={lineStyle}>拉升不稳: <span style={valueStyle}>{String(Boolean(ms.pull_up_not_stable))}</span></div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section style={panelStyle}>
        <CardTitle>按交易日问题趋势</CardTitle>
        <div style={{ color: C.dim, fontSize: 11, marginBottom: 10 }}>
          按交易日聚合 `bad_sell / weak_edge / 未覆盖成本 / 有效命中`，并补充按样本加权的 5/10 日滚动统计，用来看反T链路最近几天是在变好还是变差。
        </div>
        {dailyTrendRows.length <= 0 ? (
          <div style={{ color: C.dim, fontSize: 12 }}>当前筛选条件下没有可统计的交易日样本。</div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 1360 }}>
              <thead>
                <tr>
                  {['交易日', '总样本', '疑似误卖', '边际不足', '未覆盖成本', '有效命中', '当日净收益', '5日误卖率', '5日未覆盖率', '5日净收益', '10日误卖率', '10日未覆盖率', '10日净收益'].map(h => (
                    <th key={h} style={thStyle}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dailyTrendRows.map(row => (
                  <tr key={row.trade_day}>
                    <td style={tdStyle}>{row.trade_day}</td>
                    <td style={tdStyle}>{row.total_count}</td>
                    <td style={{ ...tdStyle, color: row.bad_sell_count > 0 ? C.red : C.dim }}>{row.bad_sell_count}</td>
                    <td style={{ ...tdStyle, color: row.weak_edge_count > 0 ? C.yellow : C.dim }}>{row.weak_edge_count}</td>
                    <td style={{ ...tdStyle, color: row.not_covered_count > 0 ? C.red : C.dim }}>{row.not_covered_count}</td>
                    <td style={{ ...tdStyle, color: row.good_hit_count > 0 ? C.green : C.dim }}>{row.good_hit_count}</td>
                    <td style={{ ...tdStyle, color: row.avg_net_ret_bps === null ? C.dim : row.avg_net_ret_bps >= 0 ? C.green : C.red }}>{fmtBps(row.avg_net_ret_bps)}</td>
                    <td style={{ ...tdStyle, color: issueRateColor(row.roll5_bad_sell_rate) }}>{fmtRatePct(row.roll5_bad_sell_rate)}</td>
                    <td style={{ ...tdStyle, color: issueRateColor(row.roll5_not_covered_rate) }}>{fmtRatePct(row.roll5_not_covered_rate)}</td>
                    <td style={{ ...tdStyle, color: row.roll5_avg_net_ret_bps === null ? C.dim : row.roll5_avg_net_ret_bps >= 0 ? C.green : C.red }}>{fmtBps(row.roll5_avg_net_ret_bps)}</td>
                    <td style={{ ...tdStyle, color: issueRateColor(row.roll10_bad_sell_rate) }}>{fmtRatePct(row.roll10_bad_sell_rate)}</td>
                    <td style={{ ...tdStyle, color: issueRateColor(row.roll10_not_covered_rate) }}>{fmtRatePct(row.roll10_not_covered_rate)}</td>
                    <td style={{ ...tdStyle, color: row.roll10_avg_net_ret_bps === null ? C.dim : row.roll10_avg_net_ret_bps >= 0 ? C.green : C.red }}>{fmtBps(row.roll10_avg_net_ret_bps)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section style={panelStyle}>
        <CardTitle>问题股排行榜</CardTitle>
        <div style={{ color: C.dim, fontSize: 11, marginBottom: 10 }}>
          按 `bad_sell`、`未覆盖成本`、`weak_edge` 优先排序，帮助我们先盯最需要收紧反T门槛的股票。
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap', marginBottom: 12 }}>
          <select value={rankingSort} onChange={e => setRankingSort(e.target.value)} style={selectStyle}>
            <option value="bad_sell">按疑似误卖排序</option>
            <option value="not_covered">按未覆盖成本排序</option>
            <option value="weak_edge">按边际不足排序</option>
            <option value="net_ret">按平均净收益排序</option>
          </select>
          <div style={{ color: C.dim, fontSize: 12 }}>
            当前排序口径: {rankingSort === 'bad_sell' ? '疑似误卖优先' : rankingSort === 'not_covered' ? '未覆盖成本优先' : rankingSort === 'weak_edge' ? '边际不足优先' : '平均净收益最低优先'}
          </div>
        </div>
        {problemStockRanking.length <= 0 ? (
          <div style={{ color: C.dim, fontSize: 12 }}>当前筛选条件下没有可统计的问题股。</div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 960 }}>
              <thead>
                <tr>
                  {['股票', '疑似误卖', '未覆盖成本', '边际不足', '总样本', '平均净收益', '当前诊断', '当前对照', '动作'].map(h => (
                    <th key={h} style={thStyle}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {problemStockRanking.map(row => (
                  <tr key={row.ts_code}>
                    <td style={tdStyle}>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <Link to={`/stock/${row.ts_code}`} style={{ color: C.cyan, textDecoration: 'none', fontFamily: 'monospace' }}>{row.ts_code}</Link>
                        <span style={{ color: C.bright }}>{row.name || '--'}</span>
                      </div>
                    </td>
                    <td style={{ ...tdStyle, color: row.bad_sell_count > 0 ? C.red : C.dim }}>{row.bad_sell_count}</td>
                    <td style={{ ...tdStyle, color: row.not_covered_count > 0 ? C.red : C.dim }}>{row.not_covered_count}</td>
                    <td style={{ ...tdStyle, color: row.weak_edge_count > 0 ? C.yellow : C.dim }}>{row.weak_edge_count}</td>
                    <td style={tdStyle}>{row.total_count}</td>
                    <td style={{ ...tdStyle, color: row.avg_net_ret_bps === null ? C.dim : row.avg_net_ret_bps >= 0 ? C.green : C.red }}>{fmtBps(row.avg_net_ret_bps)}</td>
                    <td style={tdStyle}>{row.diag_status ? statusLabel(row.diag_status) : '--'}</td>
                    <td style={tdStyle}>{row.compare_label || '--'}</td>
                    <td style={tdStyle}>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        <button onClick={() => jumpToReplaySamples(row.ts_code)} style={miniButtonStyle}>跳到回放样本</button>
                        <button onClick={() => jumpToStockTimeline(row.ts_code)} style={miniButtonStyle}>看时间轴</button>
                        <button onClick={() => setSymbolFilter(row.ts_code)} style={miniButtonStyle}>只看这只票</button>
                        <Link to={`/stock/${row.ts_code}`} target="_blank" rel="noreferrer" style={miniLinkStyle}>看K线</Link>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section ref={timelineSectionRef} style={panelStyle}>
        <CardTitle>单票反T时间轴</CardTitle>
        <div style={{ color: C.dim, fontSize: 11, marginBottom: 10 }}>
          这里按时间倒序列出单只股票最近的反T样本，方便看这只票是偶发误卖，还是持续出现“反T过早/边际不足”。
        </div>
        {!timelineMeta ? (
          <div style={{ color: C.dim, fontSize: 12 }}>
            输入具体股票代码，或在“问题股排行榜”里点击 `看时间轴`，这里就会显示该票最近的反T样本轨迹。
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <StatCard label="股票" value={`${timelineMeta.ts_code}`} color={C.cyan} />
              <StatCard label="名称" value={timelineMeta.name || '--'} />
              <StatCard label="样本数" value={timelineMeta.sample_count} />
              <StatCard label="当前诊断" value={timelineMeta.diag_status ? statusLabel(timelineMeta.diag_status) : '--'} color={timelineMeta.diag_status ? statusColor(timelineMeta.diag_status) : C.dim} />
              <StatCard label="当前对照" value={timelineMeta.compare_label || '--'} />
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: 10 }}>
              {timelineRows.map(row => {
                const q = row.quality || null
                const currentDiag = diagByCode.get(row.ts_code)
                const compare = buildReplayCompare(row, currentDiag)
                const diffItems = buildHistoryCurrentDiff(row, currentDiag)
                const diffSummary = summarizeHistoryCurrentDiff(row, currentDiag, compare, diffItems)
                const hs = row.history_snapshot || null
                const hsSnapshotMode = String((hs && hs.snapshot_mode) || '')
                const hsThreshold = hs && typeof hs.threshold === 'object' ? hs.threshold : {}
                const hsTrendGuard = hs && typeof hs.trend_guard === 'object' ? hs.trend_guard : {}
                const netRet = calcNetRetBps(row)
                const netRetColor = netRet === null ? C.dim : netRet >= 0 ? C.green : C.red
                return (
                  <div key={`timeline-${row.id}`} style={subPanelStyle}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                      <span style={{ color: C.bright, fontWeight: 700 }}>{fmtDateTime(row.triggered_at)}</span>
                      <span style={{
                        marginLeft: 'auto',
                        color: replayColor(row.classification),
                        border: `1px solid ${replayColor(row.classification)}`,
                        borderRadius: 999,
                        padding: '2px 8px',
                        fontSize: 10,
                        fontWeight: 700,
                      }}>
                        {replayLabel(row.classification)}
                      </span>
                    </div>
                    <div style={{
                      border: `1px solid ${diffSummary.color}`,
                      borderRadius: 8,
                      padding: '8px 10px',
                      background: 'rgba(255,255,255,0.02)',
                      marginBottom: 10,
                    }}>
                      <div style={{ color: diffSummary.color, fontSize: 12, fontWeight: 700, marginBottom: 4 }}>
                        {diffSummary.label}
                      </div>
                      <div style={{ color: C.text, fontSize: 11, lineHeight: 1.45 }}>
                        {diffSummary.detail}
                      </div>
                    </div>
                    <div style={kvGridStyle}>
                      <div>时段 <span style={valueStyle}>{marketPhaseLabel(String(q?.market_phase || 'pending'))}</span></div>
                      <div>强度 <span style={valueStyle}>{fmtNum(row.strength, 0)}</span></div>
                      <div>5m收益 <span style={{ ...valueStyle, color: typeof q?.ret_bps === 'number' && q.ret_bps >= 0 ? C.green : C.red }}>{fmtBps(q?.ret_bps)}</span></div>
                      <div>净收益 <span style={{ ...valueStyle, color: netRetColor }}>{fmtBps(netRet)}</span></div>
                    </div>
                    <div style={lineStyle}>成本覆盖: <span style={{ ...valueStyle, color: feeCoverageColor(row) }}>{feeCoverageLabel(row)}</span></div>
                    <div style={lineStyle}>对照结论: <span style={{ ...valueStyle, color: compare.color }}>{compare.label}</span></div>
                    <div style={lineStyle}>分类说明: <span style={valueStyle}>{row.classification_reason}</span></div>
                    <div style={lineStyle}>消息: <span style={valueStyle}>{row.message || '--'}</span></div>
                    <div style={{ marginTop: 10, display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10 }}>
                      <div style={diagInlineCardStyle}>
                        <div style={diagInlineTitleStyle}>历史触发快照</div>
                        {!hs ? (
                          <div style={{ color: C.dim, fontSize: 11 }}>这条历史样本触发时还没有持久化保护快照。</div>
                        ) : (
                          <>
                            <div style={lineStyle}>快照来源: <span style={{ ...valueStyle, color: hsSnapshotMode === 'backfill' ? C.yellow : C.green }}>{hsSnapshotMode === 'backfill' ? '回补近似' : '原生持久化'}</span></div>
                            <div style={lineStyle}>当时状态: <span style={valueStyle}>{hs.score_status || '--'}</span></div>
                            <div style={lineStyle}>仅观察: <span style={valueStyle}>{String(hs.observe_only ?? '--')}</span></div>
                            <div style={lineStyle}>触发项: <span style={valueStyle}>{Array.isArray(hs.trigger_items) && hs.trigger_items.length > 0 ? hs.trigger_items.join(', ') : '--'}</span></div>
                            <div style={lineStyle}>确认项: <span style={valueStyle}>{Array.isArray(hs.confirm_items) && hs.confirm_items.length > 0 ? hs.confirm_items.join(', ') : '--'}</span></div>
                            <div style={lineStyle}>当时 veto: <span style={valueStyle}>{hs.veto || '--'}</span></div>
                            <div style={lineStyle}>量能节奏: <span style={valueStyle}>{[hs.volume_pace_state || '', typeof hs.volume_pace_ratio === 'number' ? fmtNum(hs.volume_pace_ratio, 3) : ''].filter(Boolean).join(' / ') || '--'}</span></div>
                          </>
                        )}
                      </div>
                      <div style={diagInlineCardStyle}>
                        <div style={diagInlineTitleStyle}>历史趋势保护</div>
                        {!hs ? (
                          <div style={{ color: C.dim, fontSize: 11 }}>旧样本暂无历史趋势保护快照。</div>
                        ) : (
                          <>
                            <div style={lineStyle}>保护激活: <span style={valueStyle}>{String(hsTrendGuard.active ?? '--')}</span></div>
                            <div style={lineStyle}>主升浪上行: <span style={valueStyle}>{String(hsTrendGuard.trend_up ?? '--')}</span></div>
                            <div style={lineStyle}>需要空头确认数: <span style={valueStyle}>{String(hsTrendGuard.required_bearish_confirms ?? '--')}</span></div>
                            <div style={lineStyle}>空头确认: <span style={valueStyle}>{Array.isArray(hsTrendGuard.bearish_confirms) && hsTrendGuard.bearish_confirms.length > 0 ? hsTrendGuard.bearish_confirms.join(', ') : '--'}</span></div>
                            <div style={lineStyle}>强延续保护: <span style={valueStyle}>{String(hsTrendGuard.surge_absorb_guard ?? '--')}</span></div>
                          </>
                        )}
                      </div>
                      <div style={diagInlineCardStyle}>
                        <div style={diagInlineTitleStyle}>历史阈值快照</div>
                        {!hs ? (
                          <div style={{ color: C.dim, fontSize: 11 }}>旧样本暂无历史阈值快照。</div>
                        ) : (
                          <>
                            <div style={lineStyle}>版本: <span style={valueStyle}>{hsThreshold.threshold_version || '--'}</span></div>
                            <div style={lineStyle}>市场时段: <span style={valueStyle}>{marketPhaseLabel(String(hsThreshold.market_phase || 'unknown'))}</span></div>
                            <div style={lineStyle}>波动分桶: <span style={valueStyle}>{hsThreshold.regime || '--'}</span></div>
                            <div style={lineStyle}>observer_only: <span style={valueStyle}>{String(hsThreshold.observer_only ?? '--')}</span></div>
                            <div style={lineStyle}>blocked: <span style={valueStyle}>{String(hsThreshold.blocked ?? '--')}</span></div>
                          </>
                        )}
                      </div>
                      <div style={diagInlineCardStyle}>
                        <div style={diagInlineTitleStyle}>历史 vs 当前差异</div>
                        <div style={{
                          border: `1px solid ${diffSummary.color}`,
                          borderRadius: 8,
                          padding: '8px 10px',
                          background: 'rgba(255,255,255,0.02)',
                          marginBottom: 8,
                        }}>
                          <div style={{ color: diffSummary.color, fontSize: 11, fontWeight: 700, marginBottom: 4 }}>{diffSummary.label}</div>
                          <div style={{ color: C.text, fontSize: 11, lineHeight: 1.45 }}>{diffSummary.detail}</div>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                          {diffItems.map((it, idx) => (
                            <div key={`${row.id}-diff-${idx}`} style={{
                              border: `1px solid ${it.color}`,
                              borderRadius: 8,
                              padding: '8px 10px',
                              background: 'rgba(255,255,255,0.02)',
                            }}>
                              <div style={{ color: it.color, fontSize: 11, fontWeight: 700, marginBottom: 4 }}>{it.label}</div>
                              <div style={{ color: C.text, fontSize: 11, lineHeight: 1.45 }}>{it.detail}</div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </section>
    </div>
  )
}

const panelStyle: React.CSSProperties = {
  background: C.card,
  border: `1px solid ${C.border}`,
  borderRadius: 10,
  padding: 14,
}

const subPanelStyle: React.CSSProperties = {
  background: 'rgba(0,0,0,0.18)',
  border: `1px solid ${C.border}`,
  borderRadius: 8,
  padding: 10,
}

const diagInlineCardStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.02)',
  border: `1px solid ${C.border}`,
  borderRadius: 8,
  padding: 10,
}

const diagInlineTitleStyle: React.CSSProperties = {
  color: C.bright,
  fontSize: 12,
  fontWeight: 700,
  marginBottom: 8,
}

const actionRowStyle: React.CSSProperties = {
  display: 'flex',
  gap: 6,
  alignItems: 'center',
  flexWrap: 'wrap',
  marginBottom: 8,
}

const lineStyle: React.CSSProperties = {
  color: C.dim,
  fontSize: 11,
  lineHeight: 1.5,
  whiteSpace: 'normal',
  wordBreak: 'break-word',
}

const kvGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  gap: 6,
  color: C.dim,
  fontSize: 11,
  marginBottom: 8,
}

const valueStyle: React.CSSProperties = {
  color: C.text,
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '10px 8px',
  borderBottom: `1px solid ${C.border}`,
  color: C.dim,
  fontSize: 11,
  fontWeight: 600,
}

const tdStyle: React.CSSProperties = {
  padding: '10px 8px',
  borderBottom: `1px solid rgba(30,34,51,0.7)`,
  color: C.text,
  fontSize: 12,
  verticalAlign: 'top',
}

const buttonStyle: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  background: C.cyanBg,
  color: C.cyan,
  border: `1px solid rgba(0,200,200,0.35)`,
  borderRadius: 8,
  padding: '8px 12px',
  cursor: 'pointer',
  fontSize: 12,
  fontWeight: 600,
}

const ghostButtonStyle: React.CSSProperties = {
  background: 'transparent',
  color: C.dim,
  border: `1px solid ${C.border}`,
  borderRadius: 8,
  padding: '8px 12px',
  cursor: 'pointer',
  fontSize: 12,
}

const miniButtonStyle: React.CSSProperties = {
  background: 'transparent',
  color: C.cyan,
  border: `1px solid rgba(0,200,200,0.35)`,
  borderRadius: 6,
  padding: '4px 8px',
  cursor: 'pointer',
  fontSize: 11,
}

const miniLinkStyle: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  color: C.cyan,
  textDecoration: 'none',
  border: `1px solid rgba(0,200,200,0.35)`,
  borderRadius: 6,
  padding: '4px 8px',
  fontSize: 11,
}

const selectStyle: React.CSSProperties = {
  background: C.card,
  color: C.text,
  border: `1px solid ${C.border}`,
  borderRadius: 8,
  padding: '8px 10px',
  fontSize: 12,
}

const inputStyle: React.CSSProperties = {
  background: C.card,
  color: C.text,
  border: `1px solid ${C.border}`,
  borderRadius: 8,
  padding: '8px 10px',
  fontSize: 12,
  outline: 'none',
}
