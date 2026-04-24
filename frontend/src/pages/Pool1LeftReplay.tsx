import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'
import { Link } from 'react-router-dom'
import { Download, RefreshCw, X } from 'lucide-react'

const API = 'http://localhost:8000'

const C = {
  bg: '#0f1117',
  card: '#181d2a',
  border: '#1e2233',
  text: '#c8cdd8',
  dim: '#4a5068',
  bright: '#e8eaf0',
  cyan: '#00c8c8',
  red: '#ef4444',
  green: '#22c55e',
  yellow: '#eab308',
}

interface LeftReplayFutureSignal {
  signal_type: string
  message: string
  observe_only: boolean
  triggered_at: string
  time_diff_hours: number
}

interface LeftReplayRow {
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
  stage_label: string
  stage_reason: string
  observe_only: boolean
  observe_reason: string
  stage_c_block_type?: string
  stage_c_block_label?: string
  concept_state: string
  ret_5m_bps?: number | null
  ret_60m_bps?: number | null
  next_signal?: LeftReplayFutureSignal | null
  next_exec_build?: LeftReplayFutureSignal | null
  next_clear?: LeftReplayFutureSignal | null
  classification: string
  classification_reason: string
  stage_b_items?: string[]
  stage_c_items?: string[]
}

interface LeftReplayResp {
  ok: boolean
  checked_at: string
  hours: number
  limit: number
  promote_hours: number
  summary: Record<string, number>
  count: number
  rows: LeftReplayRow[]
}

type StageFilter = 'all' | 'B未成' | 'C观察' | '可执行' | '观察'

type ClassificationFilter =
  | 'all'
  | 'promoted_build'
  | 'watch_avoided_drop'
  | 'watch_too_early'
  | 'good_follow'
  | 'fast_fail'
  | 'mixed'
  | 'pending'

type StageCBlockFilter =
  | 'all'
  | 'industry_joint_weak'
  | 'industry_retreat'
  | 'industry_weak'
  | 'concept_heat_cliff'
  | 'concept_retreat_main'
  | 'concept_weak_main'
  | 'high_position_catchdown'
  | 'distribution_structure'

type DetailSortKey = 'risk' | 'ret_5m_desc' | 'ret_60m_desc' | 'action_desc' | 'time_desc'
type RankingSortKey = 'score' | 'success' | 'pressure' | 'samples'
type ActionChainFilter = 'all' | 'has_exec_build' | 'has_clear'

interface RankingRow {
  key: string
  label: string
  total: number
  promoted: number
  avoided: number
  tooEarly: number
  goodFollow: number
  fail: number
  mixed: number
  pending: number
  successRate: number
  pressureRate: number
  score: number
  dominantStageCBlock: string
  dominantStageCBlockLabel: string
  sampleTsCode?: string
  sampleName?: string
}

function fmtDateTime(v?: string | null): string {
  if (!v) return '--'
  const dt = new Date(v)
  if (Number.isNaN(dt.getTime())) return String(v)
  return dt.toLocaleString('zh-CN', { hour12: false })
}

function fmtPct(v?: number | null): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '--'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function fmtBps(v?: number | null): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '--'
  return `${n > 0 ? '+' : ''}${n.toFixed(1)}bps`
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

function classifyLabel(v: string): string {
  const map: Record<string, string> = {
    promoted_build: '后续转执行',
    watch_avoided_drop: '观察有效拦截',
    watch_too_early: '观察偏保守',
    good_follow: '执行有效',
    fast_fail: '执行失效',
    mixed: '边际一般',
    pending: '待验证',
  }
  return map[v] || v
}

function classifyColor(v: string): string {
  if (v === 'promoted_build' || v === 'good_follow') return C.green
  if (v === 'watch_avoided_drop') return C.cyan
  if (v === 'watch_too_early' || v === 'mixed') return C.yellow
  if (v === 'fast_fail') return C.red
  return C.dim
}

function conceptStateLabel(v?: string | null): string {
  const s = String(v || '').toLowerCase()
  if (s === 'expand') return '扩张'
  if (s === 'strong') return '强势'
  if (s === 'weak') return '转弱'
  if (s === 'retreat') return '退潮'
  if (s === 'neutral') return '中性'
  return '--'
}

function conceptStateColor(v?: string | null): string {
  const s = String(v || '').toLowerCase()
  if (s === 'expand') return C.green
  if (s === 'strong') return C.cyan
  if (s === 'weak') return C.yellow
  if (s === 'retreat') return C.red
  return C.dim
}

function stageCBlockLabel(v?: string | null): string {
  const s = String(v || '').trim()
  const map: Record<string, string> = {
    industry_joint_weak: '行业+概念共弱',
    industry_retreat: '行业退潮',
    industry_weak: '行业承接转弱',
    concept_heat_cliff: '概念热度断崖',
    concept_retreat_main: '概念退潮主跌段',
    concept_weak_main: '概念承接转弱',
    high_position_catchdown: '高位补跌',
    distribution_structure: '出货结构',
  }
  return map[s] || s || '--'
}

function stageCBlockPriority(v?: string | null): number {
  const s = String(v || '').trim()
  const map: Record<string, number> = {
    industry_joint_weak: 90,
    industry_retreat: 80,
    concept_heat_cliff: 70,
    concept_retreat_main: 60,
    distribution_structure: 50,
    high_position_catchdown: 40,
    industry_weak: 30,
    concept_weak_main: 20,
  }
  return map[s] || 0
}

function stageColor(v: string): string {
  if (v === '可执行') return C.green
  if (v === 'B未成' || v === 'C观察' || v === '观察') return C.yellow
  return C.dim
}

function normalizeStageLabel(row: LeftReplayRow): string {
  const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
  const lsm = hs?.left_state_machine && typeof hs.left_state_machine === 'object' ? hs.left_state_machine as Record<string, any> : null
  const stageA = lsm?.stage_a && typeof lsm.stage_a === 'object' ? lsm.stage_a as Record<string, any> : null
  const stageB = lsm?.stage_b && typeof lsm.stage_b === 'object' ? lsm.stage_b as Record<string, any> : null
  const stageC = lsm?.stage_c && typeof lsm.stage_c === 'object' ? lsm.stage_c as Record<string, any> : null

  if (stageA?.passed && !stageB?.passed) return 'B未成'
  if (stageA?.passed && stageB?.passed && (!stageC?.passed || stageC?.observe_only || row.observe_only)) return 'C观察'
  if (lsm?.executable_ready && !row.observe_only) return '可执行'
  if (row.observe_only) return '观察'

  const raw = String(row.stage_label || '')
  if (raw.includes('B')) return 'B未成'
  if (raw.includes('C')) return 'C观察'
  if (raw.includes('执') || raw.includes('可')) return '可执行'
  return '观察'
}

function conceptBoardOfRow(row: LeftReplayRow): string {
  const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
  const conceptEcology = hs?.concept_ecology && typeof hs.concept_ecology === 'object' ? hs.concept_ecology as Record<string, any> : null
  return String(conceptEcology?.core_concept_board || '').trim() || '未映射概念'
}

function industryOfRow(row: LeftReplayRow): string {
  const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
  const items =
    hs?.left_state_machine?.stage_c && Array.isArray(hs.left_state_machine.stage_c.items)
      ? hs.left_state_machine.stage_c.items
      : Array.isArray(row.stage_c_items)
        ? row.stage_c_items
        : []
  for (const item of items) {
    const text = String(item || '').trim()
    if (!text.includes('行业语义:')) continue
    const parts = text.split(':')
    if (parts.length >= 2) {
      const industry = String(parts[1] || '').trim()
      if (industry) return industry
    }
  }
  return '未映射行业'
}

function buildStageSnapshotSummary(row: LeftReplayRow): string {
  const hs = row.history_snapshot && typeof row.history_snapshot === 'object' ? row.history_snapshot : null
  const lsm = hs?.left_state_machine && typeof hs.left_state_machine === 'object' ? hs.left_state_machine as Record<string, any> : null
  const stageA = lsm?.stage_a && typeof lsm.stage_a === 'object' ? lsm.stage_a as Record<string, any> : null
  const stageB = lsm?.stage_b && typeof lsm.stage_b === 'object' ? lsm.stage_b as Record<string, any> : null
  const stageC = lsm?.stage_c && typeof lsm.stage_c === 'object' ? lsm.stage_c as Record<string, any> : null

  const parts: string[] = []
  if (stageA) {
    const items = Array.isArray(stageA.extreme_items) ? stageA.extreme_items.filter(Boolean).slice(0, 2).join('/') : ''
    parts.push(`A:${stageA.passed ? '到位' : '未到'}${items ? `(${items})` : ''}`)
  }
  if (stageB) {
    const items = Array.isArray(stageB.confirm_items) ? stageB.confirm_items.filter(Boolean).slice(0, 2).join('/') : ''
    parts.push(`B:${stageB.passed ? '成形' : '未成'}${items ? `(${items})` : ''}`)
  }
  if (stageC) {
    const items = Array.isArray(stageC.items) ? stageC.items.filter(Boolean).slice(0, 2).join('/') : ''
    parts.push(`C:${stageC.passed ? '通过' : '观察'}${items ? `(${items})` : ''}`)
  }
  if (parts.length > 0) return parts.join(' · ')

  const fallbackParts: string[] = []
  if (Array.isArray(row.stage_b_items) && row.stage_b_items.length > 0) fallbackParts.push(`B:${row.stage_b_items.slice(0, 2).join('/')}`)
  if (Array.isArray(row.stage_c_items) && row.stage_c_items.length > 0) fallbackParts.push(`C:${row.stage_c_items.slice(0, 2).join('/')}`)
  return fallbackParts.length > 0 ? fallbackParts.join(' · ') : '--'
}

function nextActionLabel(row: LeftReplayRow): string {
  if (row.next_clear) return `后续清仓 · ${fmtDateTime(row.next_clear.triggered_at)}`
  if (row.next_exec_build) return `后续转执行 · ${fmtDateTime(row.next_exec_build.triggered_at)}`
  if (row.next_signal) return `${row.next_signal.observe_only ? '后续观察' : '后续信号'} · ${fmtDateTime(row.next_signal.triggered_at)}`
  return '--'
}

function buildRankingRows(
  rows: LeftReplayRow[],
  keyGetter: (row: LeftReplayRow) => string,
  sortKey: RankingSortKey,
): RankingRow[] {
  const buckets = new Map<string, RankingRow>()
  for (const row of rows) {
    const rawKey = keyGetter(row).trim()
    const key = rawKey || '--'
    let bucket = buckets.get(key)
    if (!bucket) {
      bucket = {
        key,
        label: key,
        total: 0,
        promoted: 0,
        avoided: 0,
        tooEarly: 0,
        goodFollow: 0,
        fail: 0,
        mixed: 0,
        pending: 0,
        successRate: 0,
        pressureRate: 0,
        score: 0,
        dominantStageCBlock: '',
        dominantStageCBlockLabel: '--',
        sampleTsCode: row.ts_code,
        sampleName: row.name,
      }
      buckets.set(key, bucket)
    }
    bucket.total += 1
    if (row.classification === 'promoted_build') bucket.promoted += 1
    else if (row.classification === 'watch_avoided_drop') bucket.avoided += 1
    else if (row.classification === 'watch_too_early') bucket.tooEarly += 1
    else if (row.classification === 'good_follow') bucket.goodFollow += 1
    else if (row.classification === 'fast_fail') bucket.fail += 1
    else if (row.classification === 'mixed') bucket.mixed += 1
    else bucket.pending += 1
  }

  return Array.from(buckets.values()).map(bucket => {
    const related = rows.filter(row => (keyGetter(row).trim() || '--') === bucket.key)
    const stageCCount = new Map<string, number>()
    for (const row of related) {
      const block = String(row.stage_c_block_type || '').trim()
      if (!block) continue
      stageCCount.set(block, (stageCCount.get(block) || 0) + 1)
    }
    const dominantStageCBlock =
      Array.from(stageCCount.entries()).sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1]
        return stageCBlockPriority(b[0]) - stageCBlockPriority(a[0])
      })[0]?.[0] || ''
    const positive = bucket.promoted + bucket.avoided + bucket.goodFollow
    const negative = bucket.fail + bucket.tooEarly
    bucket.successRate = bucket.total > 0 ? positive / bucket.total : 0
    bucket.pressureRate = bucket.total > 0 ? negative / bucket.total : 0
    bucket.score = bucket.total > 0 ? (positive * 2 - negative * 2 - bucket.mixed) / bucket.total : 0
    bucket.dominantStageCBlock = dominantStageCBlock
    bucket.dominantStageCBlockLabel = stageCBlockLabel(dominantStageCBlock)
    return bucket
  }).sort((a, b) => {
    if (sortKey === 'success') {
      if (b.successRate !== a.successRate) return b.successRate - a.successRate
      if (b.score !== a.score) return b.score - a.score
    } else if (sortKey === 'pressure') {
      if (b.pressureRate !== a.pressureRate) return b.pressureRate - a.pressureRate
      if (b.fail !== a.fail) return b.fail - a.fail
    } else if (sortKey === 'samples') {
      if (b.total !== a.total) return b.total - a.total
      if (b.successRate !== a.successRate) return b.successRate - a.successRate
    } else {
      if (b.score !== a.score) return b.score - a.score
      if (b.successRate !== a.successRate) return b.successRate - a.successRate
    }
    if (b.total !== a.total) return b.total - a.total
    if (b.score !== a.score) return b.score - a.score
    return a.label.localeCompare(b.label, 'zh-CN')
  })
}

function StatCard({ label, value, color }: { label: string; value: string | number; color: string }) {
  return (
    <div
      style={{
        minWidth: 132,
        background: C.card,
        border: `1px solid ${C.border}`,
        borderRadius: 10,
        padding: '10px 12px',
      }}
    >
      <div style={{ color: C.dim, fontSize: 12 }}>{label}</div>
      <div style={{ color, fontSize: 20, fontWeight: 700, marginTop: 6 }}>{value}</div>
    </div>
  )
}

function FilterChip({
  active,
  onClick,
  label,
  color,
}: {
  active: boolean
  onClick: () => void
  label: string
  color?: string
}) {
  return (
    <button
      onClick={onClick}
      style={{
        borderRadius: 999,
        border: `1px solid ${active ? (color || C.cyan) : C.border}`,
        background: active ? 'rgba(0,200,200,0.10)' : '#111625',
        color: active ? (color || C.cyan) : C.text,
        padding: '8px 14px',
        cursor: 'pointer',
      }}
    >
      {label}
    </button>
  )
}

export default function Pool1LeftReplay() {
  const [data, setData] = useState<LeftReplayResp | null>(null)
  const [loading, setLoading] = useState(false)
  const [hours, setHours] = useState(72)
  const [keyword, setKeyword] = useState('')
  const [stageFilter, setStageFilter] = useState<StageFilter>('all')
  const [classificationFilter, setClassificationFilter] = useState<ClassificationFilter>('all')
  const [stageCBlockFilter, setStageCBlockFilter] = useState<StageCBlockFilter>('all')
  const [conceptFocus, setConceptFocus] = useState('')
  const [industryFocus, setIndustryFocus] = useState('')
  const [stockFocus, setStockFocus] = useState('')
  const [detailSort, setDetailSort] = useState<DetailSortKey>('risk')
  const [rankingSort, setRankingSort] = useState<RankingSortKey>('score')
  const [actionChainFilter, setActionChainFilter] = useState<ActionChainFilter>('all')
  const [conceptShowAll, setConceptShowAll] = useState(false)
  const [industryShowAll, setIndustryShowAll] = useState(false)
  const [selectedRow, setSelectedRow] = useState<LeftReplayRow | null>(null)
  const detailsRef = useRef<HTMLDivElement | null>(null)

  const scrollToDetails = () => {
    window.setTimeout(() => {
      detailsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 20)
  }

  const load = async (nextHours = hours) => {
    setLoading(true)
    try {
      const resp = await axios.get<LeftReplayResp>(`${API}/api/realtime/runtime/pool1_left_replay`, {
        params: { hours: nextHours, limit: 240, promote_hours: 48 },
      })
      setData(resp.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
    const timer = window.setInterval(() => load(), 30000)
    return () => window.clearInterval(timer)
  }, [])

  const rows = useMemo(() => (data?.rows || []).map(row => ({ ...row, stage_label: normalizeStageLabel(row) })), [data])

  const baseRows = useMemo(() => {
    const kw = keyword.trim().toLowerCase()
    return rows.filter(row => {
      if (stageFilter !== 'all' && row.stage_label !== stageFilter) return false
      if (classificationFilter !== 'all' && row.classification !== classificationFilter) return false
      if (stageCBlockFilter !== 'all' && String(row.stage_c_block_type || '') !== stageCBlockFilter) return false
      if (actionChainFilter === 'has_exec_build' && !row.next_exec_build) return false
      if (actionChainFilter === 'has_clear' && !row.next_clear) return false
      if (!kw) return true
      const text = [
        row.ts_code,
        row.name,
        row.message,
        row.stage_reason,
        row.observe_reason,
        conceptBoardOfRow(row),
        industryOfRow(row),
        stageCBlockLabel(row.stage_c_block_type),
      ].join(' ').toLowerCase()
      return text.includes(kw)
    })
  }, [rows, keyword, stageFilter, classificationFilter, stageCBlockFilter, actionChainFilter])

  const filteredRows = useMemo(() => {
    return baseRows.filter(row => {
      if (conceptFocus && conceptBoardOfRow(row) !== conceptFocus) return false
      if (industryFocus && industryOfRow(row) !== industryFocus) return false
      if (stockFocus && row.ts_code !== stockFocus) return false
      return true
    })
  }, [baseRows, conceptFocus, industryFocus, stockFocus])

  const displayRows = useMemo(() => {
    return [...filteredRows].sort((a, b) => {
      if (detailSort === 'ret_5m_desc') {
        const av = Number(a.ret_5m_bps ?? -Infinity)
        const bv = Number(b.ret_5m_bps ?? -Infinity)
        if (bv !== av) return bv - av
      } else if (detailSort === 'ret_60m_desc') {
        const av = Number(a.ret_60m_bps ?? -Infinity)
        const bv = Number(b.ret_60m_bps ?? -Infinity)
        if (bv !== av) return bv - av
      } else if (detailSort === 'action_desc') {
        const actionPriority = (row: LeftReplayRow) => {
          if (row.next_clear) return 3
          if (row.next_exec_build) return 2
          if (row.next_signal) return 1
          return 0
        }
        const av = actionPriority(a)
        const bv = actionPriority(b)
        if (bv !== av) return bv - av
      } else if (detailSort === 'risk') {
        const pa = stageCBlockPriority(a.stage_c_block_type)
        const pb = stageCBlockPriority(b.stage_c_block_type)
        if (pb !== pa) return pb - pa
      }
      const ta = new Date(a.triggered_at || '').getTime()
      const tb = new Date(b.triggered_at || '').getTime()
      return tb - ta
    })
  }, [filteredRows, detailSort])

  const summary = useMemo(() => {
    const s = {
      total: filteredRows.length,
      stageB: 0,
      stageC: 0,
      executable: 0,
      observe: 0,
      promoted: 0,
      avoided: 0,
      tooEarly: 0,
      fail: 0,
    }
    for (const row of filteredRows) {
      if (row.stage_label === 'B未成') s.stageB += 1
      else if (row.stage_label === 'C观察') s.stageC += 1
      else if (row.stage_label === '可执行') s.executable += 1
      else s.observe += 1
      if (row.classification === 'promoted_build') s.promoted += 1
      else if (row.classification === 'watch_avoided_drop') s.avoided += 1
      else if (row.classification === 'watch_too_early') s.tooEarly += 1
      else if (row.classification === 'fast_fail') s.fail += 1
    }
    return s
  }, [filteredRows])

  const stageBuckets = useMemo(() => {
    return [
      { key: 'B未成', label: 'B未成', count: summary.stageB, color: C.yellow },
      { key: 'C观察', label: 'C观察', count: summary.stageC, color: '#f59e0b' },
      { key: '可执行', label: '可执行', count: summary.executable, color: C.green },
      { key: '观察', label: '观察', count: summary.observe, color: C.dim },
      { key: 'promoted_build', label: '后续转执行', count: summary.promoted, color: C.cyan },
      { key: 'watch_avoided_drop', label: '观察有效拦截', count: summary.avoided, color: C.green },
      { key: 'watch_too_early', label: '观察偏保守', count: summary.tooEarly, color: C.yellow },
      { key: 'fast_fail', label: '执行失效', count: summary.fail, color: C.red },
    ]
  }, [summary])

  const stageCBlockBuckets = useMemo(() => {
    const counts = new Map<string, number>()
    for (const row of baseRows) {
      const key = String(row.stage_c_block_type || '').trim()
      if (!key) continue
      counts.set(key, (counts.get(key) || 0) + 1)
    }
    return Array.from(counts.entries())
      .map(([key, count]) => ({ key, label: stageCBlockLabel(key), count }))
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count
        return stageCBlockPriority(b.key) - stageCBlockPriority(a.key)
      })
  }, [baseRows])

  const conceptRanking = useMemo(() => buildRankingRows(baseRows, conceptBoardOfRow, rankingSort), [baseRows, rankingSort])
  const industryRanking = useMemo(() => buildRankingRows(baseRows, industryOfRow, rankingSort), [baseRows, rankingSort])
  const stockRanking = useMemo(() => buildRankingRows(baseRows, row => `${row.ts_code} ${row.name}`, rankingSort), [baseRows, rankingSort])

  const clearFocus = () => {
    setConceptFocus('')
    setIndustryFocus('')
    setStockFocus('')
  }

  const exportRows = (filename: string, sourceRows: LeftReplayRow[]) => {
    downloadCsv(
      filename,
      [
        '时间', '股票代码', '股票名称', '阶段', '阶段原因', 'C段卡点', '概念', '行业', '概念生态',
        'A/B/C摘要', '触发价', '涨跌幅', '5m收益bps', '60m收益bps', '后续动作', '结论', '结论原因',
      ],
      sourceRows.map(row => [
        fmtDateTime(row.triggered_at),
        row.ts_code,
        row.name,
        row.stage_label,
        row.stage_reason,
        stageCBlockLabel(row.stage_c_block_type),
        conceptBoardOfRow(row),
        industryOfRow(row),
        conceptStateLabel(row.concept_state),
        buildStageSnapshotSummary(row),
        row.price,
        fmtPct(row.pct_chg),
        row.ret_5m_bps,
        row.ret_60m_bps,
        nextActionLabel(row),
        classifyLabel(row.classification),
        row.classification_reason,
      ]),
    )
  }

  const exportRanking = (filename: string, items: RankingRow[]) => {
    downloadCsv(
      filename,
      ['名称', '总样本', '后续转执行', '观察有效拦截', '观察偏保守', '执行有效', '执行失效', '边际一般', '成功率', '压力率', '主导C卡点', '示例股票'],
      items.map(item => [
        item.label,
        item.total,
        item.promoted,
        item.avoided,
        item.tooEarly,
        item.goodFollow,
        item.fail,
        item.mixed,
        `${(item.successRate * 100).toFixed(1)}%`,
        `${(item.pressureRate * 100).toFixed(1)}%`,
        item.dominantStageCBlockLabel,
        item.sampleTsCode ? `${item.sampleTsCode} ${item.sampleName || ''}` : '',
      ]),
    )
  }

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.text, padding: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <div>
          <div style={{ color: C.bright, fontSize: 28, fontWeight: 800 }}>Pool1 左侧回放</div>
          <div style={{ color: C.dim, marginTop: 6 }}>
            回放 left_side_buy 历史样本，重点看 B 未成、C 观察和行业/概念生态卡点。
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <button
            onClick={() => load()}
            style={{ display: 'inline-flex', alignItems: 'center', gap: 6, border: `1px solid ${C.border}`, background: C.card, color: C.text, borderRadius: 8, padding: '8px 12px', cursor: 'pointer' }}
          >
            <RefreshCw size={16} /> 刷新
          </button>
          <button
            onClick={() => exportRows(`pool1_left_replay_${Date.now()}.csv`, displayRows)}
            style={{ display: 'inline-flex', alignItems: 'center', gap: 6, border: `1px solid ${C.border}`, background: C.card, color: C.text, borderRadius: 8, padding: '8px 12px', cursor: 'pointer' }}
          >
            <Download size={16} /> 导出 CSV
          </button>
          {stageCBlockFilter !== 'all' && (
            <button
              onClick={() => exportRows(`pool1_left_replay_${stageCBlockFilter}_${Date.now()}.csv`, displayRows)}
              style={{ display: 'inline-flex', alignItems: 'center', gap: 6, border: `1px solid ${C.yellow}`, background: '#241d07', color: C.yellow, borderRadius: 8, padding: '8px 12px', cursor: 'pointer' }}
            >
              <Download size={16} /> 导出当前卡点视角
            </button>
          )}
        </div>
      </div>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginTop: 16 }}>
        <StatCard label="样本总数" value={summary.total} color={C.bright} />
        <StatCard label="B未成" value={summary.stageB} color={C.yellow} />
        <StatCard label="C观察" value={summary.stageC} color={'#f59e0b'} />
        <StatCard label="可执行" value={summary.executable} color={C.green} />
        <StatCard label="后续转执行" value={summary.promoted} color={C.cyan} />
        <StatCard label="观察有效拦截" value={summary.avoided} color={C.green} />
        <StatCard label="观察偏保守" value={summary.tooEarly} color={C.yellow} />
        <StatCard label="执行失效" value={summary.fail} color={C.red} />
      </div>

      <div style={{ marginTop: 16, background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <label style={{ color: C.dim }}>
            回放小时：
            <input
              type="number"
              min={12}
              max={240}
              value={hours}
              onChange={e => setHours(Math.max(12, Math.min(240, Number(e.target.value || 72))))}
              style={{ marginLeft: 8, width: 90, background: '#111625', color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '6px 8px' }}
            />
          </label>
          <button
            onClick={() => load(hours)}
            style={{ border: `1px solid ${C.border}`, background: '#111625', color: C.text, borderRadius: 8, padding: '7px 12px', cursor: 'pointer' }}
          >
            应用时窗
          </button>
          <input
            value={keyword}
            onChange={e => setKeyword(e.target.value)}
            placeholder="搜索代码 / 名称 / 概念 / 行业 / 消息"
            style={{ minWidth: 260, background: '#111625', color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, padding: '7px 10px' }}
          />
          {(conceptFocus || industryFocus || stockFocus || stageCBlockFilter !== 'all') && (
            <button
              onClick={() => {
                clearFocus()
                setStageCBlockFilter('all')
              }}
              style={{ border: `1px solid ${C.red}`, background: '#241415', color: '#fca5a5', borderRadius: 8, padding: '7px 12px', cursor: 'pointer' }}
            >
              清空聚合筛选
            </button>
          )}
        </div>

        <div style={{ marginTop: 12, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {(['all', 'B未成', 'C观察', '可执行', '观察'] as StageFilter[]).map(v => (
            <FilterChip key={v} active={stageFilter === v} onClick={() => setStageFilter(v)} label={v === 'all' ? '全部阶段' : v} color={stageColor(v)} />
          ))}
        </div>

        <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {([
            'all',
            'promoted_build',
            'watch_avoided_drop',
            'watch_too_early',
            'good_follow',
            'fast_fail',
            'mixed',
            'pending',
          ] as ClassificationFilter[]).map(v => (
            <FilterChip key={v} active={classificationFilter === v} onClick={() => setClassificationFilter(v)} label={v === 'all' ? '全部结果' : classifyLabel(v)} color={classifyColor(v)} />
          ))}
        </div>

        <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {([
            'all',
            'industry_joint_weak',
            'industry_retreat',
            'industry_weak',
            'concept_heat_cliff',
            'concept_retreat_main',
            'concept_weak_main',
            'high_position_catchdown',
            'distribution_structure',
          ] as StageCBlockFilter[]).map(v => (
            <FilterChip key={v} active={stageCBlockFilter === v} onClick={() => setStageCBlockFilter(v)} label={v === 'all' ? '全部C段卡点' : stageCBlockLabel(v)} color={v === 'all' ? C.cyan : C.red} />
          ))}
        </div>

        {(conceptFocus || industryFocus || stockFocus) && (
          <div style={{ marginTop: 12, color: C.dim }}>
            当前聚合视角：
            {conceptFocus ? ` 概念: ${conceptFocus}` : ''}
            {industryFocus ? ` 行业: ${industryFocus}` : ''}
            {stockFocus ? ` 个股: ${stockFocus}` : ''}
          </div>
        )}

        <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {([
            ['risk', '按风险优先'],
            ['ret_5m_desc', '按5m收益'],
            ['ret_60m_desc', '按60m收益'],
            ['action_desc', '按后续动作'],
            ['time_desc', '按时间倒序'],
          ] as Array<[DetailSortKey, string]>).map(([key, label]) => (
            <FilterChip
              key={key}
              active={detailSort === key}
              onClick={() => setDetailSort(key)}
              label={label}
              color={key === 'risk' ? C.red : key === 'time_desc' ? C.dim : C.cyan}
            />
          ))}
        </div>

        <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <FilterChip
            active={classificationFilter === 'promoted_build'}
            onClick={() => setClassificationFilter(classificationFilter === 'promoted_build' ? 'all' : 'promoted_build')}
            label={`只看后续转执行 ${rows.filter(r => r.classification === 'promoted_build').length}`}
            color={C.cyan}
          />
          <FilterChip
            active={classificationFilter === 'watch_avoided_drop'}
            onClick={() => setClassificationFilter(classificationFilter === 'watch_avoided_drop' ? 'all' : 'watch_avoided_drop')}
            label={`只看观察有效拦截 ${rows.filter(r => r.classification === 'watch_avoided_drop').length}`}
            color={C.green}
          />
          <FilterChip
            active={classificationFilter === 'fast_fail'}
            onClick={() => setClassificationFilter(classificationFilter === 'fast_fail' ? 'all' : 'fast_fail')}
            label={`只看执行失效 ${rows.filter(r => r.classification === 'fast_fail').length}`}
            color={C.red}
          />
          <FilterChip
            active={classificationFilter === 'watch_too_early'}
            onClick={() => setClassificationFilter(classificationFilter === 'watch_too_early' ? 'all' : 'watch_too_early')}
            label={`只看观察偏保守 ${rows.filter(r => r.classification === 'watch_too_early').length}`}
            color={C.yellow}
          />
        </div>

        <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <FilterChip
            active={actionChainFilter === 'has_exec_build'}
            onClick={() => setActionChainFilter(actionChainFilter === 'has_exec_build' ? 'all' : 'has_exec_build')}
            label={`只看有后续转执行 ${rows.filter(r => !!r.next_exec_build).length}`}
            color={C.cyan}
          />
          <FilterChip
            active={actionChainFilter === 'has_clear'}
            onClick={() => setActionChainFilter(actionChainFilter === 'has_clear' ? 'all' : 'has_clear')}
            label={`只看有后续清仓 ${rows.filter(r => !!r.next_clear).length}`}
            color={C.red}
          />
        </div>

        <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {([
            ['score', '榜单按综合分'],
            ['success', '榜单按成功率'],
            ['pressure', '榜单按失败压力'],
            ['samples', '榜单按样本数'],
          ] as Array<[RankingSortKey, string]>).map(([key, label]) => (
            <FilterChip
              key={key}
              active={rankingSort === key}
              onClick={() => setRankingSort(key)}
              label={label}
              color={key === 'pressure' ? C.red : key === 'samples' ? C.yellow : C.cyan}
            />
          ))}
        </div>
      </div>

      <div style={{ marginTop: 16, background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
        <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>阶段分桶统计</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 12 }}>
          {stageBuckets.map(item => (
            <div key={item.key} style={{ background: '#111625', border: `1px solid ${C.border}`, borderRadius: 10, padding: '10px 12px', minWidth: 126 }}>
              <div style={{ color: C.dim, fontSize: 12 }}>{item.label}</div>
              <div style={{ color: item.color, fontSize: 20, fontWeight: 800, marginTop: 6 }}>{item.count}</div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ marginTop: 16, background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>C段卡点统计</div>
          {stageCBlockFilter !== 'all' && (
            <div style={{ color: C.yellow }}>当前卡点视角：{stageCBlockLabel(stageCBlockFilter)}</div>
          )}
        </div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 12 }}>
          {stageCBlockBuckets.map(item => (
            <button
              key={item.key}
              onClick={() => {
                setStageCBlockFilter(prev => (prev === item.key ? 'all' : item.key as StageCBlockFilter))
                clearFocus()
                scrollToDetails()
              }}
              style={{
                borderRadius: 10,
                border: `1px solid ${stageCBlockFilter === item.key ? C.red : C.border}`,
                background: stageCBlockFilter === item.key ? '#2a1517' : '#111625',
                color: stageCBlockFilter === item.key ? '#fda4af' : C.text,
                padding: '10px 12px',
                minWidth: 138,
                cursor: 'pointer',
                textAlign: 'left',
              }}
            >
              <div style={{ color: stageCBlockFilter === item.key ? '#fca5a5' : C.dim, fontSize: 12 }}>{item.label}</div>
              <div style={{ color: stageCBlockFilter === item.key ? '#fca5a5' : C.red, fontSize: 20, fontWeight: 800, marginTop: 6 }}>{item.count}</div>
            </button>
          ))}
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: 16, marginTop: 16 }}>
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <div>
              <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>概念左侧表现榜</div>
              {actionChainFilter !== 'all' && (
                <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
                  当前动作链视角：
                  {actionChainFilter === 'has_exec_build' ? ' 后续转执行' : ' 后续清仓'}
                </div>
              )}
              {stageCBlockFilter !== 'all' && (
                <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
                  当前C段卡点：{stageCBlockLabel(stageCBlockFilter)}
                </div>
              )}
            </div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <button
                onClick={() => setConceptShowAll(v => !v)}
                style={{ border: `1px solid ${C.border}`, background: '#111625', color: C.text, borderRadius: 8, padding: '6px 10px', cursor: 'pointer' }}
              >
                {conceptShowAll ? '只看Top12' : '查看全部'}
              </button>
              <button
                onClick={() => exportRanking(`pool1_left_replay_concept_${Date.now()}.csv`, conceptRanking)}
                style={{ border: `1px solid ${C.border}`, background: '#111625', color: C.text, borderRadius: 8, padding: '6px 10px', cursor: 'pointer' }}
              >
                导出概念视角
              </button>
            </div>
          </div>
          <div style={{ marginTop: 12, display: 'grid', gap: 8 }}>
            {conceptRanking.slice(0, conceptShowAll ? conceptRanking.length : 12).map(item => (
              <button
                key={item.key}
                onClick={() => {
                  setConceptFocus(item.key)
                  setIndustryFocus('')
                  setStockFocus('')
                  scrollToDetails()
                }}
                style={{
                  border: `1px solid ${conceptFocus === item.key ? C.cyan : C.border}`,
                  background: conceptFocus === item.key ? 'rgba(0,200,200,0.10)' : '#111625',
                  borderRadius: 10,
                  padding: '10px 12px',
                  cursor: 'pointer',
                  textAlign: 'left',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <div style={{ color: C.bright, fontWeight: 700 }}>{item.label}</div>
                    {conceptFocus === item.key && (
                      <span style={{ color: C.cyan, border: `1px solid ${C.cyan}`, borderRadius: 999, padding: '1px 8px', fontSize: 11 }}>
                        已选
                      </span>
                    )}
                  </div>
                  <div style={{ color: item.score >= 0 ? C.green : C.red }}>{(item.score * 100).toFixed(1)}</div>
                </div>
                <div style={{ marginTop: 6, color: C.dim, fontSize: 12 }}>
                  总样本 {item.total} · 后续转执行 {item.promoted} · 观察有效拦截 {item.avoided} · 执行失效 {item.fail}
                </div>
                <div style={{ marginTop: 4, color: C.dim, fontSize: 12 }}>
                  成功率 {(item.successRate * 100).toFixed(1)}% · 主导C卡点 {item.dominantStageCBlockLabel}
                </div>
              </button>
            ))}
          </div>
        </div>

        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <div>
              <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>行业左侧表现榜</div>
              {actionChainFilter !== 'all' && (
                <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
                  当前动作链视角：
                  {actionChainFilter === 'has_exec_build' ? ' 后续转执行' : ' 后续清仓'}
                </div>
              )}
              {stageCBlockFilter !== 'all' && (
                <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
                  当前C段卡点：{stageCBlockLabel(stageCBlockFilter)}
                </div>
              )}
            </div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <button
                onClick={() => setIndustryShowAll(v => !v)}
                style={{ border: `1px solid ${C.border}`, background: '#111625', color: C.text, borderRadius: 8, padding: '6px 10px', cursor: 'pointer' }}
              >
                {industryShowAll ? '只看Top12' : '查看全部'}
              </button>
              <button
                onClick={() => exportRanking(`pool1_left_replay_industry_${Date.now()}.csv`, industryRanking)}
                style={{ border: `1px solid ${C.border}`, background: '#111625', color: C.text, borderRadius: 8, padding: '6px 10px', cursor: 'pointer' }}
              >
                导出行业视角
              </button>
            </div>
          </div>
          <div style={{ marginTop: 12, display: 'grid', gap: 8 }}>
            {industryRanking.slice(0, industryShowAll ? industryRanking.length : 12).map(item => (
              <button
                key={item.key}
                onClick={() => {
                  setIndustryFocus(item.key)
                  setConceptFocus('')
                  setStockFocus('')
                  scrollToDetails()
                }}
                style={{
                  border: `1px solid ${industryFocus === item.key ? C.cyan : C.border}`,
                  background: industryFocus === item.key ? 'rgba(0,200,200,0.10)' : '#111625',
                  borderRadius: 10,
                  padding: '10px 12px',
                  cursor: 'pointer',
                  textAlign: 'left',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <div style={{ color: C.bright, fontWeight: 700 }}>{item.label}</div>
                    {industryFocus === item.key && (
                      <span style={{ color: C.cyan, border: `1px solid ${C.cyan}`, borderRadius: 999, padding: '1px 8px', fontSize: 11 }}>
                        已选
                      </span>
                    )}
                  </div>
                  <div style={{ color: item.score >= 0 ? C.green : C.red }}>{(item.score * 100).toFixed(1)}</div>
                </div>
                <div style={{ marginTop: 6, color: C.dim, fontSize: 12 }}>
                  总样本 {item.total} · 后续转执行 {item.promoted} · 观察有效拦截 {item.avoided} · 执行失效 {item.fail}
                </div>
                <div style={{ marginTop: 4, color: C.dim, fontSize: 12 }}>
                  成功率 {(item.successRate * 100).toFixed(1)}% · 主导C卡点 {item.dominantStageCBlockLabel}
                </div>
              </button>
            ))}
          </div>
        </div>

        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>个股左侧表现榜</div>
            <button
              onClick={() => exportRanking(`pool1_left_replay_stock_${Date.now()}.csv`, stockRanking)}
              style={{ border: `1px solid ${C.border}`, background: '#111625', color: C.text, borderRadius: 8, padding: '6px 10px', cursor: 'pointer' }}
            >
              导出个股视角
            </button>
          </div>
          <div style={{ marginTop: 12, display: 'grid', gap: 8 }}>
            {stockRanking.slice(0, 12).map(item => (
              <div
                key={item.key}
                style={{
                  border: `1px solid ${stockFocus === (item.sampleTsCode || '') ? C.cyan : C.border}`,
                  background: stockFocus === (item.sampleTsCode || '') ? 'rgba(0,200,200,0.10)' : '#111625',
                  borderRadius: 10,
                  padding: '10px 12px',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <div style={{ color: C.bright, fontWeight: 700 }}>{item.label}</div>
                    {stockFocus === (item.sampleTsCode || '') && (
                      <span style={{ color: C.cyan, border: `1px solid ${C.cyan}`, borderRadius: 999, padding: '1px 8px', fontSize: 11 }}>
                        已选
                      </span>
                    )}
                  </div>
                  <button
                    onClick={() => {
                      if (item.sampleTsCode) {
                        setStockFocus(item.sampleTsCode)
                        setConceptFocus('')
                        setIndustryFocus('')
                        scrollToDetails()
                      }
                    }}
                    style={{ border: `1px solid ${C.border}`, background: '#1a2030', color: C.text, borderRadius: 8, padding: '4px 10px', cursor: 'pointer' }}
                  >
                    只看
                  </button>
                </div>
                <div style={{ marginTop: 6, color: C.dim, fontSize: 12 }}>
                  总样本 {item.total} · 后续转执行 {item.promoted} · 观察有效拦截 {item.avoided} · 执行失效 {item.fail}
                </div>
                <div style={{ marginTop: 4, color: C.dim, fontSize: 12 }}>
                  成功率 {(item.successRate * 100).toFixed(1)}% · 主导C卡点 {item.dominantStageCBlockLabel}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div ref={detailsRef} style={{ marginTop: 16, background: C.card, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>左侧样本明细</div>
            <div style={{ color: C.dim, marginTop: 4 }}>
              当前视角样本 {displayRows.length} 条
              {conceptFocus ? ` · 概念 ${conceptFocus}` : ''}
              {industryFocus ? ` · 行业 ${industryFocus}` : ''}
              {stockFocus ? ` · 个股 ${stockFocus}` : ''}
              {actionChainFilter === 'has_exec_build' ? ' · 动作链: 后续转执行' : ''}
              {actionChainFilter === 'has_clear' ? ' · 动作链: 后续清仓' : ''}
              {detailSort === 'risk' ? ' · 排序: 风险优先' : ''}
              {detailSort === 'ret_5m_desc' ? ' · 排序: 5m收益' : ''}
              {detailSort === 'ret_60m_desc' ? ' · 排序: 60m收益' : ''}
              {detailSort === 'action_desc' ? ' · 排序: 后续动作' : ''}
              {detailSort === 'time_desc' ? ' · 排序: 时间倒序' : ''}
            </div>
          </div>
          {loading && <div style={{ color: C.cyan }}>加载中...</div>}
        </div>

        <div style={{ overflowX: 'auto', marginTop: 12 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 1360 }}>
            <thead>
              <tr style={{ color: C.dim, fontSize: 12 }}>
                {['时间', '股票', '阶段', 'A/B/C摘要', 'C段卡点', '行业', '概念', '概念生态', '触发价', '5m', '60m', '后续动作', '结论'].map(h => (
                  <th key={h} style={{ textAlign: 'left', borderBottom: `1px solid ${C.border}`, padding: '8px 10px', whiteSpace: 'nowrap' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {displayRows.map(row => (
                <tr
                  key={String(row.id)}
                  onClick={() => setSelectedRow(row)}
                  title="点击查看历史快照详情"
                  style={{ borderBottom: `1px solid ${C.border}`, cursor: 'pointer' }}
                >
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: C.text }}>{fmtDateTime(row.triggered_at)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap' }}>
                    <div style={{ color: C.bright, fontWeight: 700 }}>{row.name}</div>
                    <Link
                      to={`/stock/${row.ts_code}`}
                      onClick={e => e.stopPropagation()}
                      style={{ color: C.cyan, textDecoration: 'none', fontSize: 12 }}
                    >
                      {row.ts_code}
                    </Link>
                  </td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: stageColor(row.stage_label) }}>{row.stage_label}</td>
                  <td style={{ padding: '10px', color: C.text, minWidth: 260 }}>{buildStageSnapshotSummary(row)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: stageCBlockPriority(row.stage_c_block_type) > 0 ? C.red : C.dim }}>
                    {stageCBlockLabel(row.stage_c_block_type)}
                  </td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: C.text }}>{industryOfRow(row)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: C.text }}>{conceptBoardOfRow(row)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: conceptStateColor(row.concept_state) }}>{conceptStateLabel(row.concept_state)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: C.text }}>{row.price > 0 ? row.price.toFixed(2) : '--'}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: (row.ret_5m_bps || 0) >= 0 ? C.green : C.red }}>{fmtBps(row.ret_5m_bps)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: (row.ret_60m_bps || 0) >= 0 ? C.green : C.red }}>{fmtBps(row.ret_60m_bps)}</td>
                  <td style={{ padding: '10px', whiteSpace: 'nowrap', color: C.text }}>{nextActionLabel(row)}</td>
                  <td style={{ padding: '10px', minWidth: 220 }}>
                    <div style={{ color: classifyColor(row.classification), fontWeight: 700 }}>{classifyLabel(row.classification)}</div>
                    <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>{row.classification_reason}</div>
                  </td>
                </tr>
              ))}
              {displayRows.length <= 0 && (
                <tr>
                  <td colSpan={13} style={{ padding: '24px 10px', textAlign: 'center', color: C.dim }}>
                    当前筛选下没有左侧样本
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {selectedRow && (
        <div
          onClick={() => setSelectedRow(null)}
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(0,0,0,0.58)',
            zIndex: 60,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: 18,
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              width: 'min(980px, 96vw)',
              maxHeight: '88vh',
              overflow: 'auto',
              background: C.card,
              border: `1px solid ${C.border}`,
              borderRadius: 10,
              boxShadow: '0 24px 80px rgba(0,0,0,0.42)',
            }}
          >
            <div
              style={{
                position: 'sticky',
                top: 0,
                background: C.card,
                borderBottom: `1px solid ${C.border}`,
                padding: '14px 16px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 12,
              }}
            >
              <div>
                <div style={{ color: C.bright, fontSize: 18, fontWeight: 800 }}>
                  {selectedRow.name} · {selectedRow.ts_code}
                </div>
                <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
                  {fmtDateTime(selectedRow.triggered_at)} · {selectedRow.stage_label} · {stageCBlockLabel(selectedRow.stage_c_block_type)}
                </div>
              </div>
              <button
                onClick={() => setSelectedRow(null)}
                style={{
                  width: 34,
                  height: 34,
                  borderRadius: 8,
                  border: `1px solid ${C.border}`,
                  background: '#111625',
                  color: C.text,
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  cursor: 'pointer',
                }}
                title="关闭"
              >
                <X size={18} />
              </button>
            </div>

            <div style={{ padding: 16, display: 'grid', gap: 14 }}>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10 }}>
                <StatCard label="阶段" value={selectedRow.stage_label} color={stageColor(selectedRow.stage_label)} />
                <StatCard label="C段卡点" value={stageCBlockLabel(selectedRow.stage_c_block_type)} color={stageCBlockPriority(selectedRow.stage_c_block_type) > 0 ? C.red : C.dim} />
                <StatCard label="5m收益" value={fmtBps(selectedRow.ret_5m_bps)} color={(selectedRow.ret_5m_bps || 0) >= 0 ? C.green : C.red} />
                <StatCard label="60m收益" value={fmtBps(selectedRow.ret_60m_bps)} color={(selectedRow.ret_60m_bps || 0) >= 0 ? C.green : C.red} />
              </div>

              <div style={{ background: '#111625', border: `1px solid ${C.border}`, borderRadius: 8, padding: 12 }}>
                <div style={{ color: C.dim, fontSize: 12 }}>A/B/C摘要</div>
                <div style={{ color: C.text, marginTop: 6 }}>{buildStageSnapshotSummary(selectedRow)}</div>
              </div>

              <div style={{ background: '#111625', border: `1px solid ${C.border}`, borderRadius: 8, padding: 12 }}>
                <div style={{ color: C.dim, fontSize: 12 }}>消息</div>
                <div style={{ color: C.text, marginTop: 6, whiteSpace: 'pre-wrap' }}>{selectedRow.message || '--'}</div>
              </div>

              <div style={{ background: '#111625', border: `1px solid ${C.border}`, borderRadius: 8, padding: 12 }}>
                <div style={{ color: C.dim, fontSize: 12 }}>历史快照 JSON</div>
                <pre style={{ color: C.text, margin: '8px 0 0', whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontSize: 12, lineHeight: 1.55 }}>
                  {JSON.stringify(selectedRow.history_snapshot || {}, null, 2)}
                </pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
