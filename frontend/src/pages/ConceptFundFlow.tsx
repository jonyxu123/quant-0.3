import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'
import { useNavigate } from 'react-router-dom'
import { RefreshCw, Search } from 'lucide-react'

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

interface ConceptFundFlowItem {
  rank: number
  concept_name: string
  pct_chg?: number
  main_net_inflow?: number
  main_net_inflow_ratio?: number
  inflow_amount?: number
  outflow_amount?: number
  company_count?: number
  leader_name?: string
  leader_ts_code?: string
  leader_pct?: number
  state?: string
}

type FlowSort = 'main_net_inflow' | 'main_net_inflow_ratio' | 'pct_chg' | 'leader_pct' | 'name'
type FlowStateFilter = 'all' | 'expand' | 'strong' | 'neutral' | 'weak' | 'retreat'
type FlowInflowFilter = 'all' | 'positive' | 'negative'

function fmtPct(v?: number | null): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '--'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function fmtFlowYi(v?: number | null): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '--'
  const abs = Math.abs(n)
  const sign = n >= 0 ? '+' : '-'
  return `${sign}${abs.toFixed(2)}亿`
}

function fmtInt(v?: number | null): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '--'
  return `${Math.round(n)}`
}

function stateLabel(state?: string): string {
  const s = String(state || '').toLowerCase()
  if (s === 'expand') return '扩张'
  if (s === 'strong') return '强势'
  if (s === 'weak') return '转弱'
  if (s === 'retreat') return '退潮'
  return '中性'
}

function stateColor(state?: string): string {
  const s = String(state || '').toLowerCase()
  if (s === 'expand') return C.red
  if (s === 'strong') return C.cyan
  if (s === 'weak') return C.yellow
  if (s === 'retreat') return C.green
  return C.dim
}

function sortRows(rows: ConceptFundFlowItem[], mode: FlowSort): ConceptFundFlowItem[] {
  const next = [...rows]
  next.sort((a, b) => {
    if (mode === 'name') return String(a.concept_name || '').localeCompare(String(b.concept_name || ''), 'zh-CN')
    if (mode === 'pct_chg') return Number(b.pct_chg || 0) - Number(a.pct_chg || 0)
    if (mode === 'leader_pct') return Number(b.leader_pct || 0) - Number(a.leader_pct || 0)
    if (mode === 'main_net_inflow_ratio') return Number(b.main_net_inflow_ratio || 0) - Number(a.main_net_inflow_ratio || 0)
    return Number(b.main_net_inflow || 0) - Number(a.main_net_inflow || 0)
  })
  return next
}

export default function ConceptFundFlow() {
  const navigate = useNavigate()
  const [rows, setRows] = useState<ConceptFundFlowItem[]>([])
  const [loading, setLoading] = useState(false)
  const [search, setSearch] = useState('')
  const [stateFilter, setStateFilter] = useState<FlowStateFilter>('all')
  const [inflowFilter, setInflowFilter] = useState<FlowInflowFilter>('all')
  const [sortMode, setSortMode] = useState<FlowSort>('main_net_inflow')
  const [fetchedAt, setFetchedAt] = useState('')
  const [errorText, setErrorText] = useState('')
  const [stale, setStale] = useState(false)
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const loadRows = async (forceRefresh = false, keyword = search) => {
    setLoading(true)
    try {
      const r = await axios.get(`${API}/api/realtime/concept_fund_flow`, {
        params: {
          q: keyword.trim(),
          limit: 800,
          force_refresh: forceRefresh,
        },
      })
      setRows((r.data?.data || []) as ConceptFundFlowItem[])
      setFetchedAt(String(r.data?.fetched_at || ''))
      setErrorText(String(r.data?.error || ''))
      setStale(!!r.data?.stale)
    } catch (e: any) {
      setErrorText(e?.response?.data?.detail || '加载概念资金流向失败')
      setStale(false)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadRows(false, '')
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    searchTimerRef.current = setTimeout(() => {
      void loadRows(false, search)
    }, 250)
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search])

  useEffect(() => {
    let cancelled = false
    const loop = async () => {
      if (cancelled) return
      try {
        await loadRows(false, search)
      } finally {
        if (!cancelled) pollTimerRef.current = setTimeout(loop, 60_000)
      }
    }
    pollTimerRef.current = setTimeout(loop, 60_000)
    return () => {
      cancelled = true
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search])

  const stateCounts = useMemo(() => {
    const counts: Record<FlowStateFilter, number> = {
      all: rows.length,
      expand: 0,
      strong: 0,
      neutral: 0,
      weak: 0,
      retreat: 0,
    }
    for (const row of rows) {
      const key = (String(row.state || '').toLowerCase() || 'neutral') as FlowStateFilter
      if (key in counts && key !== 'all') counts[key] += 1
      else counts.neutral += 1
    }
    return counts
  }, [rows])

  const inflowCounts = useMemo(() => {
    let positive = 0
    let negative = 0
    for (const row of rows) {
      const n = Number(row.main_net_inflow || 0)
      if (n >= 0) positive += 1
      else negative += 1
    }
    return { all: rows.length, positive, negative }
  }, [rows])

  const viewRows = useMemo(() => {
    const keyword = search.trim()
    const filtered = rows.filter(row => {
      if (stateFilter !== 'all' && String(row.state || '').toLowerCase() !== stateFilter) return false
      const mainNet = Number(row.main_net_inflow || 0)
      if (inflowFilter === 'positive' && mainNet < 0) return false
      if (inflowFilter === 'negative' && mainNet >= 0) return false
      if (!keyword) return true
      return String(row.concept_name || '').includes(keyword) || String(row.leader_name || '').includes(keyword)
    })
    return sortRows(filtered, sortMode)
  }, [rows, search, stateFilter, inflowFilter, sortMode])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      <div
        style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 8,
          padding: '14px 16px',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <div style={{ color: C.bright, fontSize: 15, fontWeight: 700 }}>东方财富概念板块资金流向</div>
          <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
            数据源: AKShare / 东方财富概念板块资金流向
            {fetchedAt ? ` · 更新: ${new Date(fetchedAt).toLocaleString()}` : ''}
            {stale && errorText ? ` · 使用缓存(${errorText})` : ''}
          </div>
        </div>
        <div style={{ flex: 1 }} />
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            background: C.bg,
            border: `1px solid ${C.border}`,
            borderRadius: 8,
            padding: '6px 10px',
            minWidth: 260,
          }}
        >
          <Search size={14} color={C.dim} />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="搜索概念或领涨股"
            style={{
              flex: 1,
              background: 'transparent',
              border: 'none',
              outline: 'none',
              color: C.text,
              fontSize: 12,
            }}
          />
        </div>
        <button
          type="button"
          onClick={() => void loadRows(true, search)}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 6,
            padding: '8px 12px',
            borderRadius: 8,
            border: `1px solid ${C.border}`,
            background: C.bg,
            color: C.bright,
            cursor: 'pointer',
          }}
        >
          <RefreshCw size={14} />
          {loading ? '刷新中…' : '强制刷新'}
        </button>
      </div>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
        {[
          ['all', `全部状态 ${stateCounts.all}`],
          ['expand', `扩张 ${stateCounts.expand}`],
          ['strong', `强势 ${stateCounts.strong}`],
          ['neutral', `中性 ${stateCounts.neutral}`],
          ['weak', `转弱 ${stateCounts.weak}`],
          ['retreat', `退潮 ${stateCounts.retreat}`],
        ].map(([key, label]) => {
          const active = stateFilter === key
          return (
            <button
              key={key}
              type="button"
              onClick={() => setStateFilter(key as FlowStateFilter)}
              style={{
                padding: '8px 14px',
                borderRadius: 999,
                border: `1px solid ${active ? `${C.cyan}aa` : C.border}`,
                background: active ? `${C.cyan}14` : C.card,
                color: active ? C.cyan : C.text,
                cursor: 'pointer',
              }}
            >
              {label}
            </button>
          )
        })}
        {[
          ['all', `全部流向 ${inflowCounts.all}`],
          ['positive', `主力净流入为正 ${inflowCounts.positive}`],
          ['negative', `主力净流入为负 ${inflowCounts.negative}`],
        ].map(([key, label]) => {
          const active = inflowFilter === key
          return (
            <button
              key={key}
              type="button"
              onClick={() => setInflowFilter(key as FlowInflowFilter)}
              style={{
                padding: '8px 14px',
                borderRadius: 999,
                border: `1px solid ${active ? `${C.yellow}aa` : C.border}`,
                background: active ? `${C.yellow}12` : C.card,
                color: active ? C.yellow : C.text,
                cursor: 'pointer',
              }}
            >
              {label}
            </button>
          )
        })}
      </div>

      <div
        style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 8,
          padding: '12px 14px',
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ color: C.dim, fontSize: 12 }}>排序</div>
        <select
          value={sortMode}
          onChange={e => setSortMode(e.target.value as FlowSort)}
          style={{
            background: C.bg,
            color: C.text,
            border: `1px solid ${C.border}`,
            borderRadius: 8,
            padding: '6px 10px',
          }}
        >
          <option value="main_net_inflow">按主力净流入</option>
          <option value="main_net_inflow_ratio">按主力净占比</option>
          <option value="pct_chg">按涨跌幅</option>
          <option value="leader_pct">按领涨股涨幅</option>
          <option value="name">按名称</option>
        </select>
      </div>

      <div
        style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 8,
          overflow: 'hidden',
        }}
      >
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '80px 220px 120px 110px 120px 120px 120px 120px 100px 160px 110px',
            gap: 0,
            padding: '12px 16px',
            color: C.dim,
            fontSize: 12,
            borderBottom: `1px solid ${C.border}`,
            background: 'rgba(255,255,255,0.02)',
          }}
        >
          <div>排名</div>
          <div>概念名称</div>
          <div>状态</div>
          <div>涨跌幅</div>
          <div>主力净占比</div>
          <div>流入资金</div>
          <div>流出资金</div>
          <div>净额</div>
          <div>公司家数</div>
          <div>领涨股</div>
          <div>领涨幅</div>
        </div>

        {viewRows.map(row => {
          const pctColor = Number(row.pct_chg || 0) >= 0 ? C.red : C.green
          return (
            <div
              key={`${row.concept_name}-${row.rank}`}
              style={{
                display: 'grid',
                gridTemplateColumns: '80px 220px 120px 110px 120px 120px 120px 120px 100px 160px 110px',
                gap: 0,
                padding: '12px 16px',
                alignItems: 'center',
                borderBottom: `1px solid ${C.border}`,
                color: C.text,
                fontSize: 13,
              }}
            >
              <div style={{ color: C.dim }}>{row.rank}</div>
              <div>
                <button
                  type="button"
                  onClick={() => navigate(`/concepts?concept=${encodeURIComponent(String(row.concept_name || ''))}`)}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    padding: 0,
                    color: C.bright,
                    fontWeight: 700,
                    cursor: 'pointer',
                    textAlign: 'left',
                  }}
                  title="查看该概念板块成分股"
                >
                  {row.concept_name}
                </button>
              </div>
              <div>
                <span
                  style={{
                    padding: '2px 8px',
                    borderRadius: 999,
                    border: `1px solid ${stateColor(row.state)}55`,
                    color: stateColor(row.state),
                    background: `${stateColor(row.state)}12`,
                    fontSize: 11,
                  }}
                >
                  {stateLabel(row.state)}
                </span>
              </div>
              <div style={{ color: pctColor, fontWeight: 600 }}>{fmtPct(row.pct_chg)}</div>
              <div style={{ color: Number(row.main_net_inflow_ratio || 0) >= 0 ? C.red : C.green }}>
                {fmtPct(row.main_net_inflow_ratio)}
              </div>
              <div style={{ color: Number(row.inflow_amount || 0) >= 0 ? C.red : C.green }}>
                {fmtFlowYi(row.inflow_amount)}
              </div>
              <div style={{ color: Number(row.outflow_amount || 0) >= 0 ? C.green : C.red }}>
                {fmtFlowYi(row.outflow_amount)}
              </div>
              <div style={{ color: Number(row.main_net_inflow || 0) >= 0 ? C.red : C.green }}>
                {fmtFlowYi(row.main_net_inflow)}
              </div>
              <div>{fmtInt(row.company_count)}</div>
              <div>
                {row.leader_ts_code ? (
                  <button
                    type="button"
                    onClick={() => navigate(`/stock/${encodeURIComponent(String(row.leader_ts_code || ''))}`)}
                    style={{
                      background: 'transparent',
                      border: 'none',
                      padding: 0,
                      color: C.cyan,
                      cursor: 'pointer',
                      textAlign: 'left',
                    }}
                  >
                    {row.leader_name || '--'}
                  </button>
                ) : (
                  row.leader_name || '--'
                )}
              </div>
              <div style={{ color: Number(row.leader_pct || 0) >= 0 ? C.red : C.green }}>{fmtPct(row.leader_pct)}</div>
            </div>
          )
        })}

        {!viewRows.length && !loading && (
          <div style={{ padding: 28, textAlign: 'center', color: C.dim }}>
            {errorText ? `加载失败：${errorText}` : '暂无概念资金流向数据'}
          </div>
        )}
      </div>
    </div>
  )
}
