import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'
import { Link } from 'react-router-dom'
import { Download, RefreshCw, Search } from 'lucide-react'

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

const SYMBOL_OPTIONS = [
  '火箭发射',
  '快速反弹',
  '大笔买入',
  '封涨停板',
  '打开跌停板',
  '有大买盘',
  '竞价上涨',
  '高开5日线',
  '向上缺口',
  '60日新高',
  '60日大幅上涨',
  '加速下跌',
  '高台跳水',
  '大笔卖出',
  '封跌停板',
  '打开涨停板',
  '有大卖盘',
  '竞价下跌',
  '低开5日线',
  '向下缺口',
  '60日新低',
  '60日大幅下跌',
] as const

interface MarketChangeItem {
  rank: number
  time: string
  code: string
  ts_code?: string
  name: string
  board?: string
  related_info?: string
  price?: number | null
  pct_chg?: number | null
  total_mv?: number | null
  circ_mv?: number | null
}

interface WatchlistItem {
  ts_code: string
}

interface PoolMemberItem {
  ts_code: string
}

type Notice = { type: 'success' | 'error'; text: string } | null
type MemberFilter = 'all' | 'watchlist' | 'pool1' | 'pool2' | 'untracked'
type DirectionFilter = 'all' | 'up' | 'down'
type SortMode = 'time' | 'pct_chg' | 'total_mv' | 'name'

function fmtPct(v?: number | null): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return '--'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function fmtPrice(v?: number | null): string {
  const n = Number(v)
  if (!(n > 0)) return '--'
  return n >= 100 ? n.toFixed(2) : n.toFixed(3)
}

function fmtMvYi(v?: number | null): string {
  const n = Number(v)
  if (!(n > 0)) return '--'
  const yi = n / 10000
  if (yi >= 1000) return `${yi.toFixed(0)}亿`
  if (yi >= 100) return `${yi.toFixed(1)}亿`
  return `${yi.toFixed(2)}亿`
}

function timeToNumber(v?: string): number {
  const text = String(v || '').trim()
  const m = text.match(/^(\d{1,2}):(\d{2}):(\d{2})$/)
  if (!m) return -1
  return Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3])
}

function toFiveMinuteBucketLabel(v?: string): string {
  const text = String(v || '').trim()
  const m = text.match(/^(\d{1,2}):(\d{2}):(\d{2})$/)
  if (!m) return '--'
  const hour = Number(m[1])
  const minute = Number(m[2])
  const bucket = Math.floor(minute / 5) * 5
  return `${String(hour).padStart(2, '0')}:${String(bucket).padStart(2, '0')}`
}

function sortRows(rows: MarketChangeItem[], mode: SortMode): MarketChangeItem[] {
  const next = [...rows]
  next.sort((a, b) => {
    if (mode === 'name') return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN')
    if (mode === 'pct_chg') return Number(b.pct_chg || 0) - Number(a.pct_chg || 0)
    if (mode === 'total_mv') return Number(b.total_mv || 0) - Number(a.total_mv || 0)
    return timeToNumber(String(b.time || '')) - timeToNumber(String(a.time || ''))
  })
  return next
}

export default function MarketChanges() {
  const [symbol, setSymbol] = useState<(typeof SYMBOL_OPTIONS)[number]>('大笔买入')
  const [rows, setRows] = useState<MarketChangeItem[]>([])
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(false)
  const [fetchedAt, setFetchedAt] = useState('')
  const [errorText, setErrorText] = useState('')
  const [stale, setStale] = useState(false)
  const [watchlistSet, setWatchlistSet] = useState<Set<string>>(new Set())
  const [poolMembers, setPoolMembers] = useState<Record<number, Set<string>>>({})
  const [memberFilter, setMemberFilter] = useState<MemberFilter>('all')
  const [directionFilter, setDirectionFilter] = useState<DirectionFilter>('all')
  const [sortMode, setSortMode] = useState<SortMode>('time')
  const [onlyActionable, setOnlyActionable] = useState(false)
  const [boardFilter, setBoardFilter] = useState('')
  const [selectedCodes, setSelectedCodes] = useState<Set<string>>(new Set())
  const [pendingAction, setPendingAction] = useState('')
  const [notice, setNotice] = useState<Notice>(null)
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const noticeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const setFlashNotice = (next: Notice) => {
    setNotice(next)
    if (noticeTimerRef.current) clearTimeout(noticeTimerRef.current)
    if (next) noticeTimerRef.current = setTimeout(() => setNotice(null), 2500)
  }

  const loadMembershipSets = async () => {
    try {
      const [watchR, pool1R, pool2R] = await Promise.all([
        axios.get(`${API}/api/watchlist/list`),
        axios.get(`${API}/api/realtime/pool/1/members`),
        axios.get(`${API}/api/realtime/pool/2/members`),
      ])
      setWatchlistSet(new Set(((watchR.data?.data || []) as WatchlistItem[]).map(x => String(x.ts_code || ''))))
      setPoolMembers({
        1: new Set(((pool1R.data?.data || []) as PoolMemberItem[]).map(x => String(x.ts_code || ''))),
        2: new Set(((pool2R.data?.data || []) as PoolMemberItem[]).map(x => String(x.ts_code || ''))),
      })
    } catch {
      // 保持静默，页面仍可展示盘口异动
    }
  }

  const loadRows = async (forceRefresh = false, nextSymbol = symbol, keyword = search) => {
    setLoading(true)
    try {
      const r = await axios.get(`${API}/api/realtime/market_changes`, {
        params: {
          symbol: nextSymbol,
          q: keyword.trim(),
          limit: 600,
          force_refresh: forceRefresh,
        },
      })
      setRows((r.data?.data || []) as MarketChangeItem[])
      setFetchedAt(String(r.data?.fetched_at || ''))
      setErrorText(String(r.data?.error || ''))
      setStale(!!r.data?.stale)
    } catch (e: any) {
      setErrorText(e?.response?.data?.detail || '加载盘口异动失败')
      setStale(false)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadRows(false, symbol, '')
    void loadMembershipSets()
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
      if (noticeTimerRef.current) clearTimeout(noticeTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    setSelectedCodes(new Set())
    setBoardFilter('')
    void loadRows(false, symbol, search)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol])

  useEffect(() => {
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    searchTimerRef.current = setTimeout(() => {
      void loadRows(false, symbol, search)
    }, 250)
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search, symbol])

  useEffect(() => {
    let cancelled = false
    const loop = async () => {
      if (cancelled) return
      try {
        await loadRows(false, symbol, search)
      } finally {
        if (!cancelled) pollTimerRef.current = setTimeout(loop, 5000)
      }
    }
    pollTimerRef.current = setTimeout(loop, 5000)
    return () => {
      cancelled = true
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, search])

  const statusCounts = useMemo(() => {
    let watchlist = 0
    let pool1 = 0
    let pool2 = 0
    let untracked = 0
    for (const row of rows) {
      const tsCode = String(row.ts_code || '')
      const inWatchlist = watchlistSet.has(tsCode)
      const inPool1 = !!poolMembers[1]?.has(tsCode)
      const inPool2 = !!poolMembers[2]?.has(tsCode)
      if (inWatchlist) watchlist += 1
      if (inPool1) pool1 += 1
      if (inPool2) pool2 += 1
      if (!inWatchlist && !inPool1 && !inPool2) untracked += 1
    }
    return { all: rows.length, watchlist, pool1, pool2, untracked }
  }, [rows, watchlistSet, poolMembers])

  const directionCounts = useMemo(() => {
    let up = 0
    let down = 0
    for (const row of rows) {
      const pct = Number(row.pct_chg)
      if (!Number.isFinite(pct)) continue
      if (pct >= 0) up += 1
      else down += 1
    }
    return { all: rows.length, up, down }
  }, [rows])

  const actionableCount = useMemo(() => {
    let count = 0
    for (const row of rows) {
      const tsCode = String(row.ts_code || '')
      if (!tsCode) continue
      const inWatchlist = watchlistSet.has(tsCode)
      const inPool1 = !!poolMembers[1]?.has(tsCode)
      const inPool2 = !!poolMembers[2]?.has(tsCode)
      if (!inWatchlist && !inPool1 && !inPool2) count += 1
    }
    return count
  }, [rows, watchlistSet, poolMembers])

  const viewRows = useMemo(() => {
    const filtered = rows.filter(row => {
      const tsCode = String(row.ts_code || '')
      const inWatchlist = watchlistSet.has(tsCode)
      const inPool1 = !!poolMembers[1]?.has(tsCode)
      const inPool2 = !!poolMembers[2]?.has(tsCode)
      const pct = Number(row.pct_chg)
      const board = String(row.board || '').trim()
      if (memberFilter === 'watchlist' && !inWatchlist) return false
      if (memberFilter === 'pool1' && !inPool1) return false
      if (memberFilter === 'pool2' && !inPool2) return false
      if (memberFilter === 'untracked' && (inWatchlist || inPool1 || inPool2)) return false
      if (directionFilter === 'up' && !(Number.isFinite(pct) && pct >= 0)) return false
      if (directionFilter === 'down' && !(Number.isFinite(pct) && pct < 0)) return false
      if (onlyActionable && (!tsCode || inWatchlist || inPool1 || inPool2)) return false
      if (boardFilter && board !== boardFilter) return false
      return true
    })
    return sortRows(filtered, sortMode)
  }, [rows, memberFilter, directionFilter, onlyActionable, boardFilter, sortMode, watchlistSet, poolMembers])

  const selectedVisibleCodes = useMemo(
    () => viewRows.map(item => String(item.ts_code || '')).filter(code => code && selectedCodes.has(code)),
    [viewRows, selectedCodes],
  )

  const allVisibleSelected = viewRows.length > 0 && selectedVisibleCodes.length === viewRows.filter(item => item.ts_code).length

  const toggleSelectCode = (tsCode: string, checked: boolean) => {
    setSelectedCodes(prev => {
      const next = new Set(prev)
      if (checked) next.add(tsCode)
      else next.delete(tsCode)
      return next
    })
  }

  const toggleSelectAllVisible = () => {
    setSelectedCodes(prev => {
      const next = new Set(prev)
      const visibleCodes = viewRows.map(item => String(item.ts_code || '')).filter(Boolean)
      if (allVisibleSelected) {
        for (const code of visibleCodes) next.delete(code)
      } else {
        for (const code of visibleCodes) next.add(code)
      }
      return next
    })
  }

  const clearSelection = () => setSelectedCodes(new Set())

  const addToWatchlist = async (row: MarketChangeItem) => {
    const tsCode = String(row.ts_code || '')
    const actionKey = `watch-${tsCode}`
    if (!tsCode || watchlistSet.has(tsCode) || pendingAction === actionKey) return
    setPendingAction(actionKey)
    try {
      const r = await axios.post(`${API}/api/watchlist/add`, {
        ts_code: tsCode,
        name: row.name,
        industry: row.board || '',
        source_strategy: `盘口异动:${symbol}`,
      })
      if (r.data?.ok === false) {
        setFlashNotice({ type: 'error', text: String(r.data?.msg || '加入自选失败') })
      } else {
        setWatchlistSet(prev => new Set(prev).add(tsCode))
        setFlashNotice({ type: 'success', text: `${row.name} 已加入自选` })
      }
    } catch (e: any) {
      setFlashNotice({ type: 'error', text: e?.response?.data?.detail || '加入自选失败' })
    } finally {
      setPendingAction('')
    }
  }

  const addToPool = async (row: MarketChangeItem, poolId: 1 | 2) => {
    const tsCode = String(row.ts_code || '')
    const actionKey = `pool-${poolId}-${tsCode}`
    if (!tsCode || poolMembers[poolId]?.has(tsCode) || pendingAction === actionKey) return
    setPendingAction(actionKey)
    try {
      const r = await axios.post(`${API}/api/realtime/pool/${poolId}/add`, {
        ts_code: tsCode,
        note: `盘口异动:${symbol}`,
      })
      if (r.data?.ok === false) {
        setFlashNotice({ type: 'error', text: String(r.data?.msg || `加入${poolId === 1 ? '择时监控' : 'T+0监控'}失败`) })
      } else {
        setPoolMembers(prev => {
          const next = { ...prev }
          next[poolId] = new Set(prev[poolId] || [])
          next[poolId].add(tsCode)
          return next
        })
        setFlashNotice({
          type: 'success',
          text: `${row.name} 已加入${poolId === 1 ? '择时监控' : 'T+0监控'}`,
        })
      }
    } catch (e: any) {
      setFlashNotice({
        type: 'error',
        text: e?.response?.data?.detail || `加入${poolId === 1 ? '择时监控' : 'T+0监控'}失败`,
      })
    } finally {
      setPendingAction('')
    }
  }

  const batchAdd = async (target: 'watchlist' | 'pool1' | 'pool2') => {
    const visibleSelected = viewRows.filter(item => {
      const tsCode = String(item.ts_code || '')
      return tsCode && selectedCodes.has(tsCode)
    })
    if (visibleSelected.length <= 0) {
      setFlashNotice({ type: 'error', text: '请先勾选要批量加入的股票' })
      return
    }
    const eligible = visibleSelected.filter(item => {
      const tsCode = String(item.ts_code || '')
      if (!tsCode) return false
      if (target === 'watchlist') return !watchlistSet.has(tsCode)
      if (target === 'pool1') return !poolMembers[1]?.has(tsCode)
      return !poolMembers[2]?.has(tsCode)
    })
    if (eligible.length <= 0) {
      setFlashNotice({ type: 'error', text: '当前勾选股票都已在目标列表中' })
      return
    }
    const actionKey = `batch-${target}`
    setPendingAction(actionKey)
    try {
      const results = await Promise.allSettled(
        eligible.map(item => {
          const tsCode = String(item.ts_code || '')
          if (target === 'watchlist') {
            return axios.post(`${API}/api/watchlist/add`, {
              ts_code: tsCode,
              name: item.name,
              industry: item.board || '',
              source_strategy: `盘口异动:${symbol}`,
            })
          }
          const poolId = target === 'pool1' ? 1 : 2
          return axios.post(`${API}/api/realtime/pool/${poolId}/add`, {
            ts_code: tsCode,
            note: `盘口异动:${symbol}`,
          })
        }),
      )
      let successCount = 0
      for (const r of results) {
        if (r.status === 'fulfilled' && r.value?.data?.ok !== false) successCount += 1
      }
      if (successCount > 0) {
        if (target === 'watchlist') {
          setWatchlistSet(prev => {
            const next = new Set(prev)
            for (const item of eligible) next.add(String(item.ts_code || ''))
            return next
          })
        } else {
          const poolId = target === 'pool1' ? 1 : 2
          setPoolMembers(prev => {
            const next = { ...prev }
            next[poolId] = new Set(prev[poolId] || [])
            for (const item of eligible) next[poolId].add(String(item.ts_code || ''))
            return next
          })
        }
      }
      setFlashNotice({
        type: successCount > 0 ? 'success' : 'error',
        text: successCount > 0
          ? `已批量加入 ${successCount} 只到${target === 'watchlist' ? '自选' : target === 'pool1' ? '择时监控' : 'T+0监控'}`
          : '批量加入失败',
      })
    } catch {
      setFlashNotice({ type: 'error', text: '批量加入失败' })
    } finally {
      setPendingAction('')
    }
  }

  const exportCSV = () => {
    if (viewRows.length <= 0) {
      setFlashNotice({ type: 'error', text: '当前筛选结果为空，无法导出' })
      return
    }
    const header = [
      '异动类型',
      '时间',
      '代码',
      'TS代码',
      '名称',
      '板块',
      '现价',
      '涨跌幅',
      '总市值(亿)',
      '流通市值(亿)',
      '相关信息',
      '已自选',
      '已择时',
      '已T+0',
    ]
    const rowsCsv = viewRows.map(row => {
      const tsCode = String(row.ts_code || '')
      const inWatchlist = watchlistSet.has(tsCode) ? '是' : '否'
      const inPool1 = poolMembers[1]?.has(tsCode) ? '是' : '否'
      const inPool2 = poolMembers[2]?.has(tsCode) ? '是' : '否'
      return [
        symbol,
        row.time || '',
        row.code || '',
        tsCode,
        row.name || '',
        row.board || '',
        row.price ?? '',
        row.pct_chg ?? '',
        row.total_mv ? (Number(row.total_mv) / 10000).toFixed(2) : '',
        row.circ_mv ? (Number(row.circ_mv) / 10000).toFixed(2) : '',
        row.related_info || '',
        inWatchlist,
        inPool1,
        inPool2,
      ]
        .map(v => `"${String(v).replace(/"/g, '""')}"`)
        .join(',')
    })
    const csv = `${header.join(',')}\n${rowsCsv.join('\n')}`
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    link.href = URL.createObjectURL(blob)
    link.download = `盘口异动_${symbol}_${new Date().toISOString().slice(0, 10)}.csv`
    link.click()
  }

  const filterOptions: Array<{ key: MemberFilter; label: string; count: number }> = [
    { key: 'all', label: '全部', count: statusCounts.all },
    { key: 'watchlist', label: '已自选', count: statusCounts.watchlist },
    { key: 'pool1', label: '已择时', count: statusCounts.pool1 },
    { key: 'pool2', label: '已T+0', count: statusCounts.pool2 },
    { key: 'untracked', label: '未加入', count: statusCounts.untracked },
  ]

  const directionOptions: Array<{ key: DirectionFilter; label: string; count: number }> = [
    { key: 'all', label: '全部方向', count: directionCounts.all },
    { key: 'up', label: '上涨异动', count: directionCounts.up },
    { key: 'down', label: '下跌异动', count: directionCounts.down },
  ]

  const boardGroupStats = useMemo(() => {
    const groups = new Map<string, { board: string; count: number; up: number; down: number; pctSum: number; actionable: number }>()
    for (const row of viewRows) {
      const board = String(row.board || '').trim() || '未标注板块'
      const pct = Number(row.pct_chg)
      const tsCode = String(row.ts_code || '')
      const inWatchlist = watchlistSet.has(tsCode)
      const inPool1 = !!poolMembers[1]?.has(tsCode)
      const inPool2 = !!poolMembers[2]?.has(tsCode)
      if (!groups.has(board)) {
        groups.set(board, { board, count: 0, up: 0, down: 0, pctSum: 0, actionable: 0 })
      }
      const item = groups.get(board)!
      item.count += 1
      if (Number.isFinite(pct)) {
        item.pctSum += pct
        if (pct >= 0) item.up += 1
        else item.down += 1
      }
      if (tsCode && !inWatchlist && !inPool1 && !inPool2) item.actionable += 1
    }
    return Array.from(groups.values())
      .map(item => ({
        ...item,
        avgPct: item.count > 0 ? item.pctSum / item.count : 0,
      }))
      .sort((a, b) => b.count - a.count || b.actionable - a.actionable || b.avgPct - a.avgPct)
      .slice(0, 8)
  }, [viewRows, watchlistSet, poolMembers])

  const timelineBuckets = useMemo(() => {
    const buckets = new Map<string, { label: string; count: number; up: number; down: number }>()
    for (const row of viewRows) {
      const label = toFiveMinuteBucketLabel(row.time)
      if (!label || label === '--') continue
      if (!buckets.has(label)) buckets.set(label, { label, count: 0, up: 0, down: 0 })
      const item = buckets.get(label)!
      item.count += 1
      const pct = Number(row.pct_chg)
      if (Number.isFinite(pct)) {
        if (pct >= 0) item.up += 1
        else item.down += 1
      }
    }
    return Array.from(buckets.values())
      .sort((a, b) => timeToNumber(`${a.label}:00`) - timeToNumber(`${b.label}:00`))
      .slice(-12)
  }, [viewRows])

  const timelineMax = useMemo(() => {
    let max = 0
    for (const item of timelineBuckets) {
      if (item.count > max) max = item.count
    }
    return max > 0 ? max : 1
  }, [timelineBuckets])

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
          <div style={{ color: C.bright, fontSize: 15, fontWeight: 700 }}>盘口异动</div>
          <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
            数据源: AKShare / 东方财富盘口异动 · 轮询: 5秒
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
            minWidth: 320,
          }}
        >
          <Search size={14} color={C.dim} />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="搜索代码、名称、板块、相关信息"
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
          onClick={() => {
            void loadRows(true, symbol, search)
            void loadMembershipSets()
          }}
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

      {notice && (
        <div
          style={{
            background: notice.type === 'success' ? 'rgba(34,197,94,0.10)' : 'rgba(239,68,68,0.10)',
            border: `1px solid ${notice.type === 'success' ? 'rgba(34,197,94,0.35)' : 'rgba(239,68,68,0.35)'}`,
            color: notice.type === 'success' ? C.green : C.red,
            padding: '10px 14px',
            borderRadius: 8,
            fontSize: 13,
          }}
        >
          {notice.text}
        </div>
      )}

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
        {SYMBOL_OPTIONS.map(item => {
          const active = item === symbol
          return (
            <button
              key={item}
              type="button"
              onClick={() => setSymbol(item)}
              style={{
                padding: '8px 14px',
                borderRadius: 999,
                border: `1px solid ${active ? `${C.cyan}aa` : C.border}`,
                background: active ? `${C.cyan}14` : C.card,
                color: active ? C.cyan : C.text,
                cursor: 'pointer',
                fontWeight: active ? 700 : 500,
              }}
            >
              {item}
            </button>
          )
        })}
      </div>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
        {filterOptions.map(item => {
          const active = memberFilter === item.key
          return (
            <button
              key={item.key}
              type="button"
              onClick={() => setMemberFilter(item.key)}
              style={{
                padding: '8px 14px',
                borderRadius: 999,
                border: `1px solid ${active ? `${C.yellow}aa` : C.border}`,
                background: active ? `${C.yellow}12` : C.card,
                color: active ? C.yellow : C.text,
                cursor: 'pointer',
              }}
            >
              {item.label} {item.count}
            </button>
          )
        })}
        {directionOptions.map(item => {
          const active = directionFilter === item.key
          return (
            <button
              key={item.key}
              type="button"
              onClick={() => setDirectionFilter(item.key)}
              style={{
                padding: '8px 14px',
                borderRadius: 999,
                border: `1px solid ${active ? `${C.red}aa` : C.border}`,
                background: active ? 'rgba(239,68,68,0.10)' : C.card,
                color: active ? C.red : C.text,
                cursor: 'pointer',
              }}
            >
              {item.label} {item.count}
            </button>
          )
        })}
        <button
          type="button"
          onClick={() => setOnlyActionable(v => !v)}
          style={{
            padding: '8px 14px',
            borderRadius: 999,
            border: `1px solid ${onlyActionable ? `${C.green}aa` : C.border}`,
            background: onlyActionable ? 'rgba(34,197,94,0.10)' : C.card,
            color: onlyActionable ? C.green : C.text,
            cursor: 'pointer',
          }}
        >
          只看未加入可操作 {actionableCount}
        </button>
        {boardFilter ? (
          <button
            type="button"
            onClick={() => setBoardFilter('')}
            style={{
              padding: '8px 14px',
              borderRadius: 999,
              border: `1px solid ${C.cyan}aa`,
              background: `${C.cyan}14`,
              color: C.cyan,
              cursor: 'pointer',
            }}
          >
            已选板块：{boardFilter} · 清除
          </button>
        ) : null}
      </div>

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(4, minmax(0, 1fr))',
          gap: 12,
        }}
      >
        {[
          { label: '当前异动类型', value: symbol, color: C.cyan },
          { label: '记录数', value: String(rows.length), color: C.bright },
          { label: '上涨股', value: String(directionCounts.up), color: C.red },
          { label: '下跌股', value: String(directionCounts.down), color: C.green },
        ].map(item => (
          <div
            key={item.label}
            style={{
              background: C.card,
              border: `1px solid ${C.border}`,
              borderRadius: 8,
              padding: '12px 14px',
            }}
          >
            <div style={{ color: C.dim, fontSize: 12 }}>{item.label}</div>
            <div style={{ color: item.color, fontSize: 18, fontWeight: 700, marginTop: 4 }}>{item.value}</div>
          </div>
        ))}
      </div>

      <div
        style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 8,
          padding: '12px 14px',
          display: 'flex',
          flexDirection: 'column',
          gap: 10,
        }}
      >
        <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>板块分组统计</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 10 }}>
          {boardGroupStats.map(item => {
            const avgColor = item.avgPct >= 0 ? C.red : C.green
            const active = boardFilter === item.board
            return (
              <button
                key={item.board}
                type="button"
                onClick={() => setBoardFilter(prev => (prev === item.board ? '' : item.board))}
                style={{
                  background: C.bg,
                  border: `1px solid ${active ? `${C.cyan}aa` : C.border}`,
                  borderRadius: 8,
                  padding: '10px 12px',
                  cursor: 'pointer',
                  textAlign: 'left',
                }}
              >
                <div style={{ color: C.bright, fontSize: 13, fontWeight: 700, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {item.board}
                </div>
                <div style={{ color: C.dim, fontSize: 12, marginTop: 6 }}>
                  异动 {item.count} · 上涨 {item.up} · 下跌 {item.down}
                </div>
                <div style={{ color: avgColor, fontSize: 13, fontWeight: 600, marginTop: 4 }}>
                  平均涨跌 {item.avgPct >= 0 ? '+' : ''}{item.avgPct.toFixed(2)}%
                </div>
                <div style={{ color: item.actionable > 0 ? C.green : C.dim, fontSize: 12, marginTop: 4 }}>
                  可操作 {item.actionable}
                </div>
              </button>
            )
          })}
          {boardGroupStats.length <= 0 && (
            <div style={{ color: C.dim, fontSize: 12 }}>当前筛选结果暂无可统计板块</div>
          )}
        </div>
      </div>

      <div
        style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 8,
          padding: '12px 14px',
          display: 'flex',
          flexDirection: 'column',
          gap: 10,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>最近异动堆积</div>
          <div style={{ color: C.dim, fontSize: 12 }}>
            {boardFilter ? `当前板块：${boardFilter}` : '当前筛选视角'} · 5分钟分桶
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(12, minmax(0, 1fr))', gap: 8, alignItems: 'end', minHeight: 120 }}>
          {timelineBuckets.map(item => {
            const upRatio = item.count > 0 ? item.up / item.count : 0
            const barHeight = Math.max(18, Math.round((item.count / timelineMax) * 88))
            return (
              <div key={item.label} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6 }}>
                <div style={{ color: C.dim, fontSize: 11 }}>{item.count}</div>
                <div
                  title={`${item.label} · 异动${item.count} · 上涨${item.up} · 下跌${item.down}`}
                  style={{
                    width: '100%',
                    height: barHeight,
                    borderRadius: 8,
                    border: `1px solid ${C.border}`,
                    background: `linear-gradient(180deg, ${C.red} ${Math.round(upRatio * 100)}%, ${C.green} ${Math.round(upRatio * 100)}%)`,
                    opacity: 0.9,
                  }}
                />
                <div style={{ color: C.text, fontSize: 11 }}>{item.label}</div>
              </div>
            )
          })}
          {timelineBuckets.length <= 0 && (
            <div style={{ color: C.dim, fontSize: 12 }}>当前筛选结果暂无可展示的异动时间分布</div>
          )}
        </div>
      </div>

      <div
        style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 8,
          padding: '12px 14px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 12,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ color: C.dim, fontSize: 12 }}>
          当前显示 {viewRows.length} 条
          {selectedVisibleCodes.length > 0 ? ` · 已勾选 ${selectedVisibleCodes.length} 条` : ''}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <select
            value={sortMode}
            onChange={e => setSortMode(e.target.value as SortMode)}
            style={{
              padding: '5px 10px',
              borderRadius: 8,
              border: `1px solid ${C.border}`,
              background: C.bg,
              color: C.text,
              fontSize: 12,
            }}
          >
            <option value="time">按异动时间</option>
            <option value="pct_chg">按涨跌幅</option>
            <option value="total_mv">按总市值</option>
            <option value="name">按名称</option>
          </select>
          <button
            type="button"
            onClick={toggleSelectAllVisible}
            disabled={viewRows.filter(item => item.ts_code).length <= 0}
            style={{
              padding: '5px 10px',
              borderRadius: 999,
              border: `1px solid ${C.border}`,
              background: C.bg,
              color: viewRows.filter(item => item.ts_code).length > 0 ? C.text : C.dim,
              cursor: viewRows.filter(item => item.ts_code).length > 0 ? 'pointer' : 'default',
              fontSize: 11,
            }}
          >
            {allVisibleSelected ? '取消全选当前筛选' : '全选当前筛选'}
          </button>
          <button
            type="button"
            onClick={clearSelection}
            disabled={selectedCodes.size <= 0}
            style={{
              padding: '5px 10px',
              borderRadius: 999,
              border: `1px solid ${C.border}`,
              background: C.bg,
              color: selectedCodes.size > 0 ? C.text : C.dim,
              cursor: selectedCodes.size > 0 ? 'pointer' : 'default',
              fontSize: 11,
            }}
          >
            清空勾选
          </button>
          <button
            type="button"
            onClick={() => void batchAdd('watchlist')}
            disabled={selectedVisibleCodes.length <= 0 || pendingAction === 'batch-watchlist'}
            style={{
              padding: '5px 10px',
              borderRadius: 999,
              border: `1px solid ${C.yellow}`,
              background: 'rgba(234,179,8,0.10)',
              color: C.yellow,
              fontSize: 11,
              opacity: selectedVisibleCodes.length > 0 ? 1 : 0.55,
            }}
          >
            批量加入自选
          </button>
          <button
            type="button"
            onClick={() => void batchAdd('pool1')}
            disabled={selectedVisibleCodes.length <= 0 || pendingAction === 'batch-pool1'}
            style={{
              padding: '5px 10px',
              borderRadius: 999,
              border: `1px solid ${C.red}`,
              background: 'rgba(239,68,68,0.10)',
              color: C.red,
              fontSize: 11,
              opacity: selectedVisibleCodes.length > 0 ? 1 : 0.55,
            }}
          >
            批量加入择时
          </button>
          <button
            type="button"
            onClick={() => void batchAdd('pool2')}
            disabled={selectedVisibleCodes.length <= 0 || pendingAction === 'batch-pool2'}
            style={{
              padding: '5px 10px',
              borderRadius: 999,
              border: `1px solid ${C.cyan}`,
              background: 'rgba(0,200,200,0.10)',
              color: C.cyan,
              fontSize: 11,
              opacity: selectedVisibleCodes.length > 0 ? 1 : 0.55,
            }}
          >
            批量加入T+0
          </button>
          <button
            type="button"
            onClick={exportCSV}
            disabled={viewRows.length <= 0}
            style={{
              padding: '5px 10px',
              borderRadius: 999,
              border: `1px solid ${C.border}`,
              background: C.bg,
              color: viewRows.length > 0 ? C.text : C.dim,
              cursor: viewRows.length > 0 ? 'pointer' : 'default',
              fontSize: 11,
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
            }}
          >
            <Download size={12} />
            导出当前筛选 CSV
          </button>
        </div>
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
            gridTemplateColumns: '54px 70px 90px 90px 150px 100px 90px 90px 110px 1fr 270px',
            gap: 0,
            padding: '12px 16px',
            color: C.dim,
            fontSize: 12,
            borderBottom: `1px solid ${C.border}`,
            background: 'rgba(255,255,255,0.02)',
            alignItems: 'center',
          }}
        >
          <div style={{ textAlign: 'center' }}>
            <input
              type="checkbox"
              checked={allVisibleSelected}
              onChange={toggleSelectAllVisible}
              disabled={viewRows.filter(item => item.ts_code).length <= 0}
              style={{ cursor: viewRows.filter(item => item.ts_code).length > 0 ? 'pointer' : 'default' }}
            />
          </div>
          <div>排名</div>
          <div>时间</div>
          <div>代码</div>
          <div>名称</div>
          <div>板块</div>
          <div>现价</div>
          <div>涨跌幅</div>
          <div>总市值</div>
          <div>相关信息</div>
          <div style={{ textAlign: 'right' }}>操作</div>
        </div>

        {viewRows.map(row => {
          const pctColor = Number(row.pct_chg || 0) >= 0 ? C.red : C.green
          const tsCode = String(row.ts_code || '')
          const inWatchlist = watchlistSet.has(tsCode)
          const inPool1 = !!poolMembers[1]?.has(tsCode)
          const inPool2 = !!poolMembers[2]?.has(tsCode)
          const checked = !!tsCode && selectedCodes.has(tsCode)
          return (
            <div
              key={`${row.code}-${row.time}-${row.rank}`}
              style={{
                display: 'grid',
                gridTemplateColumns: '54px 70px 90px 90px 150px 100px 90px 90px 110px 1fr 270px',
                gap: 0,
                padding: '12px 16px',
                alignItems: 'center',
                borderBottom: `1px solid ${C.border}`,
                color: C.text,
                fontSize: 13,
              }}
            >
              <div style={{ textAlign: 'center' }}>
                <input
                  type="checkbox"
                  checked={checked}
                  disabled={!tsCode}
                  onChange={e => toggleSelectCode(tsCode, e.target.checked)}
                  style={{ cursor: tsCode ? 'pointer' : 'default' }}
                />
              </div>
              <div style={{ color: C.dim }}>{row.rank}</div>
              <div>{row.time || '--'}</div>
              <div>{row.code || '--'}</div>
              <div style={{ fontWeight: 700 }}>
                {tsCode ? (
                  <Link to={`/stock/${encodeURIComponent(tsCode)}`} style={{ color: C.cyan, textDecoration: 'none' }}>
                    {row.name || '--'}
                  </Link>
                ) : (
                  row.name || '--'
                )}
              </div>
              <div style={{ color: C.dim }}>{row.board || '--'}</div>
              <div>{fmtPrice(row.price)}</div>
              <div style={{ color: pctColor, fontWeight: 600 }}>{fmtPct(row.pct_chg)}</div>
              <div>{fmtMvYi(row.total_mv)}</div>
              <div style={{ color: C.bright, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {row.related_info || '--'}
              </div>
              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 6, flexWrap: 'wrap' }}>
                <button
                  type="button"
                  disabled={!tsCode || inWatchlist || pendingAction === `watch-${tsCode}`}
                  onClick={() => void addToWatchlist(row)}
                  style={{
                    padding: '6px 10px',
                    borderRadius: 6,
                    border: `1px solid ${inWatchlist ? C.yellow : C.border}`,
                    background: inWatchlist ? 'rgba(234,179,8,0.08)' : 'transparent',
                    color: inWatchlist ? C.yellow : C.text,
                    cursor: !tsCode || inWatchlist ? 'default' : 'pointer',
                    fontSize: 11,
                    opacity: tsCode ? 1 : 0.45,
                  }}
                >
                  {inWatchlist ? '已自选' : '加入自选'}
                </button>
                <button
                  type="button"
                  disabled={!tsCode || inPool1 || pendingAction === `pool-1-${tsCode}`}
                  onClick={() => void addToPool(row, 1)}
                  style={{
                    padding: '6px 10px',
                    borderRadius: 6,
                    border: `1px solid ${inPool1 ? C.cyan : C.border}`,
                    background: inPool1 ? 'rgba(0,200,200,0.10)' : 'transparent',
                    color: inPool1 ? C.cyan : C.text,
                    cursor: !tsCode || inPool1 ? 'default' : 'pointer',
                    fontSize: 11,
                    opacity: tsCode ? 1 : 0.45,
                  }}
                >
                  {inPool1 ? '已择时' : '加入择时监控'}
                </button>
                <button
                  type="button"
                  disabled={!tsCode || inPool2 || pendingAction === `pool-2-${tsCode}`}
                  onClick={() => void addToPool(row, 2)}
                  style={{
                    padding: '6px 10px',
                    borderRadius: 6,
                    border: `1px solid ${inPool2 ? C.green : C.border}`,
                    background: inPool2 ? 'rgba(34,197,94,0.10)' : 'transparent',
                    color: inPool2 ? C.green : C.text,
                    cursor: !tsCode || inPool2 ? 'default' : 'pointer',
                    fontSize: 11,
                    opacity: tsCode ? 1 : 0.45,
                  }}
                >
                  {inPool2 ? '已T+0' : '加入T+0监控'}
                </button>
              </div>
            </div>
          )
        })}

        {!viewRows.length && !loading && (
          <div style={{ padding: 28, textAlign: 'center', color: C.dim }}>
            {errorText ? `加载失败：${errorText}` : '暂无盘口异动数据'}
          </div>
        )}
      </div>
    </div>
  )
}
