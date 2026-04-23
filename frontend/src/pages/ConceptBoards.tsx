import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'
import { Link, useLocation } from 'react-router-dom'
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
  cyanBg: 'rgba(0,200,200,0.08)',
  red: '#ef4444',
  green: '#22c55e',
  yellow: '#eab308',
}

interface ConceptBoardItem {
  concept_name: string
  score?: number
  state?: string
  pct_chg?: number
  member_count?: number
  up_count?: number
  down_count?: number
  leader_pct?: number
  turnover?: number
}

interface ConceptBoardMember {
  ts_code: string
  symbol?: string
  name: string
  industry?: string
  market?: string
  board_segment?: string
  price?: number
  pct_chg?: number
  total_mv?: number
  circ_mv?: number
  price_source?: string
}

interface WatchlistItem {
  ts_code: string
}

interface PoolMemberItem {
  ts_code: string
}

type Notice = { type: 'success' | 'error'; text: string } | null
type BoardSort = 'score' | 'pct_chg' | 'member_count' | 'name'
type BoardStateFilter = 'all' | 'expand' | 'strong' | 'neutral' | 'weak' | 'retreat'
type MemberFilter = 'all' | 'watchlist' | 'pool1' | 'pool2' | 'untracked'
type MemberSort = 'total_mv' | 'pct_chg' | 'price' | 'name'

function fmtPct(v?: number | null): string {
  const n = Number(v || 0)
  if (!Number.isFinite(n)) return '--'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function fmtPrice(v?: number | null): string {
  const n = Number(v || 0)
  if (!(n > 0)) return '--'
  return n >= 100 ? n.toFixed(2) : n.toFixed(3)
}

function fmtMvYi(v?: number | null): string {
  const n = Number(v || 0)
  if (!(n > 0)) return '--'
  const yi = n / 10000
  if (yi >= 1000) return `${yi.toFixed(0)}亿`
  if (yi >= 100) return `${yi.toFixed(1)}亿`
  return `${yi.toFixed(2)}亿`
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

function sortBoards(rows: ConceptBoardItem[], mode: BoardSort): ConceptBoardItem[] {
  const next = [...rows]
  next.sort((a, b) => {
    if (mode === 'name') return String(a.concept_name || '').localeCompare(String(b.concept_name || ''), 'zh-CN')
    if (mode === 'member_count') return Number(b.member_count || 0) - Number(a.member_count || 0)
    if (mode === 'pct_chg') return Number(b.pct_chg || 0) - Number(a.pct_chg || 0)
    return Number(b.score || 0) - Number(a.score || 0)
  })
  return next
}

function sortMembers(rows: ConceptBoardMember[], mode: MemberSort): ConceptBoardMember[] {
  const next = [...rows]
  next.sort((a, b) => {
    if (mode === 'name') return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN')
    if (mode === 'price') return Number(b.price || 0) - Number(a.price || 0)
    if (mode === 'pct_chg') return Number(b.pct_chg || 0) - Number(a.pct_chg || 0)
    return Number(b.total_mv || 0) - Number(a.total_mv || 0)
  })
  return next
}

export default function ConceptBoards() {
  const location = useLocation()
  const [boards, setBoards] = useState<ConceptBoardItem[]>([])
  const [boardsLoading, setBoardsLoading] = useState(false)
  const [selectedConcept, setSelectedConcept] = useState('')
  const [boardSearch, setBoardSearch] = useState('')
  const [boardSort, setBoardSort] = useState<BoardSort>('score')
  const [boardStateFilter, setBoardStateFilter] = useState<BoardStateFilter>('all')
  const [memberSearch, setMemberSearch] = useState('')
  const [memberFilter, setMemberFilter] = useState<MemberFilter>('all')
  const [memberSort, setMemberSort] = useState<MemberSort>('total_mv')
  const [boardMeta, setBoardMeta] = useState<ConceptBoardItem | null>(null)
  const [members, setMembers] = useState<ConceptBoardMember[]>([])
  const [membersLoading, setMembersLoading] = useState(false)
  const [watchlistSet, setWatchlistSet] = useState<Set<string>>(new Set())
  const [poolMembers, setPoolMembers] = useState<Record<number, Set<string>>>({})
  const [selectedCodes, setSelectedCodes] = useState<Set<string>>(new Set())
  const [pendingAction, setPendingAction] = useState('')
  const [notice, setNotice] = useState<Notice>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const conceptFromQuery = useMemo(() => {
    try {
      return String(new URLSearchParams(location.search).get('concept') || '').trim()
    } catch {
      return ''
    }
  }, [location.search])

  const loadBoards = async (keyword = boardSearch) => {
    setBoardsLoading(true)
    try {
      const r = await axios.get(`${API}/api/realtime/concept_boards`, {
        params: {
          q: keyword.trim(),
          limit: 1000,
        },
      })
      const rows = (r.data?.data || []) as ConceptBoardItem[]
      setBoards(rows)
      setSelectedConcept(prev => {
        if (conceptFromQuery && rows.some(x => x.concept_name === conceptFromQuery)) return conceptFromQuery
        if (prev && rows.some(x => x.concept_name === prev)) return prev
        return rows[0]?.concept_name || ''
      })
    } catch (e: any) {
      setNotice({ type: 'error', text: e?.response?.data?.detail || '加载概念板块失败' })
    } finally {
      setBoardsLoading(false)
    }
  }

  const loadMembers = async (conceptName: string) => {
    if (!conceptName) {
      setBoardMeta(null)
      setMembers([])
      return
    }
    setMembersLoading(true)
    try {
      const r = await axios.get(`${API}/api/realtime/concept_board/members`, {
        params: {
          concept_name: conceptName,
          limit: 600,
        },
      })
      setBoardMeta((r.data?.board || null) as ConceptBoardItem | null)
      setMembers((r.data?.data || []) as ConceptBoardMember[])
    } catch (e: any) {
      setBoardMeta(null)
      setMembers([])
      setNotice({ type: 'error', text: e?.response?.data?.detail || '加载概念成分股失败' })
    } finally {
      setMembersLoading(false)
    }
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
      // keep silent, page can still work
    }
  }

  useEffect(() => {
    void loadBoards('')
    void loadMembershipSets()
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    setSelectedCodes(new Set())
    void loadMembers(selectedConcept)
  }, [selectedConcept])

  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => {
      void loadBoards(boardSearch)
    }, 250)
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [boardSearch])

  useEffect(() => {
    if (!conceptFromQuery) return
    setBoardSearch(conceptFromQuery)
    setSelectedConcept(conceptFromQuery)
  }, [conceptFromQuery])

  const boardStateCounts = useMemo(() => {
    const counts: Record<BoardStateFilter, number> = {
      all: boards.length,
      expand: 0,
      strong: 0,
      neutral: 0,
      weak: 0,
      retreat: 0,
    }
    for (const item of boards) {
      const key = (String(item.state || '').toLowerCase() || 'neutral') as BoardStateFilter
      if (key in counts && key !== 'all') counts[key] += 1
      else counts.neutral += 1
    }
    return counts
  }, [boards])

  const sortedBoards = useMemo(() => {
    const filtered = boards.filter(item => {
      if (boardStateFilter === 'all') return true
      return String(item.state || '').toLowerCase() === boardStateFilter
    })
    return sortBoards(filtered, boardSort)
  }, [boardSort, boardStateFilter, boards])

  const memberStatusCounts = useMemo(() => {
    let watchlist = 0
    let pool1 = 0
    let pool2 = 0
    let untracked = 0
    for (const item of members) {
      const inWatchlist = watchlistSet.has(item.ts_code)
      const inPool1 = !!poolMembers[1]?.has(item.ts_code)
      const inPool2 = !!poolMembers[2]?.has(item.ts_code)
      if (inWatchlist) watchlist += 1
      if (inPool1) pool1 += 1
      if (inPool2) pool2 += 1
      if (!inWatchlist && !inPool1 && !inPool2) untracked += 1
    }
    return {
      all: members.length,
      watchlist,
      pool1,
      pool2,
      untracked,
    }
  }, [members, watchlistSet, poolMembers])

  const filteredMembers = useMemo(() => {
    const kw = memberSearch.trim().toLowerCase()
    const filtered = members.filter(item => {
      if (kw) {
        const match =
          String(item.ts_code || '').toLowerCase().includes(kw) ||
          String(item.symbol || '').toLowerCase().includes(kw) ||
          String(item.name || '').toLowerCase().includes(kw) ||
          String(item.industry || '').toLowerCase().includes(kw)
        if (!match) return false
      }
      const inWatchlist = watchlistSet.has(item.ts_code)
      const inPool1 = !!poolMembers[1]?.has(item.ts_code)
      const inPool2 = !!poolMembers[2]?.has(item.ts_code)
      if (memberFilter === 'watchlist') return inWatchlist
      if (memberFilter === 'pool1') return inPool1
      if (memberFilter === 'pool2') return inPool2
      if (memberFilter === 'untracked') return !inWatchlist && !inPool1 && !inPool2
      return true
    })
    return sortMembers(filtered, memberSort)
  }, [memberFilter, memberSearch, memberSort, members, poolMembers, watchlistSet])

  const selectedVisibleCodes = useMemo(
    () => filteredMembers.map(item => item.ts_code).filter(code => selectedCodes.has(code)),
    [filteredMembers, selectedCodes],
  )

  const allVisibleSelected = filteredMembers.length > 0 && selectedVisibleCodes.length === filteredMembers.length

  const setFlashNotice = (next: Notice) => {
    setNotice(next)
    window.setTimeout(() => {
      setNotice(cur => (cur === next ? null : cur))
    }, 2500)
  }

  const addToWatchlist = async (item: ConceptBoardMember) => {
    const actionKey = `watch-${item.ts_code}`
    if (watchlistSet.has(item.ts_code) || pendingAction === actionKey) return
    setPendingAction(actionKey)
    try {
      const r = await axios.post(`${API}/api/watchlist/add`, {
        ts_code: item.ts_code,
        name: item.name,
        industry: item.industry,
        source_strategy: `概念板块:${selectedConcept}`,
      })
      if (r.data?.ok === false) {
        setFlashNotice({ type: 'error', text: String(r.data?.msg || '加入自选失败') })
      } else {
        setWatchlistSet(prev => new Set(prev).add(item.ts_code))
        setFlashNotice({ type: 'success', text: `${item.name} 已加入自选` })
      }
    } catch (e: any) {
      setFlashNotice({ type: 'error', text: e?.response?.data?.detail || '加入自选失败' })
    } finally {
      setPendingAction('')
    }
  }

  const addToPool = async (item: ConceptBoardMember, poolId: 1 | 2) => {
    const actionKey = `pool-${poolId}-${item.ts_code}`
    if (poolMembers[poolId]?.has(item.ts_code) || pendingAction === actionKey) return
    setPendingAction(actionKey)
    try {
      const r = await axios.post(`${API}/api/realtime/pool/${poolId}/add`, {
        ts_code: item.ts_code,
        name: item.name,
        industry: item.industry,
        note: `概念板块:${selectedConcept}`,
      })
      if (r.data?.ok === false) {
        setFlashNotice({ type: 'error', text: String(r.data?.msg || '加入监控池失败') })
      } else {
        setPoolMembers(prev => ({
          ...prev,
          [poolId]: new Set(prev[poolId] || []).add(item.ts_code),
        }))
        setFlashNotice({ type: 'success', text: `${item.name} 已加入${poolId === 1 ? '择时监控' : 'T+0监控'}` })
      }
    } catch (e: any) {
      setFlashNotice({ type: 'error', text: e?.response?.data?.detail || '加入监控池失败' })
    } finally {
      setPendingAction('')
    }
  }

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
      if (allVisibleSelected) {
        for (const item of filteredMembers) next.delete(item.ts_code)
      } else {
        for (const item of filteredMembers) next.add(item.ts_code)
      }
      return next
    })
  }

  const clearSelection = () => setSelectedCodes(new Set())

  const batchAdd = async (target: 'watchlist' | 'pool1' | 'pool2') => {
    const visibleSelected = filteredMembers.filter(item => selectedCodes.has(item.ts_code))
    if (visibleSelected.length <= 0) {
      setFlashNotice({ type: 'error', text: '请先勾选要批量加入的成分股' })
      return
    }
    const eligible = visibleSelected.filter(item => {
      if (target === 'watchlist') return !watchlistSet.has(item.ts_code)
      if (target === 'pool1') return !poolMembers[1]?.has(item.ts_code)
      return !poolMembers[2]?.has(item.ts_code)
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
          if (target === 'watchlist') {
            return axios.post(`${API}/api/watchlist/add`, {
              ts_code: item.ts_code,
              name: item.name,
              industry: item.industry,
              source_strategy: `概念板块:${selectedConcept}`,
            })
          }
          const poolId = target === 'pool1' ? 1 : 2
          return axios.post(`${API}/api/realtime/pool/${poolId}/add`, {
            ts_code: item.ts_code,
            name: item.name,
            industry: item.industry,
            note: `概念板块:${selectedConcept}`,
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
            for (const item of eligible) next.add(item.ts_code)
            return next
          })
        } else {
          const poolId = target === 'pool1' ? 1 : 2
          setPoolMembers(prev => ({
            ...prev,
            [poolId]: (() => {
              const next = new Set(prev[poolId] || [])
              for (const item of eligible) next.add(item.ts_code)
              return next
            })(),
          }))
        }
      }
      setFlashNotice({
        type: successCount > 0 ? 'success' : 'error',
        text: successCount > 0
          ? `已批量加入 ${successCount} 只到${target === 'watchlist' ? '自选' : (target === 'pool1' ? '择时监控' : 'T+0监控')}`
          : '批量加入失败',
      })
    } catch {
      setFlashNotice({ type: 'error', text: '批量加入失败' })
    } finally {
      setPendingAction('')
    }
  }

  const memberFilterOptions: Array<{ key: MemberFilter; label: string; count: number }> = [
    { key: 'all', label: '全部', count: memberStatusCounts.all },
    { key: 'watchlist', label: '已自选', count: memberStatusCounts.watchlist },
    { key: 'pool1', label: '已择时', count: memberStatusCounts.pool1 },
    { key: 'pool2', label: '已T+0', count: memberStatusCounts.pool2 },
    { key: 'untracked', label: '未加入', count: memberStatusCounts.untracked },
  ]

  const boardStateOptions: Array<{ key: BoardStateFilter; label: string }> = [
    { key: 'all', label: '全部' },
    { key: 'expand', label: '扩张' },
    { key: 'strong', label: '强势' },
    { key: 'neutral', label: '中性' },
    { key: 'weak', label: '转弱' },
    { key: 'retreat', label: '退潮' },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, minHeight: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
        <div>
          <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>东方财富概念板块</div>
          <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
            查看概念板块列表、成分股实时概览，并直接加入自选或监控池。
          </div>
        </div>
        <button
          onClick={() => {
            void loadBoards(boardSearch)
            void loadMembershipSets()
            if (selectedConcept) void loadMembers(selectedConcept)
          }}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            padding: '8px 14px',
            borderRadius: 6,
            border: `1px solid ${C.cyan}`,
            background: C.cyanBg,
            color: C.cyan,
            cursor: 'pointer',
            fontSize: 12,
          }}
        >
          <RefreshCw size={14} />
          刷新
        </button>
      </div>

      {notice && (
        <div style={{
          padding: '10px 12px',
          borderRadius: 8,
          border: `1px solid ${notice.type === 'success' ? C.green : C.red}`,
          background: notice.type === 'success' ? 'rgba(34,197,94,0.08)' : 'rgba(239,68,68,0.08)',
          color: notice.type === 'success' ? C.green : C.red,
          fontSize: 12,
        }}>
          {notice.text}
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '320px minmax(0, 1fr)', gap: 16, minHeight: 640 }}>
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflow: 'hidden', display: 'flex', flexDirection: 'column', minHeight: 640 }}>
          <div style={{ padding: 12, borderBottom: `1px solid ${C.border}` }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: '0 10px' }}>
              <Search size={14} style={{ color: C.dim }} />
              <input
                value={boardSearch}
                onChange={e => setBoardSearch(e.target.value)}
                placeholder="搜索概念板块"
                style={{
                  flex: 1,
                  height: 34,
                  border: 'none',
                  background: 'transparent',
                  color: C.text,
                  outline: 'none',
                  fontSize: 12,
                }}
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, marginTop: 10 }}>
              <div style={{ color: C.dim, fontSize: 11 }}>共 {boards.length} 个概念板块</div>
              <select
                value={boardSort}
                onChange={e => setBoardSort(e.target.value as BoardSort)}
                style={{
                  background: C.bg,
                  border: `1px solid ${C.border}`,
                  color: C.text,
                  borderRadius: 6,
                  height: 28,
                  fontSize: 11,
                  padding: '0 8px',
                }}
              >
                <option value="score">按评分</option>
                <option value="pct_chg">按涨跌幅</option>
                <option value="member_count">按成分股数</option>
                <option value="name">按名称</option>
              </select>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginTop: 10 }}>
              {boardStateOptions.map(opt => {
                const active = boardStateFilter === opt.key
                return (
                  <button
                    key={opt.key}
                    onClick={() => setBoardStateFilter(opt.key)}
                    style={{
                      padding: '5px 10px',
                      borderRadius: 999,
                      border: `1px solid ${active ? stateColor(opt.key === 'all' ? '' : opt.key) : C.border}`,
                      background: active ? `${stateColor(opt.key === 'all' ? '' : opt.key)}18` : C.bg,
                      color: active ? stateColor(opt.key === 'all' ? '' : opt.key) : C.dim,
                      cursor: 'pointer',
                      fontSize: 11,
                    }}
                  >
                    {opt.label} {boardStateCounts[opt.key]}
                  </button>
                )
              })}
            </div>
          </div>
          <div style={{ flex: 1, overflowY: 'auto' }}>
            {boardsLoading ? (
              <div style={{ padding: 24, color: C.dim, fontSize: 12 }}>概念板块加载中...</div>
            ) : sortedBoards.length <= 0 ? (
              <div style={{ padding: 24, color: C.dim, fontSize: 12 }}>暂无概念板块数据</div>
            ) : sortedBoards.map(item => {
              const active = item.concept_name === selectedConcept
              return (
                <button
                  key={item.concept_name}
                  onClick={() => setSelectedConcept(item.concept_name)}
                  style={{
                    width: '100%',
                    textAlign: 'left',
                    border: 'none',
                    borderBottom: `1px solid ${C.border}`,
                    background: active ? 'rgba(0,200,200,0.08)' : 'transparent',
                    color: C.text,
                    padding: '12px 14px',
                    cursor: 'pointer',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                    <div style={{ color: active ? C.cyan : C.bright, fontSize: 13, fontWeight: 600 }}>{item.concept_name}</div>
                    <div style={{ color: stateColor(item.state), fontSize: 11 }}>{stateLabel(item.state)}</div>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginTop: 6, color: C.dim, fontSize: 11 }}>
                    <span style={{ color: Number(item.pct_chg || 0) >= 0 ? C.red : C.green }}>{fmtPct(item.pct_chg)}</span>
                    <span>评分 {Number(item.score || 0).toFixed(0)}</span>
                    <span>{item.member_count || 0} 只</span>
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0 }}>
          <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 14 }}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
              <div>
                <div style={{ color: C.bright, fontSize: 18, fontWeight: 700 }}>{boardMeta?.concept_name || selectedConcept || '未选择概念板块'}</div>
                <div style={{ color: C.dim, fontSize: 12, marginTop: 6 }}>
                  当前板块状态：<span style={{ color: stateColor(boardMeta?.state), fontWeight: 700 }}>{stateLabel(boardMeta?.state)}</span>
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: '0 10px', minWidth: 240 }}>
                <Search size={14} style={{ color: C.dim }} />
                <input
                  value={memberSearch}
                  onChange={e => setMemberSearch(e.target.value)}
                  placeholder="筛代码 / 名称 / 行业"
                  style={{
                    flex: 1,
                    height: 34,
                    border: 'none',
                    background: 'transparent',
                    color: C.text,
                    outline: 'none',
                    fontSize: 12,
                  }}
                />
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginTop: 12 }}>
              {[
                { label: '涨跌幅', value: fmtPct(boardMeta?.pct_chg), color: Number(boardMeta?.pct_chg || 0) >= 0 ? C.red : C.green },
                { label: '成分股', value: `${boardMeta?.member_count || members.length || 0}只`, color: C.text },
                { label: '上涨/下跌', value: `${boardMeta?.up_count || 0} / ${boardMeta?.down_count || 0}`, color: C.text },
                { label: '龙头涨幅', value: fmtPct(boardMeta?.leader_pct), color: Number(boardMeta?.leader_pct || 0) >= 0 ? C.red : C.green },
                { label: '换手率', value: boardMeta?.turnover != null ? `${Number(boardMeta.turnover).toFixed(2)}%` : '--', color: C.text },
                { label: '评分', value: Number(boardMeta?.score || 0).toFixed(0), color: stateColor(boardMeta?.state) },
              ].map(item => (
                <div
                  key={item.label}
                  style={{
                    padding: '6px 10px',
                    borderRadius: 999,
                    border: `1px solid ${C.border}`,
                    background: C.bg,
                    color: C.dim,
                    fontSize: 11,
                  }}
                >
                  {item.label}: <span style={{ color: item.color, fontWeight: 700 }}>{item.value}</span>
                </div>
              ))}
            </div>
          </div>

          <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflow: 'hidden', minWidth: 0 }}>
            <div style={{ padding: '12px 14px', borderBottom: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', color: C.dim, fontSize: 12 }}>
                <span>成分股 {filteredMembers.length} / {members.length} 只</span>
                <span>已勾选 {selectedVisibleCodes.length}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                {memberFilterOptions.map(opt => {
                  const active = memberFilter === opt.key
                  return (
                    <button
                      key={opt.key}
                      onClick={() => setMemberFilter(opt.key)}
                      style={{
                        padding: '5px 10px',
                        borderRadius: 999,
                        border: `1px solid ${active ? C.cyan : C.border}`,
                        background: active ? C.cyanBg : C.bg,
                        color: active ? C.cyan : C.dim,
                        cursor: 'pointer',
                        fontSize: 11,
                      }}
                    >
                      {opt.label} {opt.count}
                    </button>
                  )
                })}
                <select
                  value={memberSort}
                  onChange={e => setMemberSort(e.target.value as MemberSort)}
                  style={{
                    background: C.bg,
                    border: `1px solid ${C.border}`,
                    color: C.text,
                    borderRadius: 6,
                    height: 28,
                    fontSize: 11,
                    padding: '0 8px',
                  }}
                >
                  <option value="total_mv">按总市值</option>
                  <option value="pct_chg">按涨跌幅</option>
                  <option value="price">按现价</option>
                  <option value="name">按名称</option>
                </select>
                <button
                  onClick={toggleSelectAllVisible}
                  style={{
                    padding: '5px 10px',
                    borderRadius: 999,
                    border: `1px solid ${C.border}`,
                    background: C.bg,
                    color: C.text,
                    cursor: 'pointer',
                    fontSize: 11,
                  }}
                >
                  {allVisibleSelected ? '取消全选当前筛选' : '全选当前筛选'}
                </button>
                <button
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
                  onClick={() => void batchAdd('watchlist')}
                  disabled={selectedVisibleCodes.length <= 0 || pendingAction === 'batch-watchlist'}
                  style={{
                    padding: '5px 10px',
                    borderRadius: 999,
                    border: `1px solid ${C.yellow}`,
                    background: 'rgba(234,179,8,0.10)',
                    color: C.yellow,
                    cursor: selectedVisibleCodes.length > 0 ? 'pointer' : 'default',
                    fontSize: 11,
                    opacity: selectedVisibleCodes.length > 0 ? 1 : 0.55,
                  }}
                >
                  批量加入自选
                </button>
                <button
                  onClick={() => void batchAdd('pool1')}
                  disabled={selectedVisibleCodes.length <= 0 || pendingAction === 'batch-pool1'}
                  style={{
                    padding: '5px 10px',
                    borderRadius: 999,
                    border: `1px solid ${C.red}`,
                    background: 'rgba(239,68,68,0.10)',
                    color: C.red,
                    cursor: selectedVisibleCodes.length > 0 ? 'pointer' : 'default',
                    fontSize: 11,
                    opacity: selectedVisibleCodes.length > 0 ? 1 : 0.55,
                  }}
                >
                  批量加入择时
                </button>
                <button
                  onClick={() => void batchAdd('pool2')}
                  disabled={selectedVisibleCodes.length <= 0 || pendingAction === 'batch-pool2'}
                  style={{
                    padding: '5px 10px',
                    borderRadius: 999,
                    border: `1px solid ${C.cyan}`,
                    background: C.cyanBg,
                    color: C.cyan,
                    cursor: selectedVisibleCodes.length > 0 ? 'pointer' : 'default',
                    fontSize: 11,
                    opacity: selectedVisibleCodes.length > 0 ? 1 : 0.55,
                  }}
                >
                  批量加入T+0
                </button>
              </div>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 1080 }}>
                <thead>
                  <tr style={{ background: '#141720' }}>
                    <th style={{ padding: '10px 12px', color: C.dim, fontSize: 11, fontWeight: 600, textAlign: 'center', borderBottom: `1px solid ${C.border}`, width: 44 }}>
                      <input
                        type="checkbox"
                        checked={allVisibleSelected}
                        onChange={toggleSelectAllVisible}
                        style={{ cursor: 'pointer' }}
                      />
                    </th>
                    {['代码', '名称', '行业', '现价', '涨跌幅', '总市值', '流通市值', '操作'].map(h => (
                      <th key={h} style={{ padding: '10px 12px', color: C.dim, fontSize: 11, fontWeight: 600, textAlign: h === '操作' ? 'right' : 'left', borderBottom: `1px solid ${C.border}` }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {membersLoading ? (
                    <tr><td colSpan={9} style={{ padding: 24, color: C.dim, fontSize: 12 }}>成分股加载中...</td></tr>
                  ) : filteredMembers.length <= 0 ? (
                    <tr><td colSpan={9} style={{ padding: 24, color: C.dim, fontSize: 12 }}>暂无成分股数据</td></tr>
                  ) : filteredMembers.map(item => {
                    const up = Number(item.pct_chg || 0) >= 0
                    const priceColor = up ? C.red : C.green
                    const inWatchlist = watchlistSet.has(item.ts_code)
                    const inPool1 = !!poolMembers[1]?.has(item.ts_code)
                    const inPool2 = !!poolMembers[2]?.has(item.ts_code)
                    const checked = selectedCodes.has(item.ts_code)
                    return (
                      <tr key={item.ts_code} style={{ borderBottom: `1px solid #161926` }}>
                        <td style={{ padding: '10px 12px', textAlign: 'center' }}>
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={e => toggleSelectCode(item.ts_code, e.target.checked)}
                            style={{ cursor: 'pointer' }}
                          />
                        </td>
                        <td style={{ padding: '10px 12px', fontSize: 12 }}>
                          <Link to={`/stock/${item.ts_code}`} style={{ color: C.cyan, textDecoration: 'none', fontFamily: 'monospace' }}>
                            {item.ts_code}
                          </Link>
                        </td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: C.bright, fontWeight: 600 }}>{item.name}</td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: C.dim }}>{item.industry || '--'}</td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: priceColor, fontWeight: 700 }}>{fmtPrice(item.price)}</td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: priceColor }}>{fmtPct(item.pct_chg)}</td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: C.text }}>{fmtMvYi(item.total_mv)}</td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: C.text }}>{fmtMvYi(item.circ_mv)}</td>
                        <td style={{ padding: '10px 12px', textAlign: 'right' }}>
                          <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                            <button
                              disabled={inWatchlist || pendingAction === `watch-${item.ts_code}`}
                              onClick={() => void addToWatchlist(item)}
                              style={{
                                padding: '6px 10px',
                                borderRadius: 6,
                                border: `1px solid ${inWatchlist ? C.yellow : C.border}`,
                                background: inWatchlist ? 'rgba(234,179,8,0.08)' : 'transparent',
                                color: inWatchlist ? C.yellow : C.text,
                                cursor: inWatchlist ? 'default' : 'pointer',
                                fontSize: 11,
                              }}
                            >
                              {inWatchlist ? '已自选' : '加入自选'}
                            </button>
                            <button
                              disabled={inPool1 || pendingAction === `pool-1-${item.ts_code}`}
                              onClick={() => void addToPool(item, 1)}
                              style={{
                                padding: '6px 10px',
                                borderRadius: 6,
                                border: `1px solid ${inPool1 ? C.red : C.border}`,
                                background: inPool1 ? 'rgba(239,68,68,0.08)' : 'transparent',
                                color: inPool1 ? C.red : C.text,
                                cursor: inPool1 ? 'default' : 'pointer',
                                fontSize: 11,
                              }}
                            >
                              {inPool1 ? '已择时' : '加入择时监控'}
                            </button>
                            <button
                              disabled={inPool2 || pendingAction === `pool-2-${item.ts_code}`}
                              onClick={() => void addToPool(item, 2)}
                              style={{
                                padding: '6px 10px',
                                borderRadius: 6,
                                border: `1px solid ${inPool2 ? C.cyan : C.border}`,
                                background: inPool2 ? C.cyanBg : 'transparent',
                                color: inPool2 ? C.cyan : C.text,
                                cursor: inPool2 ? 'default' : 'pointer',
                                fontSize: 11,
                              }}
                            >
                              {inPool2 ? '已T+0' : '加入T+0监控'}
                            </button>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
