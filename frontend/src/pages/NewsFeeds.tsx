import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'
import { Link, useLocation } from 'react-router-dom'
import { ExternalLink, RefreshCw, Search } from 'lucide-react'

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

const NEWS_SOURCE_ROUTES = [
  { path: '/news-cjzc-em', key: 'cjzc_em', label: '财经早餐-东方财富', pollMs: 300_000 },
  { path: '/news-global-em', key: 'global_em', label: '全球财经快讯-东方财富', pollMs: 30_000 },
  { path: '/news-global-sina', key: 'global_sina', label: '全球财经快讯-新浪财经', pollMs: 30_000 },
  { path: '/news-global-futu', key: 'global_futu', label: '快讯-富途牛牛', pollMs: 30_000 },
  { path: '/news-global-ths', key: 'global_ths', label: '全球财经直播-同花顺财经', pollMs: 30_000 },
  { path: '/news-global-cls', key: 'global_cls', label: '电报-财联社', pollMs: 30_000 },
] as const

interface NewsFeedItem {
  rank: number
  source: string
  source_label: string
  title: string
  summary?: string
  published_at?: string
  link?: string
}

function getCurrentSource(pathname: string) {
  return NEWS_SOURCE_ROUTES.find(x => x.path === pathname) || NEWS_SOURCE_ROUTES[1]
}

function formatPublishedAt(v?: string): string {
  const text = String(v || '').trim()
  if (!text) return '--'
  const ts = Date.parse(text)
  if (Number.isFinite(ts)) {
    try {
      return new Date(ts).toLocaleString()
    } catch {
      return text
    }
  }
  return text
}

function isTodayText(v?: string): boolean {
  const text = String(v || '').trim()
  if (!text) return false
  const ts = Date.parse(text)
  if (!Number.isFinite(ts)) return false
  const d = new Date(ts)
  const now = new Date()
  return d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth() && d.getDate() === now.getDate()
}

function isYesterdayText(v?: string): boolean {
  const text = String(v || '').trim()
  if (!text) return false
  const ts = Date.parse(text)
  if (!Number.isFinite(ts)) return false
  const now = new Date()
  const startToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime()
  const startYesterday = startToday - 24 * 60 * 60 * 1000
  return ts >= startYesterday && ts < startToday
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function renderHighlighted(text: string, keyword: string) {
  const raw = String(text || '')
  const q = String(keyword || '').trim()
  if (!q) return raw
  const re = new RegExp(`(${escapeRegExp(q)})`, 'ig')
  const parts = raw.split(re)
  return parts.map((part, idx) => {
    if (part.toLowerCase() === q.toLowerCase()) {
      return (
        <mark
          key={`${part}-${idx}`}
          style={{
            background: '#3b2f06',
            color: '#fde68a',
            padding: '0 2px',
            borderRadius: 3,
          }}
        >
          {part}
        </mark>
      )
    }
    return <span key={`${part}-${idx}`}>{part}</span>
  })
}

export default function NewsFeeds() {
  const location = useLocation()
  const current = useMemo(() => getCurrentSource(location.pathname), [location.pathname])
  const [rows, setRows] = useState<NewsFeedItem[]>([])
  const [loading, setLoading] = useState(false)
  const [search, setSearch] = useState('')
  const [todayOnly, setTodayOnly] = useState(false)
  const [fetchedAt, setFetchedAt] = useState('')
  const [errorText, setErrorText] = useState('')
  const [stale, setStale] = useState(false)
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const loadRows = async (forceRefresh = false, keyword = search, sourceKey = current.key) => {
    setLoading(true)
    try {
      const r = await axios.get(`${API}/api/realtime/news_feed`, {
        params: {
          source: sourceKey,
          q: keyword.trim(),
          limit: 120,
          force_refresh: forceRefresh,
        },
      })
      setRows((r.data?.data || []) as NewsFeedItem[])
      setFetchedAt(String(r.data?.fetched_at || ''))
      setErrorText(String(r.data?.error || ''))
      setStale(!!r.data?.stale)
    } catch (e: any) {
      setRows([])
      setFetchedAt('')
      setStale(false)
      setErrorText(e?.response?.data?.detail || '加载新闻资讯失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadRows(false, '', current.key)
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [current.key])

  useEffect(() => {
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    searchTimerRef.current = setTimeout(() => {
      void loadRows(false, search, current.key)
    }, 250)
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search, current.key])

  useEffect(() => {
    let cancelled = false
    const loop = async () => {
      if (cancelled) return
      try {
        await loadRows(false, search, current.key)
      } finally {
        if (!cancelled) pollTimerRef.current = setTimeout(loop, current.pollMs)
      }
    }
    pollTimerRef.current = setTimeout(loop, current.pollMs)
    return () => {
      cancelled = true
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [current.key, current.pollMs, search])

  const viewRows = useMemo(() => {
    let next = rows
    if (todayOnly) next = next.filter(row => isTodayText(row.published_at))
    return next
  }, [rows, todayOnly])

  const todayCount = useMemo(() => rows.filter(row => isTodayText(row.published_at)).length, [rows])
  const groupedRows = useMemo(() => {
    const groups = [
      { key: 'today', label: '今天', rows: [] as NewsFeedItem[] },
      { key: 'yesterday', label: '昨天', rows: [] as NewsFeedItem[] },
      { key: 'older', label: '更早', rows: [] as NewsFeedItem[] },
    ]
    for (const row of viewRows) {
      if (isTodayText(row.published_at)) groups[0].rows.push(row)
      else if (isYesterdayText(row.published_at)) groups[1].rows.push(row)
      else groups[2].rows.push(row)
    }
    return groups.filter(group => group.rows.length > 0)
  }, [viewRows])

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
          <div style={{ color: C.bright, fontSize: 15, fontWeight: 700 }}>{current.label}</div>
          <div style={{ color: C.dim, fontSize: 12, marginTop: 4 }}>
            数据源: AKShare / {current.label}
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
            minWidth: 280,
          }}
        >
          <Search size={14} color={C.dim} />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="搜索标题或摘要"
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
          onClick={() => setTodayOnly(v => !v)}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 6,
            background: todayOnly ? '#3b2f06' : '#0b1220',
            color: todayOnly ? '#fde68a' : C.text,
            border: `1px solid ${todayOnly ? '#f59e0b55' : C.border}`,
            borderRadius: 8,
            padding: '7px 12px',
            cursor: 'pointer',
            fontSize: 12,
          }}
        >
          只看今日 {todayCount}
        </button>
        <button
          type="button"
          onClick={() => void loadRows(true, search, current.key)}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 6,
            background: '#0b1220',
            color: C.cyan,
            border: `1px solid ${C.cyan}55`,
            borderRadius: 8,
            padding: '7px 12px',
            cursor: 'pointer',
            fontSize: 12,
          }}
        >
          <RefreshCw size={14} />
          {loading ? '刷新中...' : '刷新'}
        </button>
      </div>

      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        {NEWS_SOURCE_ROUTES.map(item => {
          const active = item.path === current.path
          return (
            <Link
              key={item.key}
              to={item.path}
              style={{
                textDecoration: 'none',
                padding: '10px 14px',
                borderRadius: 999,
                border: `1px solid ${active ? C.cyan : C.border}`,
                background: active ? '#072b33' : C.card,
                color: active ? C.cyan : C.text,
                fontSize: 13,
                fontWeight: 600,
              }}
            >
              {item.label}
            </Link>
          )
        })}
      </div>

      {errorText && !stale ? (
        <div
          style={{
            background: '#2a1418',
            color: '#fecaca',
            border: '1px solid #7f1d1d',
            borderRadius: 8,
            padding: '10px 12px',
            fontSize: 12,
          }}
        >
          {errorText}
        </div>
      ) : null}

      {groupedRows.map(group => (
        <div key={group.key} style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 10,
            }}
          >
            <div style={{ color: C.bright, fontSize: 14, fontWeight: 700 }}>{group.label}</div>
            <div
              style={{
                color: C.dim,
                fontSize: 12,
                border: `1px solid ${C.border}`,
                borderRadius: 999,
                padding: '2px 8px',
              }}
            >
              {group.rows.length} 条
            </div>
            <div style={{ flex: 1, height: 1, background: C.border }} />
          </div>

          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
              gap: 12,
            }}
          >
            {group.rows.map(row => (
              <div
                key={`${row.source}-${row.rank}-${row.title}`}
                style={{
                  background: C.card,
                  border: `1px solid ${C.border}`,
                  borderRadius: 10,
                  padding: 14,
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 10,
                  minHeight: 180,
                }}
              >
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10 }}>
                  <div
                    style={{
                      minWidth: 30,
                      height: 30,
                      borderRadius: 8,
                      background: '#0b1220',
                      border: `1px solid ${C.border}`,
                      color: C.cyan,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontSize: 12,
                      fontWeight: 700,
                    }}
                  >
                    {row.rank}
                  </div>
                  <div style={{ flex: 1 }}>
                    <div style={{ color: C.bright, fontSize: 14, fontWeight: 700, lineHeight: 1.5 }}>
                      {renderHighlighted(row.title || '--', search)}
                    </div>
                    <div style={{ color: C.dim, fontSize: 12, marginTop: 6 }}>
                      {formatPublishedAt(row.published_at)}
                    </div>
                  </div>
                </div>

                <div
                  style={{
                    color: C.text,
                    fontSize: 13,
                    lineHeight: 1.7,
                    whiteSpace: 'pre-wrap',
                  }}
                >
                  {renderHighlighted(row.summary || '暂无摘要', search)}
                </div>

                <div style={{ marginTop: 'auto', display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div
                    style={{
                      color: C.dim,
                      fontSize: 12,
                      border: `1px solid ${C.border}`,
                      borderRadius: 999,
                      padding: '4px 8px',
                    }}
                  >
                    {row.source_label}
                  </div>
                  {isTodayText(row.published_at) ? (
                    <div
                      style={{
                        color: '#fde68a',
                        fontSize: 12,
                        border: '1px solid #f59e0b55',
                        background: '#3b2f06',
                        borderRadius: 999,
                        padding: '4px 8px',
                      }}
                    >
                      今日
                    </div>
                  ) : isYesterdayText(row.published_at) ? (
                    <div
                      style={{
                        color: '#bfdbfe',
                        fontSize: 12,
                        border: '1px solid #3b82f655',
                        background: '#0f2342',
                        borderRadius: 999,
                        padding: '4px 8px',
                      }}
                    >
                      昨天
                    </div>
                  ) : null}
                  <div style={{ flex: 1 }} />
                  {row.link ? (
                    <a
                      href={row.link}
                      target="_blank"
                      rel="noreferrer"
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 6,
                        color: C.cyan,
                        textDecoration: 'none',
                        fontSize: 12,
                        fontWeight: 600,
                      }}
                    >
                      原文
                      <ExternalLink size={13} />
                    </a>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}

      {!loading && groupedRows.length <= 0 ? (
        <div
          style={{
            background: C.card,
            border: `1px solid ${C.border}`,
            borderRadius: 10,
            padding: '28px 18px',
            color: C.dim,
            textAlign: 'center',
            fontSize: 13,
          }}
        >
          当前没有可显示的资讯。
        </div>
      ) : null}
    </div>
  )
}
