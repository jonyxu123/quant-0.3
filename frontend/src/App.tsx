import { useEffect, useRef, useState } from 'react'
import { BrowserRouter, Routes, Route, useLocation, Link } from 'react-router-dom'
import { 
  LayoutDashboard, RefreshCw, BarChart2, TrendingUp,
  Activity, LineChart, ShieldAlert, PieChart, Settings,
  ChevronDown, ChevronRight, Table2, Star, Eye, Newspaper
} from 'lucide-react'
import Dashboard from './pages/Dashboard'
import DataSync from './pages/DataSync'
import Factors from './pages/Factors'
import Strategy from './pages/Strategy'
import Monitor from './pages/Monitor'
import Dataset from './pages/Dataset'
import StockSelection from './pages/StockSelection'
import Watchlist from './pages/Watchlist'
import StockDetail from './pages/StockDetail'
import RealtimeMonitor from './pages/RealtimeMonitor'
import T0ReverseReplay from './pages/T0ReverseReplay'
import T0PositiveReplay from './pages/T0PositiveReplay'
import Pool1LeftReplay from './pages/Pool1LeftReplay'
import ConceptBoards from './pages/ConceptBoards'
import ConceptFundFlow from './pages/ConceptFundFlow'
import MarketChanges from './pages/MarketChanges'
import NewsFeeds from './pages/NewsFeeds'

const API = 'http://localhost:8000'

interface SubItem { path: string; label: string; icon: any }
interface NavItem { path: string; label: string; icon: any; children?: SubItem[] }
interface SignalHistoryItem {
  id: number | string
  pool_id: number
  ts_code: string
  name: string
  signal_type: string
  channel?: string
  signal_source?: string
  strength: number
  message: string
  price: number
  pct_chg: number
  triggered_at: string
}
interface SignalHistoryResp {
  pool_id: number
  data?: SignalHistoryItem[]
}
interface GlobalToastItem {
  uid: string
  poolId: number
  tsCode: string
  name: string
  signalType: string
  strength: number
  price: number
  pctChg: number
  message: string
  triggeredAt: string
}
interface MarketStatusLite {
  is_open?: boolean
  status?: string
}

const NAV_ITEMS: NavItem[] = [
  { path: '/',         label: '仪表盘', icon: LayoutDashboard },
  { path: '/sync',     label: '数据同步', icon: RefreshCw },
  { path: '/factors',  label: '因子管理', icon: BarChart2 },
  { path: '/selection', label: '每日选股', icon: TrendingUp },
  { path: '/concepts', label: '概念板块', icon: Table2 },
  { path: '/watchlist',  label: '自选股票', icon: Star },
  {
    path: '/news-cjzc-em', label: '资讯数据', icon: Newspaper,
    children: [
      { path: '/news-cjzc-em', label: '财经早餐-东方财富', icon: ChevronRight },
      { path: '/news-global-em', label: '全球财经快讯-东方财富', icon: ChevronRight },
      { path: '/news-global-sina', label: '全球财经快讯-新浪财经', icon: ChevronRight },
      { path: '/news-global-futu', label: '快讯-富途牛牛', icon: ChevronRight },
      { path: '/news-global-ths', label: '全球财经直播-同花顺财经', icon: ChevronRight },
      { path: '/news-global-cls', label: '电报-财联社', icon: ChevronRight },
    ]
  },
  {
    path: '/realtime', label: '盯盘', icon: Eye,
    children: [
      { path: '/realtime', label: '实时盯盘', icon: Eye },
      { path: '/realtime-pool1-left-replay', label: '左侧回放', icon: ChevronRight },
      { path: '/realtime-t0-positive-replay', label: '正T回放', icon: ChevronRight },
      { path: '/realtime-t0-replay', label: '反T回放', icon: ChevronRight },
      { path: '/realtime-concept-flow', label: '概念资金流向', icon: ChevronRight },
      { path: '/realtime-market-changes', label: '盘口异动', icon: ChevronRight },
    ]
  },
  { path: '/market',   label: '市场状态', icon: Activity },
  { path: '/monitor',  label: '绩效监控', icon: LineChart },
  { path: '/risk',     label: '风险管理', icon: ShieldAlert },
  { path: '/portfolio',label: '组合持仓', icon: PieChart },
  {
    path: '/settings', label: '数据配置', icon: Settings,
    children: [
      { path: '/dataset', label: '数据集', icon: Table2 },
    ]
  },
]

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

function poolName(poolId: number): string {
  if (poolId === 1) return 'Pool1'
  if (poolId === 2) return 'Pool2'
  return `Pool${poolId}`
}

function signalTypeName(signalType: string): string {
  const m: Record<string, string> = {
    left_side_buy: '左侧买入',
    right_side_breakout: '右侧突破',
    timing_clear: '择时清仓',
    positive_t: '正T',
    reverse_t: '反T',
  }
  return m[signalType] || signalType
}

function GlobalSignalNotifier() {
  const pageVisible = usePageVisible()
  const [toasts, setToasts] = useState<GlobalToastItem[]>([])
  const seenRef = useRef<Set<string>>(new Set())
  const bootstrappedRef = useRef(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const marketRef = useRef<MarketStatusLite>({ is_open: true, status: 'unknown' })
  const marketAtRef = useRef(0)

  const loadMarketStatus = async (): Promise<MarketStatusLite> => {
    const now = Date.now()
    if (now - marketAtRef.current < 30_000) return marketRef.current
    try {
      const r = await fetch(`${API}/api/realtime/market_status`)
      const d = (await r.json()) as MarketStatusLite
      marketRef.current = d || { is_open: false, status: 'unknown' }
      marketAtRef.current = now
      return marketRef.current
    } catch {
      return marketRef.current
    }
  }

  const pushToast = (it: SignalHistoryItem) => {
    const uid = `${it.id}-${Date.now()}`
    const toast: GlobalToastItem = {
      uid,
      poolId: Number(it.pool_id || 0),
      tsCode: String(it.ts_code || ''),
      name: String(it.name || ''),
      signalType: String(it.signal_type || ''),
      strength: Number(it.strength || 0),
      price: Number(it.price || 0),
      pctChg: Number(it.pct_chg || 0),
      message: String(it.message || ''),
      triggeredAt: String(it.triggered_at || ''),
    }
    setToasts(prev => [toast, ...prev].slice(0, 3))
    window.setTimeout(() => {
      setToasts(prev => prev.filter(x => x.uid !== uid))
    }, 7000)
  }

  useEffect(() => {
    let cancelled = false

    const poll = async () => {
      if (cancelled) return
      const market = await loadMarketStatus()
      const isOpen = !!market?.is_open
      const status = String(market?.status || 'unknown')
      try {
        if (isOpen) {
          const [r1, r2] = await Promise.all([
            fetch(`${API}/api/realtime/pool/1/signal_history?hours=2&limit=20`),
            fetch(`${API}/api/realtime/pool/2/signal_history?hours=2&limit=20`),
          ])
          if (cancelled) return
          const d1: SignalHistoryResp = await r1.json()
          const d2: SignalHistoryResp = await r2.json()
          const rows = [
            ...((d1.data || []).map(x => ({ ...x, pool_id: 1 }))),
            ...((d2.data || []).map(x => ({ ...x, pool_id: 2 }))),
          ]
          rows.sort((a, b) => {
            const ta = new Date(a.triggered_at || '').getTime()
            const tb = new Date(b.triggered_at || '').getTime()
            if (ta !== tb) return ta - tb
            return Number(a.id || 0) - Number(b.id || 0)
          })

          const newItems: SignalHistoryItem[] = []
          for (const row of rows) {
            const idKey = String(row.id)
            if (!idKey) continue
            if (!seenRef.current.has(idKey)) {
              seenRef.current.add(idKey)
              newItems.push(row)
            }
          }
          if (seenRef.current.size > 4000) {
            const arr = Array.from(seenRef.current)
            seenRef.current = new Set(arr.slice(Math.max(0, arr.length - 2500)))
          }

          if (bootstrappedRef.current) {
            for (const it of newItems) pushToast(it)
          } else {
            bootstrappedRef.current = true
          }
        }
      } catch {
        // keep silent; next loop retries automatically
      }
      const nextMs = isOpen
        ? (pageVisible ? 4000 : 10000)
        : (status === 'lunch_break'
          ? (pageVisible ? 60_000 : 120_000)
          : (pageVisible ? 120_000 : 300_000))
      if (!cancelled) timerRef.current = setTimeout(poll, nextMs)
    }

    poll()
    return () => {
      cancelled = true
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [pageVisible])

  if (toasts.length <= 0) return null

  return (
    <div style={{
      position: 'fixed',
      top: 10,
      left: '50%',
      transform: 'translateX(-50%)',
      zIndex: 9999,
      display: 'flex',
      flexDirection: 'column',
      gap: 8,
      pointerEvents: 'none',
      minWidth: 520,
      maxWidth: 780,
    }}>
      {toasts.map(t => {
        const up = t.pctChg >= 0
        const c = up ? '#ef4444' : '#22c55e'
        const timeText = t.triggeredAt ? new Date(t.triggeredAt).toLocaleTimeString() : '--:--:--'
        return (
          <div key={t.uid} style={{
            pointerEvents: 'auto',
            background: 'rgba(16,19,29,0.96)',
            border: '1px solid rgba(0,200,200,0.5)',
            borderRadius: 8,
            boxShadow: '0 8px 24px rgba(0,0,0,0.35)',
            padding: '10px 12px',
            color: '#c8cdd8',
            fontSize: 12,
            fontFamily: 'monospace',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ color: '#00c8c8', fontWeight: 700 }}>{poolName(t.poolId)}</span>
              <span>{t.tsCode}</span>
              <span style={{ color: '#e8eaf0', fontWeight: 600 }}>{t.name}</span>
              <span style={{ color: '#00c8c8' }}>{signalTypeName(t.signalType)}</span>
              <span style={{ marginLeft: 'auto', color: '#7a8099' }}>{timeText}</span>
              <button
                onClick={() => setToasts(prev => prev.filter(x => x.uid !== t.uid))}
                style={{
                  marginLeft: 8, border: 'none', background: 'transparent', color: '#7a8099',
                  cursor: 'pointer', fontSize: 14, lineHeight: 1,
                }}
              >
                x
              </button>
            </div>
            <div style={{ marginTop: 6, display: 'flex', alignItems: 'center', gap: 12 }}>
              <span>强度 <span style={{ color: '#e8eaf0' }}>{Math.round(t.strength)}</span></span>
              <span>价格 <span style={{ color: '#e8eaf0' }}>{t.price > 0 ? t.price.toFixed(2) : '--'}</span></span>
              <span>涨跌幅 <span style={{ color: c }}>{up ? '+' : ''}{t.pctChg.toFixed(2)}%</span></span>
              <span style={{
                flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#e8eaf0',
              }}>
                {t.message || '信号触发'}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function Sidebar() {
  const location = useLocation()
  const [expanded, setExpanded] = useState<Record<string, boolean>>({ '/settings': true, '/realtime': true })

  const toggle = (path: string) => setExpanded((prev: Record<string, boolean>) => ({ ...prev, [path]: !prev[path] }))

  const linkStyle = (active: boolean, sub = false): React.CSSProperties => ({
    display: 'flex', alignItems: 'center', gap: 10,
    padding: sub ? '7px 12px 7px 36px' : '9px 12px',
    borderRadius: 5, marginBottom: 1,
    color: active ? '#00c8c8' : '#7a8099',
    background: active ? 'rgba(0,200,200,0.08)' : 'transparent',
    textDecoration: 'none', fontSize: sub ? 12 : 13,
    fontWeight: active ? 500 : 400, transition: 'all 0.12s',
    borderLeft: active ? '2px solid #00c8c8' : '2px solid transparent',
  })

  return (
    <aside style={{ width: 200, minWidth: 200, background: '#141720', borderRight: '1px solid #1e2233', display: 'flex', flexDirection: 'column', height: '100vh', position: 'fixed', left: 0, top: 0 }}>
      {/* Logo */}
      <div style={{ padding: '20px 18px 16px', borderBottom: '1px solid #1e2233' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ width: 28, height: 28, background: 'linear-gradient(135deg,#00c8c8,#0080ff)', borderRadius: 6, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <BarChart2 size={16} color="#fff" />
          </div>
          <div>
            <div style={{ color: '#e8eaf0', fontWeight: 700, fontSize: 14, lineHeight: 1.2 }}>多因子量化</div>
            <div style={{ color: '#4a5068', fontSize: 11 }}>v8.0 Advanced</div>
          </div>
        </div>
      </div>

      {/* 导航菜单 */}
      <nav style={{ flex: 1, padding: '10px 8px', overflowY: 'auto' }}>
        {NAV_ITEMS.map(item => {
          const active = location.pathname === item.path
          const childActive = item.children?.some(c => location.pathname === c.path)
          const open = expanded[item.path]

          return (
            <div key={item.path}>
              {item.children ? (
                /* 有子菜单：点击展开/收起，同时也可导航 */
                <div
                  onClick={() => toggle(item.path)}
                  style={{ ...linkStyle(active || !!childActive), cursor: 'pointer', userSelect: 'none' }}
                >
                  <item.icon size={16} />
                  <span style={{ flex: 1 }}>{item.label}</span>
                  {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
                </div>
              ) : (
                <Link to={item.path} style={linkStyle(active)}>
                  <item.icon size={16} />
                  {item.label}
                </Link>
              )}

              {/* 子菜单 */}
              {item.children && open && item.children.map(sub => {
                const subActive = location.pathname === sub.path
                return (
                  <Link key={sub.path} to={sub.path} style={linkStyle(subActive, true)}>
                    <sub.icon size={13} />
                    {sub.label}
                  </Link>
                )
              })}
            </div>
          )
        })}
      </nav>

      <div style={{ padding: '12px 18px', borderTop: '1px solid #1e2233', color: '#3a3f55', fontSize: 11 }}>
        Quant System (c) 2026
      </div>
    </aside>
  )
}

function Layout() {
  const location = useLocation()
  const allItems = NAV_ITEMS.flatMap(n => n.children ? [n, ...n.children] : [n])
  const current = allItems.find(n => n.path === location.pathname)
  const pageTitle = current?.label || '仪表盘'

  return (
    <div style={{ display: 'flex', minHeight: '100vh', background: '#0f1117' }}>
      <GlobalSignalNotifier />
      <Sidebar />
      <div style={{ marginLeft: 200, flex: 1, display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
        {/* 顶部页面标题栏 */}
        <div style={{ padding: '16px 24px', borderBottom: '1px solid #1e2233', background: '#0f1117' }}>
          <span style={{ color: '#e8eaf0', fontSize: 16, fontWeight: 600 }}>{pageTitle}</span>
        </div>
        {/* 内容区 */}
        <main style={{ flex: 1, padding: ['/dataset', '/selection'].includes(location.pathname) ? 0 : '20px 24px', overflowY: ['/dataset', '/selection'].includes(location.pathname) ? 'hidden' : 'auto', display: 'flex', flexDirection: 'column' }}>
          <Routes>
            <Route path="/"         element={<Dashboard />} />
            <Route path="/sync"     element={<DataSync />} />
            <Route path="/factors"  element={<Factors />} />
            <Route path="/selection" element={<StockSelection />} />
            <Route path="/concepts" element={<ConceptBoards />} />
            <Route path="/watchlist" element={<Watchlist />} />
            <Route path="/news-cjzc-em" element={<NewsFeeds />} />
            <Route path="/news-global-em" element={<NewsFeeds />} />
            <Route path="/news-global-sina" element={<NewsFeeds />} />
            <Route path="/news-global-futu" element={<NewsFeeds />} />
            <Route path="/news-global-ths" element={<NewsFeeds />} />
            <Route path="/news-global-cls" element={<NewsFeeds />} />
            <Route path="/realtime" element={<RealtimeMonitor />} />
            <Route path="/realtime-pool1-left-replay" element={<Pool1LeftReplay />} />
            <Route path="/realtime-t0-positive-replay" element={<T0PositiveReplay />} />
            <Route path="/realtime-t0-replay" element={<T0ReverseReplay />} />
            <Route path="/realtime-concept-flow" element={<ConceptFundFlow />} />
            <Route path="/realtime-market-changes" element={<MarketChanges />} />
            <Route path="/stock/:ts_code" element={<StockDetail />} />
            <Route path="/strategy" element={<Strategy />} />
            <Route path="/monitor"  element={<Monitor />} />
            <Route path="/dataset"  element={<Dataset />} />
            <Route path="/*"        element={<Placeholder />} />
          </Routes>
        </main>
      </div>
    </div>
  )
}

function Placeholder() {
  return (
    <div style={{ color: '#4a5068', padding: 40, textAlign: 'center', fontSize: 14 }}>
      该功能正在开发中...
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Layout />
    </BrowserRouter>
  )
}

export default App
