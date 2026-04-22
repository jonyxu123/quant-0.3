/**
 * 鐩洏椤甸潰 - 瀹炴椂鐩戞帶姹狅紙WebSocket 鎺ㄩ€佺増锛? *
 * 閫氳鏂瑰紡锛? *   浼樺厛 WebSocket锛坵s://localhost:8000/api/realtime/ws锛? *   闄嶇骇 HTTP 杞锛堣繛鎺ュけ璐?鏂紑鏃惰嚜鍔ㄥ垏鎹級
 *
 * WS 鍗忚锛? *   瀹㈡埛绔?鈫?鏈嶅姟绔? subscribe/unsubscribe/ping
 *   鏈嶅姟绔?鈫?瀹㈡埛绔? tick/minute/transactions/signals/market_status/pong
 */
import React, { useEffect, useState, useRef, useCallback } from 'react'
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

/* 鈹€鈹€鈹€ 棰滆壊 鈹€鈹€鈹€ */
const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', cyanBg: 'rgba(0,200,200,0.08)',
  red: '#ef4444', green: '#22c55e', yellow: '#eab308', purple: '#a855f7',
}

/* 鈹€鈹€鈹€ 绫诲瀷 鈹€鈹€鈹€ */
interface PoolInfo { pool_id: number; name: string; strategy: string; desc: string; signal_types: string[]; member_count: number }
interface Member { ts_code: string; name: string; industry?: string; added_at?: string; note?: string }
interface Signal { has_signal: boolean; type: string; direction?: 'buy' | 'sell'; price?: number; strength: number; current_strength?: number; message: string; triggered_at: number; details: Record<string, any>; state?: 'active' | 'decaying' | 'expired'; age_sec?: number; expire_reason?: string }
interface SignalRow { ts_code: string; name: string; price: number; pct_chg: number; signals: Signal[] }
interface SearchResult { ts_code: string; name: string; industry?: string }
interface TickData { price: number; open: number; high: number; low: number; pre_close: number; volume: number; amount: number; pct_chg: number; bids: [number, number][]; asks: [number, number][]; is_mock?: boolean }
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
  signal_churn?: SignalChurn
  message?: string
}
interface DriftFeature { psi: number | null; ks_stat: number | null; severity: string; drifted: boolean; precision_drop: number | null; alerted: boolean }
interface DriftStatus { days: number; total_checks: number; has_active_alert: boolean; latest_checked_at?: string; latest: Record<string, DriftFeature>; alerts: number; message?: string }
interface Pool1ObserveStats { screen_total: number; screen_pass: number; stage2_triggered: number; pass_rate: number; trigger_rate: number; summary: string }
interface Pool1ObserveResp { pool_id: number; provider: string; supported: boolean; trade_date?: string; updated_at?: number; updated_at_iso?: string; data?: Pool1ObserveStats; message?: string }
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

function mergeTickMap(prev: Record<string, TickData>, incoming: Record<string, TickData>): Record<string, TickData> {
  const src = incoming || {}
  const prevKeys = Object.keys(prev)
  const srcKeys = Object.keys(src)
  let changed = prevKeys.length !== srcKeys.length
  const next: Record<string, TickData> = {}
  for (const code of srcKeys) {
    const oldTick = prev[code]
    const newTick = src[code]
    if (isSameTick(oldTick, newTick)) {
      next[code] = oldTick
    } else {
      next[code] = newTick
      changed = true
    }
  }
  return changed ? next : prev
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

function mergeTxnsMap(prev: Record<string, Txn[]>, incoming: Record<string, Txn[]>): Record<string, Txn[]> {
  const src = incoming || {}
  const prevKeys = Object.keys(prev)
  const srcKeys = Object.keys(src)
  let changed = prevKeys.length !== srcKeys.length
  const next: Record<string, Txn[]> = {}
  for (const code of srcKeys) {
    const oldTxns = prev[code]
    const newTxns = src[code]
    if (isSameTxnList(oldTxns, newTxns)) {
      next[code] = oldTxns
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

/* 鈹€鈹€鈹€ 淇″彿鎻愰啋闊?鈹€鈹€鈹€ */
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

/* 鈹€鈹€鈹€ WebSocket Hook 鈹€鈹€鈹€ */
interface IndexData { ts_code: string; name: string; price: number; pre_close: number; pct_chg: number; amount: number; is_mock?: boolean }

function useRealtimeWS(poolId: number, soundOn: boolean) {
  const [connected, setConnected] = useState(false)
  const [tickMap, setTickMap] = useState<Record<string, TickData>>({})
  // barsMap已移除，改用tick数据绘制分时图
  const [txnsMap, setTxnsMap] = useState<Record<string, Txn[]>>({})
  const [tickHistoryMap, setTickHistoryMap] = useState<Record<string, TickDataPoint[]>>({})
  const [signalRows, setSignalRows] = useState<SignalRow[]>([])
  const [marketStatus, setMarketStatus] = useState<MarketStatus>({ is_open: true, status: 'unknown' })
  const [lastEvalAt, setLastEvalAt] = useState<number>(0)
  const [indices, setIndices] = useState<IndexData[]>([])

  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pingTimer = useRef<ReturnType<typeof setInterval> | null>(null)
  const prevSignalKeys = useRef<Set<string>>(new Set())
  const mountedRef = useRef(false)
  const reconnectAttempt = useRef(0)  // 閲嶈繛娆℃暟锛岀敤浜庢寚鏁伴€€閬?
  // HTTP 闄嶇骇杞
  const [usePolling, setUsePolling] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const pollSlowRef = useRef<ReturnType<typeof setInterval> | null>(null)

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
            new Notification(`${row.name} ${s.message}`, { body: `寮哄害 ${s.strength}` })
          }
        }
      }
    }
    prevSignalKeys.current = newKeys
  }, [soundOn])

  // WS 娑堟伅澶勭悊
  const onMessage = useCallback((event: MessageEvent) => {
    try {
      const msg = JSON.parse(event.data)
      switch (msg.type) {
        case 'tick':
          setTickMap(prev => mergeTickMap(prev, msg.data || {}))
          break
        case 'transactions':
          setTxnsMap(prev => mergeTxnsMap(prev, msg.data || {}))
          break
        case 'signals':
          const sigData: SignalRow[] = msg.data || []
          setSignalRows(sigData)
          setLastEvalAt(Date.now())
          handleSignalNotify(sigData)
          break
        case 'market_status':
          setMarketStatus({ is_open: msg.is_open, status: msg.status, desc: msg.desc })
          break
        case 'indices':
          setIndices(msg.data || [])
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

  // 杩炴帴 WS锛堜笉渚濊禆浠讳綍 state锛岄€氳繃 ref 璇诲彇鏈€鏂板€硷級
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
        // 蹇冭烦
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
        // 鎸囨暟閫€閬块噸杩烇細1s, 2s, 4s, 8s, ... 鏈€澶?30s
        reconnectAttempt.current += 1
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempt.current - 1), 30000)
        reconnectTimer.current = setTimeout(connectWS, delay)
      }

      ws.onerror = (error) => {
        if (!mountedRef.current) return
        console.error('[WS] connection error:', error)
        setConnected(false)
        // onerror 鍚庢祻瑙堝櫒浼氳嚜鍔ㄨЕ鍙?onclose锛岄噸杩炵敱 onclose 鎺ョ
      }
    } catch {
      setUsePolling(true)
    }
  }, [])  // 绌轰緷璧栵細鎵€鏈夊姩鎬佸€奸€氳繃 ref 璇诲彇

  // WS 鐢熷懡鍛ㄦ湡绠＄悊锛堝崟涓€ effect锛岄伩鍏嶅 effect 绔炴€侊級
  useEffect(() => {
    mountedRef.current = true
    if (Notification.permission === 'default') Notification.requestPermission()
    connectWS()
    return () => {
      mountedRef.current = false
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current)
      if (pingTimer.current) clearInterval(pingTimer.current)
      if (pollRef.current) clearInterval(pollRef.current)
      if (pollSlowRef.current) clearInterval(pollSlowRef.current)
      // 鍏抽棴 WS
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
  }, [poolId])

  // HTTP 闄嶇骇杞
  useEffect(() => {
    if (!usePolling) return

    const fetchFast = () => {
      axios.get(`${API}/api/realtime/pool/${poolId}/signals_fast`)
        .then(r => {
          const data: SignalRow[] = r.data.data || []
          setSignalRows(data); setLastEvalAt(Date.now())
          handleSignalNotify(data)
        })
        .catch(() => {})
      axios.get(`${API}/api/realtime/market_status`)
        .then(r => setMarketStatus(r.data))
        .catch(() => {})
      axios.get(`${API}/api/realtime/indices`)
        .then(r => setIndices(r.data.data || []))
        .catch(() => {})
    }
    const fetchTick = () => {
      // 闇€瑕佹垚鍛樺垪琛ㄦ潵鎵归噺鑾峰彇
      axios.get(`${API}/api/realtime/pool/${poolId}/members`)
        .then(r => {
          const members: Member[] = r.data.data || []
          if (members.length === 0) return
          const codes = members.map(m => m.ts_code)
          axios.post(`${API}/api/realtime/batch/tick`, { ts_codes: codes })
            .then(r => setTickMap(prev => mergeTickMap(prev, r.data.data || {}))).catch(() => {})
          axios.post(`${API}/api/realtime/batch/transactions`, { ts_codes: codes }, { params: { n: 15 } })
            .then(r => setTxnsMap(prev => mergeTxnsMap(prev, r.data.data || {}))).catch(() => {})
          // minute数据已移除，改用tick数据绘制分时图
        })
        .catch(() => {})
    }

    fetchFast(); fetchTick()
    const isFast = marketStatus.is_open
    pollRef.current = setInterval(() => { fetchFast(); fetchTick() }, isFast ? 3000 : 60000)

    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [usePolling, poolId, marketStatus.is_open, handleSignalNotify])

  // 鎵嬪姩鍒锋柊淇″彿
  const refreshSignals = useCallback(() => {
    if (connected && wsRef.current?.readyState === WebSocket.OPEN) {
      // WS 妯″紡涓嬩俊鍙风敱鏈嶅姟绔帹閫侊紝杩欓噷瑙﹀彂涓€娆TTP璇锋眰
    }
    axios.get(`${API}/api/realtime/pool/${poolId}/signals_fast`)
      .then(r => {
        const data: SignalRow[] = r.data.data || []
        setSignalRows(data); setLastEvalAt(Date.now())
        handleSignalNotify(data)
      })
      .catch(() => {})
  }, [poolId, connected, handleSignalNotify])

  return {
    connected, usePolling, tickMap, txnsMap, tickHistoryMap, signalRows, marketStatus, lastEvalAt, indices, refreshSignals,
  }
}

/* 鈹€鈹€鈹€ 涓荤粍浠?鈹€鈹€鈹€ */
export default function RealtimeMonitor() {
  const [pools, setPools] = useState<PoolInfo[]>([])
  const [activePoolId, setActivePoolId] = useState<number>(1)
  const [loading, setLoading] = useState(true)

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
      {/* 姹犲垏鎹?Tabs */}
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

/* 鈹€鈹€鈹€ 姹犻潰鏉?鈹€鈹€鈹€ */
function PoolPanel({ pool, onChanged }: { pool: PoolInfo; onChanged: () => void }) {
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
  const pool1ObserveReqInFlight = useRef(false)
  const pool1ObserveReqSeq = useRef(0)

  // WebSocket 瀹炴椂鏁版嵁
  const { connected, usePolling, tickMap, txnsMap, tickHistoryMap, signalRows, marketStatus, lastEvalAt, indices, refreshSignals } = useRealtimeWS(pool.pool_id, soundOn)

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
    const effectiveSec = pool1ObserveFailCount >= OBSERVE_FAIL_WARN_COUNT ? baseSec * 2 : baseSec
    const pollMs = Math.max(1000, effectiveSec * 1000)
    const timer = setInterval(loadPool1Observe, pollMs)
    return () => clearInterval(timer)
  }, [pool.pool_id, loadPool1Observe, pool1ObserveUi.observePollSec, pool1ObserveFailCount])

  const loadMembers = useCallback(() => {
    axios.get(`${API}/api/realtime/pool/${pool.pool_id}/members`)
      .then(r => setMembers(r.data.data || []))
      .catch(err => console.error('鍔犺浇鎴愬憳澶辫触', err))
  }, [pool.pool_id])

  useEffect(() => { loadMembers() }, [loadMembers])

  const removeMember = (ts_code: string) => {
    if (!confirm('浠庢睜涓Щ闄わ紵')) return
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

  // 鎺掑簭
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

  return (
    <div>
      {/* 鎸囨暟瀹炴椂鏁版嵁鏉?*/}
      {indices.length > 0 && (
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
              </div>
            )
          })}
        </div>
      )}

      {/* 姹犺鏄?+ 宸ュ叿鏍?*/}
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
        {/* 杩炴帴鐘舵€?*/}
        <div style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 4, color: connected ? C.green : (usePolling ? C.yellow : C.dim) }}>
          {connected ? <Wifi size={12} /> : <WifiOff size={12} />}
          {connected ? 'WS' : (usePolling ? 'HTTP杞' : '鏂紑')}
        </div>
        {/* 浜ゆ槗鏃舵 */}
        <div style={{ fontSize: 11, color: marketStatus.is_open ? C.green : C.dim, display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 6, height: 6, borderRadius: 3, background: marketStatus.is_open ? C.green : C.dim, display: 'inline-block' }} />
          {marketStatus.desc || (marketStatus.is_open ? '交易中' : '休市')}
        </div>
        <div style={{ color: C.dim, fontSize: 11 }}>
          评估: {lastEvalAt ? new Date(lastEvalAt).toLocaleTimeString() : '--'}
        </div>
        {pool.pool_id === 1 && pool1Observe?.supported && pool1Observe.data && (
          (() => {
            const ageSec = pool1Observe.updated_at ? Math.max(0, Math.floor(Date.now() / 1000) - pool1Observe.updated_at) : 0
            const freshnessColor = ageSec > pool1ObserveUi.staleErrorSec
              ? C.red
              : ageSec > pool1ObserveUi.staleWarnSec
                ? C.yellow
                : C.green
            const sampleWarn = pool1Observe.data.screen_total < pool1ObserveUi.sampleWarnMin
            return (
              <div style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '3px 8px', borderRadius: 4,
                background: C.bg,
                border: `1px solid ${sampleWarn ? C.yellow : C.border}`,
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
        <div style={{ display: 'grid', gap: 12, gridTemplateColumns: `repeat(auto-fill, minmax(${compact ? 280 : 340}px, 1fr))` }}>
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

/* 鈹€鈹€鈹€ 娣诲姞鑲＄エ鎼滅储妗?鈹€鈹€鈹€ */
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

/* 鈹€鈹€鈹€ 鑲＄エ鐩戞帶鍗＄墖 鈹€鈹€鈹€ */
function StockCard({ member, signalRow, tick, txns, tickHistory: initialTickHistory, compact, onRemove, onUpdateNote, poolId }: {
  member: Member; signalRow?: SignalRow; tick?: TickData; txns: Txn[]; tickHistory?: TickDataPoint[]
  compact: boolean; onRemove: () => void; onUpdateNote: (note: string) => void; poolId: number
}) {
  const signals = signalRow?.signals || []
  const [showDetail, setShowDetail] = useState(!compact)
  const [editingNote, setEditingNote] = useState(false)
  const [noteInput, setNoteInput] = useState(member.note || '')
  // 缂撳瓨tick鍘嗗彶鏁版嵁鐢ㄤ簬缁樺埗鍒嗘椂鍥?  // 鐢?ref 瀛樺偍鐪熷疄鏁版嵁锛宻tate 浠呭湪鍒嗛挓鍙樺寲鏃跺悓姝ワ紙瑙﹀彂閲嶆覆鏌擄級
  // 鍒嗘椂鍥炬寜鍒嗛挓鑱氬悎锛屽悓涓€鍒嗛挓鍐呭娆?tick 鏃犻渶瑙﹀彂娓叉煋
  const tickHistoryRef = useRef<TickDataPoint[]>(initialTickHistory || [])
  const [tickHistory, setTickHistory] = useState<TickDataPoint[]>(initialTickHistory || [])
  const lastRenderedMinute = useRef<number>(0)

  // 褰撴湁鏂皌ick鏃讹紝娣诲姞鍒皉ef缂撳瓨锛涢娆℃暟鎹垨璺ㄥ垎閽熸椂鍚屾鍒皊tate瑙﹀彂娓叉煋
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
      // 鏇存柊 ref
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
  const preClose = tick?.pre_close || 0
  const pctChg = preClose > 0
    ? ((price - preClose) / preClose * 100)
    : (tick?.pct_chg ?? signalRow?.pct_chg ?? 0)
  const isUp = pctChg >= 0
  const priceColor = (p: number) => p >= preClose ? C.red : C.green
  const maxBidVol = Math.max(1, ...(tick?.bids || []).map(([, v]) => v))
  const maxAskVol = Math.max(1, ...(tick?.asks || []).map(([, v]) => v))

  return (
    <div style={{ background: C.card, border: `1px solid ${signals.length > 0 ? C.cyan : C.border}`, borderRadius: 6, padding: 10, position: 'relative', boxShadow: signals.length > 0 ? '0 0 0 1px rgba(0,200,200,0.2)' : 'none', display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Link to={`/stock/${member.ts_code}`} style={{ color: C.cyan, fontSize: 12, fontFamily: 'monospace', textDecoration: 'none', display: 'block' }}>{member.ts_code}</Link>
          <div style={{ color: C.bright, fontSize: 14, fontWeight: 600, marginTop: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{member.name}</div>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ color: isUp ? C.red : C.green, fontSize: 18, fontWeight: 700, fontFamily: 'monospace', lineHeight: 1.1 }}>{price > 0 ? price.toFixed(2) : '--'}</div>
          <div style={{ color: isUp ? C.red : C.green, fontSize: 11, fontFamily: 'monospace' }}>{pctChg >= 0 ? '+' : ''}{pctChg.toFixed(2)}%</div>
        </div>
        <button onClick={onRemove} title="移除" style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.dim, padding: 2 }}><Trash2 size={12} /></button>
      </div>

      {tick && showDetail && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '3px 8px', fontSize: 10, fontFamily: 'monospace', padding: '4px 6px', background: 'rgba(0,0,0,0.2)', borderRadius: 3 }}>
          <Stat label="寮€" value={tick.open.toFixed(2)} color={priceColor(tick.open)} />
          <Stat label="高" value={tick.high.toFixed(2)} color={C.red} />
          <Stat label="低" value={tick.low.toFixed(2)} color={C.green} />
          <Stat label="昨" value={tick.pre_close.toFixed(2)} />
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
            direction: s.direction || (s.type === 'reverse_t' ? 'sell' : 'buy'),
            price: s.price as number,
            triggered_at: s.triggered_at,
            message: s.message,
            strength: s.strength,
          }))}
      />

      {signals.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {signals.map((s, i) => <SignalBadge key={i} signal={s} />)}
        </div>
      )}

      {compact && (
        <button onClick={() => setShowDetail(!showDetail)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.dim, fontSize: 10, display: 'flex', alignItems: 'center', gap: 4, justifyContent: 'center' }}>
          {showDetail ? <ChevronUp size={10} /> : <ChevronDown size={10} />}{showDetail ? '鏀惰捣' : '璇︽儏'}
        </button>
      )}

      {tick && showDetail && poolId === 2 && (
        <div>
          <SubTitle>浜旀。鐩樺彛</SubTitle>
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
          <SubTitle>閫愮瑪鎴愪氦</SubTitle>
          <div style={{ fontSize: 10, fontFamily: 'monospace', display: 'grid', gridTemplateColumns: '60px 1fr 1fr 24px', rowGap: 2, columnGap: 6 }}>
            {txns.length === 0 ? (
              <div style={{ gridColumn: '1 / -1', color: C.dim, textAlign: 'center', padding: 6 }}>鏆傛棤</div>
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
              placeholder="娣诲姞澶囨敞..." autoFocus
              style={{ flex: 1, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, fontSize: 10, padding: '2px 6px', outline: 'none' }} />
            <button onClick={() => { onUpdateNote(noteInput); setEditingNote(false) }} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.cyan, fontSize: 10 }}>OK</button>
          </div>
        ) : (
          <div onClick={() => { setNoteInput(member.note || ''); setEditingNote(true) }} style={{ fontSize: 10, color: member.note ? C.dim : C.border, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
            <Edit3 size={8} />{member.note || '娣诲姞澶囨敞'}
          </div>
        )}
      </div>
    </div>
  )
}

/* 鈹€鈹€鈹€ 灏忕粍浠?鈹€鈹€鈹€ */
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

function SignalBadge({ signal }: { signal: Signal }) {
  const direction: 'buy' | 'sell' = signal.direction || (signal.type === 'reverse_t' ? 'sell' : 'buy')
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

  const details = signal.details && typeof signal.details === 'object' ? signal.details as Record<string, any> : {}
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
  const isPool1Signal = signal.type === 'left_side_buy' || signal.type === 'right_side_breakout'
  const showResonanceLine = isPool1Signal && (resonanceValue !== null || resonanceParts.length > 0)
  const resonanceText = `60m共振:${resonanceValue === null ? '--' : (resonanceValue ? 'ON' : 'OFF')}`

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
        <Icon size={11} color={color} />
        <span style={{ color, flex: 1, fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {signal.message}
        </span>
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
      {showResonanceLine && (
        <div style={{
          paddingLeft: 22, color: C.dim, fontSize: 9, fontFamily: 'monospace',
          whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
        }}>
          <span style={{ color: resonanceValue ? C.red : C.green }}>{resonanceText}</span>
          {resonanceParts.length > 0 ? ` · ${resonanceParts.join(' · ')}` : ''}
        </div>
      )}
    </div>
  )
}
