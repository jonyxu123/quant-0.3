import React, { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import axios from 'axios'
import { Star } from 'lucide-react'

/* ─── 颜色常量 ─── */
const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', cyanBg: 'rgba(0,200,200,0.08)', cyanDark: '#006666',
  red: '#ff6b6b', green: '#4ade80', yellow: '#ffaa00',
  v60: '#3b82f6', v70: '#8b5cf6', v71: '#f59e0b', v80: '#00c8c8',
}

const VERSION_COLORS: Record<string, string> = {
  'v6.0': C.v60, 'v7.0': C.v70, 'v7.1': C.v71, 'v8.0': C.v80,
}

/* ─── 样式 ─── */
const S = {
  root: { display: 'flex', gap: 14, height: 'calc(100vh - 97px)', overflow: 'hidden' } as React.CSSProperties,
  // 左侧策略选择面板
  left: { width: 280, minWidth: 280, display: 'flex', flexDirection: 'column', gap: 10, overflowY: 'auto' } as React.CSSProperties,
  card: (extra?: React.CSSProperties): React.CSSProperties => ({ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: '14px 16px', ...extra }),
  secTitle: { color: '#9aa0b8', fontSize: 12, fontWeight: 600, marginBottom: 10 } as React.CSSProperties,
  // 策略 tag
  strategyTag: (active: boolean, color: string): React.CSSProperties => ({
    display: 'inline-flex', alignItems: 'center', gap: 4,
    padding: '3px 8px', borderRadius: 4, cursor: 'pointer',
    border: `1px solid ${active ? color : C.border}`,
    background: active ? `${color}22` : 'transparent',
    color: active ? color : C.dim,
    fontSize: 11, fontWeight: active ? 600 : 400,
    transition: 'all 0.12s', margin: '2px',
  }),
  // 右侧结果
  right: { flex: 1, display: 'flex', flexDirection: 'column', gap: 10, overflow: 'hidden' } as React.CSSProperties,
  toolbar: { display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' as const },
  input: { background: C.bg, border: `1px solid ${C.border}`, borderRadius: 5, color: C.text, padding: '6px 10px', fontSize: 12, outline: 'none', width: 120 } as React.CSSProperties,
  select: { background: C.bg, border: `1px solid ${C.border}`, borderRadius: 5, color: C.text, padding: '6px 10px', fontSize: 12, outline: 'none' } as React.CSSProperties,
  btnRun: (disabled: boolean): React.CSSProperties => ({
    background: disabled ? '#1a1f2e' : C.cyanDark, border: `1px solid ${disabled ? C.border : C.cyan}`,
    color: disabled ? C.dim : C.cyan, borderRadius: 5, padding: '6px 18px', cursor: disabled ? 'not-allowed' : 'pointer', fontSize: 12, fontWeight: 600,
  }),
  tableWrap: { flex: 1, overflowY: 'auto', background: C.card, border: `1px solid ${C.border}`, borderRadius: 8 } as React.CSSProperties,
  th: { color: C.dim, fontSize: 11, fontWeight: 500, padding: '8px 12px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, background: '#141720', position: 'sticky', top: 0, whiteSpace: 'nowrap' } as React.CSSProperties,
  td: { color: C.text, padding: '7px 12px', borderBottom: `1px solid #161926`, fontSize: 12 } as React.CSSProperties,
}

interface StrategyItem { id: string; name: string; regime: string; factor_count: number; has_hard_filter: boolean }
interface StrategyDetail {
  id: string; name: string; regime: string;
  weights: Record<string, number>;
  hard_filter: Record<string, any>;
}
interface StockItem {
  rank: number; ts_code: string; name: string; industry: string;
  strategy_score: number; appear_count?: number; strategies?: string;
  factor_values?: Record<string, number | string>;
}

// 因子ID → 中文名（常用因子内置映射，其余显示ID本身）
const FACTOR_NAMES: Record<string, string> = {
  V_EP:'盈利市值比', V_BP:'账面市值比', V_FCFY:'自由现金流收益率', V_OCFY:'经营现金流收益率',
  V_DY:'股息率', V_PE_TTM:'市盈率TTM', V_PB:'市净率',
  P_ROE:'净资产收益率', P_ROA:'总资产收益率', P_GPM:'毛利率', P_CFOA:'全部资产现金回收率',
  G_NI_YOY:'净利润同比增速', G_REV_YOY:'营收同比增速', G_NI_QOQ:'净利润环比', G_QPT:'季度利润趋势',
  Q_OCF_NI:'盈利现金保障倍数', Q_APR:'应计利润占比', Q_GPMD:'毛利率变动', Q_ATD:'资产周转率变动',
  M_MOM_12_1:'12-1月动量', M_MOM_6M:'6月动量', M_MOM_3M:'3月动量',
  M_RS_6M:'6月相对强度', M_RS_3M:'3月相对强度',
  M_MA_BULL:'均线多头排列', M_VOL_ACCEL:'量能加速', M_NEW_HIGH_20:'20日新高',
  M_NEW_HIGH_60:'60日新高', M_MACD_GOLDEN:'MACD金叉', M_RSI_OVERSOLD:'RSI超卖',
  M_RSI_50_70:'RSI强势区', M_INDU_MOM:'行业动量',
  S_LN_FLOAT:'对数流通市值', S_AMT_20_Z:'20日成交额', S_TURNOVER:'换手率',
  L_CCR:'现金流动负债比', L_DEBT_RATIO:'资产负债率',
  E_EARNINGS_MOM:'盈利预测上调', E_SUE:'标准化预期外盈利',
  I_FUND_HOLD_PCT:'基金持仓比例', I_FUND_CONSENSUS:'基金持仓一致性',
  I_FUND_NEW:'新进基金数', I_FUND_COUNT:'持仓基金数',
  I_NORTH_HOLD_PCT:'北向持仓比例',
  C_WINNER_RATE:'筹码获利比例', C_CONCENTRATION:'筹码集中度',
  MF_F_OUT_IN_RATIO:'大单资金比', MF_MAIN_NET_IN:'主力净流入',
  MF_NORTH_NET_IN_20D:'北向20日净流入', MF_NORTH_HOLD_CHANGE:'北向持仓变动',
  MF_SECTOR_FLOW_IN:'行业资金流入', MF_VOL_SHRINK:'量能萎缩',
  EV_EARNING_ALERT:'业绩预增', EV_LHB_NET_BUY:'龙虎榜机构净买入',
  EV_SHHOLDER_INCREASE:'股东增持', EV_BLOCK_TRADE_PREM:'大宗溢价',
  EV_MANAGEMENT_BUY:'管理层增持',
  TP_MACD_DIV_BOT:'MACD底背离', TP_VOL_COMPRESS:'量能压缩',
}

export default function StockSelection() {
  const [grouped, setGrouped] = useState<Record<string, StrategyItem[]>>({})
  const [selected, setSelected] = useState<Set<string>>(new Set(['D']))
  const [tradeDate, setTradeDate] = useState('')
  const [topN, setTopN] = useState(50)
  const [applyFilter, setApplyFilter] = useState(true)
  const [combineMode, setCombineMode] = useState<'union' | 'intersect' | 'score'>('union')
  const [latestDate, setLatestDate] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<{ stocks: StockItem[]; summary: any; trade_date: string; count: number; error?: string } | null>(null)
  const [hoverStrategy, setHoverStrategy] = useState<StrategyItem | null>(null)
  const [detailCache, setDetailCache] = useState<Record<string, StrategyDetail>>({})
  const [activeDetail, setActiveDetail] = useState<StrategyDetail | null>(null)
  const [expandedStock, setExpandedStock] = useState<string | null>(null)

  // 自选股票状态
  const [watchlist, setWatchlist] = useState<Set<string>>(new Set())

  // 加载自选列表（从后端API）
  const loadWatchlist = () => {
    axios.get('http://localhost:8000/api/watchlist/list')
      .then(r => {
        const items = r.data.data || []
        setWatchlist(new Set(items.map((s: any) => s.ts_code)))
      })
      .catch(() => {})
  }

  useEffect(() => { loadWatchlist() }, [])

  // 添加到自选
  const toggleWatchlist = (stock: StockItem) => {
    const exists = watchlist.has(stock.ts_code)
    if (exists) {
      // 移除
      axios.delete(`http://localhost:8000/api/watchlist/remove/${stock.ts_code}`)
        .then(() => {
          setWatchlist(prev => {
            const next = new Set(prev)
            next.delete(stock.ts_code)
            return next
          })
        })
        .catch(() => {})
    } else {
      // 添加 - 将策略ID转换为名称
      const idToName: Record<string, string> = {}
      for (const items of Object.values(grouped)) {
        for (const s of items) idToName[s.id] = s.name
      }
      const strategyNames = Array.from(selected)
        .map(id => idToName[id] || id)
        .join(',')
      axios.post('http://localhost:8000/api/watchlist/add', {
        ts_code: stock.ts_code,
        name: stock.name,
        industry: stock.industry,
        source_strategy: strategyNames,
      })
        .then(() => setWatchlist(prev => new Set(prev).add(stock.ts_code)))
        .catch(() => {})
    }
  }

  useEffect(() => {
    axios.get('/api/strategy/list').then(r => setGrouped(r.data.grouped)).catch(() => {})
    axios.get('/api/strategy/latest_date').then(r => {
      const d = r.data.latest_date
      if (d) { setLatestDate(d); setTradeDate(d) }
    }).catch(() => {})
  }, [])

  const toggleStrategy = (id: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(id)) { if (next.size > 1) next.delete(id) }
      else next.add(id)
      return next
    })
  }

  const selectAll = () => setSelected(new Set(Object.values(grouped).flat().map(s => s.id)))
  const clearAll = () => setSelected(new Set(['D']))

  const run = async () => {
    setLoading(true); setResult(null)
    try {
      const res = await axios.post('/api/strategy/select', {
        trade_date: tradeDate || undefined,
        strategy_ids: Array.from(selected),
        top_n: topN,
        apply_filter: applyFilter,
        combine_mode: combineMode,
      })
      setResult(res.data)
    } catch (e: any) {
      setResult({ stocks: [], count: 0, summary: null, trade_date: tradeDate, error: e.response?.data?.detail || '请求失败' })
    }
    setLoading(false)
  }

  const loadDetail = async (id: string) => {
    if (detailCache[id]) { setActiveDetail(detailCache[id]); return }
    try {
      const r = await axios.get(`/api/strategy/info/${id}`)
      const d: StrategyDetail = r.data
      setDetailCache(prev => ({ ...prev, [id]: d }))
      setActiveDetail(d)
    } catch {}
  }

  const multiMode = selected.size > 1

  return (
    <div style={S.root}>
      {/* ── 左侧策略面板 ── */}
      <div style={S.left}>
        {/* 策略选择 */}
        <div style={S.card()}>
          <div style={S.secTitle}>策略选择 <span style={{ color: C.cyan, fontWeight: 700 }}>{selected.size}</span> / 30</div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 10 }}>
            <button onClick={selectAll} style={{ ...S.btnRun(false), padding: '3px 8px', fontSize: 11 }}>全选</button>
            <button onClick={clearAll} style={{ background: '#1a1f2e', border: `1px solid ${C.border}`, color: C.dim, borderRadius: 4, padding: '3px 8px', fontSize: 11, cursor: 'pointer' }}>重置</button>
          </div>
          {Object.entries(grouped).map(([ver, items]) => (
            <div key={ver} style={{ marginBottom: 10 }}>
              <div style={{ color: VERSION_COLORS[ver] ?? C.dim, fontSize: 10, fontWeight: 700, marginBottom: 4, letterSpacing: 1 }}>{ver}</div>
              <div style={{ display: 'flex', flexWrap: 'wrap' }}>
                {items.map(s => (
                  <span
                    key={s.id}
                    style={S.strategyTag(selected.has(s.id), VERSION_COLORS[ver] ?? C.cyan)}
                    onClick={() => toggleStrategy(s.id)}
                    onMouseEnter={() => { setHoverStrategy(s); loadDetail(s.id) }}
                    onMouseLeave={() => { setHoverStrategy(null); setActiveDetail(null) }}
                    title={`${s.id}: ${s.regime}`}
                  >
                    {s.name}
                    {s.has_hard_filter && <span style={{ fontSize: 8, opacity: 0.7, marginLeft: 4 }}>●</span>}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* 悬停策略详情 */}
        {hoverStrategy && (
          <div style={S.card({ padding: '12px 14px', borderColor: `${C.cyan}33` })}>
            <div style={{ color: C.bright, fontSize: 12, fontWeight: 700, marginBottom: 2 }}>
              {hoverStrategy.id}：{hoverStrategy.name}
            </div>
            <div style={{ color: C.dim, fontSize: 10, marginBottom: 8, lineHeight: 1.5 }}>
              {hoverStrategy.regime}
            </div>
            {activeDetail ? (
              <>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr auto auto', gap: '2px 8px', alignItems: 'center' }}>
                  {/* 表头 */}
                  <div style={{ color: C.dim, fontSize: 10, paddingBottom: 4, borderBottom: `1px solid ${C.border}` }}>因子名称</div>
                  <div style={{ color: C.dim, fontSize: 10, paddingBottom: 4, borderBottom: `1px solid ${C.border}`, textAlign: 'center' }}>方向</div>
                  <div style={{ color: C.dim, fontSize: 10, paddingBottom: 4, borderBottom: `1px solid ${C.border}`, textAlign: 'right' }}>权重</div>
                  {/* 按权重绝对值排序 */}
                  {Object.entries(activeDetail.weights)
                    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
                    .map(([fid, w]) => (
                      <React.Fragment key={fid}>
                        <div style={{ color: C.text, fontSize: 11, padding: '3px 0' }}>
                          {FACTOR_NAMES[fid] ?? fid}
                        </div>
                        <div style={{ textAlign: 'center', fontSize: 10 }}>
                          {w >= 0
                            ? <span style={{ color: C.green }}>↑ 正向</span>
                            : <span style={{ color: C.red }}>↓ 负向</span>}
                        </div>
                        <div style={{ textAlign: 'right', fontFamily: 'monospace', fontSize: 11,
                          color: w >= 0 ? C.cyan : C.red, fontWeight: 600 }}>
                          {Math.round(Math.abs(w) * 100)}%
                        </div>
                      </React.Fragment>
                    ))
                  }
                </div>
                {hoverStrategy.has_hard_filter && (
                  <div style={{ marginTop: 8, color: C.yellow, fontSize: 10 }}>⚡ 含硬性过滤条件</div>
                )}
              </>
            ) : (
              <div style={{ color: C.dim, fontSize: 11 }}>加载中...</div>
            )}
          </div>
        )}

        {/* 多策略组合说明 */}
        {multiMode && (
          <div style={S.card({ padding: '10px 14px', borderColor: `${C.cyan}44` })}>
            <div style={{ color: C.cyan, fontSize: 11, fontWeight: 600, marginBottom: 6 }}>多策略组合模式</div>
            {[
              { v: 'union', label: '并集', desc: '出现在任一策略中，按频次+最高分排序' },
              { v: 'score', label: '评分融合', desc: '各策略得分归一化后取平均' },
              { v: 'intersect', label: '交集', desc: '同时被所有策略选中的股票' },
            ].map(opt => (
              <div key={opt.v} onClick={() => setCombineMode(opt.v as any)}
                style={{ display: 'flex', alignItems: 'flex-start', gap: 8, padding: '5px 0', cursor: 'pointer' }}>
                <div style={{ width: 14, height: 14, borderRadius: '50%', border: `2px solid ${combineMode === opt.v ? C.cyan : C.border}`, background: combineMode === opt.v ? C.cyan : 'transparent', flexShrink: 0, marginTop: 1 }} />
                <div>
                  <div style={{ color: combineMode === opt.v ? C.cyan : C.text, fontSize: 12 }}>{opt.label}</div>
                  <div style={{ color: C.dim, fontSize: 10 }}>{opt.desc}</div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── 右侧结果区 ── */}
      <div style={S.right}>
        {/* 工具栏 */}
        <div style={S.card({ padding: '12px 16px' })}>
          <div style={S.toolbar}>
            <div style={{ color: C.dim, fontSize: 11 }}>交易日</div>
            <input style={S.input} value={tradeDate} onChange={e => setTradeDate(e.target.value)} placeholder={latestDate || 'YYYYMMDD'} />
            {latestDate && <span style={{ color: C.dim, fontSize: 10 }}>最新: {latestDate}</span>}

            <div style={{ color: C.dim, fontSize: 11, marginLeft: 8 }}>选股数</div>
            <input style={{ ...S.input, width: 70 }} type="number" value={topN} min={10} max={200} onChange={e => setTopN(Number(e.target.value))} />

            <label style={{ display: 'flex', alignItems: 'center', gap: 5, color: C.dim, fontSize: 11, cursor: 'pointer', marginLeft: 8 }}>
              <input type="checkbox" checked={applyFilter} onChange={e => setApplyFilter(e.target.checked)} style={{ accentColor: C.cyan }} />
              硬性过滤
            </label>

            <button style={{ ...S.btnRun(loading), marginLeft: 'auto' }} onClick={run} disabled={loading}>
              {loading ? '计算中...' : '▶ 开始选股'}
            </button>
          </div>

          {/* 已选策略标签 */}
          <div style={{ marginTop: 10, display: 'flex', flexWrap: 'wrap', gap: 4, alignItems: 'center' }}>
            <span style={{ color: C.dim, fontSize: 11 }}>已选:</span>
            {Array.from(selected).map(id => {
              // 查找策略对象获取名称
              const allStrategies = Object.values(grouped).flat()
              const strategy = allStrategies.find(s => s.id === id)
              const ver = Object.entries(grouped).find(([, items]) => items.some(s => s.id === id))?.[0]
              const color = VERSION_COLORS[ver ?? ''] ?? C.cyan
              return <span key={id} style={{ background: `${color}22`, border: `1px solid ${color}`, color, borderRadius: 3, padding: '1px 6px', fontSize: 11 }}>{strategy?.name ?? id}</span>
            })}
            {multiMode && (
              <span style={{ color: C.dim, fontSize: 11, marginLeft: 4 }}>
                · {combineMode === 'union' ? '并集' : combineMode === 'intersect' ? '交集' : '评分融合'}
              </span>
            )}
          </div>
        </div>

        {/* 结果摘要 */}
        {result && (
          <div style={S.card({ padding: '10px 16px' })}>
            {result.error ? (
              <div style={{ color: C.red, fontSize: 12 }}>⚠ {result.error}</div>
            ) : (
              <div style={{ display: 'flex', gap: 24, alignItems: 'center', flexWrap: 'wrap' }}>
                <span style={{ color: C.dim, fontSize: 11 }}>交易日: <span style={{ color: C.bright }}>{result.trade_date}</span></span>
                <span style={{ color: C.dim, fontSize: 11 }}>选出: <span style={{ color: C.cyan, fontSize: 16, fontWeight: 700 }}>{result.count}</span> 只</span>
                {result.summary?.per_strategy_counts && Object.entries(result.summary.per_strategy_counts).map(([sid, cnt]) => (
                  <span key={sid} style={{ color: C.dim, fontSize: 11 }}>{sid}: <span style={{ color: C.text }}>{String(cnt)}</span></span>
                ))}
              </div>
            )}
          </div>
        )}

        {/* 结果表格 */}
        <div style={S.tableWrap}>
          {!result && !loading && (
            <div style={{ padding: 40, textAlign: 'center', color: C.dim, fontSize: 13 }}>
              选择策略后点击「开始选股」
            </div>
          )}
          {loading && (
            <div style={{ padding: 40, textAlign: 'center', color: C.dim, fontSize: 13 }}>
              正在计算因子并选股，请稍候...
            </div>
          )}
          {result && result.stocks.length > 0 && (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr>
                  <th style={{ ...S.th, width: 40 }}>⭐</th>
                  <th style={S.th}>#</th>
                  <th style={S.th}>代码</th>
                  <th style={S.th}>名称</th>
                  <th style={S.th}>行业</th>
                  <th style={{ ...S.th, textAlign: 'right' as const }}>策略得分</th>
                  {multiMode && combineMode === 'union' && <th style={S.th}>命中策略数</th>}
                  {multiMode && combineMode === 'union' && <th style={S.th}>命中策略</th>}
                  <th style={{ ...S.th, textAlign: 'center' }}>因子详情</th>
                </tr>
              </thead>
              <tbody>
                {result.stocks.map((stock, i) => (
                  <React.Fragment key={stock.ts_code}>
                    <tr style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)' }}>
                      {/* 加自选按钮 */}
                      <td style={{ ...S.td, textAlign: 'center', width: 40 }}>
                        <button
                          onClick={() => toggleWatchlist(stock)}
                          style={{
                            background: 'none',
                            border: 'none',
                            cursor: 'pointer',
                            padding: 4,
                            color: watchlist.has(stock.ts_code) ? C.yellow : C.dim,
                            transition: 'all 0.2s'
                          }}
                          title={watchlist.has(stock.ts_code) ? '从自选移除' : '加入自选'}
                        >
                          <Star
                            size={16}
                            fill={watchlist.has(stock.ts_code) ? C.yellow : 'transparent'}
                            strokeWidth={watchlist.has(stock.ts_code) ? 0 : 2}
                          />
                        </button>
                      </td>
                      <td style={{ ...S.td, color: C.dim, width: 36 }}>{stock.rank}</td>
                      <td style={{ ...S.td, fontFamily: 'monospace' }}>
                        <Link to={`/stock/${stock.ts_code}`} style={{ color: C.cyan, textDecoration: 'none' }} title="查看详情">
                          {stock.ts_code}
                        </Link>
                      </td>
                      <td style={{ ...S.td, color: C.bright, fontWeight: 500 }}>
                        <Link to={`/stock/${stock.ts_code}`} style={{ color: C.bright, textDecoration: 'none' }}>
                          {stock.name || '-'}
                        </Link>
                      </td>
                      <td style={{ ...S.td, color: C.dim }}>{stock.industry || '-'}</td>
                      <td style={{ ...S.td, textAlign: 'right', color: C.cyan, fontFamily: 'monospace' }}>
                        {stock.strategy_score.toFixed(4)}
                      </td>
                      {multiMode && combineMode === 'union' && (
                        <td style={{ ...S.td, textAlign: 'center' as const }}>
                          <span style={{
                            background: (stock.appear_count ?? 0) >= selected.size ? `${C.green}22` : C.cyanBg,
                            color: (stock.appear_count ?? 0) >= selected.size ? C.green : C.cyan,
                            border: `1px solid ${(stock.appear_count ?? 0) >= selected.size ? C.green : C.cyan}`,
                            borderRadius: 3, padding: '1px 6px', fontSize: 11
                          }}>
                            {stock.appear_count ?? 1}/{selected.size}
                          </span>
                        </td>
                      )}
                      {multiMode && combineMode === 'union' && (
                        <td style={{ ...S.td, color: C.dim, fontSize: 11 }}>{stock.strategies ?? '-'}</td>
                      )}
                      {/* 因子详情展开按钮 */}
                      <td style={{ ...S.td, textAlign: 'center' }}>
                        <button
                          onClick={() => setExpandedStock(expandedStock === stock.ts_code ? null : stock.ts_code)}
                          style={{
                            background: expandedStock === stock.ts_code ? `${C.cyan}33` : 'transparent',
                            border: `1px solid ${C.cyan}`,
                            color: C.cyan,
                            borderRadius: 4,
                            padding: '2px 8px',
                            fontSize: 11,
                            cursor: 'pointer',
                            transition: 'all 0.2s'
                          }}
                        >
                          {expandedStock === stock.ts_code ? '收起' : '查看'}
                        </button>
                      </td>
                    </tr>
                    {/* 展开的因子详情行 */}
                    {expandedStock === stock.ts_code && stock.factor_values && (
                      <tr style={{ background: 'rgba(0,200,200,0.02)' }}>
                        <td colSpan={multiMode && combineMode === 'union' ? 9 : 7} style={{ padding: 0 }}>
                          <div style={{ padding: '12px 16px', borderBottom: `1px solid ${C.border}` }}>
                            <div style={{ color: C.cyan, fontSize: 11, fontWeight: 600, marginBottom: 10 }}>
                              📊 因子值详情
                            </div>
                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '6px 20px' }}>
                              {Object.entries(stock.factor_values)
                                .sort(([a], [b]) => a.localeCompare(b))
                                .map(([factorId, value]) => {
                                  const isPositive = typeof value === 'number' && value > 0;
                                  const isNegative = typeof value === 'number' && value < 0;
                                  return (
                                    <div key={factorId} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0' }}>
                                      <span style={{ color: C.dim, fontSize: 11 }}>
                                        {FACTOR_NAMES[factorId] || factorId}
                                      </span>
                                      <span style={{
                                        color: isPositive ? C.green : isNegative ? C.red : C.text,
                                        fontSize: 11,
                                        fontFamily: 'monospace',
                                        fontWeight: 500
                                      }}>
                                        {typeof value === 'number' ? value.toFixed(3) : value}
                                      </span>
                                    </div>
                                  );
                                })}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          )}
          {result && result.stocks.length === 0 && !result.error && (
            <div style={{ padding: 40, textAlign: 'center', color: C.dim, fontSize: 13 }}>
              该日期无选股结果（可能因子数据为空）
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
