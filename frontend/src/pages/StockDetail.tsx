import React, { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import axios from 'axios'
import { Activity, DollarSign, Users, TrendingUp, Package, AlertCircle, Gift, Building2, ArrowLeft, ExternalLink } from 'lucide-react'
import KLineChart from '../components/KLineChart'
import ChipDistribution from '../components/ChipDistribution'

/* ─── 颜色常量 ─── */
const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', cyanBg: 'rgba(0,200,200,0.08)', cyanDark: '#006666',
  red: '#ff6b6b', green: '#4ade80', yellow: '#ffaa00', blue: '#3b82f6',
}

const S = {
  root: { padding: '16px 20px', background: C.bg, minHeight: '100%', color: C.text } as React.CSSProperties,
  backBtn: { display: 'flex', alignItems: 'center', gap: 6, color: C.dim, fontSize: 12, cursor: 'pointer', background: 'none', border: 'none', padding: '4px 8px', marginBottom: 8 } as React.CSSProperties,
  headerCard: { background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: '16px 20px', marginBottom: 12 } as React.CSSProperties,
  stockTitle: { display: 'flex', alignItems: 'baseline', gap: 12, flexWrap: 'wrap' as const } as React.CSSProperties,
  stockName: { color: C.bright, fontSize: 22, fontWeight: 700 } as React.CSSProperties,
  stockCode: { color: C.cyan, fontSize: 14, fontFamily: 'monospace' } as React.CSSProperties,
  tag: (bg: string, color: string): React.CSSProperties => ({
    padding: '2px 8px', borderRadius: 3, fontSize: 11, background: bg, color,
  }),
  metaGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 12, marginTop: 12 } as React.CSSProperties,
  metaItem: { display: 'flex', flexDirection: 'column' as const, gap: 2 } as React.CSSProperties,
  metaLabel: { color: C.dim, fontSize: 11 } as React.CSSProperties,
  metaValue: { color: C.bright, fontSize: 13, fontFamily: 'monospace' } as React.CSSProperties,
  tabs: { display: 'flex', gap: 4, borderBottom: `1px solid ${C.border}`, marginBottom: 12, overflowX: 'auto' as const } as React.CSSProperties,
  tab: (active: boolean): React.CSSProperties => ({
    padding: '8px 16px', cursor: 'pointer', fontSize: 13, whiteSpace: 'nowrap',
    color: active ? C.cyan : C.dim, borderBottom: active ? `2px solid ${C.cyan}` : '2px solid transparent',
    background: 'none', border: 'none', borderRadius: 0, display: 'flex', alignItems: 'center', gap: 6,
  }),
  tabContent: { background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 16, minHeight: 300 } as React.CSSProperties,
  table: { width: '100%', borderCollapse: 'collapse' as const, fontSize: 12 } as React.CSSProperties,
  th: { color: C.dim, fontSize: 11, fontWeight: 500, padding: '8px 10px', textAlign: 'left' as const, borderBottom: `1px solid ${C.border}`, background: '#141720' } as React.CSSProperties,
  td: { color: C.text, padding: '8px 10px', borderBottom: `1px solid #161926`, fontSize: 12 } as React.CSSProperties,
  empty: { padding: 40, textAlign: 'center' as const, color: C.dim, fontSize: 13 } as React.CSSProperties,
  loading: { padding: 40, textAlign: 'center' as const, color: C.dim, fontSize: 13 } as React.CSSProperties,
}

const API = 'http://localhost:8000'

interface BasicData {
  basic: any
  company: any
  latest_basic: any
  latest_daily: any
}

type TabKey = 'kline' | 'fina' | 'holders' | 'moneyflow' | 'top_list' | 'block_trade' | 'share_float' | 'holder_trade' | 'dividend' | 'forecast' | 'chip'

const TABS: Array<{ key: TabKey; label: string; icon: any }> = [
  { key: 'kline', label: 'K线技术', icon: Activity },
  { key: 'chip', label: '筹码分布', icon: Activity },
  { key: 'fina', label: '财务指标', icon: DollarSign },
  { key: 'holders', label: '十大股东', icon: Users },
  { key: 'moneyflow', label: '资金流向', icon: TrendingUp },
  { key: 'top_list', label: '龙虎榜', icon: TrendingUp },
  { key: 'block_trade', label: '大宗交易', icon: Package },
  { key: 'share_float', label: '限售解禁', icon: AlertCircle },
  { key: 'holder_trade', label: '股东增减持', icon: Users },
  { key: 'dividend', label: '分红送配', icon: Gift },
  { key: 'forecast', label: '业绩预告', icon: Building2 },
]

export default function StockDetail() {
  const { ts_code } = useParams<{ ts_code: string }>()
  const navigate = useNavigate()
  const [basic, setBasic] = useState<BasicData | null>(null)
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<TabKey>('kline')

  useEffect(() => {
    if (!ts_code) return
    setLoading(true)
    axios.get(`${API}/api/stock/basic/${ts_code}`)
      .then(r => setBasic(r.data))
      .catch(err => console.error('基础信息获取失败', err))
      .finally(() => setLoading(false))
  }, [ts_code])

  if (!ts_code) return <div style={S.empty}>未指定股票代码</div>
  if (loading) return <div style={S.loading}>加载中...</div>
  if (!basic) return <div style={S.empty}>股票不存在</div>

  const b = basic.basic || {}
  const lb = basic.latest_basic || {}
  const ld = basic.latest_daily || {}
  const pctChg = ld.pct_chg ?? 0
  const pctColor = pctChg > 0 ? C.red : pctChg < 0 ? C.green : C.dim

  const openEastMoney = () => {
    const [code, suffix] = ts_code.split('.')
    let url = ''
    if (suffix === 'SZ') url = `https://quote.eastmoney.com/sz${code}.html`
    else if (suffix === 'SH') url = `https://quote.eastmoney.com/sh${code}.html`
    else url = `https://quote.eastmoney.com/${code}.html`
    window.open(url, '_blank')
  }

  return (
    <div style={S.root}>
      <button style={S.backBtn} onClick={() => navigate(-1)}>
        <ArrowLeft size={14} /> 返回
      </button>

      {/* 股票头部信息 */}
      <div style={S.headerCard}>
        <div style={S.stockTitle}>
          <span style={S.stockName}>{b.name || '-'}</span>
          <span style={S.stockCode}>{b.ts_code}</span>
          <span style={S.tag(C.cyanBg, C.cyan)}>{b.industry || '其他'}</span>
          {b.market && <span style={S.tag('rgba(59,130,246,0.15)', C.blue)}>{b.market}</span>}
          {b.area && <span style={S.tag('rgba(139,92,246,0.15)', '#8b5cf6')}>{b.area}</span>}
          <button onClick={openEastMoney} style={{ marginLeft: 'auto', background: 'none', border: `1px solid ${C.border}`, color: C.dim, padding: '4px 10px', borderRadius: 4, cursor: 'pointer', fontSize: 11, display: 'flex', alignItems: 'center', gap: 4 }}>
            <ExternalLink size={12} /> 东方财富
          </button>
        </div>

        <div style={S.metaGrid}>
          <Meta label="最新价" value={ld.close?.toFixed(2) ?? '-'} color={C.bright} />
          <Meta label="涨跌幅" value={pctChg != null ? `${pctChg > 0 ? '+' : ''}${pctChg.toFixed(2)}%` : '-'} color={pctColor} />
          <Meta label="成交额(亿)" value={ld.amount ? (ld.amount / 1e5).toFixed(2) : '-'} />
          <Meta label="PE(TTM)" value={lb.pe_ttm?.toFixed(2) ?? '-'} />
          <Meta label="PB" value={lb.pb?.toFixed(2) ?? '-'} />
          <Meta label="PS(TTM)" value={lb.ps_ttm?.toFixed(2) ?? '-'} />
          <Meta label="股息率(TTM)" value={lb.dv_ttm != null ? `${lb.dv_ttm.toFixed(2)}%` : '-'} />
          <Meta label="总市值(亿)" value={lb.total_mv ? (lb.total_mv / 1e4).toFixed(2) : '-'} />
          <Meta label="流通市值(亿)" value={lb.circ_mv ? (lb.circ_mv / 1e4).toFixed(2) : '-'} />
          <Meta label="换手率" value={lb.turnover_rate != null ? `${lb.turnover_rate.toFixed(2)}%` : '-'} />
          <Meta label="上市日期" value={b.list_date || '-'} />
          <Meta label="实控人" value={b.act_name || '-'} />
        </div>

        {basic.company && (
          <div style={{ marginTop: 12, padding: '10px 12px', background: C.bg, borderRadius: 4, fontSize: 12, color: C.text, lineHeight: 1.7 }}>
            <div><span style={{ color: C.dim }}>主营业务：</span>{basic.company.main_business || '-'}</div>
            <div style={{ marginTop: 4, color: C.dim, fontSize: 11 }}>
              董事长: {basic.company.chairman || '-'} · 总经理: {basic.company.manager || '-'} ·
              员工: {basic.company.employees || '-'} · 注册资本: {basic.company.reg_capital?.toFixed(0) || '-'}万元 ·
              地区: {basic.company.province || ''}{basic.company.city || ''}
            </div>
          </div>
        )}
      </div>

      {/* Tabs */}
      <div style={S.tabs}>
        {TABS.map(t => (
          <button key={t.key} style={S.tab(activeTab === t.key)} onClick={() => setActiveTab(t.key)}>
            <t.icon size={13} /> {t.label}
          </button>
        ))}
      </div>

      <div style={S.tabContent}>
        {activeTab === 'kline' && <KLineTab tsCode={ts_code} />}
        {activeTab === 'chip' && <ChipTab tsCode={ts_code} />}
        {activeTab === 'fina' && <FinaTab tsCode={ts_code} />}
        {activeTab === 'holders' && <HoldersTab tsCode={ts_code} />}
        {activeTab === 'moneyflow' && <MoneyFlowTab tsCode={ts_code} />}
        {activeTab === 'top_list' && <TopListTab tsCode={ts_code} />}
        {activeTab === 'block_trade' && <BlockTradeTab tsCode={ts_code} />}
        {activeTab === 'share_float' && <ShareFloatTab tsCode={ts_code} />}
        {activeTab === 'holder_trade' && <HolderTradeTab tsCode={ts_code} />}
        {activeTab === 'dividend' && <DividendTab tsCode={ts_code} />}
        {activeTab === 'forecast' && <ForecastTab tsCode={ts_code} />}
      </div>
    </div>
  )
}

const Meta: React.FC<{ label: string; value: string; color?: string }> = ({ label, value, color }) => (
  <div style={S.metaItem}>
    <span style={S.metaLabel}>{label}</span>
    <span style={{ ...S.metaValue, color: color || C.bright }}>{value}</span>
  </div>
)

/* ─── Tab 组件 ─── */

type Period = 'daily' | 'weekly' | 'monthly'
type FQ = 'qfq' | 'hfq' | 'none'
type SubInd = 'MACD' | 'KDJ' | 'RSI' | 'BOLL_PCT_B' | 'JINNIU' | 'VOL'
const SUB_INDICATORS: SubInd[] = ['VOL', 'MACD', 'KDJ', 'RSI', 'BOLL_PCT_B', 'JINNIU']

/* MA 颜色 - 需与 KLineChart 内部保持一致 */
const MA_COLOR = { ma5: '#eab308', ma10: '#a855f7', ma20: '#00c8c8', ma30: '#3b82f6', ma60: '#ff6b6b' }

function KLineTab({ tsCode }: { tsCode: string }) {
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [period, setPeriod] = useState<Period>('daily')
  const [days, setDays] = useState(250)
  const [fq, setFq] = useState<FQ>('qfq')
  const [subInds, setSubInds] = useState<SubInd[]>(['VOL', 'MACD'])
  const [showBoll, setShowBoll] = useState(false)

  useEffect(() => {
    setLoading(true)
    axios.get(`${API}/api/stock/kline/${tsCode}`, { params: { period, days, fq } })
      .then(r => setData(r.data))
      .catch(err => console.error(err))
      .finally(() => setLoading(false))
  }, [tsCode, period, days, fq])

  const toggleSub = (ind: SubInd) => {
    setSubInds(prev => prev.includes(ind) ? prev.filter(x => x !== ind) : [...prev, ind])
  }

  const Btn = ({ active, onClick, children }: any) => (
    <button onClick={onClick} style={{
      padding: '4px 10px', borderRadius: 4, fontSize: 11, cursor: 'pointer',
      background: active ? C.cyanBg : 'transparent',
      border: `1px solid ${active ? C.cyan : C.border}`,
      color: active ? C.cyan : C.dim,
    }}>{children}</button>
  )

  // 取最新一根K线的 MA 值（用于顶部显示当前价）
  const latestMA = React.useMemo(() => {
    if (!data || !data.klines || data.klines.length === 0) return null
    const last = data.klines[data.klines.length - 1]
    return {
      ma5: last.ma5, ma10: last.ma10, ma20: last.ma20, ma30: last.ma30,
      date: last.trade_date,
    }
  }, [data])

  return (
    <div>
      {/* 工具栏 */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' }}>
        {/* 周期 */}
        <div style={{ display: 'flex', gap: 4 }}>
          <Btn active={period === 'daily'} onClick={() => setPeriod('daily')}>日线</Btn>
          <Btn active={period === 'weekly'} onClick={() => setPeriod('weekly')}>周线</Btn>
          <Btn active={period === 'monthly'} onClick={() => setPeriod('monthly')}>月线</Btn>
        </div>

        {/* 数量 */}
        <div style={{ display: 'flex', gap: 4 }}>
          <span style={{ color: C.dim, fontSize: 11, alignSelf: 'center' }}>数量:</span>
          {[60, 120, 250].map(d => (
            <Btn key={d} active={days === d} onClick={() => setDays(d)}>{d}</Btn>
          ))}
        </div>

        {/* 复权 (仅日线) */}
        {period === 'daily' && (
          <div style={{ display: 'flex', gap: 4 }}>
            <Btn active={fq === 'qfq'} onClick={() => setFq('qfq')}>前复权</Btn>
            <Btn active={fq === 'hfq'} onClick={() => setFq('hfq')}>后复权</Btn>
            <Btn active={fq === 'none'} onClick={() => setFq('none')}>不复权</Btn>
          </div>
        )}

        <div style={{ flex: 1 }} />

        {/* 副图指标多选 + BOLL主图开关 */}
        <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
          <span style={{ color: C.dim, fontSize: 11 }}>副图:</span>
          {SUB_INDICATORS.map(ind => (
            <Btn key={ind} active={subInds.includes(ind)} onClick={() => toggleSub(ind)}>
              {ind === 'BOLL_PCT_B' ? '%B' : ind === 'JINNIU' ? '金牛' : ind}
            </Btn>
          ))}
          <span style={{ color: C.dim, fontSize: 11, marginLeft: 8 }}>主图:</span>
          <Btn active={showBoll} onClick={() => setShowBoll(!showBoll)}>BOLL</Btn>
        </div>
      </div>

      {/* MA 图例 + 当前价格 */}
      {latestMA && period === 'daily' && (
        <div style={{
          display: 'flex', gap: 16, padding: '8px 12px', marginBottom: 8,
          background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4,
          fontSize: 12, flexWrap: 'wrap',
        }}>
          <MAItem color={MA_COLOR.ma5} label="MA5" value={latestMA.ma5} />
          <MAItem color={MA_COLOR.ma10} label="MA10" value={latestMA.ma10} />
          <MAItem color={MA_COLOR.ma20} label="MA20" value={latestMA.ma20} />
          <MAItem color={MA_COLOR.ma30} label="MA30" value={latestMA.ma30} />
          <span style={{ marginLeft: 'auto', color: C.dim, fontSize: 11 }}>
            截止 {latestMA.date}
          </span>
        </div>
      )}

      {/* K线图容器（自适应高度） */}
      <div style={{
        flex: 1,
        minHeight: 420,
        height: 'calc(100vh - 440px)',
        display: 'flex',
        flexDirection: 'column',
      }}>
        {loading ? <div style={S.loading}>加载K线...</div>
          : !data || data.count === 0 ? <div style={S.empty}>无K线数据</div>
          : <KLineChart
              klines={data.klines}
              subIndicators={subInds}
              showBoll={showBoll}
              usePrecomputed={period === 'daily'}
              height="100%"
            />}
      </div>
    </div>
  )
}

const MAItem: React.FC<{ color: string; label: string; value?: number }> = ({ color, label, value }) => (
  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
    <span style={{ width: 14, height: 3, background: color, borderRadius: 1 }} />
    <span style={{ color: C.dim, fontSize: 11 }}>{label}:</span>
    <span style={{ color, fontFamily: 'monospace', fontWeight: 600 }}>
      {value != null ? Number(value).toFixed(2) : '-'}
    </span>
  </div>
)

function ChipTab({ tsCode }: { tsCode: string }) {
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [days, setDays] = useState(120)

  useEffect(() => {
    setLoading(true)
    axios.get(`${API}/api/stock/chip/${tsCode}`, { params: { days } })
      .then(r => setData(r.data))
      .catch(err => console.error(err))
      .finally(() => setLoading(false))
  }, [tsCode, days])

  const Btn = ({ active, onClick, children }: any) => (
    <button onClick={onClick} style={{
      padding: '4px 10px', borderRadius: 4, fontSize: 11, cursor: 'pointer',
      background: active ? C.cyanBg : 'transparent',
      border: `1px solid ${active ? C.cyan : C.border}`,
      color: active ? C.cyan : C.dim,
    }}>{children}</button>
  )

  return (
    <div>
      <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
        <span style={{ color: C.dim, fontSize: 11 }}>天数:</span>
        {[60, 120, 250].map(d => (
          <Btn key={d} active={days === d} onClick={() => setDays(d)}>{d}</Btn>
        ))}
      </div>
      {loading ? <div style={S.loading}>加载筹码分布...</div>
        : !data || data.count === 0 ? <div style={S.empty}>无筹码分布数据</div>
        : <ChipDistribution data={data.data} />}
    </div>
  )
}

function GenericTab({ tsCode, endpoint, columns, empty = '暂无数据' }: {
  tsCode: string
  endpoint: string
  columns: Array<{ key: string; label: string; format?: (v: any) => string; color?: (v: any) => string | undefined }>
  empty?: string
}) {
  const [data, setData] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    setLoading(true)
    axios.get(`${API}/api/stock/${endpoint}/${tsCode}`)
      .then(r => setData(r.data.data || []))
      .catch(err => console.error(err))
      .finally(() => setLoading(false))
  }, [tsCode, endpoint])

  if (loading) return <div style={S.loading}>加载中...</div>
  if (data.length === 0) return <div style={S.empty}>{empty}</div>

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={S.table}>
        <thead>
          <tr>
            {columns.map(c => <th key={c.key} style={S.th}>{c.label}</th>)}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)' }}>
              {columns.map(c => {
                const val = row[c.key]
                const display = c.format ? c.format(val) : (val == null ? '-' : String(val))
                const color = c.color ? c.color(val) : undefined
                return <td key={c.key} style={{ ...S.td, color: color || C.text }}>{display}</td>
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const fmtNum = (n: any, decimals = 2) => n == null ? '-' : Number(n).toFixed(decimals)
const fmtPct = (n: any) => n == null ? '-' : `${Number(n).toFixed(2)}%`
const signColor = (v: any) => v == null ? undefined : Number(v) > 0 ? C.red : Number(v) < 0 ? C.green : undefined

function FinaTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="fina" columns={[
    { key: 'end_date', label: '报告期' },
    { key: 'eps', label: 'EPS(元)', format: v => fmtNum(v, 4) },
    { key: 'roe', label: 'ROE(%)', format: fmtPct, color: signColor },
    { key: 'roa', label: 'ROA(%)', format: fmtPct, color: signColor },
    { key: 'grossprofit_margin', label: '毛利率(%)', format: fmtPct },
    { key: 'netprofit_margin', label: '净利率(%)', format: fmtPct, color: signColor },
    { key: 'debt_to_assets', label: '资产负债率(%)', format: fmtPct },
    { key: 'current_ratio', label: '流动比率', format: v => fmtNum(v, 2) },
    { key: 'quick_ratio', label: '速动比率', format: v => fmtNum(v, 2) },
    { key: 'netprofit_yoy', label: '净利同比(%)', format: fmtPct, color: signColor },
    { key: 'or_yoy', label: '营收同比(%)', format: fmtPct, color: signColor },
  ]} />
}

function HoldersTab({ tsCode }: { tsCode: string }) {
  const [floatOnly, setFloatOnly] = useState(false)
  return (
    <div>
      <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
        <button
          onClick={() => setFloatOnly(false)}
          style={{ padding: '4px 12px', borderRadius: 4, fontSize: 11, cursor: 'pointer',
            background: !floatOnly ? C.cyanBg : 'transparent',
            border: `1px solid ${!floatOnly ? C.cyan : C.border}`, color: !floatOnly ? C.cyan : C.dim }}
        >十大股东</button>
        <button
          onClick={() => setFloatOnly(true)}
          style={{ padding: '4px 12px', borderRadius: 4, fontSize: 11, cursor: 'pointer',
            background: floatOnly ? C.cyanBg : 'transparent',
            border: `1px solid ${floatOnly ? C.cyan : C.border}`, color: floatOnly ? C.cyan : C.dim }}
        >十大流通股东</button>
      </div>
      <GenericTab
        key={floatOnly ? 'float' : 'normal'}
        tsCode={tsCode}
        endpoint={floatOnly ? 'top_holders/' + tsCode + '?float_only=true' : 'top_holders'}
        columns={[
          { key: 'holder_name', label: '股东名称' },
          { key: 'holder_type', label: '类型' },
          { key: 'hold_amount', label: '持股数量(股)', format: v => v == null ? '-' : Number(v).toLocaleString() },
          { key: 'hold_ratio', label: '持股占比(%)', format: fmtPct },
          { key: 'hold_float_ratio', label: '占流通(%)', format: fmtPct },
          { key: 'hold_change', label: '变动(股)', format: v => v == null ? '-' : Number(v).toLocaleString(), color: signColor },
        ]}
      />
    </div>
  )
}

// 格式化金额（亿/万）- 数据单位可能是万元，直接显示
const fmtMoney = (n: number | null) => {
  if (n == null) return '-'
  const abs = Math.abs(n)
  if (abs >= 1e8) {
    return `${(n / 1e8).toFixed(2)}亿`
  }
  return `${n.toFixed(0)}万`
}

function MoneyFlowTab({ tsCode }: { tsCode: string }) {
  const [data, setData] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  // 固定获取30日数据
  useEffect(() => {
    setLoading(true)
    axios.get(`${API}/api/stock/moneyflow/${tsCode}`, { params: { days: 30 } })
      .then(r => setData(r.data.data || []))
      .catch(err => console.error(err))
      .finally(() => setLoading(false))
  }, [tsCode])

  if (loading) return <div style={S.loading}>加载资金流向...</div>
  if (data.length === 0) return <div style={S.empty}>暂无资金流向数据</div>

  // 订单类型定义
  const orderTypes = [
    { key: 'elg', label: '超大', color: '#ff6b6b' },
    { key: 'lg', label: '大单', color: '#ff8e8e' },
    { key: 'md', label: '中单', color: '#4ade80' },
    { key: 'sm', label: '小单', color: '#6ee7b7' },
  ]

  // 计算某段时间的统计数据（净流入/净流出）
  const calcStats = (daysData: any[]) => {
    const stats = { elg: 0, lg: 0, md: 0, sm: 0 }
    daysData.forEach(d => {
      stats.elg += (d.buy_elg_amount || 0) - (d.sell_elg_amount || 0)
      stats.lg += (d.buy_lg_amount || 0) - (d.sell_lg_amount || 0)
      stats.md += (d.buy_md_amount || 0) - (d.sell_md_amount || 0)
      stats.sm += (d.buy_sm_amount || 0) - (d.sell_sm_amount || 0)
    })
    return stats
  }

  // 各周期统计
  const stats5 = calcStats(data.slice(-5))
  const stats10 = calcStats(data.slice(-10))
  const stats20 = calcStats(data.slice(-20))
  const stats30 = calcStats(data)

  // 找出所有净流入的最大绝对值（用于计算柱状图高度）
  const allNets = data.flatMap(d => [
    (d.buy_elg_amount || 0) - (d.sell_elg_amount || 0),
    (d.buy_lg_amount || 0) - (d.sell_lg_amount || 0),
    (d.buy_md_amount || 0) - (d.sell_md_amount || 0),
    (d.buy_sm_amount || 0) - (d.sell_sm_amount || 0),
  ])
  const maxAbsNet = Math.max(...allNets.map(Math.abs), 1)

  // 每日柱状图高度计算
  const BAR_MAX_HEIGHT = 60 // 最大高度（单向）

  return (
    <div>
      {/* 周期统计面板 */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ color: C.dim, fontSize: 12, marginBottom: 12, fontWeight: 600 }}>各周期资金净流入汇总</div>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(4, 1fr)',
          gap: 12,
        }}>
          {[
            { label: '5日', stats: stats5 },
            { label: '10日', stats: stats10 },
            { label: '20日', stats: stats20 },
            { label: '30日', stats: stats30 },
          ].map(({ label, stats }) => (
            <div key={label} style={{
              background: C.bg,
              borderRadius: 8,
              border: `1px solid ${C.border}`,
              padding: '12px',
            }}>
              <div style={{ color: C.cyan, fontSize: 13, fontWeight: 600, marginBottom: 8 }}>{label}</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {orderTypes.map(type => {
                  const net = stats[type.key as keyof typeof stats]
                  const isPositive = net >= 0
                  return (
                    <div key={type.key} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
                      <span style={{ color: type.color, fontWeight: 600 }}>{type.label}</span>
                      <span style={{ color: isPositive ? C.red : C.green, fontWeight: 600 }}>
                        {isPositive ? '+' : ''}{fmtMoney(net)}
                      </span>
                    </div>
                  )
                })}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* 30日每日资金流向柱状图 */}
      <div style={{ marginTop: 24 }}>
        <div style={{ color: C.dim, fontSize: 12, marginBottom: 12, fontWeight: 600 }}>最近30日每日资金流向</div>
        <div style={{
          display: 'flex',
          gap: 10,
          alignItems: 'flex-end',
          height: 280,
          padding: '16px 12px 8px',
          background: C.bg,
          borderRadius: 8,
          border: `1px solid ${C.border}`,
          overflowX: 'auto',
          position: 'relative',
        }}>
          {/* 零轴参考线（居中） */}
          <div style={{
            position: 'absolute',
            left: 12,
            right: 12,
            top: '50%',
            height: 1,
            background: 'rgba(255,255,255,0.15)',
            pointerEvents: 'none',
            zIndex: 1,
          }} />
          
          {data.map((day) => {
            const nets = orderTypes.map(type => ({
              ...type,
              net: (day[`buy_${type.key}_amount`] || 0) - (day[`sell_${type.key}_amount`] || 0),
            }))
            
            return (
              <div key={day.trade_date} style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: 2,
                flexShrink: 0,
                width: 50,
                zIndex: 2,
              }}>
                {/* 日期 */}
                <div style={{
                  fontSize: 9,
                  color: C.dim,
                  transform: 'rotate(-45deg)',
                  transformOrigin: 'left center',
                  whiteSpace: 'nowrap',
                  marginBottom: 4,
                  marginTop: -8,
                }}>
                  {day.trade_date?.slice(4)}
                </div>
                
                {/* 四个柱状图（带正负） - 统一规则：净流入红色，净流出绿色 */}
                <div style={{
                  display: 'flex',
                  alignItems: 'flex-end',
                  height: BAR_MAX_HEIGHT * 2,
                  position: 'relative',
                }}>
                  {nets.map((n, i) => {
                    const isPositive = n.net >= 0
                    const barHeight = Math.max(Math.min(Math.abs(n.net) / maxAbsNet * BAR_MAX_HEIGHT, BAR_MAX_HEIGHT), 2)
                    // 统一规则：净流入红色，净流出绿色
                    const barColor = isPositive ? C.red : C.green
                    
                    return (
                      <div key={n.key} style={{
                        width: 8,
                        height: barHeight,
                        background: barColor,
                        opacity: 0.85,
                        position: 'absolute',
                        bottom: isPositive ? BAR_MAX_HEIGHT : undefined,
                        top: isPositive ? undefined : BAR_MAX_HEIGHT,
                        left: i * 9,
                        borderRadius: 1,
                      }} title={`${day.trade_date} ${n.label}: ${n.net > 0 ? '+' : ''}${fmtMoney(n.net)}`}>
                        {/* 金额标签 */}
                        <div style={{
                          position: 'absolute',
                          fontSize: 8,
                          color: barColor,
                          fontWeight: 600,
                          whiteSpace: 'nowrap',
                          top: isPositive ? 'auto' : '100%',
                          bottom: isPositive ? '100%' : 'auto',
                          left: '50%',
                          transform: isPositive ? 'translateX(-50%) translateY(-3px)' : 'translateX(-50%) translateY(3px)',
                        }}>
                          {(() => {
                            const abs = Math.abs(n.net)
                            if (abs >= 1e8) {
                              return `${(n.net / 1e8).toFixed(1)}亿`
                            }
                            return `${n.net.toFixed(0)}万`
                          })()}
                        </div>
                      </div>
                    )
                  })}
                </div>
                
                {/* 零轴位置占位 */}
                <div style={{ height: 0, width: '100%' }} />
              </div>
            )
          })}
        </div>
        
        {/* 图例 */}
        <div style={{
          display: 'flex',
          gap: 16,
          marginTop: 12,
          padding: '8px 12px',
          background: C.bg,
          borderRadius: 4,
          border: `1px solid ${C.border}`,
        }}>
          {orderTypes.map(type => (
            <div key={type.key} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11 }}>
              <div style={{ width: 12, height: 8, background: type.color, borderRadius: 1 }} />
              <span style={{ color: C.dim }}>{type.label}单</span>
            </div>
          ))}
          <div style={{ marginLeft: 'auto', fontSize: 10, color: C.dim }}>
            ↑红柱流入 ↓绿柱流出
          </div>
        </div>
      </div>
    </div>
  )
}

function TopListTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="top_list" columns={[
    { key: 'trade_date', label: '日期' },
    { key: 'close', label: '收盘价', format: v => fmtNum(v, 2) },
    { key: 'pct_change', label: '涨跌幅(%)', format: fmtPct, color: signColor },
    { key: 'turnover_rate', label: '换手率(%)', format: fmtPct },
    { key: 'l_buy', label: '上榜买入(万)', format: v => fmtNum(v, 0) },
    { key: 'l_sell', label: '上榜卖出(万)', format: v => fmtNum(v, 0) },
    { key: 'net_amount', label: '净买入(万)', format: v => fmtNum(v, 0), color: signColor },
    { key: 'reason', label: '上榜原因' },
  ]} empty="暂无龙虎榜记录" />
}

function BlockTradeTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="block_trade" columns={[
    { key: 'trade_date', label: '日期' },
    { key: 'price', label: '成交价', format: v => fmtNum(v, 2) },
    { key: 'vol', label: '成交量(万股)', format: v => fmtNum(v, 0) },
    { key: 'amount', label: '成交额(万)', format: v => fmtNum(v, 0) },
    { key: 'buyer', label: '买方营业部' },
    { key: 'seller', label: '卖方营业部' },
  ]} empty="暂无大宗交易" />
}

function ShareFloatTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="share_float" columns={[
    { key: 'ann_date', label: '公告日' },
    { key: 'float_date', label: '解禁日' },
    { key: 'float_share', label: '解禁数量(万股)', format: v => fmtNum(v, 0) },
    { key: 'float_ratio', label: '占总股本(%)', format: fmtPct },
    { key: 'holder_name', label: '股东名称' },
    { key: 'share_type', label: '类型' },
  ]} empty="暂无限售解禁" />
}

function HolderTradeTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="holder_trade" columns={[
    { key: 'ann_date', label: '公告日' },
    { key: 'holder_name', label: '股东' },
    { key: 'holder_type', label: '类型' },
    { key: 'in_de', label: '增减', color: (v) => v === 'IN' ? C.red : v === 'DE' ? C.green : undefined },
    { key: 'change_vol', label: '变动股数', format: v => v == null ? '-' : Number(v).toLocaleString(), color: signColor },
    { key: 'change_ratio', label: '占总股本(%)', format: fmtPct, color: signColor },
    { key: 'after_ratio', label: '变动后持股(%)', format: fmtPct },
    { key: 'avg_price', label: '均价', format: v => fmtNum(v, 2) },
  ]} empty="暂无增减持记录" />
}

function DividendTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="dividend" columns={[
    { key: 'end_date', label: '报告期' },
    { key: 'ann_date', label: '公告日' },
    { key: 'div_proc', label: '进度' },
    { key: 'stk_div', label: '送股(每10股)', format: v => fmtNum(v, 2) },
    { key: 'stk_bo_rate', label: '转增(每10股)', format: v => fmtNum(v, 2) },
    { key: 'cash_div_tax', label: '现金红利(税前元)', format: v => fmtNum(v, 2) },
    { key: 'record_date', label: '股权登记日' },
    { key: 'ex_date', label: '除权除息日' },
    { key: 'pay_date', label: '派息日' },
  ]} empty="暂无分红记录" />
}

function ForecastTab({ tsCode }: { tsCode: string }) {
  return <GenericTab tsCode={tsCode} endpoint="forecast" columns={[
    { key: 'ann_date', label: '公告日' },
    { key: 'end_date', label: '报告期' },
    { key: 'type', label: '类型' },
    { key: 'p_change_min', label: '预增下限(%)', format: fmtPct, color: signColor },
    { key: 'p_change_max', label: '预增上限(%)', format: fmtPct, color: signColor },
    { key: 'net_profit_min', label: '净利下限(万)', format: v => fmtNum(v, 0) },
    { key: 'net_profit_max', label: '净利上限(万)', format: v => fmtNum(v, 0) },
    { key: 'summary', label: '摘要' },
  ]} empty="暂无业绩预告" />
}
