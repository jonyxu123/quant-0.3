import { useState, useEffect, useRef } from 'react'
import axios from 'axios'

const API = 'http://localhost:8000/api/factors'

const C = {
  bg: '#0f1117', card: '#161b27', border: '#1e2d3d',
  cyan: '#00c8c8', text: '#e2e8f0', muted: '#6b7fa3',
  green: '#22c55e', red: '#ef4444', yellow: '#f59e0b',
  row: '#1a2235', rowHover: '#1e2a40',
}

type DateRow = { trade_date: string; stock_count: number }
type FactorRow = { factor_id: string; dimension: string; name: string; direction: string }
type CoverageRow = { factor_id: string; valid_count: number; total: number; coverage_pct: number }
type TaskState = { running: boolean; trade_date: string | null; progress: string; error: string | null; done: boolean }

export default function Factors() {
  const [tab, setTab] = useState<'compute' | 'registry' | 'coverage' | 'data'>('compute')
  const [latestDataDate, setLatestDataDate] = useState('')
  const [latestComputedDate, setLatestComputedDate] = useState('')
  const [computeDate, setComputeDate] = useState('')
  const [task, setTask] = useState<TaskState>({ running: false, trade_date: null, progress: '', error: null, done: false })
  const [dates, setDates] = useState<DateRow[]>([])
  const [registry, setRegistry] = useState<FactorRow[]>([])
  const [registryFilter, setRegistryFilter] = useState('')
  const [selectedDateCoverage, setSelectedDateCoverage] = useState('')
  const [coverage, setCoverage] = useState<CoverageRow[]>([])
  const [coverageLoading, setCoverageLoading] = useState(false)
  const [dataDate, setDataDate] = useState('')
  const [dataPage, setDataPage] = useState(1)
  const [dataSearch, setDataSearch] = useState('')
  const [dataResult, setDataResult] = useState<any>(null)
  const [dataLoading, setDataLoading] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    fetchLatestDate()
    fetchDates()
    fetchRegistry()
  }, [])

  async function fetchLatestDate() {
    try {
      const r = await axios.get(`${API}/latest_date`)
      setLatestDataDate(r.data.latest_data_date || '')
      setLatestComputedDate(r.data.latest_computed_date || '')
      if (!computeDate) setComputeDate(r.data.latest_data_date || '')
    } catch {}
  }

  async function fetchDates() {
    try {
      const r = await axios.get(`${API}/dates`)
      setDates(r.data.dates || [])
    } catch {}
  }

  async function fetchRegistry() {
    try {
      const r = await axios.get(`${API}/registry`)
      setRegistry(r.data.factors || [])
    } catch {}
  }

  function startPoll() {
    if (pollRef.current) return
    pollRef.current = setInterval(async () => {
      try {
        const r = await axios.get(`${API}/task_status`)
        setTask(r.data)
        if (!r.data.running) {
          clearInterval(pollRef.current!)
          pollRef.current = null
          fetchDates()
          fetchLatestDate()
        }
      } catch {}
    }, 2000)
  }

  async function triggerCompute() {
    if (task.running) return
    try {
      await axios.post(`${API}/compute`, { trade_date: computeDate || null })
      setTask({ running: true, trade_date: computeDate, progress: '任务已提交...', error: null, done: false })
      startPoll()
    } catch (e: any) {
      alert('启动失败: ' + (e.response?.data?.detail || e.message))
    }
  }

  async function loadCoverage(date: string) {
    if (!date) return
    setSelectedDateCoverage(date)
    setCoverageLoading(true)
    try {
      const r = await axios.get(`${API}/coverage/${date}`)
      setCoverage(r.data.coverage || [])
    } catch {}
    setCoverageLoading(false)
  }

  async function loadData(page = dataPage) {
    if (!dataDate) return
    setDataLoading(true)
    try {
      const r = await axios.get(`${API}/data/${dataDate}`, { params: { page, page_size: 50, search: dataSearch } })
      setDataResult(r.data)
    } catch {}
    setDataLoading(false)
  }

  async function deleteDate(date: string) {
    if (!confirm(`确定删除 ${date} 的因子数据？`)) return
    await axios.delete(`${API}/data/${date}`)
    fetchDates()
    fetchLatestDate()
  }

  const filteredRegistry = registry.filter(f =>
    !registryFilter || f.factor_id.toLowerCase().includes(registryFilter.toLowerCase()) ||
    f.name.includes(registryFilter) || f.dimension.includes(registryFilter)
  )

  const tabs = [
    { key: 'compute', label: '因子计算' },
    { key: 'registry', label: `因子注册表 (${registry.length})` },
    { key: 'coverage', label: '覆盖率统计' },
    { key: 'data', label: '数据浏览' },
  ] as const

  const directionColor = (d: string) =>
    d === 'positive' ? C.green : d === 'negative' ? C.red : C.yellow

  const coverageColor = (pct: number) =>
    pct >= 80 ? C.green : pct >= 40 ? C.yellow : C.red

  return (
    <div style={{ background: C.bg, minHeight: '100vh', padding: '24px', color: C.text, fontFamily: 'monospace' }}>
      {/* 页头 */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: C.cyan, margin: 0 }}>因子管理</h1>
        <p style={{ color: C.muted, marginTop: 4, fontSize: 13 }}>
          最新数据日期：<span style={{ color: C.text }}>{latestDataDate || '-'}</span>
          &nbsp;&nbsp;已计算至：<span style={{ color: latestComputedDate === latestDataDate ? C.green : C.yellow }}>
            {latestComputedDate || '未计算'}
          </span>
          {latestComputedDate && latestComputedDate !== latestDataDate &&
            <span style={{ color: C.yellow }}> ⚠ 有新数据未计算</span>
          }
        </p>
      </div>

      {/* Tab 切换 */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, borderBottom: `1px solid ${C.border}` }}>
        {tabs.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{ padding: '8px 18px', background: 'none', border: 'none', cursor: 'pointer',
              color: tab === t.key ? C.cyan : C.muted, fontSize: 13, fontWeight: tab === t.key ? 700 : 400,
              borderBottom: tab === t.key ? `2px solid ${C.cyan}` : '2px solid transparent' }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* ── Tab: 因子计算 ── */}
      {tab === 'compute' && (
        <div style={{ display: 'flex', gap: 20 }}>
          {/* 计算面板 */}
          <div style={{ flex: '0 0 360px', background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 20 }}>
            <h3 style={{ color: C.cyan, margin: '0 0 16px', fontSize: 15 }}>触发因子计算</h3>
            <label style={{ color: C.muted, fontSize: 12 }}>计算日期</label>
            <div style={{ display: 'flex', gap: 8, margin: '6px 0 16px' }}>
              <input value={computeDate} onChange={e => setComputeDate(e.target.value)}
                placeholder="YYYYMMDD"
                style={{ flex: 1, background: '#0d1520', border: `1px solid ${C.border}`, borderRadius: 6,
                  color: C.text, padding: '8px 12px', fontSize: 13 }} />
              <button onClick={() => setComputeDate(latestDataDate)}
                style={{ background: '#1e2d3d', border: 'none', borderRadius: 6, color: C.cyan,
                  padding: '8px 12px', fontSize: 12, cursor: 'pointer' }}>最新</button>
            </div>
            <button onClick={triggerCompute} disabled={task.running}
              style={{ width: '100%', padding: '10px', borderRadius: 6, border: 'none', cursor: task.running ? 'not-allowed' : 'pointer',
                background: task.running ? '#1e2d3d' : C.cyan, color: task.running ? C.muted : '#000', fontWeight: 700, fontSize: 14 }}>
              {task.running ? '⏳ 计算中...' : '▶ 开始计算'}
            </button>

            {/* 任务状态 */}
            {(task.progress || task.error) && (
              <div style={{ marginTop: 16, padding: 12, background: '#0d1520', borderRadius: 6,
                border: `1px solid ${task.error ? C.red : task.done && !task.error ? C.green : C.border}` }}>
                <div style={{ fontSize: 12, color: task.error ? C.red : task.done ? C.green : C.cyan }}>
                  {task.error ? '❌ ' + task.error : task.done ? '✅ ' + task.progress : '⏳ ' + task.progress}
                </div>
                {task.trade_date && <div style={{ color: C.muted, fontSize: 11, marginTop: 4 }}>日期: {task.trade_date}</div>}
              </div>
            )}

            <div style={{ marginTop: 20, color: C.muted, fontSize: 11, lineHeight: 1.8 }}>
              <div>• 计算约需 1-3 分钟</div>
              <div>• 后台异步执行，页面可继续使用</div>
              <div>• 完成后自动刷新日期列表</div>
              <div>• 重复计算同日期会覆盖旧数据</div>
            </div>
          </div>

          {/* 已计算日期列表 */}
          <div style={{ flex: 1, background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 20 }}>
            <h3 style={{ color: C.cyan, margin: '0 0 16px', fontSize: 15 }}>已计算日期 ({dates.length})</h3>
            <div style={{ overflowY: 'auto', maxHeight: 420 }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                <thead>
                  <tr style={{ color: C.muted, borderBottom: `1px solid ${C.border}` }}>
                    <th style={{ textAlign: 'left', padding: '6px 12px' }}>交易日期</th>
                    <th style={{ textAlign: 'right', padding: '6px 12px' }}>股票数</th>
                    <th style={{ textAlign: 'center', padding: '6px 12px' }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {dates.map(d => (
                    <tr key={d.trade_date}
                      style={{ borderBottom: `1px solid ${C.border}`, transition: 'background 0.1s' }}
                      onMouseEnter={e => (e.currentTarget.style.background = C.rowHover)}
                      onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}>
                      <td style={{ padding: '8px 12px', color: C.text }}>{d.trade_date}</td>
                      <td style={{ padding: '8px 12px', textAlign: 'right', color: C.green }}>{d.stock_count.toLocaleString()}</td>
                      <td style={{ padding: '8px 12px', textAlign: 'center', display: 'flex', gap: 6, justifyContent: 'center' }}>
                        <button onClick={() => { setTab('coverage'); loadCoverage(d.trade_date) }}
                          style={{ background: '#1e2d3d', border: 'none', borderRadius: 4, color: C.cyan,
                            padding: '3px 8px', fontSize: 11, cursor: 'pointer' }}>覆盖率</button>
                        <button onClick={() => { setTab('data'); setDataDate(d.trade_date); setDataPage(1) }}
                          style={{ background: '#1e2d3d', border: 'none', borderRadius: 4, color: C.muted,
                            padding: '3px 8px', fontSize: 11, cursor: 'pointer' }}>浏览</button>
                        <button onClick={() => deleteDate(d.trade_date)}
                          style={{ background: 'transparent', border: `1px solid ${C.red}33`, borderRadius: 4,
                            color: C.red, padding: '3px 8px', fontSize: 11, cursor: 'pointer' }}>删除</button>
                      </td>
                    </tr>
                  ))}
                  {dates.length === 0 && (
                    <tr><td colSpan={3} style={{ padding: 24, textAlign: 'center', color: C.muted }}>暂无因子数据，请先计算</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ── Tab: 因子注册表 ── */}
      {tab === 'registry' && (
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
            <h3 style={{ color: C.cyan, margin: 0, fontSize: 15 }}>因子注册表 ({registry.length} 个因子)</h3>
            <input value={registryFilter} onChange={e => setRegistryFilter(e.target.value)}
              placeholder="搜索因子ID/名称/维度"
              style={{ background: '#0d1520', border: `1px solid ${C.border}`, borderRadius: 6,
                color: C.text, padding: '6px 12px', fontSize: 12, width: 220 }} />
          </div>
          <div style={{ overflowY: 'auto', maxHeight: 'calc(100vh - 280px)' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead style={{ position: 'sticky', top: 0, background: C.card }}>
                <tr style={{ color: C.muted, borderBottom: `1px solid ${C.border}` }}>
                  {['因子ID', '维度', '名称', '方向'].map(h => (
                    <th key={h} style={{ textAlign: 'left', padding: '6px 12px', fontWeight: 600 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredRegistry.map((f, i) => (
                  <tr key={f.factor_id}
                    style={{ background: i % 2 === 0 ? 'transparent' : C.row, borderBottom: `1px solid ${C.border}22` }}>
                    <td style={{ padding: '7px 12px', color: C.cyan, fontFamily: 'monospace' }}>{f.factor_id}</td>
                    <td style={{ padding: '7px 12px', color: C.muted }}>{f.dimension}</td>
                    <td style={{ padding: '7px 12px', color: C.text }}>{f.name || '-'}</td>
                    <td style={{ padding: '7px 12px' }}>
                      <span style={{ color: directionColor(f.direction), fontSize: 11 }}>
                        {f.direction === 'positive' ? '↑ 正向' : f.direction === 'negative' ? '↓ 负向' : '↕ 双向'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Tab: 覆盖率统计 ── */}
      {tab === 'coverage' && (
        <div>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 16 }}>
            <select value={selectedDateCoverage} onChange={e => loadCoverage(e.target.value)}
              style={{ background: '#0d1520', border: `1px solid ${C.border}`, borderRadius: 6,
                color: C.text, padding: '7px 12px', fontSize: 13 }}>
              <option value="">选择日期</option>
              {dates.map(d => <option key={d.trade_date} value={d.trade_date}>{d.trade_date}</option>)}
            </select>
            {coverageLoading && <span style={{ color: C.muted, fontSize: 12 }}>加载中...</span>}
            {coverage.length > 0 && !coverageLoading && (
              <span style={{ color: C.muted, fontSize: 12 }}>
                {coverage.length} 个因子 &nbsp;|&nbsp;
                高覆盖(&gt;80%): <span style={{ color: C.green }}>{coverage.filter(c => c.coverage_pct >= 80).length}</span>&nbsp;
                中(&gt;40%): <span style={{ color: C.yellow }}>{coverage.filter(c => c.coverage_pct >= 40 && c.coverage_pct < 80).length}</span>&nbsp;
                低(&lt;40%): <span style={{ color: C.red }}>{coverage.filter(c => c.coverage_pct < 40).length}</span>
              </span>
            )}
          </div>
          {coverage.length > 0 && (
            <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, overflow: 'hidden' }}>
              <div style={{ overflowY: 'auto', maxHeight: 'calc(100vh - 280px)' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead style={{ position: 'sticky', top: 0, background: C.card }}>
                    <tr style={{ color: C.muted, borderBottom: `1px solid ${C.border}` }}>
                      {['因子ID', '有效数量', '覆盖率', '覆盖率条形'].map(h => (
                        <th key={h} style={{ textAlign: 'left', padding: '7px 12px' }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {coverage.map((c, i) => (
                      <tr key={c.factor_id}
                        style={{ background: i % 2 === 0 ? 'transparent' : C.row, borderBottom: `1px solid ${C.border}22` }}>
                        <td style={{ padding: '6px 12px', color: C.cyan, fontFamily: 'monospace' }}>{c.factor_id}</td>
                        <td style={{ padding: '6px 12px', color: C.text }}>{c.valid_count.toLocaleString()} / {c.total.toLocaleString()}</td>
                        <td style={{ padding: '6px 12px', color: coverageColor(c.coverage_pct), fontWeight: 600 }}>{c.coverage_pct}%</td>
                        <td style={{ padding: '6px 16px', minWidth: 160 }}>
                          <div style={{ background: '#0d1520', borderRadius: 3, height: 6, overflow: 'hidden' }}>
                            <div style={{ width: `${c.coverage_pct}%`, height: '100%',
                              background: coverageColor(c.coverage_pct), borderRadius: 3, transition: 'width 0.3s' }} />
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
          {!selectedDateCoverage && <div style={{ color: C.muted, textAlign: 'center', padding: 40 }}>请选择日期查看覆盖率</div>}
        </div>
      )}

      {/* ── Tab: 数据浏览 ── */}
      {tab === 'data' && (
        <div>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 16, flexWrap: 'wrap' }}>
            <select value={dataDate} onChange={e => { setDataDate(e.target.value); setDataPage(1) }}
              style={{ background: '#0d1520', border: `1px solid ${C.border}`, borderRadius: 6,
                color: C.text, padding: '7px 12px', fontSize: 13 }}>
              <option value="">选择日期</option>
              {dates.map(d => <option key={d.trade_date} value={d.trade_date}>{d.trade_date}</option>)}
            </select>
            <input value={dataSearch} onChange={e => setDataSearch(e.target.value)}
              placeholder="搜索股票代码"
              style={{ background: '#0d1520', border: `1px solid ${C.border}`, borderRadius: 6,
                color: C.text, padding: '7px 12px', fontSize: 12, width: 160 }} />
            <button onClick={() => { setDataPage(1); loadData() }}
              style={{ background: C.cyan, border: 'none', borderRadius: 6, color: '#000',
                padding: '7px 16px', fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>查询</button>
            {dataLoading && <span style={{ color: C.muted, fontSize: 12 }}>加载中...</span>}
            {dataResult && <span style={{ color: C.muted, fontSize: 12 }}>共 {dataResult.total} 条</span>}
          </div>

          {dataResult && !dataResult.error && (
            <>
              <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, overflow: 'auto', maxHeight: 'calc(100vh - 340px)' }}>
                <table style={{ borderCollapse: 'collapse', fontSize: 11, whiteSpace: 'nowrap' }}>
                  <thead style={{ position: 'sticky', top: 0, background: C.card }}>
                    <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                      {(dataResult.columns || []).slice(0, 20).map((col: string) => (
                        <th key={col} style={{ padding: '7px 10px', color: C.muted, textAlign: 'left', fontWeight: 600 }}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {dataResult.data.map((row: any, i: number) => (
                      <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : C.row, borderBottom: `1px solid ${C.border}22` }}>
                        {(dataResult.columns || []).slice(0, 20).map((col: string) => (
                          <td key={col} style={{ padding: '6px 10px', color: col === 'ts_code' ? C.cyan : C.text }}>
                            {row[col] == null ? <span style={{ color: C.muted }}>-</span>
                              : typeof row[col] === 'number' ? row[col].toFixed(4)
                              : String(row[col])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {/* 翻页 */}
              <div style={{ display: 'flex', gap: 8, marginTop: 12, alignItems: 'center', justifyContent: 'flex-end' }}>
                <button disabled={dataPage <= 1} onClick={() => { const p = dataPage - 1; setDataPage(p); loadData(p) }}
                  style={{ background: '#1e2d3d', border: 'none', borderRadius: 6, color: dataPage <= 1 ? C.muted : C.text,
                    padding: '6px 14px', cursor: dataPage <= 1 ? 'not-allowed' : 'pointer', fontSize: 12 }}>上一页</button>
                <span style={{ color: C.muted, fontSize: 12 }}>第 {dataPage} 页 / 共 {Math.ceil((dataResult.total || 0) / 50)} 页</span>
                <button disabled={dataPage >= Math.ceil((dataResult.total || 0) / 50)}
                  onClick={() => { const p = dataPage + 1; setDataPage(p); loadData(p) }}
                  style={{ background: '#1e2d3d', border: 'none', borderRadius: 6, color: C.text,
                    padding: '6px 14px', cursor: 'pointer', fontSize: 12 }}>下一页</button>
              </div>
            </>
          )}
          {!dataDate && <div style={{ color: C.muted, textAlign: 'center', padding: 40 }}>请选择日期后点击查询</div>}
        </div>
      )}
    </div>
  )
}
