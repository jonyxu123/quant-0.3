import { useState, useEffect } from 'react'
import axios from 'axios'

const S = {
  card: { background: '#181d2a', border: '1px solid #1e2233', borderRadius: 8, padding: '18px 20px' } as React.CSSProperties,
  label: { color: '#4a5068', fontSize: 11, marginBottom: 5 } as React.CSSProperties,
  row2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14 } as React.CSSProperties,
  input: { width: '100%', background: '#0f1117', border: '1px solid #1e2233', borderRadius: 5, color: '#c8cdd8', padding: '7px 10px', fontSize: 12, outline: 'none', boxSizing: 'border-box' } as React.CSSProperties,
  select: { width: '100%', background: '#0f1117', border: '1px solid #1e2233', borderRadius: 5, color: '#c8cdd8', padding: '7px 10px', fontSize: 12, outline: 'none' } as React.CSSProperties,
  btnCyan: { background: '#006666', border: '1px solid #00c8c8', color: '#00c8c8', borderRadius: 5, padding: '8px 0', width: '100%', cursor: 'pointer', fontSize: 13, fontWeight: 600 } as React.CSSProperties,
  btnDisabled: { background: '#1a1f2e', border: '1px solid #2a2e3d', color: '#3a4060', borderRadius: 5, padding: '8px 0', width: '100%', cursor: 'not-allowed', fontSize: 13 } as React.CSSProperties,
  statusDot: (on: boolean): React.CSSProperties => ({ width: 7, height: 7, borderRadius: '50%', background: on ? '#00c8c8' : '#2a2e3d', display: 'inline-block', marginRight: 6 }),
  tableGrid: { display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 10, marginTop: 14 } as React.CSSProperties,
  tableCard: { background: '#0f1117', border: '1px solid #1e2233', borderRadius: 6, padding: '10px 14px' } as React.CSSProperties,
}

interface SyncStatus { sync_running: boolean; current_task: string | null; progress: number; message: string; tables: Record<string, { count: number; last_date: string | null }> }

function DataSync() {
  const [interfaces, setInterfaces] = useState<Record<string, string[]>>({})
  const [status, setStatus] = useState<SyncStatus | null>(null)
  const [sel, setSel] = useState('')
  const [startDate, setStartDate] = useState('20230101')
  const [endDate, setEndDate] = useState('20260415')
  const [workers, setWorkers] = useState(4)
  const [msg, setMsg] = useState('')

  useEffect(() => {
    axios.get('/api/sync/interfaces').then(r => setInterfaces(r.data.categories)).catch(() => {})
    const poll = () => axios.get('/api/sync/status').then(r => setStatus(r.data)).catch(() => {})
    poll()
    const t = setInterval(poll, 2500)
    return () => clearInterval(t)
  }, [])

  const doIncremental = async () => {
    try { await axios.post('/api/sync/incremental'); setMsg('增量同步已启动') }
    catch (e: any) { setMsg(e.response?.data?.detail || '启动失败') }
  }

  const doFull = async () => {
    if (!sel) { setMsg('请先选择接口'); return }
    try { await axios.post('/api/sync/full', { interface: sel, start_date: startDate, end_date: endDate, workers }); setMsg('全量同步已启动') }
    catch (e: any) { setMsg(e.response?.data?.detail || '启动失败') }
  }

  const running = status?.sync_running ?? false

  return (
    <div>
      {/* 状态栏 */}
      <div style={{ ...S.card, marginBottom: 14, display: 'flex', alignItems: 'center', gap: 24 }}>
        <div>
          <span style={S.statusDot(running)} />
          <span style={{ color: running ? '#00c8c8' : '#4a5068', fontSize: 12 }}>{running ? '同步中' : '空闲'}</span>
        </div>
        {running && <div style={{ color: '#9aa0b8', fontSize: 12 }}>任务: <span style={{ color: '#e8eaf0' }}>{status?.current_task}</span></div>}
        {msg && <div style={{ color: '#00c8c8', fontSize: 12, marginLeft: 'auto' }}>{msg}</div>}
      </div>

      {/* 操作区 */}
      <div style={S.row2}>
        {/* 增量同步 */}
        <div style={S.card}>
          <div style={{ color: '#9aa0b8', fontSize: 13, fontWeight: 600, marginBottom: 10 }}>增量同步</div>
          <div style={{ color: '#4a5068', fontSize: 12, marginBottom: 16, lineHeight: 1.6 }}>
            自动检测最新日期，补充所有每日类数据（每日执行）
          </div>
          <button style={running ? S.btnDisabled : S.btnCyan} onClick={doIncremental} disabled={running}>
            ▶ 启动增量同步
          </button>
        </div>

        {/* 全量同步 */}
        <div style={S.card}>
          <div style={{ color: '#9aa0b8', fontSize: 13, fontWeight: 600, marginBottom: 10 }}>全量同步</div>
          <div style={{ marginBottom: 8 }}>
            <div style={S.label}>接口</div>
            <select style={S.select} value={sel} onChange={e => setSel(e.target.value)}>
              <option value="">选择接口...</option>
              {Object.entries(interfaces).map(([cat, items]) => (
                <optgroup key={cat} label={cat}>
                  {(items as string[]).map(item => <option key={item} value={item}>{item}</option>)}
                </optgroup>
              ))}
            </select>
          </div>
          <div style={{ ...S.row2, marginBottom: 8 }}>
            <div>
              <div style={S.label}>开始日期</div>
              <input style={S.input} value={startDate} onChange={e => setStartDate(e.target.value)} placeholder="20230101" />
            </div>
            <div>
              <div style={S.label}>结束日期</div>
              <input style={S.input} value={endDate} onChange={e => setEndDate(e.target.value)} placeholder="20260415" />
            </div>
          </div>
          <div style={{ marginBottom: 10 }}>
            <div style={S.label}>并发数</div>
            <input style={S.input} type="number" value={workers} min={1} max={16} onChange={e => setWorkers(Number(e.target.value))} />
          </div>
          <button style={running || !sel ? S.btnDisabled : S.btnCyan} onClick={doFull} disabled={running || !sel}>
            ▶ 启动全量同步
          </button>
        </div>
      </div>

      {/* 数据表状态 */}
      <div style={{ ...S.card, marginTop: 14 }}>
        <div style={{ color: '#9aa0b8', fontSize: 13, fontWeight: 600, marginBottom: 12 }}>数据表状态</div>
        <div style={S.tableGrid}>
          {Object.entries(status?.tables ?? {}).slice(0, 18).map(([table, info]: [string, any]) => (
            <div key={table} style={S.tableCard}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                <span style={{ color: '#c8cdd8', fontFamily: 'monospace', fontSize: 11 }}>{table}</span>
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: info.last_date ? '#00c8c8' : '#2a2e3d', display: 'inline-block' }} />
              </div>
              <div style={{ color: '#4a5068', fontSize: 11 }}>
                {info.count.toLocaleString()} 条 · {info.last_date ?? 'N/A'}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default DataSync
