import { useEffect, useState } from 'react'
import axios from 'axios'

const S = {
  grid4: { display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 14 } as React.CSSProperties,
  grid3: { display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 14 } as React.CSSProperties,
  card: { background: '#181d2a', border: '1px solid #1e2233', borderRadius: 8, padding: '18px 20px' } as React.CSSProperties,
  label: { color: '#4a5068', fontSize: 12, marginBottom: 6 } as React.CSSProperties,
  val: { color: '#e8eaf0', fontSize: 22, fontWeight: 700 } as React.CSSProperties,
  valCyan: { color: '#00c8c8', fontSize: 20, fontWeight: 700 } as React.CSSProperties,
  bar: { background: '#1e2233', borderRadius: 2, height: 3, marginTop: 10, overflow: 'hidden' } as React.CSSProperties,
  section: { background: '#181d2a', border: '1px solid #1e2233', borderRadius: 8, padding: '18px 20px', marginTop: 14 } as React.CSSProperties,
  th: { color: '#4a5068', fontSize: 11, fontWeight: 500, padding: '6px 12px', textAlign: 'left', borderBottom: '1px solid #1e2233' } as React.CSSProperties,
  td: { color: '#c8cdd8', fontSize: 12, padding: '8px 12px', borderBottom: '1px solid #161926' } as React.CSSProperties,
}

interface Sys { cpu_percent: number; memory: { total: number; used: number; percent: number }; disk: { total: number; used: number; percent: number } }
interface DB { total_tables: number; total_rows: number; tables: Array<{ table: string; rows: number }> }

function Bar({ pct, color = '#00c8c8' }: { pct: number; color?: string }) {
  return <div style={S.bar}><div style={{ height: 3, width: `${Math.min(pct, 100)}%`, background: color, transition: 'width 0.4s' }} /></div>
}

function Dashboard() {
  const [sys, setSys] = useState<Sys | null>(null)
  const [db, setDb] = useState<DB | null>(null)
  const [latest, setLatest] = useState<Record<string, string>>({})

  const load = async () => {
    try {
      const [s, d, l] = await Promise.all([
        axios.get('/api/monitor/system'),
        axios.get('/api/monitor/database'),
        axios.get('/api/monitor/latest_data'),
      ])
      setSys(s.data); setDb(d.data); setLatest(l.data)
    } catch { /* 后端未启动时静默 */ }
  }

  useEffect(() => { load(); const t = setInterval(load, 6000); return () => clearInterval(t) }, [])

  return (
    <div>
      {/* 顶部统计卡片行 */}
      <div style={S.grid4}>
        {[
          { label: '数据表总数',  val: db?.total_tables ?? '--',                color: '#e8eaf0' },
          { label: '总记录数',    val: (db?.total_rows ?? 0) > 0 ? (db!.total_rows / 1e6).toFixed(2) + ' M' : '--', color: '#00c8c8' },
          { label: '日线最新',   val: latest.daily ?? '--',                    color: '#00c8c8' },
          { label: '因子最新',   val: latest.factor_raw ?? '--',               color: '#00c8c8' },
        ].map(item => (
          <div key={item.label} style={S.card}>
            <div style={S.label}>{item.label}</div>
            <div style={{ ...S.val, color: item.color }}>{String(item.val)}</div>
          </div>
        ))}
      </div>

      {/* 系统资源 */}
      <div style={{ ...S.grid3, marginTop: 14 }}>
        <div style={S.card}>
          <div style={S.label}>CPU 使用率</div>
          <div style={S.valCyan}>{sys ? sys.cpu_percent.toFixed(1) + '%' : '--'}</div>
          <Bar pct={sys?.cpu_percent ?? 0} />
        </div>
        <div style={S.card}>
          <div style={S.label}>内存</div>
          <div style={S.valCyan}>
            {sys ? `${sys.memory.used.toFixed(1)} / ${sys.memory.total.toFixed(1)} GB` : '--'}
          </div>
          <Bar pct={sys?.memory.percent ?? 0} color={sys && sys.memory.percent > 85 ? '#ff6b6b' : '#00c8c8'} />
        </div>
        <div style={S.card}>
          <div style={S.label}>磁盘</div>
          <div style={S.valCyan}>
            {sys ? `${sys.disk.used.toFixed(0)} / ${sys.disk.total.toFixed(0)} GB` : '--'}
          </div>
          <Bar pct={sys?.disk.percent ?? 0} color={sys && sys.disk.percent > 80 ? '#ffaa00' : '#00c8c8'} />
        </div>
      </div>

      {/* 数据表排行 */}
      <div style={S.section}>
        <div style={{ color: '#9aa0b8', fontSize: 13, fontWeight: 600, marginBottom: 14 }}>数据表排行 Top 15</div>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={S.th}>#</th>
              <th style={S.th}>表名</th>
              <th style={{ ...S.th, textAlign: 'right' }}>记录数</th>
            </tr>
          </thead>
          <tbody>
            {(db?.tables ?? []).slice(0, 15).map((t, i) => (
              <tr key={t.table}>
                <td style={{ ...S.td, color: '#3a4060', width: 32 }}>{i + 1}</td>
                <td style={{ ...S.td, fontFamily: 'monospace' }}>{t.table}</td>
                <td style={{ ...S.td, textAlign: 'right', color: '#00c8c8' }}>{t.rows.toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default Dashboard
