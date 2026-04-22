import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'

/* ─── 样式常量 ─── */
const S = {
  root: { display: 'flex', gap: 0, height: 'calc(100vh - 97px)' } as React.CSSProperties,

  /* 左侧表列表 */
  sidebar: { width: 220, minWidth: 220, background: '#141720', borderRight: '1px solid #1e2233', display: 'flex', flexDirection: 'column', overflow: 'hidden' } as React.CSSProperties,
  sideHead: { padding: '12px 14px', borderBottom: '1px solid #1e2233', display: 'flex', flexDirection: 'column', gap: 8 } as React.CSSProperties,
  searchInput: { width: '100%', background: '#0f1117', border: '1px solid #1e2233', borderRadius: 4, color: '#c8cdd8', padding: '5px 8px', fontSize: 11, outline: 'none', boxSizing: 'border-box' } as React.CSSProperties,
  tableList: { flex: 1, overflowY: 'auto' } as React.CSSProperties,
  tableItem: (active: boolean): React.CSSProperties => ({
    padding: '7px 14px',
    cursor: 'pointer',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    borderLeft: active ? '2px solid #00c8c8' : '2px solid transparent',
    background: active ? 'rgba(0,200,200,0.07)' : 'transparent',
    color: active ? '#00c8c8' : '#7a8099',
    fontSize: 12,
    fontFamily: 'monospace',
    transition: 'all 0.12s',
  }),
  badge: (active: boolean): React.CSSProperties => ({
    color: active ? '#00c8c8' : '#3a4060', fontSize: 10,
  }),

  /* 右侧数据区 */
  main: { flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' } as React.CSSProperties,
  toolbar: { padding: '10px 16px', borderBottom: '1px solid #1e2233', display: 'flex', alignItems: 'center', gap: 12, flexShrink: 0 } as React.CSSProperties,
  searchInput2: { background: '#0f1117', border: '1px solid #1e2233', borderRadius: 4, color: '#c8cdd8', padding: '5px 10px', fontSize: 12, outline: 'none', width: 200 } as React.CSSProperties,
  tag: { background: '#1a2a2a', border: '1px solid #006666', color: '#00c8c8', borderRadius: 3, padding: '2px 8px', fontSize: 11 } as React.CSSProperties,

  /* 表格 */
  tableWrap: { flex: 1, overflow: 'auto' } as React.CSSProperties,
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 } as React.CSSProperties,
  th: { color: '#4a5068', fontSize: 11, fontWeight: 500, padding: '7px 12px', textAlign: 'left', borderBottom: '1px solid #1e2233', background: '#141720', position: 'sticky', top: 0, whiteSpace: 'nowrap' } as React.CSSProperties,
  td: { color: '#c8cdd8', padding: '6px 12px', borderBottom: '1px solid #161926', whiteSpace: 'nowrap', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' } as React.CSSProperties,
  tdNull: { color: '#3a4060', padding: '6px 12px', borderBottom: '1px solid #161926', fontStyle: 'italic', fontSize: 11 } as React.CSSProperties,

  /* 分页 */
  pager: { padding: '8px 16px', borderTop: '1px solid #1e2233', display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0, background: '#0f1117' } as React.CSSProperties,
  pgBtn: (disabled: boolean): React.CSSProperties => ({
    background: disabled ? '#1a1f2e' : '#141720',
    border: '1px solid #1e2233',
    color: disabled ? '#2a2e3d' : '#7a8099',
    borderRadius: 3, padding: '3px 10px', cursor: disabled ? 'not-allowed' : 'pointer', fontSize: 12,
  }),
}

interface TableMeta { name: string; rows: number }
interface TableData {
  columns: string[]
  rows: any[][]
  total: number
  page: number
  total_pages: number
}

function Dataset() {
  const [tables, setTables] = useState<TableMeta[]>([])
  const [tableSearch, setTableSearch] = useState('')
  const [selected, setSelected] = useState<string>('')
  const [data, setData] = useState<TableData | null>(null)
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(false)

  /* 加载表列表 */
  useEffect(() => {
    axios.get('/api/monitor/tables')
      .then(r => setTables(r.data.tables))
      .catch(() => {})
  }, [])

  /* 加载表数据 */
  const loadData = useCallback((table: string, pg: number, q: string) => {
    if (!table) return
    setLoading(true)
    axios.get(`/api/monitor/tables/${encodeURIComponent(table)}/data`, {
      params: { page: pg, page_size: 50, search: q }
    })
      .then(r => { setData(r.data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const selectTable = (name: string) => {
    setSelected(name)
    setPage(1)
    setSearch('')
    loadData(name, 1, '')
  }

  const handleSearch = () => {
    setPage(1)
    loadData(selected, 1, search)
  }

  const goPage = (p: number) => {
    setPage(p)
    loadData(selected, p, search)
  }

  const filtered = tables.filter(t => t.name.toLowerCase().includes(tableSearch.toLowerCase()))

  return (
    <div style={S.root}>
      {/* 左侧表列表 */}
      <div style={S.sidebar}>
        <div style={S.sideHead}>
          <div style={{ color: '#4a5068', fontSize: 11 }}>共 {tables.length} 张表</div>
          <input
            style={S.searchInput}
            placeholder="搜索表名..."
            value={tableSearch}
            onChange={e => setTableSearch(e.target.value)}
          />
        </div>
        <div style={S.tableList}>
          {filtered.map(t => (
            <div key={t.name} style={S.tableItem(selected === t.name)} onClick={() => selectTable(t.name)}>
              <span>{t.name}</span>
              <span style={S.badge(selected === t.name)}>{t.rows > 0 ? (t.rows >= 10000 ? (t.rows / 10000).toFixed(1) + 'w' : t.rows.toLocaleString()) : '0'}</span>
            </div>
          ))}
        </div>
      </div>

      {/* 右侧数据展示 */}
      <div style={S.main}>
        {!selected ? (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#3a4060', fontSize: 13 }}>
            ← 选择左侧数据表查看数据
          </div>
        ) : (
          <>
            {/* 工具栏 */}
            <div style={S.toolbar}>
              <span style={S.tag}>{selected}</span>
              {data && <span style={{ color: '#4a5068', fontSize: 11 }}>{data.total.toLocaleString()} 条</span>}
              <input
                style={S.searchInput2}
                placeholder="关键词搜索..."
                value={search}
                onChange={e => setSearch(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSearch()}
              />
              <button
                onClick={handleSearch}
                style={{ background: '#006666', border: '1px solid #00c8c8', color: '#00c8c8', borderRadius: 4, padding: '4px 14px', cursor: 'pointer', fontSize: 12 }}
              >
                搜索
              </button>
              {loading && <span style={{ color: '#4a5068', fontSize: 11 }}>加载中...</span>}
            </div>

            {/* 数据表格 */}
            <div style={S.tableWrap}>
              {data && data.columns.length > 0 ? (
                <table style={S.table}>
                  <thead>
                    <tr>
                      {data.columns.map(col => (
                        <th key={col} style={S.th}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.rows.map((row, ri) => (
                      <tr key={ri} style={{ background: ri % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)' }}>
                        {row.map((cell, ci) => (
                          cell === null || cell === '' ? (
                            <td key={ci} style={S.tdNull}>null</td>
                          ) : (
                            <td key={ci} style={S.td} title={String(cell)}>{String(cell)}</td>
                          )
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div style={{ padding: 24, color: '#3a4060', fontSize: 12 }}>暂无数据</div>
              )}
            </div>

            {/* 分页 */}
            {data && data.total_pages > 1 && (
              <div style={S.pager}>
                <button style={S.pgBtn(page <= 1)} disabled={page <= 1} onClick={() => goPage(1)}>«</button>
                <button style={S.pgBtn(page <= 1)} disabled={page <= 1} onClick={() => goPage(page - 1)}>‹</button>
                <span style={{ color: '#7a8099', fontSize: 12, padding: '0 6px' }}>
                  第 <span style={{ color: '#e8eaf0' }}>{page}</span> / <span style={{ color: '#e8eaf0' }}>{data.total_pages}</span> 页
                </span>
                <button style={S.pgBtn(page >= data.total_pages)} disabled={page >= data.total_pages} onClick={() => goPage(page + 1)}>›</button>
                <button style={S.pgBtn(page >= data.total_pages)} disabled={page >= data.total_pages} onClick={() => goPage(data.total_pages)}>»</button>
                <span style={{ color: '#3a4060', fontSize: 11, marginLeft: 8 }}>每页 50 条</span>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

export default Dataset
