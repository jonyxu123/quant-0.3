import React, { useState, useEffect, useRef } from 'react'
import { Link } from 'react-router-dom'
import axios from 'axios'
import { Star, Trash2, ExternalLink, Download, X, Edit2, Check, Plus, Search, Eye } from 'lucide-react'

/* ─── 颜色常量（与 StockSelection 保持一致）─── */
const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', cyanBg: 'rgba(0,200,200,0.08)', cyanDark: '#006666',
  red: '#ff6b6b', green: '#4ade80', yellow: '#ffaa00',
}

/* ─── 样式 ─── */
const S = {
  root: { padding: '20px 24px', maxWidth: 1200, margin: '0 auto' } as React.CSSProperties,
  header: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 } as React.CSSProperties,
  title: { color: C.bright, fontSize: 18, fontWeight: 600, display: 'flex', alignItems: 'center', gap: 8 } as React.CSSProperties,
  card: { background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: '16px 20px' } as React.CSSProperties,
  toolbar: { display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16, flexWrap: 'wrap' as const } as React.CSSProperties,
  btn: (variant: 'primary' | 'danger' | 'ghost' = 'primary'): React.CSSProperties => {
    const base = { display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px', borderRadius: 5, fontSize: 12, fontWeight: 500, cursor: 'pointer', transition: 'all 0.12s', border: '1px solid transparent' }
    if (variant === 'primary') return { ...base, background: C.cyanDark, borderColor: C.cyan, color: C.cyan }
    if (variant === 'danger') return { ...base, background: 'rgba(255,107,107,0.1)', borderColor: C.red, color: C.red }
    return { ...base, background: 'transparent', borderColor: C.border, color: C.dim }
  },
  input: { background: C.bg, border: `1px solid ${C.border}`, borderRadius: 5, color: C.text, padding: '6px 10px', fontSize: 12, outline: 'none', width: 200 } as React.CSSProperties,
  tableWrap: { background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, overflow: 'hidden' } as React.CSSProperties,
  th: { color: C.dim, fontSize: 11, fontWeight: 500, padding: '10px 14px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, background: '#141720' } as React.CSSProperties,
  td: { color: C.text, padding: '10px 14px', borderBottom: `1px solid #161926`, fontSize: 12 } as React.CSSProperties,
  empty: { padding: 60, textAlign: 'center', color: C.dim, fontSize: 14 } as React.CSSProperties,
  starActive: { color: C.yellow, fill: C.yellow } as React.CSSProperties,
  starInactive: { color: C.dim } as React.CSSProperties,
}

/* ─── 类型 ─── */
interface WatchlistItem {
  ts_code: string
  name: string
  industry?: string
  added_at: string
  source_strategy?: string
  note?: string
}

const STORAGE_KEY = 'quant_watchlist_v1'  // 旧localStorage键，用于一次性迁移

interface SearchResult {
  ts_code: string
  symbol: string
  name: string
  industry?: string
  area?: string
  market?: string
}

const API = 'http://localhost:8000'

/* ─── 组件 ─── */
export default function Watchlist() {
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([])
  const [search, setSearch] = useState('')
  const [editingNote, setEditingNote] = useState<string | null>(null)
  const [noteInput, setNoteInput] = useState('')

  // 搜索添加股票
  const [addQuery, setAddQuery] = useState('')
  const [searchResults, setSearchResults] = useState<SearchResult[]>([])
  const [searching, setSearching] = useState(false)
  const [showDropdown, setShowDropdown] = useState(false)
  const searchBoxRef = useRef<HTMLDivElement>(null)
  const searchDebounce = useRef<ReturnType<typeof setTimeout> | null>(null)

  // 监控池成员状态（用于判断股票是否已在池中）
  const [poolMembers, setPoolMembers] = useState<Record<number, Set<string>>>({})

  const loadPoolMembers = () => {
    for (const poolId of [1, 2]) {
      axios.get(`${API}/api/realtime/pool/${poolId}/members`)
        .then(r => {
          const codes: string[] = (r.data.data || []).map((m: any) => m.ts_code)
          setPoolMembers(prev => ({ ...prev, [poolId]: new Set(codes) }))
        })
        .catch(() => {})
    }
  }

  // 加载数据（从后端API）
  const loadWatchlist = () => {
    axios.get(`${API}/api/watchlist/list`)
      .then(r => setWatchlist(r.data.data || []))
      .catch(err => console.error('加载自选股失败', err))
  }

  useEffect(() => {
    // 首次加载：尝试将 localStorage 数据迁移到后端
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) {
      try {
        const localItems = JSON.parse(raw)
        if (localItems.length > 0) {
          // 迁移到后端
          axios.post(`${API}/api/watchlist/sync`, localItems)
            .then(() => {
              localStorage.removeItem(STORAGE_KEY)
              loadWatchlist()
            })
            .catch(() => loadWatchlist())
          return
        }
      } catch {}
      localStorage.removeItem(STORAGE_KEY)
    }
    loadWatchlist()
    loadPoolMembers()
  }, [])

  // 删除单项
  const remove = (ts_code: string) => {
    axios.delete(`${API}/api/watchlist/remove/${ts_code}`)
      .then(() => loadWatchlist())
      .catch(err => console.error('删除失败', err))
  }

  // 清空全部
  const clearAll = () => {
    if (confirm('确定要清空全部自选股票吗？')) {
      axios.post(`${API}/api/watchlist/sync`, [])
        .then(() => loadWatchlist())
        .catch(err => console.error('清空失败', err))
    }
  }

  // 导出 CSV
  const exportCSV = () => {
    if (watchlist.length === 0) return
    const header = '代码,名称,行业,添加时间,来源策略,备注\n'
    const rows = watchlist.map(s => [
      s.ts_code, s.name || '', s.industry || '', s.added_at,
      s.source_strategy || '', s.note || ''
    ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(','))
    const csv = header + rows.join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    link.href = URL.createObjectURL(blob)
    link.download = `自选股票_${new Date().toISOString().slice(0,10)}.csv`
    link.click()
  }

  // 编辑备注
  const startEditNote = (item: WatchlistItem) => {
    setEditingNote(item.ts_code)
    setNoteInput(item.note || '')
  }

  const saveNote = (ts_code: string) => {
    axios.put(`${API}/api/watchlist/note/${ts_code}`, null, { params: { note: noteInput.trim() } })
      .then(() => { loadWatchlist(); setEditingNote(null); setNoteInput('') })
      .catch(err => console.error('保存备注失败', err))
  }

  const cancelEditNote = () => {
    setEditingNote(null)
    setNoteInput('')
  }

  // 添加股票到自选
  const addStock = (stock: SearchResult) => {
    if (watchlist.some(s => s.ts_code === stock.ts_code)) {
      alert(`${stock.name} 已在自选列表中`)
      return
    }
    axios.post(`${API}/api/watchlist/add`, {
      ts_code: stock.ts_code,
      name: stock.name,
      industry: stock.industry,
      source_strategy: '手动添加',
    })
      .then(r => {
        if (r.data.ok) loadWatchlist()
        else alert(r.data.msg)
      })
      .catch(err => console.error('添加失败', err))
    setAddQuery('')
    setSearchResults([])
    setShowDropdown(false)
  }

  // 实时搜索（debounce 300ms）
  useEffect(() => {
    if (searchDebounce.current) clearTimeout(searchDebounce.current)
    if (!addQuery.trim()) {
      setSearchResults([])
      return
    }
    setSearching(true)
    searchDebounce.current = setTimeout(() => {
      axios.get(`${API}/api/stock/search`, { params: { q: addQuery.trim(), limit: 10 } })
        .then(r => setSearchResults(r.data.data || []))
        .catch(err => { console.error(err); setSearchResults([]) })
        .finally(() => setSearching(false))
    }, 300)
    return () => {
      if (searchDebounce.current) clearTimeout(searchDebounce.current)
    }
  }, [addQuery])

  // 点击外部关闭下拉
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (searchBoxRef.current && !searchBoxRef.current.contains(e.target as Node)) {
        setShowDropdown(false)
      }
    }
    document.addEventListener('mousedown', onClick)
    return () => document.removeEventListener('mousedown', onClick)
  }, [])

  // 过滤
  const filtered = watchlist.filter(s =>
    s.ts_code.toLowerCase().includes(search.toLowerCase()) ||
    (s.name && s.name.toLowerCase().includes(search.toLowerCase())) ||
    (s.industry && s.industry.toLowerCase().includes(search.toLowerCase()))
  )

  // 加入监控池（1号·择时 / 2号·T+0）
  const addToPool = (item: WatchlistItem, poolId: 1 | 2) => {
    // 已在池中则不操作
    if (poolMembers[poolId]?.has(item.ts_code)) return
    axios.post(`${API}/api/realtime/pool/${poolId}/add`, {
      ts_code: item.ts_code,
      name: item.name,
      industry: item.industry,
      note: item.note,
    })
      .then(r => {
        if (r.data.ok) {
          loadPoolMembers()
        } else {
          alert(r.data.msg)
        }
      })
      .catch(err => { console.error('加入监控池失败', err); alert('加入监控池失败') })
  }

  // 打开股票详情（东财/同花顺链接）
  const openStockPage = (ts_code: string) => {
    // 提取数字代码和交易所后缀
    const [code, suffix] = ts_code.split('.')
    let url = ''
    if (suffix === 'SZ') url = `https://quote.eastmoney.com/sz${code}.html`
    else if (suffix === 'SH') url = `https://quote.eastmoney.com/sh${code}.html`
    else if (suffix === 'BJ') url = `https://quote.eastmoney.com/bj${code}.html`
    else url = `https://quote.eastmoney.com/${code}.html`
    window.open(url, '_blank')
  }

  return (
    <div style={S.root}>
      {/* 标题栏 */}
      <div style={S.header}>
        <div style={S.title}>
          <Star size={20} style={S.starActive} />
          自选股票
          <span style={{ color: C.dim, fontSize: 13, fontWeight: 400 }}>（{watchlist.length} 只）</span>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {/* 搜索添加框 */}
          <div ref={searchBoxRef} style={{ position: 'relative' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <Search size={14} style={{ color: C.dim, position: 'absolute', left: 10, pointerEvents: 'none' }} />
              <input
                style={{
                  ...S.input,
                  paddingLeft: 30, width: 240,
                  borderColor: showDropdown ? C.cyan : C.border,
                }}
                placeholder="输入代码/名称/拼音添加股票"
                value={addQuery}
                onChange={e => { setAddQuery(e.target.value); setShowDropdown(true) }}
                onFocus={() => setShowDropdown(true)}
              />
              {addQuery && (
                <button
                  onClick={() => { setAddQuery(''); setSearchResults([]) }}
                  style={{ position: 'absolute', right: 8, background: 'none', border: 'none', color: C.dim, cursor: 'pointer', padding: 2 }}
                >
                  <X size={14} />
                </button>
              )}
            </div>

            {/* 下拉结果 */}
            {showDropdown && addQuery.trim() && (
              <div style={{
                position: 'absolute', top: '100%', left: 0, right: 0, marginTop: 4,
                background: C.card, border: `1px solid ${C.border}`, borderRadius: 4,
                boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
                maxHeight: 320, overflowY: 'auto', zIndex: 100,
              }}>
                {searching ? (
                  <div style={{ padding: '12px 14px', color: C.dim, fontSize: 12 }}>搜索中...</div>
                ) : searchResults.length === 0 ? (
                  <div style={{ padding: '12px 14px', color: C.dim, fontSize: 12 }}>无匹配结果</div>
                ) : (
                  searchResults.map(r => {
                    const exists = watchlist.some(s => s.ts_code === r.ts_code)
                    return (
                      <div
                        key={r.ts_code}
                        onClick={() => !exists && addStock(r)}
                        style={{
                          padding: '8px 12px', borderBottom: `1px solid ${C.border}`,
                          cursor: exists ? 'not-allowed' : 'pointer',
                          display: 'flex', alignItems: 'center', gap: 8,
                          opacity: exists ? 0.5 : 1,
                          fontSize: 12,
                        }}
                        onMouseEnter={e => !exists && (e.currentTarget.style.background = C.cyanBg)}
                        onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                      >
                        <span style={{ fontFamily: 'monospace', color: C.cyan, width: 90 }}>{r.ts_code}</span>
                        <span style={{ color: C.bright, fontWeight: 500, flex: 1 }}>{r.name}</span>
                        <span style={{ color: C.dim, fontSize: 11 }}>{r.industry || '-'}</span>
                        {exists ? (
                          <span style={{ color: C.yellow, fontSize: 11 }}>已添加</span>
                        ) : (
                          <Plus size={14} style={{ color: C.green }} />
                        )}
                      </div>
                    )
                  })
                )}
              </div>
            )}
          </div>

          <button style={S.btn('ghost')} onClick={exportCSV} disabled={watchlist.length === 0}>
            <Download size={14} /> 导出 CSV
          </button>
          <button style={S.btn('danger')} onClick={clearAll} disabled={watchlist.length === 0}>
            <Trash2 size={14} /> 清空全部
          </button>
        </div>
      </div>

      {/* 工具栏 */}
      <div style={S.card}>
        <div style={S.toolbar}>
          <input
            style={S.input}
            placeholder="搜索代码/名称/行业..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
          <span style={{ color: C.dim, fontSize: 12 }}>
            显示 {filtered.length} / {watchlist.length} 只
          </span>
        </div>

        {/* 表格 */}
        <div style={S.tableWrap}>
          {filtered.length === 0 ? (
            <div style={S.empty}>
              {watchlist.length === 0 ? (
                <>
                  <Star size={40} style={{ marginBottom: 12, opacity: 0.3 }} />
                  <div>暂无自选股票</div>
                  <div style={{ fontSize: 12, marginTop: 8 }}>在「每日选股」页面点击 ⭐ 添加</div>
                </>
              ) : (
                <div>没有匹配的股票</div>
              )}
            </div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr>
                  <th style={{ ...S.th, width: 50 }}>#</th>
                  <th style={S.th}>代码</th>
                  <th style={S.th}>名称</th>
                  <th style={S.th}>行业</th>
                  <th style={S.th}>来源策略</th>
                  <th style={S.th}>添加时间</th>
                  <th style={S.th}>备注</th>
                  <th style={{ ...S.th, textAlign: 'center' }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((s, i) => (
                  <tr key={s.ts_code} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)' }}>
                    <td style={{ ...S.td, color: C.dim }}>{i + 1}</td>
                    <td style={{ ...S.td, fontFamily: 'monospace' }}>
                      <Link to={`/stock/${s.ts_code}`} style={{ color: C.cyan, textDecoration: 'none' }} title="查看详情">
                        {s.ts_code}
                      </Link>
                    </td>
                    <td style={{ ...S.td, color: C.bright, fontWeight: 500 }}>
                      <Link to={`/stock/${s.ts_code}`} style={{ color: C.bright, textDecoration: 'none' }}>
                        {s.name || '-'}
                      </Link>
                    </td>
                    <td style={{ ...S.td, color: C.dim }}>{s.industry || '-'}</td>
                    <td style={{ ...S.td }}>
                      {s.source_strategy ? (
                        <span style={{ background: C.cyanBg, color: C.cyan, padding: '2px 8px', borderRadius: 3, fontSize: 11 }}>
                          {s.source_strategy}
                        </span>
                      ) : '-'}
                    </td>
                    <td style={{ ...S.td, color: C.dim, fontSize: 11 }}>
                      {new Date(s.added_at).toLocaleString('zh-CN', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
                    </td>
                    <td style={S.td}>
                      {editingNote === s.ts_code ? (
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                          <input
                            style={{ ...S.input, width: 120, padding: '4px 8px' }}
                            value={noteInput}
                            onChange={e => setNoteInput(e.target.value)}
                            placeholder="输入备注..."
                            autoFocus
                          />
                          <button onClick={() => saveNote(s.ts_code)} style={{ color: C.green, cursor: 'pointer', background: 'none', border: 'none' }}>
                            <Check size={14} />
                          </button>
                          <button onClick={cancelEditNote} style={{ color: C.dim, cursor: 'pointer', background: 'none', border: 'none' }}>
                            <X size={14} />
                          </button>
                        </div>
                      ) : (
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                          <span style={{ color: s.note ? C.text : C.dim, fontSize: 12 }}>
                            {s.note || '-'}
                          </span>
                          <button onClick={() => startEditNote(s)} style={{ color: C.dim, cursor: 'pointer', background: 'none', border: 'none', padding: 0 }}>
                            <Edit2 size={12} />
                          </button>
                        </div>
                      )}
                    </td>
                    <td style={{ ...S.td, textAlign: 'center' }}>
                      <div style={{ display: 'flex', justifyContent: 'center', gap: 6, alignItems: 'center' }}>
                        {(() => {
                          const inPool1 = poolMembers[1]?.has(s.ts_code)
                          const inPool2 = poolMembers[2]?.has(s.ts_code)
                          const disabledStyle = { opacity: 0.3, cursor: 'not-allowed' as const }
                          return (
                            <>
                              <button
                                onClick={() => addToPool(s, 1)}
                                disabled={inPool1}
                                style={{
                                  display: 'flex', alignItems: 'center', gap: 3,
                                  color: inPool1 ? C.dim : C.cyan, cursor: inPool1 ? 'not-allowed' : 'pointer',
                                  background: inPool1 ? 'rgba(0,0,0,0.2)' : C.cyanBg, border: `1px solid ${inPool1 ? C.border : C.cyanDark}`,
                                  padding: '2px 6px', borderRadius: 3, fontSize: 10,
                                  ...(inPool1 ? disabledStyle : {}),
                                }}
                                title={inPool1 ? '已在 1号·择时监控池' : '加入 1号·择时监控池'}
                              >
                                <Eye size={11} /> {inPool1 ? '已入' : '1号池'}
                              </button>
                              <button
                                onClick={() => addToPool(s, 2)}
                                disabled={inPool2}
                                style={{
                                  display: 'flex', alignItems: 'center', gap: 3,
                                  color: inPool2 ? C.dim : C.yellow, cursor: inPool2 ? 'not-allowed' : 'pointer',
                                  background: inPool2 ? 'rgba(0,0,0,0.2)' : 'rgba(255,170,0,0.08)', border: `1px solid ${inPool2 ? C.border : C.yellow}`,
                                  padding: '2px 6px', borderRadius: 3, fontSize: 10,
                                  ...(inPool2 ? disabledStyle : {}),
                                }}
                                title={inPool2 ? '已在 2号·T+0 监控池' : '加入 2号·T+0 监控池'}
                              >
                                <Eye size={11} /> {inPool2 ? '已入' : '2号池'}
                              </button>
                            </>
                          )
                        })()}
                        <button
                          onClick={() => openStockPage(s.ts_code)}
                          style={{ color: C.cyan, cursor: 'pointer', background: 'none', border: 'none', padding: 4 }}
                          title="查看行情"
                        >
                          <ExternalLink size={14} />
                        </button>
                        <button
                          onClick={() => remove(s.ts_code)}
                          style={{ color: C.red, cursor: 'pointer', background: 'none', border: 'none', padding: 4 }}
                          title="删除"
                        >
                          <Trash2 size={14} />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* 使用提示 */}
      <div style={{ marginTop: 16, color: C.dim, fontSize: 11, lineHeight: 1.6 }}>
        <div>💡 提示：</div>
        <ul style={{ margin: '4px 0 0 16px', padding: 0 }}>
          <li>在「每日选股」页面点击股票行右侧的 ⭐ 按钮可添加自选</li>
          <li>点击代码可跳转东方财富查看详细行情</li>
          <li>数据存储在服务端数据库，跨设备共享</li>
        </ul>
      </div>
    </div>
  )
}
