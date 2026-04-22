import { useRef, useEffect } from 'react'

const C = {
  bg: '#0f1117', card: '#181d2a', border: '#1e2233',
  text: '#c8cdd8', dim: '#4a5068', bright: '#e8eaf0',
  cyan: '#00c8c8', red: '#ef4444', green: '#22c55e',
  yellow: '#eab308', purple: '#a855f7', blue: '#3b82f6',
}

interface ChipPoint {
  trade_date: string
  his_low: number
  his_high: number
  cost_5pct: number
  cost_15pct: number
  cost_50pct: number
  cost_85pct: number
  cost_95pct: number
  weight_avg: number
  winner_rate: number
}

interface Props {
  data: ChipPoint[]
}

/**
 * 同花顺风格筹码分布图（Canvas 绘制）
 * - 纵向柱状图：横轴价格，纵轴筹码量
 * - 基于成本分布位（5%/15%/50%/85%/95%）插值模拟筹码分布曲线
 * - 颜色：高于当前价格为获利盘（绿），低于为亏损盘（红）
 */
export default function ChipDistribution({ data }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  if (!data || data.length === 0) {
    return <div style={{ padding: 40, textAlign: 'center', color: C.dim }}>无筹码分布数据</div>
  }

  const latest = data[data.length - 1]
  const currentPrice = latest.weight_avg || latest.cost_50pct // 用加权成本或50%成本位作为当前价参考

  // 基于成本分布位生成模拟筹码分布曲线
  const generateChipCurve = () => {
    const priceRange = latest.his_high - latest.his_low
    if (priceRange <= 0) return []

    const points: Array<{ price: number; volume: number }> = []
    const step = priceRange / 100 // 100个价格区间

    for (let p = latest.his_low; p <= latest.his_high; p += step) {
      // 根据价格在成本分布位中的位置计算筹码量（模拟）
      let volume = 0
      if (p <= latest.cost_5pct) volume = 5
      else if (p <= latest.cost_15pct) volume = 15
      else if (p <= latest.cost_50pct) volume = 50
      else if (p <= latest.cost_85pct) volume = 85
      else if (p <= latest.cost_95pct) volume = 95
      else volume = 100

      // 添加平滑过渡（正态分布模拟）
      const center = latest.weight_avg
      const spread = priceRange * 0.3
      const gaussian = Math.exp(-Math.pow(p - center, 2) / (2 * spread * spread))
      volume = volume * (0.5 + gaussian * 0.5) // 混合分布位和正态分布

      points.push({ price: p, volume })
    }
    return points
  }

  const chipCurve = generateChipCurve()

  // Canvas 绘制
  useEffect(() => {
    const canvas = canvasRef.current
    const container = containerRef.current
    if (!canvas || !container) return

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    // 设置 Canvas 尺寸
    const dpr = window.devicePixelRatio || 1
    const rect = container.getBoundingClientRect()
    canvas.width = rect.width * dpr
    canvas.height = rect.height * dpr
    canvas.style.width = `${rect.width}px`
    canvas.style.height = `${rect.height}px`
    ctx.scale(dpr, dpr)

    const width = rect.width
    const height = rect.height
    const padding = { top: 20, right: 50, bottom: 30, left: 50 }

    // 清空
    ctx.clearRect(0, 0, width, height)

    // 绘图区域
    const chartW = width - padding.left - padding.right
    const chartH = height - padding.top - padding.bottom

    // 价格范围
    const minPrice = latest.his_low
    const maxPrice = latest.his_high
    const priceRange = maxPrice - minPrice || 1

    // 筹码量范围
    const maxVol = Math.max(...chipCurve.map(c => c.volume))

    // 绘制网格
    ctx.strokeStyle = C.border
    ctx.lineWidth = 1
    ctx.beginPath()
    // 横向网格（价格）
    for (let i = 0; i <= 5; i++) {
      const y = padding.top + (chartH * i) / 5
      ctx.moveTo(padding.left, y)
      ctx.lineTo(width - padding.right, y)
      // 价格标签
      const price = maxPrice - (priceRange * i) / 5
      ctx.fillStyle = C.dim
      ctx.font = '11px monospace'
      ctx.fillText(price.toFixed(2), 5, y + 4)
    }
    ctx.stroke()

    // 绘制筹码分布柱状图
    const barWidth = chartW / chipCurve.length

    chipCurve.forEach((point, i) => {
      const x = padding.left + i * barWidth
      const barH = (point.volume / maxVol) * chartH
      const y = padding.top + chartH - barH

      // 判断获利/亏损
      const isProfit = point.price >= currentPrice
      ctx.fillStyle = isProfit ? C.green : C.red
      ctx.globalAlpha = 0.6
      ctx.fillRect(x, y, barWidth - 1, barH)
      ctx.globalAlpha = 1
    })

    // 绘制当前价格线
    const currentY = padding.top + chartH - ((currentPrice - minPrice) / priceRange) * chartH
    ctx.strokeStyle = C.cyan
    ctx.lineWidth = 2
    ctx.setLineDash([5, 5])
    ctx.beginPath()
    ctx.moveTo(padding.left, currentY)
    ctx.lineTo(width - padding.right, currentY)
    ctx.stroke()
    ctx.setLineDash([])

    // 当前价格标签
    ctx.fillStyle = C.cyan
    ctx.font = 'bold 12px monospace'
    ctx.fillText(`当前: ${currentPrice.toFixed(2)}`, width - padding.right + 5, currentY + 4)

    // 绘制成本分布位标记线
    const costLines = [
      { price: latest.cost_5pct, label: '5%', color: C.yellow },
      { price: latest.cost_15pct, label: '15%', color: C.yellow },
      { price: latest.cost_50pct, label: '50%', color: C.cyan },
      { price: latest.cost_85pct, label: '85%', color: C.yellow },
      { price: latest.cost_95pct, label: '95%', color: C.yellow },
    ]

    costLines.forEach(line => {
      const y = padding.top + chartH - ((line.price - minPrice) / priceRange) * chartH
      ctx.strokeStyle = line.color
      ctx.lineWidth = 1
      ctx.setLineDash([2, 2])
      ctx.beginPath()
      ctx.moveTo(padding.left, y)
      ctx.lineTo(width - padding.right, y)
      ctx.stroke()
      ctx.setLineDash([])

      ctx.fillStyle = line.color
      ctx.font = '10px monospace'
      ctx.fillText(line.label, 5, y + 3)
    })

  }, [chipCurve, latest, currentPrice])

  return (
    <div>
      {/* 顶部摘要 */}
      <div style={{
        display: 'flex', gap: 16, padding: '10px 12px',
        background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4,
        marginBottom: 12, fontSize: 12,
      }}>
        <span>加权成本: <span style={{ color: C.bright, fontFamily: 'monospace' }}>{latest.weight_avg.toFixed(2)}</span></span>
        <span>获利盘: <span style={{ color: latest.winner_rate > 50 ? C.green : C.red, fontFamily: 'monospace' }}>{latest.winner_rate.toFixed(1)}%</span></span>
        <span>50%成本位: <span style={{ color: C.bright, fontFamily: 'monospace' }}>{latest.cost_50pct.toFixed(2)}</span></span>
        <span>价格区间: <span style={{ color: C.dim, fontFamily: 'monospace' }}>{latest.his_low.toFixed(2)} - {latest.his_high.toFixed(2)}</span></span>
      </div>

      {/* Canvas 容器 */}
      <div ref={containerRef} style={{ height: 400, position: 'relative' }}>
        <canvas ref={canvasRef} style={{ width: '100%', height: '100%' }} />
      </div>

      {/* 图例 */}
      <div style={{ display: 'flex', gap: 20, marginTop: 12, fontSize: 11, color: C.dim }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 12, height: 12, background: C.green, opacity: 0.6, borderRadius: 2 }} />
          获利盘（高于当前价）
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 12, height: 12, background: C.red, opacity: 0.6, borderRadius: 2 }} />
          亏损盘（低于当前价）
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 20, height: 0, borderBottom: `1px dashed ${C.cyan}` }} />
          当前价格
        </div>
      </div>

      {/* 说明 */}
      <div style={{ marginTop: 12, padding: '8px 12px', background: C.bg, borderRadius: 4, fontSize: 11, color: C.dim }}>
        <strong style={{ color: C.text }}>筹码分布说明：</strong>
        纵向柱状图显示不同价格区间的筹码量分布。绿色表示获利盘（高于当前价），红色表示亏损盘（低于当前价）。
        成本分布位（5%/15%/50%/85%/95%）用黄色虚线标注。筹码分布基于成本分布位数据模拟生成。
      </div>
    </div>
  )
}
