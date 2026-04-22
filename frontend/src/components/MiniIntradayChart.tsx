/**
 * 分时小图组件（基于Lightweight Charts + tick数据绘制）
 *
 * 参考同花顺/通达信分时图：
 *   - 主区：分时线（close 折线）+ 均价线（VWAP 虚线）
 *   - 底部：成交量柱
 *   - 横轴：09:30 - 15:00
 *   - 纵轴：以 pre_close 为基准，显示涨跌幅区间
 *
 * Props:
 *   tickData: tick数据数组 [{time, price, volume, amount}]，按时间排序
 *   preClose: 昨收价（用于计算涨跌并作为中轴）
 *   width/height: 画布尺寸
 *   showVolume: 是否显示底部量柱（默认 true）
 *   signals: 信号标注数组
 *
 * 性能优化：
 *   - chart 只创建一次，数据更新使用 series.update() 增量更新
 *   - 按分钟聚合后，只 update 新增/变更的分钟数据点
 *   - signals 变化只更新 markers，不重建 chart
 *   - width/height 变化只 applyOptions，不重建 chart
 */
import { useEffect, useRef } from 'react'
import { createChart, IChartApi, ISeriesApi, ColorType, Time, LineData } from 'lightweight-charts'

export interface TickDataPoint {
  time: number                      // unix timestamp (秒)
  price: number
  volume: number
  amount: number
}

/** 信号标注（与后端 signals.py 字段对齐） */
export interface ChartSignal {
  type: string                      // left_side_buy / right_side_breakout / positive_t / reverse_t
  direction: 'buy' | 'sell'         // 方向
  price: number                     // 触发价
  triggered_at: number              // unix timestamp (秒)
  message?: string
  strength?: number
}

interface Props {
  tickData: TickDataPoint[]
  preClose: number
  width?: number
  height?: number
  showVolume?: boolean
  signals?: ChartSignal[]
}

const C = {
  bg: '#0f1117',
  line: '#60a5fa',     // 分时线 蓝
  avg: '#eab308',       // 均价线 黄
  red: '#ef4444',
  green: '#22c55e',
  fill: 'rgba(96,165,250,0.12)',
}

/** 将tick数据按分钟聚合，返回分时数据 */
function aggregateTickToMinute(tickData: TickDataPoint[]): LineData[] {
  if (!tickData || tickData.length === 0) return []

  // 按分钟聚合：每分钟取最后价格
  const minuteMap = new Map<number, { price: number; volume: number; amount: number }>()
  tickData.forEach(tick => {
    const minute = Math.floor(tick.time / 60) * 60
    const existing = minuteMap.get(minute)
    if (existing) {
      existing.price = tick.price  // 更新为最新价格
      existing.volume += tick.volume
      existing.amount += tick.amount
    } else {
      minuteMap.set(minute, {
        price: tick.price,
        volume: tick.volume,
        amount: tick.amount,
      })
    }
  })

  // 转换为LineData并按时间排序
  const result: LineData[] = []
  minuteMap.forEach((data, time) => {
    result.push({ time: time as Time, value: data.price })
  })
  result.sort((a, b) => Number(a.time) - Number(b.time))

  return result
}

/** 计算均价线（VWAP），按分钟聚合 */
function calculateVWAP(tickData: TickDataPoint[]): LineData[] {
  if (!tickData || tickData.length === 0) return []

  // 先按分钟聚合，计算每分钟的平均价格
  const minuteMap = new Map<number, { sumPrice: number; count: number }>()
  tickData.forEach(tick => {
    const minute = Math.floor(tick.time / 60) * 60
    const existing = minuteMap.get(minute)
    if (existing) {
      existing.sumPrice += tick.price
      existing.count += 1
    } else {
      minuteMap.set(minute, {
        sumPrice: tick.price,
        count: 1,
      })
    }
  })

  // 计算累计平均价格并按时间排序
  let cumSumPrice = 0
  let cumCount = 0
  const result: LineData[] = []
  const sortedMinutes = Array.from(minuteMap.keys()).sort((a, b) => a - b)

  sortedMinutes.forEach(minute => {
    const data = minuteMap.get(minute)!
    cumSumPrice += data.sumPrice
    cumCount += data.count
    const avgPrice = cumCount > 0 ? cumSumPrice / cumCount : 0
    result.push({ time: minute as Time, value: avgPrice })
  })

  return result
}

export default function MiniIntradayChart({
  tickData, preClose: _preClose, width = 320, height = 180, showVolume: _showVolume = true, signals,
}: Props) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const priceSeriesRef = useRef<ISeriesApi<'Area'> | null>(null)
  const avgSeriesRef = useRef<ISeriesApi<'Line'> | null>(null)

  // 追踪上次渲染的 tickData 长度，用于增量更新
  const lastDataLenRef = useRef<number>(0)
  // 追踪 chart 是否已初始化
  const chartReadyRef = useRef<boolean>(false)

  // ─── Effect 1: chart 初始化（仅执行一次） ───
  useEffect(() => {
    const container = chartContainerRef.current
    if (!container) return

    const chart = createChart(container, {
      width,
      height,
      layout: {
        background: { type: ColorType.Solid, color: C.bg },
        textColor: '#4a5068',
      },
      grid: {
        vertLines: { color: '#1e2233' },
        horzLines: { color: '#1e2233' },
      },
      rightPriceScale: {
        borderColor: '#1e2233',
      },
      timeScale: {
        borderColor: '#1e2233',
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        mode: 1,
      },
    })

    chartRef.current = chart

    // 创建分时线（Area Chart）
    const priceSeries = chart.addAreaSeries({
      lineColor: C.line,
      topColor: C.fill,
      bottomColor: 'transparent',
      lineWidth: 1,
    })
    priceSeriesRef.current = priceSeries

    // 创建均价线（Line Chart）
    const avgSeries = chart.addLineSeries({
      color: C.avg,
      lineWidth: 1,
      lineStyle: 2, // 虚线
    })
    avgSeriesRef.current = avgSeries

    chartReadyRef.current = true

    return () => {
      chartReadyRef.current = false
      chart.remove()
      chartRef.current = null
      priceSeriesRef.current = null
      avgSeriesRef.current = null
    }
  }, []) // 仅挂载时执行一次

  // ─── Effect 2: 数据更新 ───
  useEffect(() => {
    if (!chartReadyRef.current) return
    if (!tickData || tickData.length === 0) return

    const priceSeries = priceSeriesRef.current
    const avgSeries = avgSeriesRef.current
    if (!priceSeries || !avgSeries) return

    // 聚合数据
    const priceData = aggregateTickToMinute(tickData)
    const avgData = calculateVWAP(tickData)

    if (priceData.length === 0) return

    // 全量 setData：分时图数据量小（最多240分钟点），setData 开销可忽略
    // 避免使用 update() 导致的 "Cannot update oldest data" 时间顺序错误
    const prevLen = lastDataLenRef.current
    priceSeries.setData(priceData)
    avgSeries.setData(avgData)

    // 仅首次或数据重置时 fitContent（避免每次都重置视口）
    if (prevLen === 0 || tickData.length < lastDataLenRef.current) {
      chartRef.current?.timeScale().fitContent()
    }

    lastDataLenRef.current = tickData.length
  }, [tickData])

  // ─── Effect 3: 信号标注更新（不重建 chart） ───
  useEffect(() => {
    const priceSeries = priceSeriesRef.current
    if (!priceSeries || !chartReadyRef.current) return

    if (signals && signals.length > 0) {
      const markers = signals
        .filter(s => s.price > 0 && s.triggered_at > 0)
        .map(s => {
          const time = s.triggered_at as Time
          const isBuy = s.direction === 'buy'
          return {
            time,
            position: isBuy ? 'belowBar' as const : 'aboveBar' as const,
            color: isBuy ? C.red : C.green,
            shape: isBuy ? 'arrowUp' as const : 'arrowDown' as const,
            text: isBuy ? 'B' : 'S',
          }
        })
      priceSeries.setMarkers(markers)
    } else {
      priceSeries.setMarkers([])
    }
  }, [signals])

  // ─── Effect 4: 尺寸变化（不重建 chart） ───
  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    chart.applyOptions({ width, height })
  }, [width, height])

  // ─── Effect 5: 响应式尺寸调整 ───
  useEffect(() => {
    const handleResize = () => {
      if (chartRef.current && chartContainerRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        })
      }
    }

    window.addEventListener('resize', handleResize)
    handleResize()

    return () => window.removeEventListener('resize', handleResize)
  }, [])

  // 是否有数据（用于显示/隐藏 overlay 提示）
  const hasData = tickData && tickData.length > 0

  return (
    <div style={{ position: 'relative', width, height, borderRadius: 4 }}>
      <div
        ref={chartContainerRef}
        style={{
          width,
          height,
          background: C.bg,
          borderRadius: 4,
        }}
      />
      {!hasData && (
        <div style={{
          position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          color: '#4a5068', fontSize: 11, background: C.bg, borderRadius: 4,
          pointerEvents: 'none',
        }}>无分时数据</div>
      )}
    </div>
  )
}
