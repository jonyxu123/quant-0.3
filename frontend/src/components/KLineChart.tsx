import { useEffect, useRef, useMemo } from 'react'
import { init, dispose, registerIndicator, Chart, KLineData } from 'klinecharts'
import { calcJinniu } from './jinniu'

/**
 * K线图组件（klinecharts v10）
 *
 * 数据来源：
 * - 日线：stk_factor_pro 表（含 DB 预计算指标：MA/MACD/KDJ/BOLL/RSI）
 * - 周/月线：weekly/monthly 表（只有 OHLCV，指标由 klinecharts 前端计算）
 *
 * 指标使用规则：
 * - 日线 → 使用自定义指标，calc 从数据中读取 DB 预计算值
 * - 周/月线 → 使用 klinecharts 内置指标，前端实时计算
 */

/* ───────── 类型定义 ───────── */
export interface KLinePoint {
  trade_date: string
  open: number; high: number; low: number; close: number
  vol: number; amount: number; pct_chg?: number
  // 可选预计算指标（仅日线）
  ma5?: number; ma10?: number; ma20?: number; ma30?: number; ma60?: number
  macd_dif?: number; macd_dea?: number; macd?: number
  kdj_k?: number; kdj_d?: number; kdj_j?: number
  boll_upper?: number; boll_mid?: number; boll_lower?: number
  rsi6?: number; rsi12?: number; rsi24?: number
}

export type SubIndicatorKey = 'MACD' | 'KDJ' | 'RSI' | 'BOLL_PCT_B' | 'JINNIU' | 'VOL'

interface Props {
  klines: KLinePoint[]
  /** 副图指标多选（可同时显示） */
  subIndicators?: SubIndicatorKey[]
  /** 是否在主图叠加BOLL三轨 */
  showBoll?: boolean
  /** 是否使用数据库预计算指标（日线=true；周/月=false） */
  usePrecomputed?: boolean
  /** 高度: 数字(像素) 或 CSS 值如 '100%' / 'calc(100vh - 200px)' */
  height?: number | string
}

/* ───────── 工具函数 ───────── */
const dateToTs = (d: string): number => {
  if (!d) return 0
  const s = d.replace(/-/g, '')
  if (s.length !== 8) return 0
  return new Date(
    parseInt(s.slice(0, 4), 10),
    parseInt(s.slice(4, 6), 10) - 1,
    parseInt(s.slice(6, 8), 10)
  ).getTime()
}

/* ───────── 暗色主题 ───────── */
const DARK_THEME = {
  grid: { horizontal: { color: '#1e2233' }, vertical: { color: '#1e2233' } },
  candle: {
    bar: {
      upColor: '#ef4444', downColor: '#22c55e', noChangeColor: '#888',
      upBorderColor: '#ef4444', downBorderColor: '#22c55e',
      upWickColor: '#ef4444', downWickColor: '#22c55e',
    },
    priceMark: {
      high: { color: '#c8cdd8' }, low: { color: '#c8cdd8' },
      last: { upColor: '#ef4444', downColor: '#22c55e', text: { color: '#ffffff' } },
    },
    tooltip: {
      text: { color: '#c8cdd8' },
      title: { template: '{ticker} · {period}' },
    },
  },
  indicator: {
    tooltip: { text: { color: '#c8cdd8' } },
    bars: [{ upColor: '#ef4444', downColor: '#22c55e', noChangeColor: '#888' }],
    lines: [
      { color: '#eab308' }, { color: '#a855f7' },
      { color: '#00c8c8' }, { color: '#3b82f6' },
    ],
  },
  xAxis: {
    axisLine: { color: '#1e2233' }, tickLine: { color: '#1e2233' },
    tickText: { color: '#4a5068' },
  },
  yAxis: {
    axisLine: { color: '#1e2233' }, tickLine: { color: '#1e2233' },
    tickText: { color: '#4a5068' },
  },
  crosshair: {
    horizontal: {
      line: { color: '#4a5068' },
      text: { backgroundColor: '#1e2233', color: '#c8cdd8', borderColor: '#1e2233' },
    },
    vertical: {
      line: { color: '#4a5068' },
      text: { backgroundColor: '#1e2233', color: '#c8cdd8', borderColor: '#1e2233' },
    },
  },
  separator: { color: '#1e2233' },
}

/* ───────── MA 颜色配置（A股标准） ───────── */
const MA_COLORS = {
  ma5: '#eab308',   // 黄
  ma10: '#a855f7',  // 紫
  ma20: '#00c8c8',  // 青
  ma30: '#3b82f6',  // 蓝
  ma60: '#ff6b6b',  // 红
}

/* ───────── 自定义指标注册（使用 DB 预计算值） ───────── */

function registerCustomIndicators() {
  // MA（主图叠加，5色5/10/20/30/60）- 从 KLineData 扩展字段读取
  registerIndicator({
    name: 'MA_DB',
    shortName: 'MA',
    series: 'price',
    precision: 2,
    calcParams: [5, 10, 20, 30, 60],
    figures: [
      { key: 'ma5', title: 'MA5: ', type: 'line', styles: () => ({ color: MA_COLORS.ma5 }) },
      { key: 'ma10', title: 'MA10: ', type: 'line', styles: () => ({ color: MA_COLORS.ma10 }) },
      { key: 'ma20', title: 'MA20: ', type: 'line', styles: () => ({ color: MA_COLORS.ma20 }) },
      { key: 'ma30', title: 'MA30: ', type: 'line', styles: () => ({ color: MA_COLORS.ma30 }) },
      { key: 'ma60', title: 'MA60: ', type: 'line', styles: () => ({ color: MA_COLORS.ma60 }) },
    ],
    calc: (dataList: KLineData[]) =>
      dataList.map((d: any) => ({
        ma5: d.ma5, ma10: d.ma10, ma20: d.ma20, ma30: d.ma30, ma60: d.ma60,
      })),
  } as any)

  // MACD（副图）
  registerIndicator({
    name: 'MACD_DB',
    shortName: 'MACD',
    precision: 3,
    figures: [
      {
        key: 'macd', title: 'MACD: ', type: 'bar',
        baseValue: 0,
        styles: (data: any) => {
          const v = data?.current?.indicatorData?.macd
          return { color: v == null || v >= 0 ? '#ef4444' : '#22c55e' }
        },
      },
      { key: 'dif', title: 'DIF: ', type: 'line', styles: () => ({ color: '#eab308' }) },
      { key: 'dea', title: 'DEA: ', type: 'line', styles: () => ({ color: '#00c8c8' }) },
    ],
    calc: (dataList: KLineData[]) =>
      dataList.map((d: any) => ({
        macd: d.macd, dif: d.macd_dif, dea: d.macd_dea,
      })),
  } as any)

  // KDJ（副图）
  registerIndicator({
    name: 'KDJ_DB',
    shortName: 'KDJ',
    precision: 2,
    figures: [
      { key: 'k', title: 'K: ', type: 'line', styles: () => ({ color: '#eab308' }) },
      { key: 'd', title: 'D: ', type: 'line', styles: () => ({ color: '#00c8c8' }) },
      { key: 'j', title: 'J: ', type: 'line', styles: () => ({ color: '#a855f7' }) },
    ],
    calc: (dataList: KLineData[]) =>
      dataList.map((d: any) => ({ k: d.kdj_k, d: d.kdj_d, j: d.kdj_j })),
  } as any)

  // RSI（副图）
  registerIndicator({
    name: 'RSI_DB',
    shortName: 'RSI',
    precision: 2,
    minValue: 0, maxValue: 100,
    figures: [
      { key: 'rsi6', title: 'RSI6: ', type: 'line', styles: () => ({ color: '#eab308' }) },
      { key: 'rsi12', title: 'RSI12: ', type: 'line', styles: () => ({ color: '#00c8c8' }) },
      { key: 'rsi24', title: 'RSI24: ', type: 'line', styles: () => ({ color: '#a855f7' }) },
    ],
    calc: (dataList: KLineData[]) =>
      dataList.map((d: any) => ({ rsi6: d.rsi6, rsi12: d.rsi12, rsi24: d.rsi24 })),
  } as any)

  // BOLL（主图叠加 - 三轨线叠加在K线上）
  registerIndicator({
    name: 'BOLL_DB',
    shortName: 'BOLL',
    series: 'price',
    precision: 2,
    figures: [
      { key: 'upper', title: 'UB: ', type: 'line', styles: () => ({ color: '#f97316' }) },  // 上轨 橙
      { key: 'mid', title: 'BOLL: ', type: 'line', styles: () => ({ color: '#e8eaf0' }) },   // 中轨 白
      { key: 'lower', title: 'LB: ', type: 'line', styles: () => ({ color: '#ec4899' }) }, // 下轨 粉
    ],
    calc: (dataList: KLineData[]) =>
      dataList.map((d: any) => ({
        upper: d.boll_upper, mid: d.boll_mid, lower: d.boll_lower,
      })),
  } as any)

  // BOLL %B 柱状图（副图 - 显示价格在布林带中的相对位置）
  registerIndicator({
    name: 'BOLL_PCT_B',
    shortName: '%B',
    precision: 2,
    minValue: 0, maxValue: 1,
    figures: [
      {
        key: 'pct_b', title: '%B: ', type: 'bar',
        baseValue: 0.5,
        styles: (data: any) => {
          const v = data?.current?.indicatorData?.pct_b
          return { color: v == null || v >= 0.5 ? '#ef4444' : '#22c55e' }
        },
      },
    ],
    calc: (dataList: KLineData[]) =>
      dataList.map((d: any) => {
        const upper = d.boll_upper
        const lower = d.boll_lower
        const range = upper - lower
        const pct_b = range > 0 ? (d.close - lower) / range : 0.5
        return { pct_b }
      }),
  } as any)

  // 金牛暴起 异动起飞选股（副图 - 多信号组合指标）
  // 颜色严格对应通达信公式定义
  registerIndicator({
    name: 'JINNIU',
    shortName: '金牛暴起',
    precision: 2,
    minValue: 0, maxValue: 120,
    figures: [
      // 波: 画白色
      { key: 'bo', title: '波: ', type: 'line', styles: () => ({ color: '#ffffff' }) },
      // 段: 画黄色
      { key: 'duan', title: '段: ', type: 'line', styles: () => ({ color: '#ffff00' }) },
      // 趋势1: 画深灰色DOTLINE
      { key: 'trend1', title: '趋势1: ', type: 'line', styles: () => ({ color: '#808080' }) },
      // 趋势: 画红色(上升)/画蓝色(下降) 动态色
      {
        key: 'trend', title: '趋势: ', type: 'line',
        styles: (data: any) => {
          const cur = data?.current?.indicatorData?.trend
          const prev = data?.prev?.indicatorData?.trend
          return { color: cur != null && prev != null && cur >= prev ? '#ff0000' : '#0000ff' }
        },
      },
      // 资金入场: 画红色
      { key: 'capitalIn', title: '资金: ', type: 'line', styles: () => ({ color: '#ff0000' }) },
      // 高卖Q: 画绿色 线宽2
      { key: 'highSellQ', title: '高卖Q: ', type: 'line', styles: () => ({ color: '#00ff00', size: 2 }) },
      // 低买Q: 画红色 线宽2 (柱状)
      {
        key: 'lowBuyQ', title: '低买Q: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#ff0000' }),
      },
      // 分批买入: COLOR000080 深蓝
      {
        key: 'batchBuy', title: '分批买: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#000080' }),
      },
      // 分批卖出: COLOR008360 深绿
      {
        key: 'batchSell', title: '分批卖: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#008360' }),
      },
      // 金牛: COLOR0000FF 蓝色
      {
        key: 'jinniu', title: '金牛: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#0000ff' }),
      },
      // 散户吸筹: 画绿色
      {
        key: 'retail', title: '散户: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#00ff00' }),
      },
      // 短线出击: 画青色
      {
        key: 'shortStrike', title: '短线: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#00ffff' }),
      },
      // 妖股: 画洋红色 线宽3
      {
        key: 'monster', title: '妖股: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#ff00ff' }),
      },
      // 妖底确定: 画红色
      {
        key: 'monsterBottom', title: '妖底: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#ff0000' }),
      },
      // 超牛: 画白色
      {
        key: 'superBull', title: '超牛: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#ffffff' }),
      },
      // 黑马启动: 画红色
      {
        key: 'darkHorse', title: '黑马: ', type: 'bar',
        baseValue: 0,
        styles: () => ({ color: '#ff0000' }),
      },
    ],
    calc: (dataList: KLineData[]) => {
      const results = calcJinniu(dataList as any)
      return results.map(r => ({
        bo: r.bo,
        duan: r.duan,
        trend1: r.trend1,
        trend: r.trend,
        capitalIn: r.capitalIn,
        highSellQ: r.highSellQ,
        lowBuyQ: r.lowBuyQ,
        batchBuy: r.batchBuy * 40,
        batchSell: r.batchSell * 70,
        jinniu: r.jinniu * 35,
        retail: r.retail,
        shortStrike: r.shortStrike,
        monster: r.monster,
        monsterBottom: r.monsterBottom,
        superBull: r.superBull,
        darkHorse: r.darkHorse,
      }))
    },
  } as any)
}

// 指标名映射：根据是否使用预计算值选择对应的指标名
const indicatorName = (key: SubIndicatorKey, usePrecomputed: boolean): string => {
  if (key === 'VOL') return 'VOL'          // VOL 用内置即可
  if (key === 'BOLL_PCT_B') return 'BOLL_PCT_B'  // %B 始终用自定义
  if (key === 'JINNIU') return 'JINNIU'    // 金牛暴起始终用自定义
  if (!usePrecomputed) return key           // 周/月线用内置
  return `${key}_DB`                        // 日线用自定义(读DB)
}

// BOLL主图指标名
const bollIndicatorName = (usePrecomputed: boolean): string => {
  return usePrecomputed ? 'BOLL_DB' : 'BOLL'  // 日线用DB预计算，周/月线用内置
}

/* ───────── 组件 ───────── */
export default function KLineChart({
  klines,
  subIndicators = ['MACD'],
  showBoll = false,
  usePrecomputed = true,
  height = '100%',
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<Chart | null>(null)
  // 存放所有副图 paneId（简单数组；变化时全量重建）
  const subPaneIds = useRef<string[]>([])

  // 注册自定义指标（全局单次）
  useMemo(() => registerCustomIndicators(), [])

  // 转换数据（保留预计算字段供自定义指标 calc 使用）
  const kData = useMemo<KLineData[]>(() => {
    if (!klines) return []
    return klines
      .filter(k => k.open != null && k.close != null)
      .map(k => ({
        timestamp: dateToTs(k.trade_date),
        open: Number(k.open),
        high: Number(k.high),
        low: Number(k.low),
        close: Number(k.close),
        volume: Number(k.vol || 0),
        turnover: Number(k.amount || 0),
        // 扩展字段（自定义指标通过 (d:any) 访问）
        ma5: k.ma5, ma10: k.ma10, ma20: k.ma20, ma30: k.ma30, ma60: k.ma60,
        macd: k.macd, macd_dif: k.macd_dif, macd_dea: k.macd_dea,
        kdj_k: k.kdj_k, kdj_d: k.kdj_d, kdj_j: k.kdj_j,
        rsi6: k.rsi6, rsi12: k.rsi12, rsi24: k.rsi24,
        boll_upper: k.boll_upper, boll_mid: k.boll_mid, boll_lower: k.boll_lower,
      }) as any)
      .sort((a: any, b: any) => a.timestamp - b.timestamp)
  }, [klines])

  // 初始化图表
  useEffect(() => {
    if (!containerRef.current) return
    const chart = init(containerRef.current, { styles: DARK_THEME as any })
    if (!chart) return
    chartRef.current = chart

    chart.setSymbol({ ticker: 'STOCK' })
    chart.setPeriod({ span: 1, type: 'day' })

    // 主图 MA（固定显示 5/10/20/30）
    const maName = usePrecomputed ? 'MA_DB' : 'MA'
    chart.createIndicator(
      { name: maName, calcParams: [5, 10, 20, 30, 60] } as any,
      true,
      { id: 'candle_pane' }
    )

    // DataLoader
    chart.setDataLoader({
      getBars: ({ callback }: any) => callback(kData),
    })

    return () => {
      if (containerRef.current) dispose(containerRef.current)
      chartRef.current = null
      subPaneIds.current = []
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 数据更新
  useEffect(() => {
    if (!chartRef.current) return
    chartRef.current.setDataLoader({
      getBars: ({ callback }: any) => callback(kData),
    })
    chartRef.current.resetData()
  }, [kData])

  // 主图指标同步（MA + BOLL叠加）
  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    // 移除主图上所有指标
    const mainInds = chart.getIndicators({ paneId: 'candle_pane' })
    for (const ind of mainInds) {
      chart.removeIndicator({
        id: (ind as any).id,
        paneId: 'candle_pane',
        name: (ind as any).name,
      })
    }
    // 创建 MA
    const maName = usePrecomputed ? 'MA_DB' : 'MA'
    chart.createIndicator(
      { name: maName, calcParams: [5, 10, 20, 30, 60] } as any,
      true,
      { id: 'candle_pane' }
    )
    // 创建 BOLL 叠加（如果开启）
    if (showBoll) {
      const bollName = bollIndicatorName(usePrecomputed)
      chart.createIndicator(
        { name: bollName } as any,
        true,
        { id: 'candle_pane' }
      )
    }
  }, [usePrecomputed, showBoll])

  // 副图指标多选同步（全量重建，保证取消选择时立即移除）
  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return

    // 1. 枚举并移除所有非主图指标（candle_pane 是主图）
    const all = chart.getIndicators()
    for (const ind of all) {
      if ((ind as any).paneId && (ind as any).paneId !== 'candle_pane') {
        chart.removeIndicator({
          id: (ind as any).id,
          paneId: (ind as any).paneId,
          name: (ind as any).name,
        })
      }
    }
    subPaneIds.current = []

    // 2. 按当前选择重新创建
    for (const key of subIndicators) {
      const name = indicatorName(key, usePrecomputed)
      const paneId = chart.createIndicator(name, false, { height: 100 })
      if (paneId) subPaneIds.current.push(paneId)
    }

    // 3. 触发重绘
    chart.resize()
  }, [subIndicators, usePrecomputed])

  // 容器尺寸变化时重绘图表
  useEffect(() => {
    if (!containerRef.current || !chartRef.current) return
    const ro = new ResizeObserver(() => {
      chartRef.current?.resize()
    })
    ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [])

  return (
    <div
      ref={containerRef}
      style={{ width: '100%', height, background: '#0f1117' }}
    />
  )
}
