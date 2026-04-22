/**
 * 金牛暴起 异动起飞选股 - 通达信副图公式移植
 *
 * 从通达信公式翻译为 TypeScript，基于 klinecharts 的 KLineData 数据结构。
 * 实现全部核心模块：
 *   - 波/段（36日KDJ三重平滑）  画白色/画黄色
 *   - 趋势1（27日KDJ的J值）     画深灰色DOTLINE
 *   - 分批买入/卖出信号          COLOR000080 / COLOR008360
 *   - 趋势（动力线）             画红色/画蓝色（动态）
 *   - 资金入场                   画红色
 *   - 散户吸筹                   画绿色
 *   - 金牛（光头大阳线）         COLOR000099~COLOR0000FF 渐变蓝
 *   - 低买Q/高卖Q                画红色线宽2 / 画绿色线宽2
 *   - 短线出击                   画青色
 *   - 妖股                       画洋红色线宽3
 *   - 妖底确定                   画红色
 *   - 超牛                       画白色
 *   - 黑马启动                   画红色
 */

/* ───────── 基础计算函数 ───────── */

/** LLV: N周期最低值 */
function llv(src: number[], n: number, i: number): number {
  const start = Math.max(0, i - n + 1)
  let min = Infinity
  for (let j = start; j <= i; j++) min = Math.min(min, src[j])
  return min === Infinity ? 0 : min
}

/** HHV: N周期最高值 */
function hhv(src: number[], n: number, i: number): number {
  const start = Math.max(0, i - n + 1)
  let max = -Infinity
  for (let j = start; j <= i; j++) max = Math.max(max, src[j])
  return max === -Infinity ? 0 : max
}

/** SMA: 通达信加权移动平均 Y = (weight*X + (n-weight)*Y') / n */
function sma(src: number[], n: number, weight: number): number[] {
  const result: number[] = []
  for (let i = 0; i < src.length; i++) {
    if (i === 0) {
      result.push(src[0])
    } else {
      result.push((weight * src[i] + (n - weight) * result[i - 1]) / n)
    }
  }
  return result
}

/** EMA: 指数移动平均 */
function ema(src: number[], n: number): number[] {
  const result: number[] = []
  const k = 2 / (n + 1)
  for (let i = 0; i < src.length; i++) {
    if (i === 0) {
      result.push(src[0])
    } else {
      result.push(k * src[i] + (1 - k) * result[i - 1])
    }
  }
  return result
}

/** MA: 简单移动平均 */
function ma(src: number[], n: number): number[] {
  const result: number[] = []
  for (let i = 0; i < src.length; i++) {
    if (i < n - 1) {
      let sum = 0
      for (let j = 0; j <= i; j++) sum += src[j]
      result.push(sum / (i + 1))
    } else {
      let sum = 0
      for (let j = i - n + 1; j <= i; j++) sum += src[j]
      result.push(sum / n)
    }
  }
  return result
}

/** CROSS: 上穿判断 */
function cross(shortArr: number[], longArr: number[], i: number): boolean {
  if (i < 1) return false
  return shortArr[i] > longArr[i] && shortArr[i - 1] <= longArr[i - 1]
}

/** REF: N日前的值 */
function ref(src: number[], n: number, i: number): number {
  return i >= n ? src[i - n] : src[0]
}

/** FILTER: 信号过滤，N日内只保留第一个 */
function filterSignal(cond: boolean[], n: number): boolean[] {
  const result: boolean[] = new Array(cond.length).fill(false)
  let lastBar = -Infinity
  for (let i = 0; i < cond.length; i++) {
    if (cond[i] && (i - lastBar) >= n) {
      result[i] = true
      lastBar = i
    }
  }
  return result
}

/** COUNT: N周期内满足条件的次数 */
function count(cond: boolean[], n: number, i: number): number {
  const start = Math.max(0, i - n + 1)
  let c = 0
  for (let j = start; j <= i; j++) if (cond[j]) c++
  return c
}

/** BETWEEN: A介于B和C之间 */
function between(a: number, b: number, c: number): boolean {
  return a >= Math.min(b, c) && a <= Math.max(b, c)
}

/** BARSLAST: 上次条件成立到当前的周期数 */
function barslast(cond: boolean[], i: number): number {
  for (let j = i; j >= 0; j--) {
    if (cond[j]) return i - j
  }
  return i + 1
}

/** MAX */
function max(a: number, b: number): number {
  return Math.max(a, b)
}

/** ABS */
function abs(a: number): number {
  return Math.abs(a)
}

/* ───────── MACD 计算 ───────── */
function calcMACD(close: number[], short = 12, long_ = 26, mid = 9) {
  const emaShort = ema(close, short)
  const emaLong = ema(close, long_)
  const dif = emaShort.map((v, i) => v - emaLong[i])
  const dea = ema(dif, mid)
  const macd = dif.map((v, i) => (v - dea[i]) * 2)
  return { dif, dea, macd }
}

/* ───────── 主计算函数 ───────── */

export interface JinniuResult {
  /** 波（36日KDJ的D线，画白色） */
  bo: number
  /** 段（36日KDJ的三重平滑，画黄色） */
  duan: number
  /** 趋势1（27日KDJ的J值，画深灰色DOTLINE） */
  trend1: number
  /** 趋势（动力线，0~120，画红色/画蓝色动态） */
  trend: number
  /** 资金入场（画红色） */
  capitalIn: number
  /** 低买Q (0或50，画红色线宽2) */
  lowBuyQ: number
  /** 高卖Q (80或120，画绿色线宽2) */
  highSellQ: number
  /** 分批买入信号 (0或1，COLOR000080深蓝) */
  batchBuy: number
  /** 分批卖出信号 (0或1，COLOR008360深绿) */
  batchSell: number
  /** 金牛信号 (0或1，COLOR000099~COLOR0000FF渐变蓝) */
  jinniu: number
  /** 散户吸筹（画绿色） */
  retail: number
  /** 短线出击 (0或20，画青色) */
  shortStrike: number
  /** 妖股 (0或35，画洋红色线宽3) */
  monster: number
  /** 妖底确定 (0或10，画红色) */
  monsterBottom: number
  /** 超牛 (0或10，画白色) */
  superBull: number
  /** 黑马启动 (0或11，画红色) */
  darkHorse: number
}

/**
 * 计算金牛暴起指标
 */
export function calcJinniu(dataList: any[]): JinniuResult[] {
  const len = dataList.length
  if (len === 0) return []

  // 提取基础序列
  const C = dataList.map((d: any) => d.close as number)
  const H = dataList.map((d: any) => d.high as number)
  const L = dataList.map((d: any) => d.low as number)
  const V = dataList.map((d: any) => (d.volume as number) || 0)
  const A = dataList.map((d: any) => (d.turnover as number) || 0)

  // ─── 模块1: 波/段（36日KDJ三重平滑）画白色/画黄色 ───
  const rsv36: number[] = []
  for (let i = 0; i < len; i++) {
    const ll = llv(L, 36, i)
    const hh = hhv(H, 36, i)
    rsv36.push(hh !== ll ? (C[i] - ll) / (hh - ll) * 100 : 50)
  }
  const gup2 = sma(rsv36, 3, 1)  // K
  const gup3 = sma(gup2, 3, 1)   // D = 波
  const gup4 = sma(gup3, 3, 1)   // 段

  // ─── 模块2: 趋势1（27日KDJ的J值）画深灰色DOTLINE ───
  const rsv27: number[] = []
  for (let i = 0; i < len; i++) {
    const ll = llv(L, 27, i)
    const hh = hhv(H, 27, i)
    rsv27.push(hh !== ll ? (C[i] - ll) / (hh - ll) * 100 : 50)
  }
  const sma27_5 = sma(rsv27, 5, 1)
  const sma27_5_3 = sma(sma27_5, 3, 1)
  const trend1 = sma27_5.map((v, i) => 3 * v - 2 * sma27_5_3[i])

  // ─── 模块3: 分批买入/卖出 ───
  // 分批卖出: COLOR008360(深绿)  分批买入: COLOR000080(深蓝)
  const batchSell: number[] = new Array(len).fill(0)
  const batchBuy: number[] = new Array(len).fill(0)
  for (let i = 0; i < len; i++) {
    if (cross(gup2, gup3, i) && gup3[i] > 80 && gup3[i] > gup4[i]) {
      batchSell[i] = 1
    }
    if (cross(trend1, gup2, i) && trend1[i] < 20 && trend1[i] < gup4[i]) {
      batchBuy[i] = 1
    }
  }

  // ─── 模块4: 资金入场 画红色 ───
  const gup02Src: number[] = []
  for (let i = 0; i < len; i++) {
    const ll25 = llv(L, 25, i)
    const hh25 = hhv(H, 25, i)
    const range25 = hh25 - ll25
    const diff = C[i] - ll25
    gup02Src.push(diff !== 0 ? range25 / diff : 100)
  }
  const capitalInArr = ema(gup02Src, 5)

  // ─── 模块5: 散户吸筹 画绿色 ───
  const retail: number[] = new Array(len).fill(0)
  for (let i = 0; i < len; i++) {
    const gub4 = llv(L, 34, i)
    if (L[i] <= gub4 && i >= 12) {
      const avgPrice = A[i] / (V[i] || 1)
      const deviation = (C[i] - avgPrice) / (avgPrice || 1)
      retail[i] = max(-deviation * 30, 0)
    }
  }
  const retailEma = ema(retail, 3)

  // ─── 模块6: 金牛（光头大阳线）COLOR000099~COLOR0000FF渐变蓝 ───
  const jinniuCond: boolean[] = new Array(len).fill(false)
  for (let i = 0; i < len; i++) {
    const pctChg = i >= 1 ? C[i] / C[i - 1] : 1
    const upperWick = H[i] / C[i]
    if (pctChg > 1.05 && upperWick < 1.01 && C[i] > (i >= 1 ? C[i - 1] : C[i])) {
      jinniuCond[i] = true
    }
  }
  const jinniuFiltered = filterSignal(jinniuCond, 45)

  // ─── 模块7: 低买Q/高卖Q 画红色线宽2/画绿色线宽2 ───
  const NQ = 9
  const rsvq: number[] = []
  for (let i = 0; i < len; i++) {
    const ll = llv(L, NQ, i)
    const hh = hhv(H, NQ, i)
    rsvq.push(hh !== ll ? (C[i] - ll) / (hh - ll) * 100 : 50)
  }
  const varbq = rsvq.map(v => (v / 2 + 22) * 1)

  const filterQ = ema(A, 13).map((v, i) => {
    const volEma = ema(V, 13)[i]
    return volEma !== 0 ? v / volEma / 100 : C[i]
  })
  const refineQ = filterQ.map((v, i) => (C[i] - v) / v * 100)

  const lowBuyQ: number[] = new Array(len).fill(0)
  const highSellQ: number[] = new Array(len).fill(120)
  for (let i = 0; i < len; i++) {
    // 黄金Q: 提纯Q<0 AND 直线拟合(通达信中直线拟合非零即真，几乎永远成立)
    const goldQ = refineQ[i] < 0
    if (goldQ && rsvq[i] < varbq[i] - 2) {
      lowBuyQ[i] = 50
    }
    if (goldQ && rsvq[i] > varbq[i]) {
      highSellQ[i] = 80
    }
  }

  // ─── 模块8: 趋势（动力线）画红色/画蓝色动态 ───
  const powerSrc: number[] = []
  for (let i = 0; i < len; i++) {
    const ll = llv(L, 10, i)
    const hh = hhv(H, 25, i)
    powerSrc.push(hh !== ll ? (C[i] - ll) / (hh - ll) * 4 : 2)
  }
  const powerEma = ema(powerSrc, 4)
  const trendLine = ma(powerEma, 2).map(v => v * 30)

  // ─── 模块9: 短线出击 画青色 ───
  const macdResult = calcMACD(C)
  const shortStrike: number[] = new Array(len).fill(0)
  const bbbSrc1: number[] = []
  const bbbSrc2: number[] = []
  for (let i = 0; i < len; i++) {
    const diff = C[i] - ref(C, 1, i)
    bbbSrc1.push(max(diff, 0))
    bbbSrc2.push(abs(diff))
  }
  const bbb = sma(bbbSrc1, 5, 1).map((v, i) => {
    const d = sma(bbbSrc2, 5, 1)[i]
    return d !== 0 ? v / d * 100 : 50
  })
  const hhh = bbb.map((v, i) => v - llv(bbb, 10, i))
  const ss = ma(hhh, 2).map((v, i) => (v * 3 + hhh[i] * 13) / 16)
  const shortBuyPt = ss.map(v => v > 13 ? v : v / 6)
  for (let i = 1; i < len; i++) {
    const macdCond = macdResult.dif[i] > macdResult.dea[i]
      && macdResult.dif[i] < 0.2
      && macdResult.dif[i] > 0
    if (shortBuyPt[i] > 1 && shortBuyPt[i - 1] <= 1
      && shortBuyPt[i] < 30 && macdCond) {
      shortStrike[i] = 20
    }
  }

  // ─── 模块10: 妖股 画洋红色线宽3 ───
  // VA2:=V>MA(V,89); VA3:=EMA(C,5); VA4:=EMA(C,29); VA5:=VA3>VA4;
  // LC:=REF(C,1);
  // RSI1:=SMA(MAX(C-LC,0),12,1)/SMA(ABS(C-LC),12,1)*100;
  // RSI2:=SMA(MAX(C-LC,0),56,1)/SMA(ABS(C-LC),56,1)*56;
  // VA6:=RSI1>RSI2 AND VA5 AND VA2;
  // VASS1:=HHV(H,30); VASS2:=LLV(L,30); VASS3:=REF((VASS1/VASS2-1)*100)<=30;
  // AK1:=ABS((3.48*C+H+L)/4-EMA(C,23))/EMA(C,23);
  // AK2:=DMA((2.15*C+L+H)/4, AK1);  → 用EMA近似DMA
  // 游资:=EMA(AK2,200)*1.1;
  // 妖股:=CROSS(C,游资) AND REF(C)*1.097<C AND VA6 AND VASS3;
  const va2 = V.map((v, i) => v > ma(V, 89)[i])
  const ema5c = ema(C, 5)
  const ema29c = ema(C, 29)
  const va5 = ema5c.map((v, i) => v > ema29c[i])

  // RSI
  const rsi1Src1: number[] = []
  const rsi1Src2: number[] = []
  const rsi2Src1: number[] = []
  const rsi2Src2: number[] = []
  for (let i = 0; i < len; i++) {
    const lc = ref(C, 1, i)
    const diff = C[i] - lc
    rsi1Src1.push(max(diff, 0))
    rsi1Src2.push(abs(diff))
    rsi2Src1.push(max(diff, 0))
    rsi2Src2.push(abs(diff))
  }
  const rsi1 = sma(rsi1Src1, 12, 1).map((v, i) => {
    const d = sma(rsi1Src2, 12, 1)[i]
    return d !== 0 ? v / d * 100 : 50
  })
  const rsi2 = sma(rsi2Src1, 56, 1).map((v, i) => {
    const d = sma(rsi2Src2, 56, 1)[i]
    return d !== 0 ? v / d * 100 : 50
  })
  const va6 = rsi1.map((v, i) => v > rsi2[i] && va5[i] && va2[i])

  // VASS3: 30日内振幅不超过30%
  const vass3: boolean[] = new Array(len).fill(false)
  for (let i = 0; i < len; i++) {
    const hh30 = hhv(H, 30, i)
    const ll30 = llv(L, 30, i)
    vass3[i] = i >= 1 ? (ref(H, 1, i) / Math.max(ref(L, 1, i), 0.01) - 1) * 100 <= 30
      : (hh30 / Math.max(ll30, 0.01) - 1) * 100 <= 30
  }

  // DMA近似: AK1=偏离度, AK2=以AK1为权重的DMA
  const ema23c = ema(C, 23)
  const ak1: number[] = []
  for (let i = 0; i < len; i++) {
    const weighted = (3.48 * C[i] + H[i] + L[i]) / 4
    ak1.push(ema23c[i] !== 0 ? abs(weighted - ema23c[i]) / ema23c[i] : 0)
  }
  // DMA近似: 用变权EMA，权重=clamp(AK1, 0.01, 1)
  const ak2: number[] = []
  for (let i = 0; i < len; i++) {
    const w = Math.min(Math.max(ak1[i], 0.01), 1)
    const src = (2.15 * C[i] + L[i] + H[i]) / 4
    if (i === 0) {
      ak2.push(src)
    } else {
      ak2.push(w * src + (1 - w) * ak2[i - 1])
    }
  }
  const youzi = ema(ak2, 200).map(v => v * 1.1)

  const monster: number[] = new Array(len).fill(0)
  for (let i = 1; i < len; i++) {
    const crossYouzi = C[i] > youzi[i] && C[i - 1] <= youzi[i - 1]
    const limitUp = C[i - 1] * 1.097 < C[i]
    if (crossYouzi && limitUp && va6[i] && vass3[i]) {
      monster[i] = 35
    }
  }

  // ─── 模块11: 妖底确定 画红色 ───
  // C1:=((MA(C,30)-L)/MA(C,60))*200;
  // M2:=SMA(MAX(C-REF(C,1),0),7,1)/SMA(ABS(C-REF(C,1)),7,1)*100;
  // G1:=FILTER(REF(M2)<20 AND M2>REF(M2),5);
  // TU:=C/MA(C,40)<0.74;
  // SMMA:=EMA(EMA(C,5),5);
  // IM:=EMA(C,5)-REF(EMA(C,5));
  // TSMMA:=SMMA-REF(SMMA);
  // DIVMA:=ABS(EMA(C,5)-SMMA);
  // TDJ:=(H-L)/REF(C)>0.05;
  // ET:=(IM+TSMMA)/2;
  // TDF:=POW(DIVMA,1)*POW(ET,3);
  // NTDF:=TDF/5/HHV(ABS(TDF),3);
  // YUL:=COUNT(TDJ,5)>1;
  // 启动:=TU AND TDJ AND YUL;
  // 确定:=CROSS(NTDF,-0.9);
  // 波段:=(G1 AND C1>20 OR C>REF(C)) AND REF(启动,1)的10日过滤;
  // 选股:=REF(启动) AND (确定 OR C>REF(C)) AND MACD>-1.5 的10日过滤;
  // 妖底确定:=(COUNT(选股,13)>=1 AND 波段)*10;
  const ma30c = ma(C, 30)
  const ma60c = ma(C, 60)
  const c1 = ma60c.map((v, i) => v !== 0 ? ((ma30c[i] - L[i]) / v) * 200 : 0)

  const m2Src1: number[] = []
  const m2Src2: number[] = []
  for (let i = 0; i < len; i++) {
    const diff = C[i] - ref(C, 1, i)
    m2Src1.push(max(diff, 0))
    m2Src2.push(abs(diff))
  }
  const m2 = sma(m2Src1, 7, 1).map((v, i) => {
    const d = sma(m2Src2, 7, 1)[i]
    return d !== 0 ? v / d * 100 : 50
  })

  // G1: REF(M2)<20 AND M2>REF(M2) 的5日过滤
  const g1Cond: boolean[] = m2.map((v, i) => ref(m2, 1, i) < 20 && v > ref(m2, 1, i))
  const g1 = filterSignal(g1Cond, 5)

  const ma40c = ma(C, 40)
  const tu = C.map((v, i) => ma40c[i] !== 0 ? v / ma40c[i] < 0.74 : false)

  // TDJ: (H-L)/REF(C)>0.05
  const tdj: boolean[] = H.map((v, i) => {
    const rc = ref(C, 1, i)
    return rc !== 0 ? (v - L[i]) / rc > 0.05 : false
  })

  // YUL: COUNT(TDJ,5)>1
  const yul: boolean[] = tdj.map((_, i) => count(tdj, 5, i) > 1)

  // 启动: TU AND TDJ AND YUL
  const qidong: boolean[] = tu.map((v, i) => v && tdj[i] && yul[i])

  // NTDF计算
  const ema5cArr = ema(C, 5)
  const smma = ema(ema5cArr, 5)
  const im = ema5cArr.map((v, i) => v - ref(ema5cArr, 1, i))
  const tsmma = smma.map((v, i) => v - ref(smma, 1, i))
  const divma = ema5cArr.map((v, i) => abs(v - smma[i]))
  const et = im.map((v, i) => (v + tsmma[i]) / 2)
  const tdf = divma.map((v, i) => Math.pow(v, 1) * Math.pow(et[i], 3))
  const ntdf = tdf.map((v, i) => {
    const hhvAbsTdf = hhv(tdf.map(x => abs(x)), 3, i)
    return hhvAbsTdf !== 0 ? v / 5 / hhvAbsTdf : 0
  })

  // 确定: CROSS(NTDF, -0.9)
  const queding: boolean[] = ntdf.map((_, i) => cross(ntdf, new Array(len).fill(-0.9), i))

  // 波段: (G1 AND C1>20 OR C>REF(C)) AND REF(启动,1) 的10日过滤
  const boduanCond: boolean[] = g1.map((v, i) => {
    const refQidong = i >= 1 ? qidong[i - 1] : qidong[0]
    return (v && c1[i] > 20 || C[i] > ref(C, 1, i)) && refQidong
  })
  const boduan = filterSignal(boduanCond, 10)

  // 选股: REF(启动) AND (确定 OR C>REF(C)) AND MACD>-1.5 的10日过滤
  const xuangCond: boolean[] = qidong.map((_, i) => {
    const refQidong = i >= 1 ? qidong[i - 1] : qidong[0]
    return refQidong && (queding[i] || C[i] > ref(C, 1, i)) && macdResult.macd[i] > -1.5
  })
  const xuang = filterSignal(xuangCond, 10)

  // 妖底确定: (COUNT(选股,13)>=1 AND 波段)*10
  const monsterBottom: number[] = boduan.map((v, i) =>
    (count(xuang, 13, i) >= 1 && v) ? 10 : 0
  )

  // ─── 模块12: 超牛 画白色 ───
  // A:=REF(C,4)>REF(C,3); A1:=REF(C,3)>REF(C,2); A2:=REF(C,2)>REF(C,1);
  // A3:=A AND A1 AND A2;
  // A5:=BARSLAST(C/REF(C)>1.065); A6:=A5>6 AND C>REF(C)*1.03;
  // B:=EMA(MAX(C-REF(C),0),5)/EMA(ABS(C-REF(C)),5)*100;
  // B1:=EMA(MAX(C-REF(C),0),8)/EMA(ABS(C-REF(C)),8)*100;
  // B2:=REF(C,3)>REF(C,2) AND REF(C,2)>REF(C,1) AND REF(C,4)>REF(C,3);
  // B3:=CROSS(B,20) AND CROSS(B,B1);
  // XGG:=A3 AND A6 AND B3;
  // D3:=C/MA(C,24)<1;  (即C<MA24)
  // D4:=CROSS(C,MA(C,24)) AND V/MA(REF(V),5)>1.016;
  // XG1:=D3 AND D4 AND BETWEEN(MA(C,24),L,C);
  // 超牛:=(XG1 AND XGG)*10;
  const a3: boolean[] = C.map((_, i) =>
    ref(C, 4, i) > ref(C, 3, i) && ref(C, 3, i) > ref(C, 2, i) && ref(C, 2, i) > ref(C, 1, i)
  )

  // A5: BARSLAST(C/REF(C)>1.065)
  const limitUpCond: boolean[] = C.map((v, i) => {
    const rc = ref(C, 1, i)
    return rc !== 0 ? v / rc > 1.065 : false
  })
  const a6: boolean[] = C.map((_, i) => {
    const a5 = barslast(limitUpCond, i)
    return a5 > 6 && C[i] > ref(C, 1, i) * 1.03
  })

  // B, B1
  const bSrc1: number[] = []
  const bSrc2: number[] = []
  for (let i = 0; i < len; i++) {
    const diff = C[i] - ref(C, 1, i)
    bSrc1.push(max(diff, 0))
    bSrc2.push(abs(diff))
  }
  const bArr = ema(bSrc1, 5).map((v, i) => {
    const d = ema(bSrc2, 5)[i]
    return d !== 0 ? v / d * 100 : 50
  })
  const b1Arr = ema(bSrc1, 8).map((v, i) => {
    const d = ema(bSrc2, 8)[i]
    return d !== 0 ? v / d * 100 : 50
  })

  // B3: CROSS(B,20) AND CROSS(B,B1)
  const b3: boolean[] = bArr.map((_, i) =>
    cross(bArr, new Array(len).fill(20), i) && cross(bArr, b1Arr, i)
  )

  // XGG: A3 AND A6 AND B3
  const xgg: boolean[] = a3.map((v, i) => v && a6[i] && b3[i])

  // D3: C<MA(C,24)
  const ma24c = ma(C, 24)
  const d3 = C.map((v, i) => v < ma24c[i])

  // D4: CROSS(C,MA24) AND V/MA(REF(V),5)>1.016
  // 简化: MA(REF(V),5) ≈ MA(V,5)偏移1日
  const maV5 = ma(V, 5)
  const d4: boolean[] = C.map((v, i) => {
    const crossMa24 = v > ma24c[i] && (i >= 1 ? C[i - 1] <= ma24c[i - 1] : false)
    const volRatio = i >= 1 && maV5[i - 1] !== 0 ? V[i] / maV5[i - 1] : 1
    return crossMa24 && volRatio > 1.016
  })

  // XG1: D3 AND D4 AND BETWEEN(MA24, L, C)
  const xg1: boolean[] = d3.map((v, i) =>
    v && d4[i] && between(ma24c[i], L[i], C[i])
  )

  // 超牛: (XG1 AND XGG)*10
  const superBull: number[] = xg1.map((v, i) => (v && xgg[i]) ? 10 : 0)

  // ─── 模块13: 黑马启动 画红色 ───
  // FA1:=ABS((3.48*C+H+L)/4-EMA(C,23))/EMA(C,23);
  // FA2:=DMA((2.15*C+L+H)/4, FA1);
  // 金线王:=EMA(FA2,200)*1.118;
  // 条件:=(C-REF(C))/REF(C)*100>8;
  // 金K线:=CROSS(C,金线王) AND 条件;
  // 黑马启动:=金K线*11;
  const fa2: number[] = []
  for (let i = 0; i < len; i++) {
    const w = Math.min(Math.max(ak1[i], 0.01), 1)
    const src = (2.15 * C[i] + L[i] + H[i]) / 4
    if (i === 0) {
      fa2.push(src)
    } else {
      fa2.push(w * src + (1 - w) * fa2[i - 1])
    }
  }
  const goldLineKing = ema(fa2, 200).map(v => v * 1.118)
  const darkHorse: number[] = new Array(len).fill(0)
  for (let i = 1; i < len; i++) {
    const cond = (C[i] - C[i - 1]) / C[i - 1] * 100 > 8
    if (C[i] > goldLineKing[i] && C[i - 1] <= goldLineKing[i - 1] && cond) {
      darkHorse[i] = 11
    }
  }

  // ─── 组装结果 ───
  return dataList.map((_: any, i: number) => ({
    bo: gup3[i],
    duan: gup4[i],
    trend1: trend1[i],
    trend: trendLine[i],
    capitalIn: Math.min(capitalInArr[i], 120),
    lowBuyQ: lowBuyQ[i],
    highSellQ: highSellQ[i],
    batchBuy: batchBuy[i],
    batchSell: batchSell[i],
    jinniu: jinniuFiltered[i] ? 1 : 0,
    retail: Math.min(retailEma[i], 30),
    shortStrike: shortStrike[i],
    monster: monster[i],
    monsterBottom: monsterBottom[i],
    superBull: superBull[i],
    darkHorse: darkHorse[i],
  }))
}
