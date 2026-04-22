"""
实时盯盘模块 - 多数据源行情 + 信号计算

Tick 数据源（通过 config.TICK_PROVIDER 配置）:
  - mootdx: 通达信协议（主动查询模式，默认）
  - gm: 掘金量化（subscribe 订阅模式）

固定使用 mootdx 的数据:
  - 逐笔成交 (transactions)
  - 分钟线 (minute bars)
  - 指数实时数据 (indices)
"""
