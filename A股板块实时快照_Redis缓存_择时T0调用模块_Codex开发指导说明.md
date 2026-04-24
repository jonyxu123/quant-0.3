# A股板块实时快照 + Redis缓存 + 择时/T+0调用模块 Codex开发指导说明

> 用途：把本说明直接交给 Codex，让它按要求生成完整工程代码。  
> 目标：实现一个生产可用的「东方财富概念/行业板块实时数据服务」，服务于 A股择时系统、T+0系统、Level2订阅池、前端盯盘面板。  
> 核心原则：**东方财富接口只由采集器统一调用，其他模块全部读 Redis，不直接请求外部接口。**

---

## 1. 项目背景

当前系统需要一个用于 A股盯盘、择时和 T+0 的板块强弱模块。

数据来源采用东方财富 `push2.eastmoney.com` 网页接口：

- 概念板块实时快照
- 行业板块实时快照
- 板块成分股列表
- 板块对应股票池

系统后续还会接入 Level2 实时行情源。Level2 数据包含：

- `Market`：十档盘口行情
- `Tran`：逐笔成交
- `Order`：逐笔委托

注意：当前 Level2 服务不依赖 `Queue` 买卖一档队列，系统不要设计成必须依赖 Queue。

---

## 2. 总体目标

请生成一个可直接集成到现有量化系统中的 Python 模块，完成以下能力：

1. 获取东方财富全部概念板块实时快照。
2. 获取东方财富全部行业板块实时快照。
3. 获取每个概念板块/行业板块的成分股。
4. 对板块实时快照进行字段清洗、类型转换、评分。
5. 将板块快照、强势板块、候选股票池、板块成分股映射写入 Redis。
6. 提供给择时模块、T+0模块、Level2订阅模块直接读取的接口。
7. 不保存 CSV，不落本地文件。
8. 支持限频、重试、异常保护、断网容错、Redis 缓存过期控制。
9. 代码结构清晰，可测试，可扩展。

---

## 3. 技术栈要求

使用：

```text
Python 3.10+
requests
pandas
redis
pydantic 或 dataclasses，可选
pytest，可选
```

安装依赖：

```bash
pip install requests pandas redis pydantic pytest
```

不要使用：

```text
akshare
tushare
baostock
selenium
playwright
本地 CSV 作为主流程存储
```

可以预留扩展，但本版本直接调用东方财富 push2 接口。

---

## 4. 推荐目录结构

请生成如下结构：

```text
quant_board_service/
├── README.md
├── requirements.txt
├── config.py
├── main_board_collector.py
├── services/
│   ├── __init__.py
│   ├── eastmoney_board_service.py
│   ├── redis_board_cache.py
│   └── board_signal_service.py
├── models/
│   ├── __init__.py
│   └── board_models.py
├── examples/
│   ├── example_read_for_timing.py
│   ├── example_read_for_t0.py
│   └── example_level2_subscribe_pool.py
└── tests/
    ├── test_stock_code_normalize.py
    ├── test_board_score.py
    └── test_redis_cache.py
```

如果要简化，也可以只生成核心文件：

```text
config.py
eastmoney_board_service.py
redis_board_cache.py
main_board_collector.py
example_usage.py
```

---

## 5. Redis Key 设计

必须使用以下 Redis Key：

```text
em:boards:snapshot:latest
em:boards:strong:latest
em:boards:candidate_pool:latest
em:boards:members:all

em:boards:meta:last_update
em:boards:meta:status
em:boards:meta:error
```

| Key | 内容 | TTL |
|---|---|---:|
| `em:boards:snapshot:latest` | 全部概念板块 + 行业板块实时快照 | 15秒 |
| `em:boards:strong:latest` | 强势板块筛选结果 | 15秒 |
| `em:boards:candidate_pool:latest` | 由强势板块生成的候选股票池 | 15秒 |
| `em:boards:members:all` | 全部板块成分股映射 | 24小时或不设置 |
| `em:boards:meta:last_update` | 最近采集更新时间戳 | 60秒或不设置 |
| `em:boards:meta:status` | 采集状态：ok/error | 60秒或不设置 |
| `em:boards:meta:error` | 最近错误信息 | 60秒或不设置 |

Redis 中 DataFrame 的存储方式：

- 使用 `DataFrame.to_json(orient="records", force_ascii=False, date_format="iso")`
- 再用 gzip 压缩为 bytes
- 不使用 pickle

原因：

- 避免 pickle 安全问题
- 跨 Python 版本更稳定
- 方便其他语言读取

---

## 6. 东方财富接口说明

### 6.1 通用 URL

```text
https://push2.eastmoney.com/api/qt/clist/get
```

### 6.2 获取概念板块列表/实时快照

```text
fs = m:90 t:3 f:!50
```

### 6.3 获取行业板块列表/实时快照

```text
fs = m:90 t:2 f:!50
```

### 6.4 获取某个板块成分股

```text
fs = b:<板块代码>
```

例如：

```text
fs = b:BK1173
```

---

## 7. 东方财富请求参数

所有请求统一使用：

```python
params = {
    "pn": "1",
    "pz": "500",
    "po": "1",
    "np": "1",
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
    "fltt": "2",
    "invt": "2",
    "fid": "f3",
    "fs": fs,
    "fields": fields,
    "_": str(int(time.time() * 1000)),
}
```

必须实现分页拉取，避免单页不完整：

```text
pn: 当前页
pz: 每页数量，建议 500
total: 东方财富返回的总数
```

循环直到：

```text
len(all_rows) >= total
或 本页 rows 数量 < page_size
或 rows 为空
```

---

## 8. 请求头

必须使用 `requests.Session`，并设置 Headers：

```python
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/center/boardlist.html",
    "Accept": "application/json,text/plain,*/*",
}
```

---

## 9. 限频策略

东方财富 push2 属于网页接口，没有公开稳定的官方频率额度。必须内置限频。

### 9.1 采集频率建议

| 数据 | 建议频率 |
|---|---:|
| 板块实时快照 | 5秒一次 |
| 强势板块计算 | 5秒一次 |
| 候选股票池计算 | 5秒一次 |
| 板块成分股映射 | 每日盘前/启动时刷新一次 |
| 非交易时间 | 停止或 60秒以上 |

### 9.2 单请求限频

类中实现：

```python
min_request_interval = 0.8
```

任意两次外部 HTTP 请求至少间隔 0.8 秒，并加随机抖动：

```python
random.uniform(0.05, 0.2)
```

### 9.3 重试策略

最多重试 3 次：

```text
第1次失败：等待3秒
第2次失败：等待6秒
第3次失败：等待9秒或15秒封顶
```

不得失败后疯狂重试。

---

## 10. 板块字段映射

板块接口 fields：

```text
f2,f3,f4,f5,f6,f8,f12,f14,f20,f104,f105,f128,f136,f140,f141
```

字段映射：

```python
BOARD_FIELD_MAP = {
    "f2": "最新价",
    "f3": "涨跌幅",
    "f4": "涨跌额",
    "f5": "成交量",
    "f6": "成交额",
    "f8": "换手率",
    "f12": "板块代码",
    "f14": "板块名称",
    "f20": "总市值",
    "f104": "上涨家数",
    "f105": "下跌家数",
    "f128": "领涨股票",
    "f140": "领涨股票代码",
    "f141": "领涨股票市场",
    "f136": "领涨股票涨跌幅",
}
```

板块 DataFrame 必须最终保留：

```python
[
    "采集时间",
    "板块类型",
    "板块代码",
    "板块名称",
    "最新价",
    "涨跌幅",
    "涨跌额",
    "成交量",
    "成交额",
    "换手率",
    "总市值",
    "上涨家数",
    "下跌家数",
    "上涨占比",
    "领涨股票",
    "领涨股票代码",
    "领涨股票市场",
    "领涨股票完整代码",
    "领涨股票涨跌幅",
    "板块强度分",
]
```

---

## 11. 成分股字段映射

成分股接口 fields：

```text
f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f62,f115
```

字段映射：

```python
STOCK_FIELD_MAP = {
    "f2": "股票最新价",
    "f3": "股票涨跌幅",
    "f4": "股票涨跌额",
    "f5": "股票成交量",
    "f6": "股票成交额",
    "f7": "股票振幅",
    "f8": "股票换手率",
    "f9": "股票市盈率动态",
    "f10": "股票量比",
    "f12": "股票代码",
    "f13": "市场标识",
    "f14": "股票名称",
    "f15": "股票最高价",
    "f16": "股票最低价",
    "f17": "股票开盘价",
    "f18": "股票昨收价",
    "f20": "股票总市值",
    "f21": "股票流通市值",
    "f23": "股票市净率",
    "f62": "股票主力净流入",
    "f115": "股票市盈率TTM",
}
```

成分股 DataFrame 必须最终保留：

```python
[
    "采集时间",
    "板块类型",
    "板块代码",
    "板块名称",
    "股票代码",
    "市场标识",
    "股票完整代码",
    "股票名称",
    "股票最新价",
    "股票涨跌幅",
    "股票涨跌额",
    "股票成交量",
    "股票成交额",
    "股票振幅",
    "股票换手率",
    "股票量比",
    "股票开盘价",
    "股票最高价",
    "股票最低价",
    "股票昨收价",
    "股票总市值",
    "股票流通市值",
    "股票市盈率动态",
    "股票市盈率TTM",
    "股票市净率",
    "股票主力净流入",
]
```

---

## 12. 股票代码规范化

必须实现：

```python
normalize_stock_code(code: str) -> str
```

规则：

```text
000001 -> 000001.SZ
002176 -> 002176.SZ
300750 -> 300750.SZ
600000 -> 600000.SH
688633 -> 688633.SH
430000 -> 430000.BJ
830000 -> 830000.BJ
```

如果东方财富返回：

```text
f13 = 0 -> SZ
f13 = 1 -> SH
f13 = 2 -> BJ
```

使用 f13 优先判断。

领涨股票也要生成：

```text
领涨股票完整代码
```

---

## 13. 板块强度评分

必须实现：

```python
score_boards(df: pd.DataFrame) -> pd.DataFrame
```

评分逻辑：在概念板块和行业板块内部各自排名，避免行业板块数量少被概念板块淹没。

字段：

```text
涨跌幅
成交额
上涨占比
领涨股票涨跌幅
```

计算分位数：

```python
df["涨跌幅分位"] = df.groupby("板块类型")["涨跌幅"].rank(pct=True)
df["成交额分位"] = df.groupby("板块类型")["成交额"].rank(pct=True)
df["上涨占比分位"] = df.groupby("板块类型")["上涨占比"].rank(pct=True)
df["领涨股分位"] = df.groupby("板块类型")["领涨股票涨跌幅"].rank(pct=True)
```

综合分：

```python
df["板块强度分"] = (
    df["涨跌幅分位"] * 45
    + df["成交额分位"] * 25
    + df["上涨占比分位"] * 20
    + df["领涨股分位"] * 10
).round(2)
```

上涨占比：

```python
上涨占比 = 上涨家数 / (上涨家数 + 下跌家数)
```

注意分母为 0 时要处理为 NaN 或 0，不能报错。

---

## 14. 强势板块筛选

必须实现：

```python
get_strong_boards(
    min_pct: float = 1.0,
    min_up_ratio: float = 0.55,
    min_amount: float = 500_000_000,
    min_score: float = 65,
    top_n_each_type: int | None = 15,
) -> pd.DataFrame
```

筛选条件：

```text
涨跌幅 >= min_pct
上涨占比 >= min_up_ratio
成交额 >= min_amount
板块强度分 >= min_score
```

如果 `top_n_each_type` 不为空，则每类板块分别取前 N：

```text
概念板块 Top N
行业板块 Top N
```

---

## 15. 候选股票池生成

必须实现：

```python
build_candidate_pool_from_strong_boards(
    strong_boards: pd.DataFrame,
    max_stocks: int = 100,
) -> pd.DataFrame
```

逻辑：

1. 从强势板块中取板块代码。
2. 关联成分股缓存。
3. 一个股票可能属于多个强势板块。
4. 对每个股票聚合：
   - 所属强势板块数量
   - 最高板块强度分
   - 平均板块强度分
   - 最高板块涨跌幅
   - 最强板块名称
   - 股票涨跌幅
   - 股票成交额
   - 股票换手率
   - 股票量比
5. 计算候选股票评分。

候选股票评分：

```python
候选股票评分 = (
    最高板块强度分 * 0.60
    + 平均板块强度分 * 0.25
    + min(所属强势板块数量, 5) * 3
    + clip(股票涨跌幅, -5, 10) * 1.2
)
```

最后按：

```text
候选股票评分 DESC
最高板块强度分 DESC
股票成交额 DESC
```

排序，取前 `max_stocks`。

候选股票池用于 Level2 订阅池，所以默认 `max_stocks=100`。

候选池 DataFrame 至少包含：

```python
[
    "股票完整代码",
    "股票名称",
    "股票涨跌幅",
    "股票成交额",
    "股票换手率",
    "股票量比",
    "所属强势板块数量",
    "最高板块强度分",
    "平均板块强度分",
    "最高板块涨跌幅",
    "最强板块类型",
    "最强板块代码",
    "最强板块名称",
    "候选股票评分",
]
```

---

## 16. 持仓股板块暴露

必须实现：

```python
get_stock_board_exposure(
    stock_codes: Iterable[str],
    board_snapshot: pd.DataFrame | None = None,
) -> pd.DataFrame
```

用途：

- T+0 模块判断持仓股是否处于强势题材/强势行业。
- 判断持仓股应该正 T、倒 T、观望、减仓。
- 判断持仓是否属于多个强势板块。

返回内容：

```text
股票完整代码
股票名称
板块类型
板块代码
板块名称
板块涨跌幅
板块成交额
板块上涨占比
板块强度分
```

排序：

```text
股票完整代码 ASC
板块强度分 DESC
板块涨跌幅 DESC
```

---

## 17. T+0上下文构建

必须实现：

```python
build_t0_context(
    holdings: Iterable[str] | None = None,
    watchlist: Iterable[str] | None = None,
    top_n_boards_each_type: int = 15,
    max_candidate_stocks: int = 100,
) -> dict[str, pd.DataFrame]
```

返回：

```python
{
    "board_snapshot": board_snapshot,
    "strong_boards": strong_boards,
    "candidate_pool": candidate_pool,
    "holding_exposure": holding_exposure,
    "watchlist_exposure": watchlist_exposure,
}
```

---

## 18. 核心类设计

### 18.1 EastMoneyBoardService

文件：

```text
services/eastmoney_board_service.py
```

必须包含方法：

```python
class EastMoneyBoardService:
    def fetch_boards(self, board_type: Literal["concept", "industry"]) -> pd.DataFrame: ...
    def fetch_all_boards_snapshot(self) -> pd.DataFrame: ...
    def get_board_snapshot(self, force: bool = False) -> pd.DataFrame: ...
    def score_boards(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def get_strong_boards(...) -> pd.DataFrame: ...
    def fetch_board_members(self, board_code: str, board_name: str, board_type_name: str) -> pd.DataFrame: ...
    def refresh_members_cache(...) -> pd.DataFrame: ...
    def get_members_for_boards(...) -> pd.DataFrame: ...
    def build_candidate_pool_from_strong_boards(...) -> pd.DataFrame: ...
    def get_stock_board_exposure(...) -> pd.DataFrame: ...
    def build_t0_context(...) -> dict[str, pd.DataFrame]: ...
```

内部维护两个内存缓存：

```python
self.board_snapshot_cache
self.board_snapshot_ts
self.board_members_cache
self.board_members_ts
```

### 18.2 RedisBoardCache

文件：

```text
services/redis_board_cache.py
```

必须包含方法：

```python
class RedisBoardCache:
    def set_df(self, key: str, df: pd.DataFrame, ttl: int | None = None): ...
    def get_df(self, key: str) -> pd.DataFrame: ...
    def set_board_snapshot(self, df: pd.DataFrame, ttl: int = 15): ...
    def get_board_snapshot(self) -> pd.DataFrame: ...
    def set_strong_boards(self, df: pd.DataFrame, ttl: int = 15): ...
    def get_strong_boards(self) -> pd.DataFrame: ...
    def set_candidate_pool(self, df: pd.DataFrame, ttl: int = 15): ...
    def get_candidate_pool(self) -> pd.DataFrame: ...
    def set_board_members(self, df: pd.DataFrame, ttl: int | None = 86400): ...
    def get_board_members(self) -> pd.DataFrame: ...
    def set_error(self, error: str): ...
    def get_status(self) -> dict: ...
```

### 18.3 BoardSignalService

文件：

```text
services/board_signal_service.py
```

作用：给择时/T+0模块提供更业务化的方法。

必须包含：

```python
class BoardSignalService:
    def get_market_style(self) -> dict: ...
    def get_top_strong_boards(self, n: int = 10) -> pd.DataFrame: ...
    def get_level2_subscribe_pool(self, max_stocks: int = 100) -> list[str]: ...
    def get_t0_board_score_for_holdings(self, holdings: list[str]) -> pd.DataFrame: ...
```

说明：

- `get_level2_subscribe_pool()` 从 Redis 的 candidate_pool 读取前 N 个股票代码。
- 该结果用于 L2API 订阅：

```text
DY,000001.SZ,600000.SH,...
```

---

## 19. BoardCollector 主进程

文件：

```text
main_board_collector.py
```

职责：

1. 启动时初始化服务。
2. 启动时刷新一次板块成分股缓存。
3. 写入 Redis `em:boards:members:all`。
4. 每 5 秒：
   - 获取全部板块实时快照
   - 计算强势板块
   - 生成候选股票池
   - 写入 Redis
   - 更新状态 Key

伪代码：

```python
def run_board_collector(interval_seconds: int = 5):
    service = EastMoneyBoardService(...)
    cache = RedisBoardCache(...)

    members_df = service.refresh_members_cache()
    cache.set_board_members(members_df, ttl=86400)

    while True:
        try:
            board_snapshot = service.get_board_snapshot(force=True)
            strong_boards = service.get_strong_boards(...)
            candidate_pool = service.build_candidate_pool_from_strong_boards(...)

            cache.set_board_snapshot(board_snapshot, ttl=15)
            cache.set_strong_boards(strong_boards, ttl=15)
            cache.set_candidate_pool(candidate_pool, ttl=15)

        except Exception as e:
            cache.set_error(str(e))

        sleep(...)
```

必须保证：

- 单轮异常不退出进程。
- 采集失败时 Redis 中上一帧数据可以继续保留到 TTL。
- 日志中打印本轮数量：

```text
板块总数
强势板块数
候选股票数
耗时
```

---

## 20. 配置文件

文件：

```text
config.py
```

包含：

```python
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

BOARD_SNAPSHOT_TTL = 15
STRONG_BOARDS_TTL = 15
CANDIDATE_POOL_TTL = 15
BOARD_MEMBERS_TTL = 86400

BOARD_COLLECT_INTERVAL_SECONDS = 5
EASTMONEY_MIN_REQUEST_INTERVAL = 0.8
EASTMONEY_TIMEOUT = 10
EASTMONEY_MAX_RETRIES = 3

STRONG_BOARD_MIN_PCT = 1.0
STRONG_BOARD_MIN_UP_RATIO = 0.55
STRONG_BOARD_MIN_AMOUNT = 500_000_000
STRONG_BOARD_MIN_SCORE = 65
TOP_N_BOARDS_EACH_TYPE = 15
MAX_CANDIDATE_STOCKS = 100
```

---

## 21. 示例脚本

### 21.1 择时模块读取示例

文件：

```text
examples/example_read_for_timing.py
```

功能：

```python
cache = RedisBoardCache(...)
strong_boards = cache.get_strong_boards()
candidate_pool = cache.get_candidate_pool()

print(strong_boards.head())
print(candidate_pool.head())
```

### 21.2 T+0模块读取示例

文件：

```text
examples/example_read_for_t0.py
```

功能：

```python
holdings = ["002176.SZ", "603906.SH", "000001.SZ"]

members = cache.get_board_members()
board_snapshot = cache.get_board_snapshot()

holding_exposure = members[members["股票完整代码"].isin(holdings)].merge(
    board_snapshot[["板块类型", "板块代码", "板块名称", "板块强度分", "涨跌幅", "成交额", "上涨占比"]],
    on=["板块类型", "板块代码", "板块名称"],
    how="left",
)

print(holding_exposure.head())
```

### 21.3 Level2订阅池示例

文件：

```text
examples/example_level2_subscribe_pool.py
```

功能：

```python
candidate_pool = cache.get_candidate_pool()

subscribe_codes = (
    candidate_pool["股票完整代码"]
    .dropna()
    .drop_duplicates()
    .head(100)
    .tolist()
)

print(subscribe_codes)
```

---

## 22. 与 Level2 系统的衔接

Level2 订阅模块不要全市场订阅，只订阅：

```text
1. 持仓股
2. 自选股
3. Redis candidate_pool 前 30/60/100 只
4. 临近涨停/跌停/突破点股票
```

当前候选池默认上限：

```text
100只
```

因为 Level2 数据套餐通常按 30/60/100 只订阅。

Level2 数据使用：

```text
Market：十档盘口、最新价、委买委卖、盘口压力
Tran：逐笔成交、主动买卖、大单成交、扫单/砸盘
Order：逐笔委托、撤单、大单挂撤、盘口欺骗
```

不要依赖：

```text
Queue
```

---

## 24. 异常处理要求

必须处理：

```text
HTTP超时
HTTP 403/429/5xx
JSON解析失败
data为空
diff为空
Redis连接失败
DataFrame字段缺失
数值字段返回 "-"
股票代码缺前导0
```

外部接口失败时：

- 不让主进程崩溃。
- 写 Redis 状态：

```text
em:boards:meta:status = error
em:boards:meta:error = 错误信息
```

- 日志记录异常栈。
- 下轮继续采集。

Redis 写入失败时：

- 日志记录错误。
- 不无限重试。
- 下轮继续。

---

## 25. 测试要求

请生成基础测试。

### 25.1 股票代码规范化测试

```python
assert normalize_stock_code("1") == "000001.SZ"
assert normalize_stock_code("000001") == "000001.SZ"
assert normalize_stock_code("600000") == "600000.SH"
assert normalize_stock_code("688633") == "688633.SH"
assert normalize_stock_code("830000") == "830000.BJ"
```

### 25.2 板块评分测试

构造 DataFrame，确保：

- 输出有 `板块强度分`
- 分数在 0~100 附近
- 强板块分数高于弱板块

### 25.3 Redis序列化测试

构造 DataFrame：

```python
df = pd.DataFrame([{"a": 1, "b": "测试"}])
```

写入 Redis，再读出，内容一致。

---

## 26. 日志要求

使用 Python logging，不要只用 print。

日志格式：

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
```

关键日志：

```text
开始刷新板块成分股
板块成分股刷新完成，行数：xxx
板块快照采集完成，行数：xxx
强势板块数量：xxx
候选股票数量：xxx
Redis写入完成
采集异常
Redis异常
```

---

## 27. 性能要求

成分股映射刷新可能会请求数百个板块，注意：

- 只在启动时或盘前刷新。
- 不要每 5 秒刷新成分股。
- 每次请求间隔至少 1 秒。
- 可以保留 `max_boards` 参数方便测试，例如只刷新前 10 个板块。

板块快照：

- 概念 + 行业两次请求。
- 每 30 秒一次即可。
- 不要每个业务模块单独请求东方财富。

---

## 28. 运行方式

启动 Redis 后运行：

```bash
python main_board_collector.py
```

开发测试时可用：

```bash
python main_board_collector.py --once
python main_board_collector.py --interval 5
python main_board_collector.py --refresh-members-only
```

如果不实现 argparse，也至少在代码里提供：

```python
run_once()
run_loop(interval_seconds=5)
refresh_members_only()
```

---

## 29. 验收标准

完成后必须满足：

1. 能获取全部概念板块。
2. 能获取全部行业板块。
3. 能获取板块成分股。
4. 能计算 `上涨占比`。
5. 能计算 `板块强度分`。
6. 能筛选强势板块。
7. 能生成候选股票池。
8. 能将以下数据写入 Redis：
   - `em:boards:snapshot:latest`
   - `em:boards:strong:latest`
   - `em:boards:candidate_pool:latest`
   - `em:boards:members:all`
9. 能从 Redis 读回 DataFrame。
10. 择时模块示例能读取强势板块。
11. T+0示例能读取持仓股板块暴露。
12. Level2订阅示例能输出前 100 只股票代码。
13. 采集失败不会导致程序退出。
14. 不保存 CSV。
15. 代码可直接运行。

---

## 30. 输出要求

请直接生成完整代码，不要只给伪代码。

优先生成这些文件：

```text
config.py
services/eastmoney_board_service.py
services/redis_board_cache.py
services/board_signal_service.py
main_board_collector.py
examples/example_read_for_timing.py
examples/example_read_for_t0.py
examples/example_level2_subscribe_pool.py
requirements.txt
README.md
```

代码要有中文注释，但不要过度注释。

所有函数都要有基本 docstring。

遇到接口字段缺失时，不能报错退出，要补空列。

---

## 31. 重要注意事项

1. 东方财富 push2 是网页接口，不保证长期稳定，代码要便于替换数据源。
2. 不要高频轰炸接口。
3. 不要把成分股映射当作实时高频接口。
4. 盘中只高频刷新板块快照、强势板块、候选池。
5. 生产系统中，所有业务模块都读 Redis。
6. 不要在择时模块/T+0模块中直接请求东方财富。


---


```


