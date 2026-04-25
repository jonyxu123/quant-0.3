请基于当前仓库最新结构，对 Pool2 T+0 系统做“第一阶段增量改造”。

【重要：这是增量改造，不是重构新系统】
不要推倒重写。
不要新建第二套 T+0 系统。
不要把逻辑从 gm_runtime 再迁回旧版 gm_tick.py。

当前仓库真实结构：
- backend/realtime/signals.py
  - 保留 detect_positive_t / detect_reverse_t 主体
- backend/realtime/gm_runtime/signal_engine.py
  - 当前 Pool2 运行时信号后处理主链
- backend/realtime/gm_runtime/position_state.py
  - 当前 Pool2 inventory / 底仓 / 今日动作状态
- config.py
  - 当前实时策略配置中心

本次改造主落点必须是：
1. backend/realtime/gm_runtime/signal_engine.py
2. backend/realtime/gm_runtime/position_state.py
3. config.py
4. backend/realtime/signals.py（只允许少量 details 补充，不重写主触发逻辑）

────────────────────
一、改造目标
────────────────────

把当前 Pool2 T+0 从：
“多层硬门控证明系统”
改成：
“核心套利评分 + 风险降档 + 动态仓位”

第一阶段重点：
- 保留现有主触发和确认项主体
- 优先改执行层、环境门、指数门、主升浪保护和仓位系数
- 不大改 detect_positive_t / detect_reverse_t 的触发条件
- 不推翻现有 inventory_gate / anti_churn / edge gate

核心原则：
- 硬风险继续一票否决
- 软环境只降档，不直接否决
- 高确定性用标准仓位
- 中等确定性用小仓试T
- 板块、指数、主线只影响仓位和风险，不主导买卖点

────────────────────
二、必须保留
────────────────────

1. Pool2 底仓逻辑
2. positive_t = buy_then_sell_old
3. reverse_t = sell_old_then_buy_back
4. inventory_gate
5. anti_churn
6. fee / slippage / impact / edge 门
7. VWAP / Boll / robust_zscore 主触发
8. 状态机去重、过期、反向互斥
9. panic_selloff、spoofing、wash_trade 等硬风险拦截

注意：
当前仓库里 Pool2 inventory state 已存在于 backend/realtime/gm_runtime/position_state.py，
必须复用，不允许再新造一套独立库存状态。

────────────────────
三、动作档位
────────────────────

新增：
class T0ActionLevel(str, Enum):
    BLOCK = "block"
    OBSERVE = "observe"
    TEST = "test"
    EXECUTE = "execute"
    AGGRESSIVE = "aggressive"

新增档位仓位系数：
ACTION_QTY_MULTIPLIER = {
    "block": 0.00,
    "observe": 0.00,
    "test": 0.10,
    "execute": 0.25,
    "aggressive": 0.40,
}

第一阶段要求：
- AGGRESSIVE 只做 shadow log，不真实执行
- 实盘执行最多到 EXECUTE
- 若 action_level == aggressive 且 enable_aggressive_live=False：
  - 保留 shadow_action_level = aggressive
  - 真实执行档位降为 execute 或直接不下单

────────────────────
四、分数到档位映射
────────────────────

新增函数：
def score_to_action_level(score: float) -> str:
    if score < 55:
        return "block"
    if score < 65:
        return "observe"
    if score < 75:
        return "test"
    if score < 88:
        return "execute"
    return "aggressive"

要求：
- 这是执行层动作映射
- 不替代 signals.py 里的原始 strength
- 原 strength 继续保留兼容前端和历史接口
- 新增 score 用于 action_level 和 final_qty

────────────────────
五、统一降档器
────────────────────

新增：
ACTION_LEVEL_ORDER = {
    "block": 0,
    "observe": 1,
    "test": 2,
    "execute": 3,
    "aggressive": 4,
}

def downgrade_action_level(level: str, steps: int = 1) -> str:
    ...

def cap_action_level(level: str, max_level: str) -> str:
    ...

要求：
- 放在 backend/realtime/gm_runtime/signal_engine.py 或其 helper 中
- 用于 index / session / theme / main_rally 等软门
- 绝不替代硬风险 block

────────────────────
六、硬风险门（继续一票否决）
────────────────────

以下情况继续 hard block：

1. 非交易动作时段 / 午休 / 不允许动作的 session policy
2. 没有 Pool2 底仓
3. 没有可 T 数量
4. 没有现金做正T
5. 今日 T 次数超限
6. 手续费 + 滑点 + 冲击成本后没有净边际
7. 预期回归边际不足
8. spoofing 严重
9. wash_trade + spread_abnormal
10. positive_t 遇到 panic_selloff
11. 跌停磁吸 / 极端 limit magnet
12. 流动性枯竭
13. 风险警示强限制
14. 最终交易数量不足 100 股

要求：
- 新增 hard_block_reasons 列表
- 最终写入 details / debug / log payload
- 硬风险不参与 soft downgrade，直接 block

────────────────────
七、软环境门（由硬否决改成降档）
────────────────────

目标：
把当前过于保守、容易压成 observe_only 的软环境逻辑改成：
降档 + 降仓

如果当前代码里不存在以下同名变量，请映射到现有等价逻辑：
- index_context
- session_policy / market_phase_detail
- main_rally_guard / trend_guard / surge_absorb_guard
- theme_guard / board ecology / concept ecology

规则：

1. require_whitelist_for_execute = False
- 若当前仓库无此变量，不要新增白名单硬门
- 没有白名单不再直接 observe_only
- 只降低 env_score 或 action_level

2. 指数环境：
- risk_off 下 positive_t：降一档，qty_multiplier * 0.7
- risk_on 下 reverse_t：降一档，qty_multiplier * 0.7
- panic_selloff 下 positive_t：继续 hard block

3. 主升浪保护 / 主线保护：
- reverse_t 在普通 active 状态：降一档，qty_multiplier * 0.5 或 0.6
- hard_protect + surge + 无卖出证据：最多 observe
- 若已有 >=3 个出货证据，则允许恢复正常反T仓位

4. open_strict：
- 最多 TEST
- qty_multiplier 再 * 0.7
- 不直接 block，除非已有硬风险

新增统一函数：
def apply_soft_downgrades(ctx, action_level, qty_multiplier):
    ...
    return action_level, qty_multiplier, downgrade_reasons

────────────────────
八、评分体系（第一阶段）
────────────────────

注意：
- 不要重写 detect_positive_t / detect_reverse_t 主体
- 评分应在 signal_engine.py 中，在信号产出后、执行决策前计算
- 原有 strength 保持兼容
- score 是新增执行层概念

1. 正T评分：
positive_t_score =
    position_score * 0.30
  + mean_reversion_score * 0.25
  + orderflow_score * 0.25
  + inventory_score * 0.10
  + env_score * 0.10
  - micro_risk_penalty

2. 反T评分：
reverse_t_score =
    overextension_score * 0.35
  + buy_fading_score * 0.25
  + orderflow_sell_score * 0.20
  + inventory_score * 0.10
  + env_score * 0.10
  - main_rally_penalty

要求：
- 尽量复用 detect_* 现有 details 字段
- 子分数缺失时按中性值处理，不要崩整条信号
- 不改变现有 trigger / confirm 主体
- 第一阶段先做 score 层，不做大规模 trigger 重构

────────────────────
九、inventory_score 与 base_action_qty
────────────────────

必须复用 backend/realtime/gm_runtime/position_state.py 中现有 Pool2 inventory state。

当前已存在的关键字段包括：
- overnight_base_qty
- tradable_t_qty
- reserve_qty
- today_t_count
- today_positive_t_qty
- today_reverse_t_qty
- cash_available_for_t
- inventory_anchor_cost
- today_action_log

要求：
1. inventory_gate 继续保留
2. base_action_qty 必须基于现有可交易库存逻辑推导
3. 正T和反T的 base_action_qty 允许不同：
   - positive_t 更受 cash_available_for_t 约束
   - reverse_t 更受 tradable_t_qty 约束
4. 不要新造第二套 inventory model

────────────────────
十、最终交易数量
────────────────────

新增：
final_qty = floor_to_100(
    base_action_qty
    * ACTION_QTY_MULTIPLIER[action_level]
    * soft_qty_multiplier
)

规则：
- BLOCK / OBSERVE：final_qty = 0
- TEST：小仓试T
- EXECUTE：标准仓位
- AGGRESSIVE：只 shadow log，不实盘
- 若 final_qty < 100：
  - action_level 自动降为 observe
  - final_qty = 0
  - 增加 hard_block_reason = qty_too_small_after_multiplier

────────────────────
十一、日志字段
────────────────────

每条 Pool2 信号新增或补齐以下字段到 details / debug / log payload：

action_level_before_downgrade
action_level_after_downgrade
qty_multiplier
soft_qty_multiplier
hard_block_reasons
downgrade_reasons
score
base_action_qty
final_qty
net_executable_edge_bps
env_score
orderflow_score
micro_risk_score
inventory_score
position_score / mean_reversion_score / overextension_score / buy_fading_score（按信号类型输出）
shadow_action_level
shadow_only

要求：
- 不删除原有 details 字段
- 新增字段优先兼容 signal_history / debug log
- 方便后续比较 TEST vs EXECUTE 的质量差异

────────────────────
十二、插入层级要求
────────────────────

1. backend/realtime/signals.py
- 保留 detect_positive_t / detect_reverse_t 主体
- 只补必要 details 字段
- 不大改 trigger / confirm

2. backend/realtime/gm_runtime/signal_engine.py
- 新增 score 计算
- 新增 action_level
- 新增 hard block / soft downgrade
- 新增 final_qty 计算
- 新增 shadow aggressive
- 新增日志字段
- 这是本次改造核心落点

3. backend/realtime/gm_runtime/position_state.py
- 复用现有 Pool2 inventory state
- 如有必要，补 today_action_log / last_action_level / shadow aggressive 记录
- 不要重写已有底仓结构

4. config.py
- 新增动作档位、软环境降档、shadow aggressive、qty multiplier 配置
- 不要拆出第二个配置系统

────────────────────
十三、第一阶段非目标（不要做）
────────────────────

1. 不要推倒重写 detect_positive_t / detect_reverse_t
2. 不要大改主触发与确认项
3. 不要重构整个 realtime runtime 目录
4. 不要把 T+0 改成题材预测系统
5. 不要删除原有 anti_churn / edge gate / hard veto
6. 不要新造第二套 Pool2 inventory model

────────────────────
十四、测试要求
────────────────────

至少新增：

1. score -> action_level 映射测试
2. downgrade_action_level / cap_action_level 测试
3. final_qty 按 100 股取整测试
4. final_qty < 100 自动降 observe 测试
5. positive_t 在 panic_selloff 下仍 hard block 测试
6. reverse_t 在 main_rally_guard_active 下改为降档而非硬 veto 测试
7. aggressive 只 shadow log、不真实执行测试
8. 复用现有 Pool2 inventory state 的单元测试（不要新状态）