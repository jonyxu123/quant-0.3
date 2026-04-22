# 多因子量化系统 Web UI 使用指南

## 系统架构

- **前端**: React + TypeScript + TailwindCSS + Vite
- **后端**: FastAPI + DuckDB
- **风格**: 暗黑金融风格界面

## 功能模块

### 1. 监控面板 (Dashboard)
- 系统资源监控 (CPU/内存/磁盘)
- 数据库统计信息
- 数据表排行榜
- 最新数据日期

### 2. 数据同步 (Data Sync)
- 增量同步：自动检测最新日期，补充所有每日类数据
- 全量同步：按接口、日期范围、并发数同步
- 实时同步状态监控
- 数据表状态一览

### 3. 因子计算 (Factors)
- 因子列表查看
- 因子计算任务
- 因子数据查询

### 4. 策略回测 (Strategy)
- 策略选股
- 回测分析
- 持仓组合

## 快速启动

### 前置依赖

- Python 3.10+
- Node.js 18+
- npm 或 yarn

### 安装依赖

#### 1. 安装 Python 依赖

```powershell
cd e:\quant_system\quant-0.3
pip install fastapi uvicorn psutil
```

#### 2. 安装前端依赖

```powershell
cd frontend
npm install
```

### 启动服务

#### 方式一：手动分别启动（推荐开发调试）

**终端1 - 启动 FastAPI 后端**
```powershell
cd e:\quant_system\quant-0.3
python backend/api/main.py
```

后端将运行在: http://localhost:8000  
API 文档: http://localhost:8000/docs

**终端2 - 启动 React 前端**
```powershell
cd e:\quant_system\quant-0.3\frontend
npm run dev
```

前端将运行在: http://localhost:5173

#### 方式二：命令行直接启动

```powershell
# 后端
cd e:\quant_system\quant-0.3
python -m backend.api.main

# 前端（新终端）
cd e:\quant_system\quant-0.3\frontend
npm run dev
```

## 使用说明

### 访问 Web UI

浏览器打开: **http://localhost:5173**

### 功能导航

- **监控** - 系统实时监控面板
- **数据同步** - 执行增量/全量数据同步
- **因子计算** - 计算和查看因子数据
- **策略回测** - 选股策略回测分析

### 数据同步操作

#### 增量同步
1. 点击「数据同步」标签页
2. 在「增量同步」卡片中点击「启动增量同步」
3. 系统自动检测最新日期并补齐数据

#### 全量同步
1. 选择接口（如 `daily`, `daily_basic` 等）
2. 设置日期范围和并发数
3. 点击「启动全量同步」

### API 接口文档

访问 http://localhost:8000/docs 查看完整的 Swagger API 文档

## 目录结构

```
quant-0.3/
├── backend/
│   ├── api/
│   │   ├── main.py              # FastAPI 主应用
│   │   └── routers/
│   │       ├── sync.py          # 数据同步路由
│   │       ├── factors.py       # 因子计算路由
│   │       ├── strategy.py      # 策略回测路由
│   │       └── monitor.py       # 监控面板路由
│   └── data/
│       ├── full_sync.py         # 全量同步
│       └── incremental_sync.py  # 增量同步
├── frontend/
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx    # 监控面板
│   │   │   ├── DataSync.tsx     # 数据同步
│   │   │   ├── Factors.tsx      # 因子计算
│   │   │   └── Strategy.tsx     # 策略回测
│   │   ├── App.tsx              # 主应用
│   │   ├── main.tsx             # 入口文件
│   │   └── index.css            # 样式文件
│   ├── package.json
│   ├── vite.config.ts
│   ├── tailwind.config.js
│   └── index.html
└── WEB_UI_README.md             # 本文档
```

## 开发说明

### 前端开发

```powershell
cd frontend
npm run dev      # 开发服务器
npm run build    # 生产构建
npm run preview  # 预览生产构建
```

### 后端开发

FastAPI 支持热重载，修改代码后自动重启。

API 路由文件:
- `backend/api/routers/sync.py` - 数据同步接口
- `backend/api/routers/factors.py` - 因子计算接口
- `backend/api/routers/strategy.py` - 策略回测接口
- `backend/api/routers/monitor.py` - 监控面板接口

### 添加新接口

1. 在 `backend/api/routers/` 创建新路由文件
2. 在 `backend/api/main.py` 注册路由
3. 在前端对应页面调用 API

## 常见问题

### 端口被占用

**前端端口冲突**
修改 `frontend/vite.config.ts` 中的 `server.port`

**后端端口冲突**
修改 `backend/api/main.py` 中的 `uvicorn.run(port=...)`

### 跨域问题

已在 FastAPI 配置 CORS，允许 `localhost:5173` 和 `localhost:3000`

### 数据库连接

确保 `DB_PATH` 配置正确，默认为 `e:/quant_system/quant-0.3/quant_v8.db`

## 技术栈

### 前端
- React 18
- TypeScript
- TailwindCSS (暗黑金融主题)
- Lucide Icons
- Axios (HTTP 客户端)
- React Router (路由)
- Recharts (图表,待集成)

### 后端
- FastAPI (Web 框架)
- Uvicorn (ASGI 服务器)
- DuckDB (数据库)
- Pydantic (数据验证)
- psutil (系统监控)

## 界面风格

- **主题**: 暗黑金融风格
- **色调**: 深蓝/灰黑主基调
- **强调色**: 亮蓝色 (#3B82F6)
- **字体**: 系统默认无衬线字体
- **图标**: Lucide React Icons

## 下一步扩展

- [ ] 实时数据刷新 (WebSocket)
- [ ] 图表可视化 (Recharts)
- [ ] 因子计算进度条
- [ ] 策略回测报告生成
- [ ] 用户认证与权限管理
- [ ] 数据导出功能
- [ ] 移动端适配

## 联系方式

项目路径: `e:\quant_system\quant-0.3`

---

**启动顺序**: 先启动后端 → 再启动前端 → 浏览器访问 http://localhost:5173
