# Material DB UI

面向 `auto-design-alloy` 的 FastAPI 可视化界面，用于浏览与分析项目内 SQLite 数据库（workflow 日志、预测日志、材料数据、工具调用轨迹等）。

## 功能概览

- Dashboard：核心数据统计概览
- Explorer：任意库/表浏览、搜索、分页
- Viewer：面向 workflow 日志的聚合浏览与高级过滤
- Tool Trace：按步骤查看 agent/tool 调用输入输出
- Material Data：材料数据（`material_dataset_rows`）预览与管理
- Recycle Bin：软删除恢复
- Classification：标签与标注状态管理（`ui_classifications.db`）

## 目录结构（UI）

```text
ui/
  app.py                  # FastAPI 入口
  config.py               # 路径和分页等配置
  routers/                # 页面路由 + API 路由
  services/               # 业务逻辑
  db/repositories/        # SQLite 查询与数据访问
  templates/              # Jinja2 页面模板
  static/                 # CSS/JS
  migrations/             # UI 分类相关 SQL
```

## 环境准备

在项目根目录执行：

```bash
pip install -r requirements.txt
```

> [!NOTE]
> 项目根目录存在 `.env`。如果你需要启动主工作流服务（`src.app`）再产生日志数据，请先确保其中模型相关配置可用。

## 启动 UI

在项目根目录执行：

```bash
uvicorn ui.app:app --reload --port 8010
```

打开：`http://127.0.0.1:8010`

健康检查：`GET /healthz`

## CSV 初始化到 DB（重点）

`ui` 的材料数据页面依赖 `db/material_agent_shared.db` 中的 `material_dataset_rows` 表。你可以用以下任一方式初始化。

### 方式 A：命令行脚本初始化（推荐）

在项目根目录执行：

```bash
python -m src.import_csv_to_db
```

或：

```bash
python src/import_csv_to_db.py
```

成功后会输出类似：

```json
{"files": 5, "rows_scanned": 1234, "rows_written": 1234}
```

该脚本会读取 `datasets/` 下在 registry 中注册的 CSV，并写入：

- 数据库：`db/material_agent_shared.db`
- 表：`material_dataset_rows`
- `source` 字段：`csv`

### 方式 B：通过 UI API 初始化

先启动 UI，再调用：

```bash
curl -X POST http://127.0.0.1:8010/api/material-data/import-csv
```

返回示例：

```json
{"ok": true, "files": 5, "rows_scanned": 1234, "rows_written": 1234}
```

### 初始化后验证

- 打开页面：`/material-data`
- 或调用接口：

```bash
curl "http://127.0.0.1:8010/api/material-data/rows?page=1&page_size=20"
```

## 主要页面

- `/`：Dashboard
- `/explorer`：通用数据库浏览器
- `/viewer`：结果查看器（带 trace/session/step/tool 等过滤）
- `/tool-trace`：工具调用链路分析
- `/material-data`：材料数据预览
- `/recycle-bin`：回收站

## 主要 API

- `GET /api/logs/predictions`
- `GET /api/logs/workflow-events`
- `GET /api/lineage/{trace_or_session_id}`
- `GET /api/tool-trace/logs`
- `GET /api/tool-trace/filter-options`
- `GET /api/viewer/filter-options`
- `POST /api/material-data/import-csv`
- `GET /api/material-data/rows`
- `POST /api/material-data/batch-delete`
- `POST /api/material-data/normalize-processing`
- `GET /api/records/recycle-bin`
- `POST /api/records/batch-delete`
- `POST /api/records/restore`
- `GET /api/classifications/tags`
- `POST /api/classifications/tags`
- `GET /api/classifications/annotations`
- `POST /api/classifications/assign`
- `POST /api/classifications/state`

## 数据库文件说明

默认位于项目根目录 `db/`：

- `material_agent_shared.db`：workflow 主数据 + `material_dataset_rows`
- `material_discovery_workflow.db`：兼容保留（部分历史版本）
- `prediction_prompt_logs.db`：预测提示词日志
- `workflow_audit.db`：workflow 审计日志
- `ui_classifications.db`：UI 标签/标注数据

> [!TIP]
> `ui` 启动时会自动执行 `ui/migrations/001_classification.sql`，确保 `ui_classifications.db` 所需表结构存在。
