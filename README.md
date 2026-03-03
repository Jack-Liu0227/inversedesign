# InverseDesign (Agno + Fewshot + HITL)

一个用于新材料推荐与性能预测的工作流项目：

- `material_recommender_agent`：基于历史数据（CSV 知识库）推荐候选组分。
- `material_predictor_agent`：基于相似样本检索（few-shot）预测性能。
- `material_discovery_workflow`：人在回路（HITL）闭环，支持暂停、人工确认、实验反馈、继续迭代。

## 1. 项目结构

```text
inversedesign/
├─ datasets/                  # 各类合金数据
├─ examples/                  # 旧示例代码
├─ src/
│  ├─ agents/
│  │  ├─ material_recommender_agent.py
│  │  ├─ material_predictor_agent.py
│  │  └─ material_review_agent.py
│  ├─ fewshot/
│  │  ├─ skills/
│  │  │  ├─ dataset_registry.json   # 数据集配置（可配置）
│  │  │  └─ routing_rules.json      # 路由规则（可配置）
│  │  ├─ predictor.py
│  │  └─ dataset_registry.py
│  ├─ workflows/
│  │  └─ material_discovery_workflow.py
│  ├─ model_config/
│  │  ├─ providers.json             # provider 配置
│  │  └─ agent_models.json          # agent -> provider/model 绑定
│  ├─ run_router_debug.py
│  ├─ run_workflow.py
│  └─ app.py
└─ .env
```

## 2. 环境准备

推荐 Python 3.10+。

### 2.1 安装依赖（最小）

```bash
pip install agno python-dotenv pandas scikit-learn pydantic
```

可选（语义检索）：

```bash
pip install sentence-transformers
```

如果你的环境里 `agno` 是本地源码（本仓库内有 `agno/`），可用 editable 安装：

```bash
pip install -e ./agno/libs/agno
```

## 3. 模型与 Provider 配置

### 3.1 Provider 配置

文件：`src/model_config/providers.json`

- 定义 provider 名称与对应的环境变量映射（API key/base_url/default_model）。

### 3.2 Agent 模型绑定

文件：`src/model_config/agent_models.json`

- 按 `log_tag` 指定每个 agent 使用的 provider/model。
- 支持默认配置 `default`。

> `src/common/model_factory.py` 会优先读取这两个 JSON；缺失时回退到 `.env` 旧逻辑。

## 4. 数据集路由（skills 驱动）

### 4.1 数据集清单

文件：`src/fewshot/skills/dataset_registry.json`

- 配置 `material_type -> dataset_path/target_cols/template/default_top_k`。

### 4.2 路由规则

文件：`src/fewshot/skills/routing_rules.json`

- 配置 `aliases` 和 `keywords`。
- 输入 `goal/material_type` 后，系统自动路由到对应材料体系。

## 5. 运行方式

### 5.1 路由调试

```bash
python src/run_router_debug.py --goal "high strength and high ductility titanium alloy" 
python src/run_router_debug.py --material-type steel
python src/run_router_debug.py --list
```

功能：输出命中的 `material_type`、CSV 路径、目标列、模板路径。

### 5.2 运行 HITL 工作流（CLI）

```bash
python src/run_workflow.py
```

流程：

1. Router 选择材料数据集。
2. 推荐候选组分。
3. 预测性能。
4. 人工确认是否继续实验。
5. 输入实验反馈（`measured_values_json`）。
6. 自动判断是否停止迭代。

### 5.3 启动 AgentOS App

```bash
python -m src.app
```

或通过你自己的 uvicorn 入口集成。

## 6. 当前实现说明

- 当前“新材料推荐”是基于历史样本打分后提取候选组分（启发式，不是生成式优化器）。
- 当前“性能预测”是检索增强 few-shot 预测。
- 如果模型不可用，预测路径支持 mock fallback（避免流程完全中断）。

## 7. 常见问题

### 7.1 `ModuleNotFoundError: No module named 'src'`

已在 `run_router_debug.py` 和 `run_workflow.py` 中处理；直接使用：

```bash
python src/run_router_debug.py --goal "..."
```

### 7.2 `ModuleNotFoundError: No module named 'agno...`

说明当前环境未安装 `agno` 包。请先安装：

```bash
pip install agno
```

或本地 editable：

```bash
pip install -e ./agno/libs/agno
```
