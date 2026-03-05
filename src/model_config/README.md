# 模型切换配置说明

本目录用于管理项目的模型提供商（provider）与模型 ID（model_id）切换逻辑，核心入口为 [`src/common/model_factory.py`](../common/model_factory.py)。

## 目录结构

- `providers.json`: 定义可用 provider、默认 provider、以及各字段从哪些环境变量读取。
- `agent_models.json`: 定义默认绑定和各 agent 的覆盖绑定。
- `.env`（项目根目录）: 存放 API Key、Base URL、默认模型名、全局覆盖模型等运行时配置。

## 生效链路与优先级

`build_model(log_tag)` 的行为分两种路径：

1. `providers.json` 与 `agent_models.json` 都存在且可解析：
- 进入「JSON 驱动模式」（推荐）。
- 先根据 `log_tag` 从 `agent_models.json` 找到 agent 绑定；找不到则走 `default`。
- provider 的解析优先级：
  - `agent_models.json` 中当前绑定的 `provider`
  - 否则 `providers.json.default_provider`
- model_id 的解析优先级：
  - `agent_models.json` 当前绑定中的 `model_id_env` 指向的环境变量（默认 `MODEL_ID`）
  - 否则当前绑定的 `model_id`
  - 否则 provider 的 `default_model`（通常来自 provider 的 `default_model_env`）

2. 任一 JSON 配置缺失：
- 进入「ENV 回退模式」。
- provider 取 `MODEL_PROVIDER`（默认 `ricardo`）。
- model_id 取 `MODEL_ID`，为空则使用当前 provider 的默认模型。

> [!IMPORTANT]
> 只要 `providers.json` 和 `agent_models.json` 存在，`MODEL_PROVIDER` 基本不会再参与 provider 选择。

## `providers.json` 说明

当前文件结构：

```json
{
  "default_provider": "deepseek",
  "providers": {
    "deepseek": {
      "api_key_env": "DEEPSEEK_API_KEY",
      "base_url_env": "DEEPSEEK_BASE_URLS",
      "default_model_env": "DEEPSEEK_MODEL",
      "requires_api_key": true
    }
  }
}
```

字段含义：

- `default_provider`: 当 agent 绑定未显式指定 provider 时使用。
- `providers.<name>.api_key_env`: API Key 的环境变量名。
- `providers.<name>.base_url_env`: Base URL 的环境变量名。
- `providers.<name>.default_model_env`: 默认模型名的环境变量名。
- `providers.<name>.requires_api_key`: 是否强制要求 API Key。

> [!TIP]
> 如果是本地 `ollama`，可设置 `requires_api_key=false`，并在 `.env` 中给 `OLLAMA_API_KEY=ollama`（占位即可）。

## `agent_models.json` 说明

当前文件结构（简化）：

```json
{
  "default": {
    "provider": "deepseek",
    "model_id_env": "MODEL_ID"
  },
  "agents": {
    "material_router/agent": {
      "provider": "",
      "model_id": ""
    }
  }
}
```

字段含义：

- `default`: 所有未命中的 `log_tag` 的默认绑定。
- `agents.<log_tag>`: 某个 agent 的单独绑定。
- `provider`: 留空表示回退到 `providers.json.default_provider`。
- `model_id_env`: 指定从哪个环境变量读取模型 ID（只在该绑定生效）。
- `model_id`: 硬编码模型 ID；仅当 `model_id_env` 读不到值时兜底。

## 与代码中的 `log_tag` 对齐

当前代码中存在以下 `build_model(log_tag)` 调用：

- `material_router/agent`
- `material_predictor/fewshot`
- `material_predictor/agent`
- `material_recommender/agent`
- `material_rationality/agent`

建议在 `agent_models.json.agents` 中覆盖这些 key，避免漏配时全部落到 `default`。

## `.env` 推荐模板

请在项目根目录 `.env` 中使用类似配置（示例值请替换为你自己的）：

```bash
# ===== DeepSeek =====
DEEPSEEK_API_KEY=sk-xxxx
DEEPSEEK_BASE_URLS=https://api.deepseek.com/v1
DEEPSEEK_MODEL=deepseek-chat

# ===== EchoFlow =====
ECHOFLOW_API_KEY=sk-xxxx
ECHOFLOW_BASE_URLS=https://api.echoflow.cn/v1
ECHOFLOW_MODEL=gemini-3-flash-preview

# ===== Ricardo =====
RICARDO_API_KEY=sk-xxxx
RICARDO_BASE_URLS=https://api.ricardochat.cn/v1
RICARDO_API_MODEL=[High]glm4.7

# ===== OpenRouter =====
OPENROUTER_API_KEY=sk-or-v1-xxxx
OPENROUTER_BASE_URLS=https://openrouter.ai/api/v1
OPENROUTER_MODEL=openai/gpt-oss-20b:free

# ===== Ollama =====
OLLAMA_API_KEY=ollama
OLLAMA_BASE_URLS=http://localhost:11434/v1
OLLAMA_MODEL=gpt-oss:20b

# ===== Optional Global Override =====
# 在 JSON 驱动模式下，默认由 agent_models.default.model_id_env=MODEL_ID 读取
# 留空则按 agent 绑定或 provider 默认模型
MODEL_ID=

# 仅在 ENV 回退模式下生效（当 JSON 配置文件缺失时）
MODEL_PROVIDER=deepseek
```

> [!WARNING]
> 当前 `.env` 中 `MODEL_PROVIDER=deeoseek` 拼写有误（应为 `deepseek`）。
> 在 JSON 驱动模式下影响不大，但在 ENV 回退模式下会导致 `Unsupported MODEL_PROVIDER` 报错。

## 常见切换场景

### 场景 1：全项目统一切到某 provider

1. 修改 `providers.json.default_provider`，例如改为 `openrouter`。
2. 保持 `agent_models.json` 中各 agent 的 `provider` 为空或删除覆盖。
3. 在 `.env` 确认目标 provider 的 `*_API_KEY`、`*_BASE_URLS`、`*_MODEL` 都已设置。

### 场景 2：只给某个 agent 单独换模型

在 `agent_models.json` 中添加或修改：

```json
{
  "agents": {
    "material_router/agent": {
      "provider": "deepseek",
      "model_id": "deepseek-chat"
    }
  }
}
```

如果希望用环境变量驱动该 agent：

```json
{
  "agents": {
    "material_router/agent": {
      "provider": "deepseek",
      "model_id_env": "ROUTER_MODEL_ID"
    }
  }
}
```

并在 `.env` 增加：

```bash
ROUTER_MODEL_ID=deepseek-chat
```

### 场景 3：临时全局覆盖模型 ID

保持 `agent_models.default.model_id_env=MODEL_ID`，直接在 `.env` 设置：

```bash
MODEL_ID=deepseek-chat
```

所有未单独设置 `model_id/model_id_env` 的 agent 将使用该值。

## 运行时校验与报错对照

`build_model()` 启动时会进行严格校验，常见错误：

- `Unsupported provider 'xxx'`:
  - `agent_models.json` 中 `provider` 不在 `providers.json.providers` 列表。
- `Unsupported MODEL_PROVIDER='xxx'`:
  - ENV 回退模式下 `MODEL_PROVIDER` 拼写错误或未支持。
- `Missing API key for provider 'xxx'`:
  - 该 provider `requires_api_key=true`，但 API Key 为空。
- `Missing base URL for provider 'xxx'`:
  - 对应 `*_BASE_URLS` 未配置。
- `Missing model id for provider 'xxx'`:
  - `MODEL_ID`、绑定 `model_id`、provider 默认模型都为空。

## 快速自检清单

- `providers.json.providers` 是否包含目标 provider。
- `agent_models.json` 的 `log_tag` 是否与代码调用一致。
- `.env` 中 `*_API_KEY`、`*_BASE_URLS`、`*_MODEL` 是否齐全。
- 是否误把生产 key 提交到仓库（建议配合 `.gitignore` 与密钥轮换）。
- 启动日志是否打印出预期的 `[log_tag] provider=... model=...`。

## 最小实践建议

- 长期推荐使用「JSON 驱动模式」管理模型路由，避免把策略散落在 `.env`。
- 给关键 agent 显式配置 `provider` 与 `model_id_env`，减少隐式回退带来的不确定性。
- 把 `MODEL_ID` 仅作为临时调试开关，稳定后固化到 `agent_models.json`。
