# DataFlow Jobs

一个使用 Python 构建的 CSV 异步处理平台。上传文件、配置处理步骤后，任务由独立 Worker 在后台执行；通过中文网页或 REST API 查看状态、追踪执行事件并下载结果。

适用于 CSV 批量合并、订单去重、按地区汇总金额等场景，也可用于学习任务调度、故障恢复和结果一致性的实现。依赖由 **uv** 管理，支持本机启动与 Docker Compose 部署。

## 功能概览

| 能力 | 已实现内容 |
| --- | --- |
| 数据处理 | 多文件有序合并、指定字段去重、基于 Decimal 的分组求和 |
| 任务管理 | 异步提交、分页查询、执行事件、取消、自动重试、失败任务手动重试 |
| 执行可靠性 | 幂等提交、事务 outbox、原子领取、租约与心跳、过期任务恢复、结果发布保护 |
| 文件管理 | 上传大小与摘要记录、独立结果文件、输入压缩归档、归档复用、孤儿文件清理 |
| 运行监控 | 页面运行概况、组件心跳、队列与耗时指标、Prometheus、Alertmanager |
| 交互方式 | 中文响应式网页、REST API、OpenAPI 交互文档、命令行演示脚本 |

技术栈：Python · FastAPI · Pydantic · SQLAlchemy · Alembic · Celery · Redis · PostgreSQL。网页使用 Jinja2 和原生 JavaScript，无需单独构建前端。

## 快速开始

### 本机运行

准备好 Git 和 uv。项目支持 Python 3.12+，默认通过 `.python-version` 使用 Python 3.13，uv 可自动下载对应解释器。

```bash
git clone https://github.com/duskto/dataflow-jobs.git
cd dataflow-jobs
uv sync --frozen
cp .env.example .env
uv run python scripts/dev.py
```

启动脚本自动执行数据库迁移，并启动 API、调度进程、维护进程及两个独立 Worker。默认使用 SQLite，不需要 Redis 或 Docker；按 `Ctrl+C` 结束这组进程。

| 入口 | 地址 |
| --- | --- |
| 网页工作台 | http://127.0.0.1:8000 |
| API 文档 | http://127.0.0.1:8000/docs |
| 就绪检查 | http://127.0.0.1:8000/api/v1/health/ready |

需要更换端口或调整并发时：

```bash
uv run python scripts/dev.py --port 8001 --workers 4
```

### 完成第一个任务

在工作台上传 `samples/january.csv` 和 `samples/february.csv`，保留默认的“合并 → 按 order_id 去重 → 按 region 汇总 amount”流程，提交后等待成功并下载结果。

也可以另开终端运行完整演示：

```bash
uv run python scripts/demo.py
# 服务使用其他端口时
uv run python scripts/demo.py --url http://127.0.0.1:8001
```

脚本会验证上传、幂等提交、后台执行和下载，并将结果与 `samples/expected.csv` 对照。若服务启用了令牌，运行脚本时需在环境变量中提供相同的 `DF_API_TOKEN`；该脚本不会自行读取 `.env`。

## Docker Compose 部署

在仓库根目录执行，需要 Docker Engine 和 Compose v2。默认端口与本机启动相同，请先停止占用 8000 端口的服务。

```bash
docker compose up --build -d
docker compose ps
```

Compose 使用 **PostgreSQL + Redis + Celery**，并启动 API、dispatcher、maintenance 和一次性 migrate 服务。数据库就绪后先执行迁移，应用服务等待迁移成功再启动。每个 Worker 容器默认包含两个计算子进程。

```bash
# 使用其他 API 端口
DF_API_PORT=8001 docker compose up -d

# 扩展至两个 Worker 容器，共四个计算子进程
docker compose up -d --scale worker=2

# 查看执行与派发日志
docker compose logs -f worker dispatcher

# 停止服务并保留数据卷
docker compose down
```

自定义的 Compose 变量建议保存在 `.env` 中，使后续命令使用同一组配置。API 默认仅绑定 `127.0.0.1`，PostgreSQL 和 Redis 不发布主机端口。数据库账号为本地演示配置；对外部署时需配置数据库凭据、`DF_API_TOKEN`、反向代理和 HTTPS。

### 监控与告警

```bash
docker compose --profile monitoring up -d
```

- Prometheus：http://127.0.0.1:9090
- Alertmanager：http://127.0.0.1:9093

监控覆盖组件离线、Redis 不可用、任务排队超时、租约过期、失败率和维护异常。exporter 仅在容器网络内提供指标；默认 Alertmanager 只记录告警，不发送外部通知。未启用监控 profile 时，工作台仍可显示运行概况和当前异常。

## API 使用

API 前缀为 `/api/v1`。配置 `DF_API_TOKEN` 后，业务接口需携带 `Authorization: Bearer <令牌>`；健康检查公开。完整请求与响应结构可在运行中的 `/docs` 查看。

先上传文件：

```bash
curl -F 'file=@samples/january.csv' http://127.0.0.1:8000/api/v1/files
curl -F 'file=@samples/february.csv' http://127.0.0.1:8000/api/v1/files
```

将响应中的 `file_id` 填入任务请求。以下示例用于未配置令牌的默认环境：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: sales-demo-001' \
  -d '{
    "input_file_ids": ["替换为第一个文件的UUID", "替换为第二个文件的UUID"],
    "steps": [
      {"type": "merge", "schema_policy": "strict"},
      {"type": "deduplicate", "keys": ["order_id"], "keep": "first"},
      {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]}
    ],
    "csv_options": {"encoding": "utf-8-sig", "delimiter": ","},
    "max_attempts": 3
  }'
```

创建成功返回 `202` 和任务 `id`。使用该 ID 查询状态，成功后下载结果；结果尚未就绪时下载接口返回 `409`。同一幂等键和相同请求返回同一个任务，同一键配合不同请求返回 `409`。

下表路径均省略 `/api/v1` 前缀：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` / `GET` | `/files` / `/files/{id}` | 上传文件 / 查询文件元信息 |
| `POST` / `GET` | `/jobs` | 创建任务 / 按状态分页查询任务 |
| `GET` | `/jobs/{id}` | 状态、阶段、处理行数、错误和执行尝试 |
| `POST` | `/jobs/{id}/cancel` | 取消任务或提交取消请求 |
| `POST` | `/jobs/{id}/retry` | 为失败任务创建关联的新任务 |
| `GET` | `/jobs/{id}/events` | 按游标查询执行事件 |
| `GET` | `/jobs/{id}/result` | 下载成功结果 |
| `GET` | `/jobs/{id}/events/archive` | 下载已归档的完整事件日志 |
| `GET` | `/health/live` / `/health/ready` | 进程存活 / 依赖就绪 |
| `GET` | `/ops` / `/metrics` | 运行概况 / Prometheus 指标 |

错误响应包含 `code`、`message` 和 `request_id`，响应头包含 `X-Request-ID`，便于排查问题。

## 处理规则与任务语义

**CSV 规则**：每个任务接收 1–20 个有序输入和 1–8 个处理步骤。多个文件必须先合并，且表头名称与顺序必须一致。支持 UTF-8、UTF-8 BOM、GB18030，以及逗号、分号、Tab 和竖线分隔符；输出统一为 UTF-8 BOM、逗号分隔的 CSV。

**去重与汇总**：去重按指定字段组合保留首次出现的记录，不自动去除字段前后空格。汇总使用 Decimal，分组按首次出现的顺序输出；空值、NaN、Infinity 和非法数值会导致任务失败。无表头、空列名、重复列名或字段数量不一致也会明确报错。

**状态与重试**：任务经历 `QUEUED`、`RUNNING`，最终进入 `SUCCEEDED`、`FAILED` 或 `CANCELLED`；临时故障可进入 `RETRY_WAIT`，退避后重新排队。`max_attempts` 包括首次执行，默认 3 次，最多 5 次。格式与参数错误不自动重试；手动重试仅适用于失败任务，并创建一个关联原任务的新任务。

**进度与取消**：页面展示当前阶段及该阶段已处理行数，阶段切换或重试时行数可从零开始。总行数未知时不估算百分比。排队任务可立即取消，运行中的任务由 Worker 在检查点响应取消请求；成功与取消以数据库中先提交的有效状态转换为准。

## 可靠性设计

API 接收请求并记录任务，CSV 计算由独立 Worker 执行。业务状态以数据库为准，不依赖 Celery 结果后端。

- **提交与派发**：任务与 outbox 记录在同一事务中创建；dispatcher 负责派发，允许消息重复投递。
- **领取与恢复**：Worker 通过条件更新原子领取任务；每次执行拥有独立 attempt 和租约，心跳续租，过期后由恢复逻辑安排重试。
- **结果发布**：先将结果完整写入独立文件，再提交数据库引用；只有当前有效 attempt 能发布结果，旧 Worker 无权覆盖新执行结果。
- **归档与清理**：输入默认闲置 30 天后压缩，终态任务的事件默认 30 天后归档。归档输入可再次提交任务；维护进程默认每小时执行归档和孤儿清理，成功结果与有效归档持续保留。

该设计允许故障后的重复计算，并通过状态和租约约束结果发布。事务边界与状态机详见 [架构说明](docs/architecture.md)，归档并发保护与告警语义详见 [运维说明](docs/operations.md)。

## 配置与数据存储

本机应用从 `DF_` 环境变量及 `.env` 读取配置，环境变量优先；修改后需重启进程。Compose 使用 `compose.yaml` 中声明的容器环境变量与插值配置：数据库、Redis 和存储地址已为容器网络设定，新增参数需在对应服务的 `environment` 中传入。

| 常用配置 | 默认值 | 说明 |
| --- | --- | --- |
| `DF_DATABASE_URL` | `sqlite:///./data/dataflow.db` | 本机数据库连接 |
| `DF_STORAGE_ROOT` | `./data/storage` | 上传、结果及归档存储位置 |
| `DF_EXECUTION_MODE` | `local` | 本机轮询或 `celery` 模式 |
| `DF_REDIS_URL` | `redis://localhost:6379/0` | Celery 消息代理 |
| `DF_API_TOKEN` | 空 | 可选的共享账号访问令牌 |
| `DF_MAX_UPLOAD_BYTES` | `104857600` | 单文件上限，100 MiB |
| `DF_MAX_ROWS` | `1000000` | 单任务所有输入的累计数据行数上限 |
| `DF_MAX_UNIQUE_KEYS` / `DF_MAX_GROUPS` | `200000` / `100000` | 去重键数 / 汇总分组数上限 |
| `DF_LEASE_SECONDS` | `30` | 执行租约秒数 |
| `DF_INPUT_ARCHIVE_DAYS` / `DF_EVENT_ARCHIVE_DAYS` | `30` / `30` | 输入闲置 / 终态事件归档天数 |
| `DF_MAINTENANCE_INTERVAL_SECONDS` | `3600` | 自动维护间隔秒数 |

配置示例见 [.env.example](.env.example)，全部参数及校验范围见 [Settings](app/core/config.py)。Compose 另支持 `DF_API_PORT`、`DF_WORKER_CONCURRENCY`、`DF_PROMETHEUS_PORT` 和 `DF_ALERTMANAGER_PORT`。

本机数据库与运行文件位于 `data/`。Compose 使用 `job-data` 存储文件、`postgres-data` 存储数据库、`redis-data` 存储 Redis AOF；API 与 Worker 共享同一文件卷。虚拟环境、私密配置、运行数据和日志均已加入 `.gitignore`。

需要手动清理孤儿文件时，先预览再执行：

```bash
uv run python -m scripts.cleanup
uv run python -m scripts.cleanup --apply
# 对 Compose 的数据库与存储执行清理
docker compose exec api python -m scripts.cleanup --apply
```

## 开发与验证

在已完成 `uv sync --frozen` 的环境中运行：

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q --cov=app --cov-report=term-missing
uv run alembic upgrade head
uv run alembic check
```

使用 PostgreSQL 运行同一套测试时，连接账号需具备 `CREATE SCHEMA` 权限；每例测试使用独立临时 schema，并在结束后清理：

```bash
DF_TEST_DATABASE_URL='postgresql+psycopg://user:password@localhost/testdb' uv run pytest -q
```

浏览器测试需要先启动应用，并安装独立依赖分组和 Chromium：

```bash
uv sync --frozen --group browser
uv run --group browser playwright install --with-deps chromium
uv run --group browser python scripts/browser_smoke.py
```

已有验收记录：SQLite 与 PostgreSQL 各 **57 项测试通过**；真实 Celery 负载实验完成 **1670 个任务，0 失败**；Redis 停机、Worker 强制退出、PostgreSQL 提交中断及告警触发与恢复均经过实际验证。完整环境、数据和测试边界见 [验收报告](docs/verification.md)。其中持续负载阶段为 5 分钟，不代表数天级耐久测试或生产容量保证。

### 目录结构

```text
app/
  api/             HTTP 接口
  core/            配置、数据库连接与统一错误
  models/          数据模型
  schemas/         请求校验与处理步骤定义
  processors/      CSV 处理逻辑
  services/        任务状态、幂等与归档服务
  workers/         本机和 Celery 执行入口
  scheduler/       派发、恢复与定期维护
  observability/   心跳、指标与 exporter
  storage/         文件存储与归档读写
  templates/       页面模板
  static/          JavaScript 与样式
migrations/        Alembic 数据库迁移
monitoring/        Prometheus 与 Alertmanager 配置
scripts/           启动、演示、基准与故障验证工具
tests/             单元、集成、可靠性与运维测试
samples/           示例输入与预期结果
docs/              架构、运维说明及验收记录
```

## 适用范围与限制

- 当前采用单演示账号与可选共享令牌，尚无注册、角色管理或多租户配额；处理器仅支持已定义的 CSV 步骤，不执行用户代码，不支持 Excel 或通用 DAG。
- SQLite 适合本机体验；更高写入并发使用 PostgreSQL。当前文件存储依赖单主机共享卷，跨主机部署需接入所有 Worker 可访问的存储。
- 去重和分组需要维护内存状态，并受唯一键数与分组数上限限制，尚未实现磁盘溢写；文件大小和总行数限制同时生效。
- 归档用于压缩和减少在线事件数据，成功结果与有效归档不会自动销毁；长期运行需规划存储容量与备份。
- 页面采用轮询，当前仅展示前 100 条在线事件；API 支持游标查询，已归档事件可完整下载。进程心跳与就绪检查不覆盖所有内部故障。
- 指标从业务表聚合，大量历史数据下需评估查询成本；实际并发与吞吐应在目标环境中测量。

## 相关文档

| 文档 | 内容 |
| --- | --- |
| [架构说明](docs/architecture.md) | 任务状态机、事务边界、幂等与结果一致性 |
| [运维说明](docs/operations.md) | 指标定义、告警规则、归档一致性与故障操作 |
| [验收报告](docs/verification.md) | 功能验证、测试环境、基准数据与真实故障记录 |
