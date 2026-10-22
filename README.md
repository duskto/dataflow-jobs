# DataFlow Jobs

基于 Python、FastAPI、SQLAlchemy、Celery 的 CSV 后台处理平台。支持有序合并、按字段去重、Decimal 分组求和，以及任务查询、执行事件、下载、重试和取消。提供中文网页和 OpenAPI 文档。

![工作台](docs/screenshots/desktop.png)

## 用 uv 在本机启动

需要 uv 和 Python 3.12+；项目 `.python-version` 固定为 3.13，`uv.lock` 锁定完整依赖。uv 可自动下载对应 Python。

```bash
uv sync --frozen
cp .env.example .env
uv run python scripts/dev.py
```

打开 **http://127.0.0.1:8000**；API 文档在 **http://127.0.0.1:8000/docs**。脚本先执行 Alembic 迁移，再启动 API、dispatcher、maintenance、两个独立 Worker 进程；Ctrl+C 会结束这组进程。API 不执行 CSV 计算。

本机默认使用 SQLite + 数据库轮询 Worker，不需要 Redis 或 Docker，适合体验和小规模演示。可使用 `--port 8001 --workers 4` 调整端口和 Worker 数量。实际部署使用下方 PostgreSQL/Celery 模式。

另开终端验证完整 HTTP 流程：

```bash
uv run python scripts/demo.py
# 如果使用其他端口：
uv run python scripts/demo.py --url http://127.0.0.1:8001
```

脚本会上传 `samples/january.csv` 和 `samples/february.csv`、重复提交相同幂等键、等待后台处理、下载并对照 `samples/expected.csv`。页面也可直接上传这两个文件，使用默认三步流程。

## Compose 部署

需要 Docker Engine 和 Compose v2；请先停止占用 8000 端口的本机服务。

```bash
docker compose up --build -d
docker compose ps
uv run python scripts/demo.py
```

包含 api、worker、dispatcher、maintenance、postgres、redis 和一次性 migrate 服务。依赖健康检查通过后，migrate 执行迁移，其他服务等待迁移成功再启动。worker 默认有两个 Celery 子进程。

可选启用持续监控：

```bash
docker compose --profile monitoring up -d
```

Prometheus 位于 http://127.0.0.1:9090，Alertmanager 位于 http://127.0.0.1:9093。exporter 只在 Compose 内部网络暴露，无须把 API 令牌写进 Prometheus 配置。默认只在本地 UI 查看告警，不发送邮件或外部消息。页面“运行概况”无需启用此 profile，也会显示 Worker、排队情况和当前告警。

```bash
# 增加 Worker 容器；每个容器有两个计算进程
docker compose up -d --scale worker=2
# 查看业务执行、派发日志
docker compose logs -f worker dispatcher
# 停止本项目服务，保留数据卷
docker compose down
```

API 只绑定本机 `127.0.0.1:8000`。PostgreSQL/Redis 不映射到主机端口。Compose 中的数据库账号是固定的本地演示账号。对外使用前配置密码、反向代理、HTTPS 和 `DF_API_TOKEN`。页面的“演示账号令牌”会发送 `Authorization: Bearer …`，只保存在当前标签页；健康检查与首页公开。

## 配置与存储

配置来自 `DF_` 环境变量或 `.env`。环境变量优先。修改环境配置后重启进程。

| 配置 | 默认值 | 说明 |
|---|---|---|
| `DF_DATABASE_URL` | `sqlite:///./data/dataflow.db` | SQLAlchemy 数据库 URL |
| `DF_STORAGE_ROOT` | `./data/storage` | 上传和结果存储根目录 |
| `DF_EXECUTION_MODE` | `local` | `local` 或 `celery` |
| `DF_REDIS_URL` | `redis://localhost:6379/0` | Celery broker |
| `DF_API_TOKEN` | 空 | 可选的共享演示账号令牌 |
| `DF_LEASE_SECONDS` | 30 | Worker 租约；心跳间隔为其 1/3 |
| `DF_RETRY_BASE_SECONDS` | 2 | 指数退避基数，附加 0–25% 抖动 |
| `DF_MAX_UPLOAD_BYTES` | 104857600 | 每个文件最多 100 MiB，实际读取计数 |
| `DF_MAX_ROWS` | 1000000 | 所有输入文件累计数据行数 |
| `DF_MAX_UNIQUE_KEYS` | 200000 | 每个去重步骤唯一键上限 |
| `DF_MAX_GROUPS` | 100000 | 每个汇总步骤分组数上限 |
| `DF_MAX_COLUMNS` | 200 | CSV 列数上限 |
| `DF_MAX_FIELD_CHARS` | 1000000 | 单字段字符数上限 |
| `DF_ORPHAN_GRACE_SECONDS` | 86400 | 孤儿对象清理宽限期，至少 1 小时 |
| `DF_INPUT_ARCHIVE_DAYS` | 30 | 输入自上次提交使用起，闲置多少天后压缩归档 |
| `DF_EVENT_ARCHIVE_DAYS` | 30 | 任务终态多少天后，将执行事件归档为 JSONL.gz |
| `DF_MAINTENANCE_INTERVAL_SECONDS` | 3600 | 自动归档与孤儿清理间隔；启动时也执行一次 |
| `DF_MAINTENANCE_BATCH_SIZE` | 100 | 每轮最多归档的输入/任务日志组数量 |
| `DF_HEARTBEAT_INTERVAL_SECONDS` | 5 | Worker、dispatcher、maintenance 进程心跳间隔 |
| `DF_HEARTBEAT_TIMEOUT_SECONDS` | 20 | 进程离线判断阈值 |
| `DF_ALERT_QUEUE_AGE_SECONDS` | 120 | 最早排队任务告警阈值 |
| `DF_ALERT_FAILURE_RATE` | 0.2 | 近一小时最终失败率阈值，排除取消 |
| `DF_ALERT_FAILURE_MIN_JOBS` | 5 | 失败率告警最少完成任务样本数 |
| `DF_DB_CONNECT_TIMEOUT_SECONDS` | 3 | PostgreSQL 连接超时 |
| `DF_DB_STATEMENT_TIMEOUT_MS` | 5000 | PostgreSQL 单条语句超时 |

本机数据库和文件在 `data/` 下；Compose 中 API 与 Worker 共用 `job-data` 卷 `/data/storage`，数据库使用 `postgres-data`，Redis AOF 使用 `redis-data`。文件名和路径都由服务端生成，API 不返回内部路径。上传请求在 multipart 解析前也有总字节限制（文件上限 + 1 MiB 协议开销），支持无 Content-Length 的上传。

文件完整落盘并记录大小和摘要后为 READY；编码和 CSV 语义在任务执行时验证，非法 CSV 使任务明确失败。所有文件、任务、事件和结果查询受服务端固定 `owner_id` 限制；这是单演示账号，尚未提供用户注册/独立令牌映射。

分别启动服务时：

```bash
uv run alembic upgrade head
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
# 另开终端
uv run python -m app.scheduler.dispatcher
uv run python -m app.scheduler.maintenance
uv run python -m app.workers.local
```

Celery 模式设置 PostgreSQL URL、Redis URL 和 `DF_EXECUTION_MODE=celery`，然后把最后一条命令替换为：

```bash
uv run celery -A app.workers.celery_app:celery_app worker --loglevel=INFO --concurrency=2
```

## API 示例

统一前缀 `/api/v1`。错误统一返回 `code`、`message`、`request_id`；响应头含 `X-Request-ID`。UUID 标识，时间使用带 UTC 时区的 ISO 8601。

```bash
curl -F 'file=@samples/january.csv' http://127.0.0.1:8000/api/v1/files
curl -F 'file=@samples/february.csv' http://127.0.0.1:8000/api/v1/files
```

将响应中的两个 `file_id` 填入请求：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: sales-demo-001' \
  -d '{
    "input_file_ids": ["替换为第一个UUID", "替换为第二个UUID"],
    "steps": [
      {"type":"merge","schema_policy":"strict"},
      {"type":"deduplicate","keys":["order_id"],"keep":"first"},
      {"type":"aggregate","group_by":["region"],"sum":["amount"]}
    ],
    "csv_options":{"encoding":"utf-8-sig","delimiter":","},
    "max_attempts":3
  }'
```

| 方法 | 路径 | 行为 |
|---|---|---|
| POST | `/files` | 上传 CSV，201 |
| GET | `/files/{id}` | 元信息 |
| POST | `/jobs` | 创建任务，202；相同幂等键和内容返回同一任务，不同内容 409 |
| GET | `/jobs?status=RUNNING&limit=20&offset=0` | 分页；limit ≤ 100，返回 next_offset |
| GET | `/jobs/{id}` | 阶段、行数、错误、执行次数与 attempt 记录 |
| POST | `/jobs/{id}/cancel` | 取消或标记取消请求 |
| POST | `/jobs/{id}/retry` | 仅 FAILED 可创建新任务，保留 parent_job_id；支持幂等键 |
| GET | `/jobs/{id}/events?after=0&limit=50` | 游标事件；单条最多 2000 字符 |
| GET | `/jobs/{id}/result` | 成功后下载；未成功 409 |
| GET | `/health/live` | 进程存活 |
| GET | `/health/ready` | 数据库、迁移、存储及 Celery 模式 Redis 检查 |
| GET | `/ops` | 运行概况、当前告警、归档巡检记录 |
| GET | `/metrics` | Prometheus 状态、队列、耗时直方图、心跳和归档指标 |
| GET | `/jobs/{id}/events/archive` | 下载归档日志；同样验证所属账号 |

## CSV 与状态语义

- 1–20 个有序输入，1–8 个白名单步骤。多个文件必须先 merge；merge 只能在第一步。后续去重/汇总可按需要排序，支持页面 JSON 自定义。
- 合并严格检查列名和顺序；不自动对齐缺失列。CSV 使用标准库，支持 BOM、引号、逗号和字段内换行。默认 UTF-8/BOM，可显式选 GB18030，分隔符支持逗号、分号、Tab、竖线。
- 去重用字段 tuple，保留第一条；空字符串属于同一个值。不隐式 trim 字段内容。
- 分组顺序为首次出现顺序。求和用 Decimal，单数值最多 38 位有效数字、十进制指数范围 -18 至 18，拒绝空值、NaN、Infinity 和其他非法金额。错误带当前汇总阶段的逻辑行号和列名。输出固定 UTF-8 BOM + 逗号。
- 无表头文件、重复/空列名、字段数不符会失败。只有表头允许输出空结果。
- 处理过程显示阶段和本阶段已处理行数；不同阶段和新的 attempt 可以从零开始。总行数未知时 progress 为 null，只在成功后为 100，不显示估算百分比。
- `QUEUED` 等待领取；`RUNNING` 已领取；`RETRY_WAIT` 退避中；`SUCCEEDED`、`FAILED`、`CANCELLED` 为终态。
- 排队/等待重试任务可立即取消；运行中设置 `cancel_requested`，Worker 在检查点确认退出。成功和取消按数据库行上最先提交的状态转换决定。成功后取消仍返回成功。
- `max_attempts` 包括首次执行，默认 3，最多 5。格式和参数错误不自动重试；临时存储/连接故障以及租约过期可重试。手动重试创建新任务。

## 可靠性与模块

API → Service → PostgreSQL；创建任务与 outbox 处于同一事务。dispatcher 发消息后标记 outbox；标记前崩溃可能重复派发。Worker 通过条件 UPDATE 原子领取，生成 attempt 并设置租约。心跳、失败和结果提交都要求当前 attempt 和有效租约；恢复进程二次检查过期租约并安排重试。已派发但长期未领取的任务会重新派发，以覆盖 Redis 已接收后丢消息的情况。

每次尝试写入 `jobs/{job_id}/{attempt_id}/result.tmp`，fsync 后重命名为结果文件，再在事务中绑定 `result_file_id`。文件已写但提交失败只会产生孤儿文件，不会提前宣告成功。业务状态来自数据库，不使用 Celery 结果后端。消息只有 job_id；允许重复执行请求，业务结果仅发布一次。

```text
app/api/          HTTP、权限范围与响应
app/core/         配置、会话、统一错误、上传字节限制
app/models/       业务表、组件心跳、归档索引与巡检记录
app/schemas/      请求、CSV 参数与步骤白名单
app/services/     幂等创建、领取、状态转换、attempt 防护
app/processors/   不依赖 HTTP/数据库的 CSV 流式处理
app/workers/      执行协调、独立心跳、Celery 与本机 Worker
app/scheduler/    outbox 派发、租约恢复、定期归档与清理
app/observability/ 进程心跳、持久化指标、内部 exporter
app/storage/      上传、摘要、独立结果对象
app/templates/    Jinja2 页面
app/static/       原生 JavaScript 与响应式样式
migrations/       显式 Alembic 建表/索引/约束迁移
scripts/          启动、演示、故障实验、基准测试、清理
```

孤儿清理默认只预览，跳过数据库引用对象和 RUNNING 任务目录：

```bash
uv run python -m scripts.cleanup
uv run python -m scripts.cleanup --apply
# Compose 的存储/数据库：
docker compose exec api python -m scripts.cleanup --apply
```

maintenance 进程启动时执行一次，此后默认每小时自动运行。最近成功时间、处理数量和错误显示在 `/ops` 与页面中；不需要另配 cron。

输入默认闲置 30 天后写入 `archives/inputs/...csv.gz`；排队、运行、等待重试任务引用的输入始终受保护。压缩成功后，条件更新归档元数据，提交成功才移除原始副本；创建新任务会锁定输入并更新 last_used_at，与归档提交互斥。归档输入可直接创建任务和重试，Worker 自动解压并校验原始大小与 SHA-256，执行后删除临时解压文件。

终态任务默认 30 天后将日志写为 `archives/events/...jsonl.gz`，先 fsync 文件，再在同一数据库事务中登记归档并删除对应的在线事件。详情页提供归档下载。归档对象本身不会因孤儿清理被删除；结果文件也不自动过期。已有归档时，Alembic 会拒绝丢失归档元数据的降级。

## 测试与故障演示

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q --cov=app --cov-report=term-missing
uv run alembic check
# 使用有 CREATE SCHEMA 权限的 PostgreSQL 连接测试，临时 schema 自动清理：
DF_TEST_DATABASE_URL='postgresql+psycopg://user:password@localhost/testdb' uv run pytest -q
```

浏览器验收依赖独立分组：

```bash
uv sync --frozen --group browser
uv run --group browser playwright install --with-deps chromium
uv run --group browser python scripts/browser_smoke.py
```

真实故障实验会暂时停止**本项目 Compose** 的 Redis，并 SIGKILL Worker 容器，最后恢复它们。需先启动 Compose：

```bash
uv run python scripts/generate_data.py --mb 10
uv run python scripts/fault_demo.py
```

它验证停 Redis 时仍能创建 QUEUED 任务、恢复后 outbox 派发，以及运行中杀 Worker 后租约恢复、attempt 增加、结果和正常执行一致。更多确定性故障测试见 `tests/faults/` 和 `tests/operations/`，涵盖旧 Worker 返回、并发重复消息、取消竞争、写文件后数据库提交失败、孤儿清理和重试上限。

性能脚本使用隔离的临时 SQLite 与 1/2/4 个真实本机 Worker 进程，记录吞吐、排队/执行时间、采样内存和错误率：

```bash
uv run python scripts/generate_data.py --mb 10
uv run python -m scripts.benchmark data/benchmark.csv --jobs 4
uv run python scripts/generate_data.py --mb 100 --output data/benchmark-100mb.csv
uv run python -m scripts.benchmark data/benchmark-100mb.csv --jobs 4
```

脚本为 100 MiB 用例单独提升行数/文件上限，不改变正常服务配置。测量内存是每 50ms 采样的所有 Worker RSS 总和，共享页可能重复计数；测试包含 Worker 启动，未隔离 CPU。结果只能作为当前环境演示，不能当成跨机器保证。[实测报告](docs/verification.md)和两份原始 JSON 已随项目保存。

## 持续压测和真实数据库中断

使用独立项目 `dataflow-verification`、独立数据卷和 18000 端口，不操作正常演示数据。先构建新镜像并准备数据：

```bash
docker compose build api
uv run python scripts/generate_data.py --mb 10
uv run python scripts/generate_data.py --mb 100 --output data/benchmark-100mb.csv
uv run python -m scripts.celery_benchmark --duration 60 --soak-seconds 300
```

该脚本依次运行 10/100 MiB、1/2/4 个 Celery Worker 容器，每容器一个子进程，每组持续提交 60 秒，再进行 300 秒持续负载。每个 Worker 最多保持两个未完成任务；所有成功结果都与同输入的基线 SHA-256 比较。记录队列/执行 P95、吞吐、错误率和容器内存采样。可提高 `--duration`、`--soak-seconds` 做更长测试。100 MiB 测试只在隔离项目放宽行数/字节数上限。

完成压测并等待队列为空后：

```bash
uv run python -m scripts.postgres_fault_demo
uv run python -m scripts.monitoring_demo
```

数据库实验在专用测试进程的结果落盘后暂停提交，实际停止 PostgreSQL，再放行真实提交调用；没有模拟 SQL 异常，也没有添加生产调试接口。验证数据库离线时 live=200、ready=503、业务查询=500，恢复连接后任务仍未成功且结果下载=409，最后通过租约恢复重新执行并对照结果。

监控实验会暂时停止隔离项目的 Worker，验证心跳失效、Prometheus 规则进入 firing、Alertmanager 收到告警，以及启动 Worker 后告警恢复。日志和 JSON 证据保存在 `docs/`（运行日志在忽略的 `data/`）。实验结束可清理独立服务：

```bash
DF_API_PORT=18000 DF_PROMETHEUS_PORT=19090 DF_ALERTMANAGER_PORT=19093 \
  docker compose -p dataflow-verification --profile monitoring down
```

详见 [运维说明](docs/operations.md) 和 [验收报告](docs/verification.md)。

## 已知限制

- 单演示账号，不包含注册、角色管理、Excel、DAG 或执行用户代码；并非公网多租户服务。
- 本地共享 volume 仅适用于同一主机；跨主机必须换成各 Worker 均可访问的存储。
- 去重和分组占用 O(U)/O(G) 内存，有显式上限；未实现外部排序或磁盘状态溢出。总行数限制也会影响低于文件大小上限的数据。
- SQLite 用于本机演示，写并发受串行化限制。生产规模并发使用 PostgreSQL 并按实测资源限制 Worker 数量。
- 输入和事件按配置自动压缩归档，但归档和成功结果不自动销毁，尚无账号存储配额。孤儿清理宽限期应大于正常上传事务耗时。
- 页面每 3 秒轮询；事件 API 支持游标，页面展示前 100 条在线事件，归档可完整下载。ready 检查依赖就绪，独立心跳反映进程存活；二者均不能保证某个 Worker 的全部内部组件都正常。
- 指标从业务表聚合，避免多进程重复统计；大规模长期历史下应进一步引入聚合表。短期持续测试不能替代数天稳定性和容量规划。
- 正常停止等待当前工作完成；超出停止宽限期的强制退出通过租约恢复。无“恰好执行一次”承诺。

官方语义参考：[FastAPI 文件上传](https://fastapi.tiangolo.com/tutorial/request-files/)、[Celery 任务与消息确认](https://docs.celeryq.dev/en/stable/userguide/tasks.html)、[SQLAlchemy 条件更新](https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html)、[Python csv](https://docs.python.org/3/library/csv.html)。实际依赖版本以 `uv.lock` 为准。
