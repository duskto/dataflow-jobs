# 运维说明

本文面向已按 [README](../README.md) 启动的 DataFlow Jobs，说明日常检查、配置变更、监控、归档和故障定位。命令均在仓库根目录执行；默认地址为本机 `8000`，Compose 服务名以 [compose.yaml](../compose.yaml) 为准。

本机 uv 模式与 Compose 使用不同的数据位置。执行迁移、清理或验证前，先确认命令连接的是目标数据库和存储目录。故障实验的复现步骤与影响范围单独列在 [验收报告](verification.md)。

## 日常检查

按“依赖就绪 → 组件存活 → 任务处理”的顺序检查：

```bash
curl -fsS http://127.0.0.1:8000/api/v1/health/live
curl -fsS http://127.0.0.1:8000/api/v1/health/ready
curl -fsS http://127.0.0.1:8000/api/v1/ops
```

若启用了 `DF_API_TOKEN`，`/ops` 与其他业务接口需要令牌。以下命令假定令牌已设置在当前终端的环境变量中：

```bash
curl -fsS -H "Authorization: Bearer ${DF_API_TOKEN}" \
  http://127.0.0.1:8000/api/v1/ops
```

| 检查项 | 能确认的内容 | 不能据此确认的内容 |
| --- | --- | --- |
| `/health/live` | API 进程能够响应 HTTP | 数据库、Redis 或 Worker 正常 |
| `/health/ready` | `jobs` 表可查询、存储可写；Celery 模式下 Redis 可访问 | 所有迁移均为最新、Worker 正在消费或所有内部组件健康 |
| `/ops` | 任务数量、组件心跳、排队情况、告警及最近维护记录 | 所有任务必然能在指定时间内完成 |
| 完整演示任务 | 上传、创建、后台处理、下载及样本结果可用 | 任意输入或目标生产负载的性能 |

本机默认有两个 Worker；Compose 默认一个 Worker 容器、两个计算子进程，因此 `/ops` 中 Worker 数通常为 2，而非容器数。刚启动时，心跳与首次维护尚未完成，可能短暂显示相关告警。

Compose 日志入口：

```bash
docker compose ps
docker compose logs --tail=100 api worker dispatcher maintenance
docker compose logs --tail=100 postgres redis
```

## 配置与服务变更

本机应用读取 `DF_` 环境变量和 `.env`，环境变量优先。Compose 的 `.env` 用于变量插值；只有 `compose.yaml` 中传入容器的变量才会生效。新增调优参数需加入应用服务的 `environment`，保持 API、Worker、dispatcher、maintenance 和 exporter 配置一致。

全部默认值及取值校验见 [config.py](../app/core/config.py)。常用运维参数如下：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `DF_LEASE_SECONDS` | 30 秒 | 执行租约；任务心跳间隔为其三分之一 |
| `DF_DISPATCH_INTERVAL` | 1 秒 | 派发与恢复循环间隔 |
| `DF_HEARTBEAT_INTERVAL_SECONDS` | 5 秒 | 组件进程心跳间隔 |
| `DF_HEARTBEAT_TIMEOUT_SECONDS` | 20 秒 | 未正常退出的进程心跳过期阈值 |
| `DF_ALERT_QUEUE_AGE_SECONDS` | 120 秒 | 最早到期排队任务的等待告警阈值 |
| `DF_ALERT_FAILURE_RATE` | 0.2 | 最近一小时最终失败率阈值 |
| `DF_ALERT_FAILURE_MIN_JOBS` | 5 | 失败率告警所需的最少完成样本数 |
| `DF_DB_CONNECT_TIMEOUT_SECONDS` | 3 秒 | PostgreSQL 建连超时 |
| `DF_DB_STATEMENT_TIMEOUT_MS` | 5000 毫秒 | PostgreSQL 单条语句超时 |
| `DF_INPUT_ARCHIVE_DAYS` / `DF_EVENT_ARCHIVE_DAYS` | 30 / 30 天 | 输入闲置 / 终态事件归档期限 |
| `DF_MAINTENANCE_INTERVAL_SECONDS` | 3600 秒 | 每轮维护结束后的等待间隔 |
| `DF_MAINTENANCE_BATCH_SIZE` | 100 | 每轮分别处理的输入数、任务事件组数上限 |
| `DF_ORPHAN_GRACE_SECONDS` | 86400 秒 | 无引用对象清理宽限期，配置下限为 3600 秒 |

修改本机配置后重启对应进程。修改 Compose 环境变量后使用 `docker compose up -d` 重建受影响容器；单纯 `restart` 不会载入新的容器环境配置。仅修改监控挂载文件时，可重启相应监控服务载入配置。

### 升级与迁移

升级前备份数据库及其对应的文件存储。维护窗口内先停止请求接入与写入组件，确认其已退出，再执行迁移；当前未提供滚动升级兼容性保证。

本机停止 `scripts/dev.py` 后，在目标版本执行：

```bash
uv sync --frozen
uv run alembic upgrade head
uv run alembic current
uv run alembic check
uv run python scripts/dev.py
```

对已运行的 Compose 项目，可按以下顺序更新。第一条命令也停止可选 exporter，PostgreSQL 与 Redis 保持运行；备份应在停止写入后、迁移前完成：

```bash
docker compose --profile monitoring stop api worker dispatcher maintenance exporter
# 此处完成数据库与文件卷的配套备份，再继续
docker compose build api
docker compose run --rm migrate
docker compose --profile monitoring up -d --no-build
docker compose exec api alembic current
docker compose exec api alembic check
```

上例会启用监控；不需要监控时，启动命令省略 `--profile monitoring`。更新后检查 ready、组件心跳和维护记录，再执行一次演示任务。

归档元数据迁移会将已有输入的 `last_used_at` 回填为 `created_at`；上线后首次维护可能立即归档符合期限的旧输入。若已存在归档记录，迁移脚本会拒绝删除这些元数据的降级。回退应根据迁移兼容性及配套备份制定，不能只替换应用镜像。

## Prometheus 与告警

```bash
docker compose --profile monitoring up -d
```

Prometheus 默认位于 `http://127.0.0.1:9090`，Alertmanager 位于 `http://127.0.0.1:9093`。内部 exporter 不发布主机端口，因此采集配置无需存储业务 API 令牌。默认保留 15 天指标数据；Alertmanager 的 `local-only` 接收器不发送邮件、Webhook 或聊天消息。

采集和规则计算间隔均为 5 秒，规则条件持续 30 秒后进入 `firing`。Alertmanager 的 `group_wait` 为 10 秒，影响通知分组等待，不代表接收记录一定延迟 10 秒。页面直接显示应用当前条件，时间可能早于 Prometheus 告警。

| 应用告警 code | 触发条件 | 优先检查 |
| --- | --- | --- |
| `NO_WORKERS` | 当前模式下无新鲜 Worker 心跳 | Worker 日志、数据库连接、配置模式 |
| `NO_DISPATCHER` | 无 dispatcher 心跳 | 调度进程及其数据库连接 |
| `NO_MAINTENANCE` | 无 maintenance 心跳 | 维护进程是否启动 |
| `BROKER_UNAVAILABLE` | Celery 模式无法访问 Redis | Redis 服务与连接配置 |
| `QUEUE_DELAY` | 最早到期任务等待超过阈值 | Worker 数量、派发和处理耗时 |
| `LEASE_EXPIRED` | 存在租约过期的 RUNNING 任务 | dispatcher 恢复循环与数据库 |
| `FAILURE_RATE` | 一小时完成样本达到下限且失败率超过阈值 | 失败任务错误及输入分布 |
| `MAINTENANCE_FAILED` | 最近维护留下错误 | 维护日志、存储权限和空间 |
| `MAINTENANCE_OVERDUE` | 尚未成功维护，或距成功超过两倍维护间隔加心跳超时 | 首次启动状态、维护耗时与异常 |

上述条件映射为 `DataFlowOperationalAlert`，通过 `code` 区分原因。`DataFlowExporterUnavailable` 单独使用 `up{job="dataflow"} == 0` 检测采集失败；数据库无法查询时，应检查该告警，而不能把采集不到业务指标理解为系统正常。

规则文件见 [alerts.yml](../monitoring/alerts.yml)，可独立校验：

```bash
docker run --rm --entrypoint promtool -v "$PWD/monitoring:/config:ro" -w /config \
  prom/prometheus:v3.5.0 check rules alerts.yml
docker run --rm --entrypoint promtool -v "$PWD/monitoring:/config:ro" -w /config \
  prom/prometheus:v3.5.0 test rules alerts.test.yml
```

### 指标定义

`/api/v1/metrics` 与 `/api/v1/ops` 在配置令牌后均需认证。指标基于数据库聚合，API 重启不会清空已保存的观测；默认部署仅采集一个 exporter，同一份数据库指标不应跨多个采集副本直接求和。

| 指标 | 含义 |
| --- | --- |
| `dataflow_jobs{status}` | 六种状态的当前任务数，零值也返回 |
| `dataflow_queue_oldest_seconds` | 当前已到期 QUEUED 任务中最长的等待时间 |
| `dataflow_outbox_pending` | QUEUED 任务关联的未派发 outbox 条目数 |
| `dataflow_expired_leases` | RUNNING 且租约已过期的任务数 |
| `dataflow_components_online{role}` | 当前执行模式下的新鲜进程心跳数 |
| `dataflow_attempt_duration_seconds_*` | 已结束 attempt 的执行耗时累计直方图 |
| `dataflow_queue_wait_seconds_*` | attempt 领取时记录的排队耗时累计直方图 |
| `dataflow_recent_finished` / `dataflow_recent_failed` | 最近一小时成功加失败数 / 失败数 |
| `dataflow_recent_failure_rate` | 最近一小时失败 /（成功 + 失败），取消不计入 |
| `dataflow_broker_up` | Redis 可访问性，本机模式恒为 1 |
| `dataflow_maintenance_last_success_timestamp_seconds` | 最近成功维护的 Unix 时间戳，尚无成功时为 0 |
| `dataflow_inputs_archived` / `dataflow_events_archived` | 已归档输入数 / 事件条数，后者不是归档文件数 |
| `dataflow_alert_active{code}` | 当前应用告警条件，0 或 1 |

执行耗时为 `finished_at − started_at`，包括失败、取消、存储等待和故障等待；过期 attempt 的结束时间是恢复进程确认时间。每次重试单独计数。排队耗时为领取时间减 `next_run_at`，不含尚未到期的退避；旧版本没有排队观测的记录保持 NULL 并排除。

进程心跳即使空闲也持续上报。正常退出会标记停止，强制退出依靠超时判定；超过 7 天未更新的记录由 maintenance 清理。心跳只反映进程及其数据库连接存活，不能替代实际任务检查。

PromQL 示例：

```promql
# 最近 5 分钟完成的执行尝试耗时 P95
histogram_quantile(0.95, sum by (le) (rate(dataflow_attempt_duration_seconds_bucket[5m])))

# 最近 5 分钟新领取尝试的平均排队时间
rate(dataflow_queue_wait_seconds_sum[5m]) / rate(dataflow_queue_wait_seconds_count[5m])

# 当前在线计算进程数
dataflow_components_online{role="worker"}
```

`rate` 需要多个采样点，没有新观测时分位数或平均值可能不可用。历史记录还原或手工删除会改变聚合结果；当前没有自动删除 job/attempt 历史的策略。

## 存储、归档与备份

| 数据 | 本机位置或 Compose 卷 | 保留方式 |
| --- | --- | --- |
| 业务数据库 | `data/dataflow.db` / `postgres-data` | 任务、文件引用、执行历史与归档索引 |
| 输入、结果和归档 | `data/storage/` / `job-data` 中的 `/data/storage` | 输入到期压缩，成功结果和有效归档持续保留 |
| Redis AOF | `redis-data` | 消息代理持久化；业务状态仍以数据库为准 |
| Prometheus 数据 | `prometheus-data` | 默认保留 15 天 |
| Alertmanager 状态 | `alertmanager-data` | 告警管理状态 |

maintenance 启动即执行一次，之后每轮结束等待配置间隔；默认每轮最多归档 100 个输入和 100 组任务事件。输入需闲置超过 30 天且没有非终态任务引用；终态事件超过 30 天后写为 JSONL.gz。归档输入可直接用于新任务，Worker 自动解压并校验大小及 SHA-256。

归档先持久化文件，再提交元数据；输入原件或在线事件仅在满足提交条件后移除。并发控制细节见 [架构说明](architecture.md)。维护异常会记录在 `/ops` 的 `maintenance.last_error`，成功周期清除错误并更新数量；数据库本身不可用时，错误记录也可能写入失败，需查看进程日志。

孤儿清理默认宽限期为 24 小时，保护数据库引用的对象及 RUNNING 任务目录。手动清理可先预览：

```bash
# 本机配置对应的数据
uv run python -m scripts.cleanup
uv run python -m scripts.cleanup --apply

# Compose 项目对应的数据
docker compose exec api python -m scripts.cleanup
docker compose exec api python -m scripts.cleanup --apply
```

清理宽限期应覆盖正常上传与提交耗时。压缩归档并不等于删除历史数据；存储占用仍会增长，需安排容量监测和备份。

备份必须同时覆盖数据库与其引用的输入、结果和归档文件。简单的维护窗口备份应先停止所有写入组件，再保存数据库和文件卷的一致版本；SQLite 应在相关进程退出后复制，PostgreSQL 可使用数据库备份工具。恢复时也应配套还原，并核对迁移版本、下载结果和归档复用。不要将数据库备份单独恢复后就视为文件已恢复。

`docker compose down` 保留命名卷；`down -v` 会删除本项目命名卷，不应作为日常停止命令。

## 常见问题定位

| 现象 | 检查顺序与处理方向 |
| --- | --- |
| 任务长期 QUEUED | 查看 `/ops` 的 Worker、dispatcher、Redis 和队列等待；检查派发日志。恢复组件后由现有任务继续处理，无需重复提交 |
| RUNNING 且行数不变 | 先看当前阶段、心跳和存储耗时；阻塞 I/O 时行数可能不变。若租约过期，检查 dispatcher 是否恢复 |
| ready 返回 503 | 检查数据库连接、`jobs` 表、存储可写和 Redis；另用 `alembic current` / `check` 核对迁移 |
| 数据库中断时任务查询返回 500 | 数据库状态无法读取；恢复连接后查询原任务，观察租约恢复，不能根据磁盘文件自行标记成功 |
| 结果下载返回 409 | 任务尚未成功，查询任务状态和事件 |
| 结果下载返回 503 / `RESULT_MISSING` | 数据库引用存在但文件不可用；核对共享卷、路径配置及备份，不能靠重试下载重建文件 |
| 上传返回 413 或处理达到上限 | 区分上传字节、总行数、唯一键和分组数限制；按资源容量调整，Compose 需传入对应变量 |
| 归档未发生或维护告警 | 检查闲置时间、非终态引用、批次上限、维护记录与存储空间 |
| 页面正常但外部通知未收到 | 默认接收器没有外部通知渠道；先确认 Prometheus firing 和 Alertmanager 接收记录 |

需要主动验证故障恢复时，使用 [验收报告中的复现步骤](verification.md#复现步骤)。其中 Redis/Worker 脚本操作当前 Compose 项目，数据库与告警脚本默认操作独立的 `dataflow-verification` 项目；执行前应区分目标。
