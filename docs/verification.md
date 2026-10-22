# 验收与复现记录

本报告记录 **2026-10-22** 的实际验证结果，并提供复现方法。后续文档整理不代表重新执行了这些实验；再次运行时应保存新的环境信息与结果文件。

项目启动见 [README](../README.md)，设计原理见 [架构说明](architecture.md)，服务检查与故障定位见 [运维说明](operations.md)。

## 结果概览

| 验证项 | 已记录结果 | 验证范围 |
| --- | --- | --- |
| Ruff lint 与格式检查 | 通过 | Python 源码、迁移与测试 |
| SQLite 测试 | 57 passed | 单元、API、并发、故障与运维用例 |
| PostgreSQL 17 测试 | 57 passed | 相同测试集；每例使用独立临时 schema |
| 语句覆盖率 | 86% | `app` 代码；独立进程和容器实验未纳入覆盖率 |
| Alembic | 通过 | 升级、降级后再升级、已有数据保留、模型差异检查 |
| 本机 uv / Compose HTTP 流程 | 通过 | 上传、幂等创建、后台处理与下载比对 |
| 浏览器流程 | 通过 | 桌面与 390px 移动端、运行概况、下载结果；无 JS 异常和横向溢出 |
| 自动归档与清理 | 通过 | 归档复用、并发保护、失败保留原件、维护状态记录 |
| Redis 停机 | 通过 | 离线期间创建 QUEUED 任务，恢复后完成 |
| Celery Worker SIGKILL | 通过 | 租约恢复后再次执行成功，结果与基线一致 |
| PostgreSQL 提交中断 | 通过 | 文件落盘后提交失败不虚假成功；第 2 次执行完成 |
| Prometheus 指标与规则 | 通过 | 指标格式、规则语法及规则用例 |
| 告警触发与恢复 | 通过 | 停 Worker、Prometheus firing、Alertmanager 接收、恢复后解除 |
| 真实 Celery 负载 | 1670 个任务，0 失败 | 10/100 MiB、1/2/4 Worker，以及 300 秒持续负载 |

本轮 pytest 出现一条 Starlette TestClient 的 httpx 适配弃用提示，未影响锁定依赖下的结果。测试数量与覆盖率以本次记录为准，后续代码变更应重新测量。

### 自动化测试覆盖的关键场景

| 测试目录 | 重点 |
| --- | --- |
| [tests/unit](../tests/unit) | CSV 编码与结构、合并、去重、Decimal 汇总、资源边界 |
| [tests/integration](../tests/integration) | 上传、任务 API、幂等、所属账号、错误响应与下载 |
| [tests/faults](../tests/faults) | 重复领取、旧 Worker 提交、租约过期、取消竞争、重试上限、派发失败、孤儿清理 |
| [tests/operations](../tests/operations) | 归档复用与损坏检测、并发归档、已有数据升级、指标与心跳、维护状态 |

确定性故障测试包含在结果文件完成后、事务提交前注入 SQLAlchemy 异常，便于稳定复现。真实 PostgreSQL 实验则实际停止数据库服务，在同一提交窗口验证恢复；两者的证据范围不同。

## 实验环境与测量口径

本机环境为 WSL2 / Debian 13、Linux 6.18.26.1、Intel Core i5-13500H，16 个可见逻辑 CPU、约 7.62 GiB 可见内存，Python 3.13.5、uv 0.12.11。Compose 使用 PostgreSQL 17、Redis 7；应用依赖以 [uv.lock](../uv.lock) 为准。

输入由 [generate_data.py](../scripts/generate_data.py) 生成：`order_id` 循环 100000 个唯一值，10 个 region，amount 固定为 123.45。每个任务执行合并、去重、分组求和。10 MiB 输入有 388361 行，100 MiB 输入有 3883614 行；生成器保留完整记录，实际字节数可能略高于目标。

测试使用同一主机和本地磁盘，未隔离 CPU，也未排除其他后台负载。100 MiB 输入超过默认行数上限，生成后的字节数也可能超过默认上传上限；基准脚本仅在隔离测试配置中放宽限制，不能据此认为默认服务可直接接收同样的测试文件。

## 性能结果

### 本机 SQLite 与轮询 Worker

每个场景只提交 4 个任务，运行一次；每组创建临时 SQLite 和独立存储目录，不通过 HTTP 测量上传。耗时从 Worker 启动计起，包含进程启动；排队时间从任务创建到 attempt 开始。

| 输入 | Worker 进程 | 总耗时(s) | 吞吐(任务/s) | 平均排队(s) | 平均执行(s) | 采样峰值 RSS 总和(MiB) | 错误率 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 10 MiB | 1 | 2.299 | 1.7402 | 1.018 | 0.503 | 74.2 | 0% |
| 10 MiB | 2 | 1.379 | 2.9009 | 0.505 | 0.535 | 148.2 | 0% |
| 10 MiB | 4 | 1.236 | 3.2365 | 0.306 | 0.860 | 284.9 | 0% |
| 100 MiB | 1 | 15.078 | 0.2653 | 5.764 | 3.691 | 74.3 | 0% |
| 100 MiB | 2 | 8.222 | 0.4865 | 2.172 | 3.929 | 148.2 | 0% |
| 100 MiB | 4 | 5.099 | 0.7844 | 0.265 | 4.761 | 284.9 | 0% |

原始数据：[10 MiB](benchmark-local-10mb.json)、[100 MiB](benchmark-local-100mb.json)。内存通过 Linux `/proc` 约每 50ms 采样各 Worker RSS 后求和，共享页可能重复计数，未包含 API、数据库等服务，也不是进程的严格内存高水位。

此样本中增加 Worker 提高了总吞吐，但没有缩短单任务执行时间。内存状态主要取决于唯一键和分组数量，不能将这些低基数样本推广到任意 CSV 数据。

### PostgreSQL、Redis 与 Celery

在独立 Compose 项目 `dataflow-verification` 中，经真实 HTTP 上传、提交、查询和下载。每个 Worker 容器固定一个计算子进程，每个 Worker 最多维持两个未完成任务，属于有界闭环负载。每个矩阵场景持续提交 60 秒，停止提交后排空；最后用 10 MiB 输入和 4 个 Worker 持续提交 300 秒。

| 输入 | Worker | 持续提交(s) | 完成任务 | 吞吐(任务/s) | 排队 P95(s) | 执行 P95(s) | 采样 Worker 容器内存总和(MiB) |
|---|---:|---:|---:|---:|---:|---:|---:|
| 10 MiB | 1 | 60 | 75 | 1.2283 | 0.956 | 0.778 | 140.0 |
| 10 MiB | 2 | 60 | 132 | 2.1533 | 1.038 | 0.929 | 281.6 |
| 10 MiB | 4 | 60 | 230 | 3.7555 | 1.002 | 1.114 | 563.2 |
| 100 MiB | 1 | 60 | 12 | 0.1707 | 6.330 | 6.058 | 143.4 |
| 100 MiB | 2 | 60 | 22 | 0.3179 | 7.060 | 6.289 | 260.7 |
| 100 MiB | 4 | 60 | 36 | 0.4788 | 9.583 | 8.589 | 502.1 |
| 10 MiB 持续负载 | 4 | 300 | 1163 | 3.8560 | 0.943 | 1.099 | 583.5 |

共完成 **1670 个任务，0 失败**，所有下载结果的 SHA-256 均与同输入基线一致。吞吐分母包含停止提交后的排空时间，而不只是表中的持续提交秒数。原始数据见 [benchmark-celery.json](benchmark-celery.json)。

内存由 `docker stats` 循环采样，每次命令完成后等待 1 秒，实际间隔还包含命令耗时；数值为 Worker 容器内存之和，不含 API、PostgreSQL、Redis 等服务。该口径与本机 RSS 不同，不能据此直接比较两种框架的内存或吞吐优劣。

## 真实故障与告警证据

### Redis 与 Worker

Redis 停止期间，API 仍可将任务及 outbox 提交到可用数据库，任务保持 QUEUED；Redis 恢复后任务成功。另一次实验在任务 RUNNING 时强杀 Worker，随后通过租约恢复产生新的执行尝试，结果与正常基线一致。

该实验使用 [fault_demo.py](../scripts/fault_demo.py)，证据为当次命令输出；仓库未单独保存其结构化 JSON 报告。测试依赖捕获任务运行窗口，输入过小、任务提前完成时脚本会明确失败。

### PostgreSQL 在结果落盘后中断

专用测试进程在结果文件写入并同步后暂停最终提交，实际停止 PostgreSQL，再放行提交调用。没有模拟 SQL 异常；暂停逻辑仅用于测试进程，应用未增加生产调试接口。

| 检查时点 | 记录结果 |
| --- | --- |
| 数据库停止时 | live 返回 200，ready 返回 503，任务查询返回 500 |
| 数据库恢复、恢复进程尚未启动时 | 任务仍为 RUNNING、无结果引用，下载返回 409 |
| Worker 与 dispatcher 恢复后 | 任务 SUCCEEDED，共 2 次执行，结果与正常基线一致 |

原始证据：[postgres-fault.json](postgres-fault.json)。此实验验证服务停止与连接中断，不覆盖所有网络分区、连接黑洞或磁盘损坏模式。

### 告警触发与解除

在空队列的独立项目中正常停止 Worker，应用显示 `NO_WORKERS`；条件持续后，Prometheus 进入 firing，Alertmanager 实际收到记录。重新启动 Worker 后，应用告警指标归零，Prometheus 规则告警消失。未发送外部通知。

[原始记录](monitoring-verification.json) 中，Worker 离线检测为 0.03 秒，等待 firing 为 32.04 秒，随后检查 Alertmanager 时记录已存在，等待为 0 秒；恢复后指标归零检查耗时为 2.01 秒。这些都是顺序检查各阶段的等待时间，不是端到端延迟承诺。正常退出会主动标记停止，因此 0.03 秒不能当作强杀后的心跳超时。

## 复现步骤

所有命令在仓库根目录运行，先执行 `uv sync --frozen`。以下将新输出放到忽略的 `data/`，避免覆盖本次已提交的 JSON 和浏览器记录。脚本若使用默认输出路径，部分会覆盖 `docs/` 中的原始记录。

### 1. 代码检查与数据库测试

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q --cov=app --cov-report=term-missing
```

使用 PostgreSQL 时提供独立测试数据库连接，账号需要 `CREATE SCHEMA` 权限。测试夹具按用例创建临时 schema，正常结束后删除，并将文件写入临时目录：

```bash
DF_TEST_DATABASE_URL='postgresql+psycopg://user:password@localhost/testdb' uv run pytest -q
```

迁移检查会操作当前应用配置指向的数据库，应与隔离测试区分：

```bash
uv run alembic upgrade head
uv run alembic current
uv run alembic check
```

### 2. HTTP 与浏览器流程

先按 README 启动本机服务或 Compose，再运行：

```bash
uv run python scripts/demo.py
uv sync --frozen --group browser
uv run --group browser playwright install --with-deps chromium
uv run --group browser python scripts/browser_smoke.py --output data/browser-smoke
```

`--with-deps` 会安装 Chromium 所需系统依赖，可能需要系统包管理权限。浏览器脚本还要求测试实例未启用令牌且当前无运维告警；它没有自动输入令牌的逻辑，并会断言页面显示“服务运行正常”。HTTP 演示脚本则支持从当前终端的 `DF_API_TOKEN` 环境变量读取令牌。

浏览器测试对照下载内容、检查 JavaScript 异常及移动端横向溢出，并生成桌面和手机截图。原验收环境在忽略的 `data/browser-deps/` 与 `data/browser-fonts/` 中补充了系统库和中文字体，通过 `LD_LIBRARY_PATH` / `FONTCONFIG_FILE` 使用；这些本机辅助文件不随仓库分发。

### 3. 本机基准

该脚本通过 Linux `/proc` 读取 RSS，内存测量应在 Linux 或 WSL 中运行。数据库和存储使用临时目录，结束时清理：

```bash
uv run python scripts/generate_data.py --mb 10
uv run python scripts/generate_data.py --mb 100 --output data/benchmark-100mb.csv
uv run python -m scripts.benchmark data/benchmark.csv --jobs 4 \
  --output data/benchmark-local-10mb.json
uv run python -m scripts.benchmark data/benchmark-100mb.csv --jobs 4 \
  --output data/benchmark-local-100mb.json
```

### 4. 独立 Celery 负载、数据库与告警实验

需要 Docker Engine、Compose v2，以及可用的 18000、19090、19093 端口。先构建应用镜像并准备两份输入，随后按顺序运行：

```bash
docker compose build api
uv run python scripts/generate_data.py --mb 10
uv run python scripts/generate_data.py --mb 100 --output data/benchmark-100mb.csv
uv run python -m scripts.celery_benchmark --duration 60 --soak-seconds 300 \
  --output data/benchmark-celery.json
uv run python -m scripts.postgres_fault_demo --output data/postgres-fault.json
uv run python -m scripts.monitoring_demo --output data/monitoring-verification.json
```

负载脚本创建独立的 `dataflow-verification` 项目，使用独立数据卷、每容器一个计算进程，并在测试环境中将上传上限设为 128 MiB、总行数设为 1000 万。脚本会关闭验证实例的 API 令牌，端口仍绑定本机；不会修改正常项目的环境配置。

负载结束后，独立服务保留运行。数据库和监控脚本要求该 API 已就绪且无未完成任务；监控实验还要求存在已记录的执行尝试，因此上述顺序可直接满足前提。数据库实验暂时停止独立项目的 PostgreSQL、Worker 与 dispatcher；监控实验停止其 Worker，并开启独立监控服务。

这些脚本共享本机 Docker 资源，独立数据卷不等于资源隔离。使用 `--project` 或 `--port` 自定义前，应确认所有关联脚本和清理命令采用一致值；监控脚本固定使用 19090 和 19093。

实验结束后停止独立服务并保留数据卷：

```bash
DF_API_PORT=18000 DF_PROMETHEUS_PORT=19090 DF_ALERTMANAGER_PORT=19093 \
  docker compose -p dataflow-verification --profile monitoring down
```

### 5. 当前 Compose 项目的 Redis 与 Worker 故障

此脚本直接调用当前项目的 `docker compose stop redis` 和 `kill -s SIGKILL worker`，不自动选择独立验证栈。只在可中断的演示实例中执行；默认目标 API 为 8000，`--url` 必须与 Compose 项目对应。

```bash
docker compose up --build -d
uv run python scripts/generate_data.py --mb 10
uv run python scripts/fault_demo.py
```

脚本在 `finally` 中尝试恢复停止的服务；若终端或脚本被强制终止，应通过 `docker compose ps` 和 ready 检查确认恢复。Prometheus 规则的独立校验命令见 [运维说明](operations.md#prometheus-与告警)。

## 结论的适用范围

本次记录表明：已测试输入下的处理结果一致，所覆盖的并发与故障窗口能够按设计恢复，监控和归档流程可用。它不构成任意部署条件下的性能或可用性保证。

尚未覆盖数天级持续运行、跨主机共享存储、完整网络分区矩阵、多租户隔离及大规模历史数据下的查询成本。5 分钟持续负载和固定低基数样本不能替代目标环境的容量规划；复测时应记录机器、并发、数据分布、配置变化和采样方式，再比较结果。
