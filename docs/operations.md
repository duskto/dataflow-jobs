# 监控、保留策略与故障操作

## 运维数据

`/api/v1/ops` 与 `/api/v1/metrics` 都受演示账号令牌保护。指标来源于业务数据库，重启 API、增加 API 副本不会重置或重复累加历史观测。

| 指标 | 含义 |
|---|---|
| `dataflow_jobs{status}` | 六种状态当前任务数，零值也返回 |
| `dataflow_queue_oldest_seconds` | 当前已到期 QUEUED 任务等待最久的秒数 |
| `dataflow_outbox_pending` | QUEUED 任务尚未派发的 outbox 条目数 |
| `dataflow_expired_leases` | RUNNING 但租约已过期的任务数 |
| `dataflow_components_online{role}` | 当前 execution_mode 的新鲜进程心跳数 |
| `dataflow_attempt_duration_seconds_*` | 已结束执行尝试的耗时直方图，包含失败和取消 |
| `dataflow_queue_wait_seconds_*` | 领取尝试时记录的等待时长直方图，排除退避尚未到期的时间 |
| `dataflow_recent_failure_rate` | 近 1 小时最终失败 / (成功 + 失败)，取消不计入 |
| `dataflow_broker_up` | Redis 可访问性，本机模式恒为 1 |
| `dataflow_maintenance_last_success_timestamp_seconds` | 最近成功完成自动归档/清理的时间 |
| `dataflow_inputs_archived` / `dataflow_events_archived` | 已归档输入数 / 事件数 |
| `dataflow_alert_active{code}` | 应用端瞬时告警条件，0/1 |

执行耗时包含领取后的读取、处理、写出和故障等待，以 attempt.started_at 到 finished_at 计。恢复失败尝试的 finished_at 是恢复进程确认时间。每次重试算一次独立观测。旧版本的历史尝试没有排队耗时，迁移保留 NULL 并从队列直方图中排除，不补造数值。

Worker 的进程心跳独立于任务租约心跳：空闲也持续上报。Celery 在计算子进程初始化后启动心跳线程，正常退出标记 stopped；强杀后自然超时。此检查反映进程及其数据库连接存活，不能检测所有类型的消费循环死锁。长时间排队告警提供另一层检测。过期超过 7 天的心跳记录由 maintenance 清理。

## Prometheus 与告警

```bash
docker compose --profile monitoring up -d
```

采集/规则计算每 5 秒执行；异常持续 30 秒后进入 firing，Alertmanager 默认再等待 10 秒聚合。应用页面即时显示条件，因此页面与 Alertmanager 出现/消失的时间可能不同。

当前告警覆盖：无 Worker、无 dispatcher、无 maintenance、Redis 失败、排队超时、租约过期、近一小时失败率过高、巡检失败/过期，以及 exporter 无法采集。阈值在配置中定义，规则消费固定 code 标签，避免为每个 job/文件生成高基数时序。

Prometheus 保留 15 天数据。默认 Alertmanager 接收器只有本地 UI，没有外部发送渠道。地址均绑定本机；内部 exporter 不发布主机端口。

查询示例：

```promql
# 最近 5 分钟执行尝试耗时的 P95；需要至少两个采样点
histogram_quantile(0.95, sum by (le) (rate(dataflow_attempt_duration_seconds_bucket[5m])))

# 5 分钟内平均排队时间
rate(dataflow_queue_wait_seconds_sum[5m]) / rate(dataflow_queue_wait_seconds_count[5m])

# 计算进程数
dataflow_components_online{role="worker"}
```

规则可独立验证：

```bash
docker run --rm --entrypoint promtool -v "$PWD/monitoring:/config:ro" -w /config \
  prom/prometheus:v3.5.0 check rules alerts.yml
docker run --rm --entrypoint promtool -v "$PWD/monitoring:/config:ro" -w /config \
  prom/prometheus:v3.5.0 test rules alerts.test.yml
```

## 自动归档的一致性

输入在“闲置期限已到、尚未归档、没有非终态引用”时进入候选集。压缩在数据库事务外完成，避免长时间占用业务行锁。完成后再条件更新 File：创建任务也会按文件 UUID 排序锁定 File 并更新 last_used_at。如果新任务先提交，旧归档候选不能发布；如果归档先提交，新任务读取归档对象。成功提交元数据后才移除原文件。

Worker 在自己的 attempt 目录解压输入，校验未压缩大小和 SHA-256，整个过程受租约和心跳保护；成功或失败都清理临时解压文件。原文件丢失、归档损坏不会静默跳过。

事件归档只针对超过保留期的终态任务。先流式生成带摘要的 JSONL.gz，再锁定任务，将归档索引和在线事件删除一起提交。同一任务并发归档只允许一个索引获胜。失败的归档写入可能留下孤儿对象，宽限期后清理；有效归档永远由数据库引用保护。

默认每小时处理最多 100 个输入和 100 组任务日志。没有对归档对象做自动销毁，因此当前保留策略是“压缩、移出热路径”，并非彻底删除历史数据。成功结果也持续保留。维护失败会记录错误并触发告警；下一周期重试。

有归档数据时不允许直接降级删除归档元数据。升级脚本可对已有上传数据执行，last_used_at 以原 created_at 回填。维护与迁移都只操作当前配置的数据根目录/数据库。

## 可重现的实际故障

- `scripts/fault_demo.py`：原演示栈上的 Redis 停机和 Worker SIGKILL。
- `python -m scripts.postgres_fault_demo`：独立验证栈，实际停止 PostgreSQL，覆盖结果已落盘但不能提交的窗口。
- `python -m scripts.monitoring_demo`：独立验证栈，停止 Worker 直到告警到达 Alertmanager，然后恢复并验证告警消失。

数据库实验的等待栅栏只在单次测试进程通过 stdin 执行；生产任务代码没有暂停开关或测试 API。数据库异常由真正的连接失败产生。实验脚本的 finally 恢复相关服务。数据库网络黑洞与直接停止服务的 TCP 行为仍有区别；本次实测是服务停止与连接中断，未冒称覆盖所有网络分区形态。

官方参考：[Prometheus 告警规则](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)、[直方图](https://prometheus.io/docs/practices/histograms/)、[Celery 进程信号](https://docs.celeryq.dev/en/stable/userguide/signals.html)。
