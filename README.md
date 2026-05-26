
# LogTrack 统一日志收集系统设计文档

## 一、项目概述

### 1.1 项目背景

LogTrack 是一个轻量级的统一日志收集系统，专为 Go 微服务架构设计。它通过 TCP 协议接收业务服务发送的多类日志数据，异步写入 Kafka，并在 Kafka 不可用时自动降级到本地文件缓存，确保数据不丢失。

### 1.2 设计目标

| 目标         | 说明                                      |
| ------------ | ----------------------------------------- |
| **低侵入**   | 业务方通过轻量级 SDK 一行代码完成日志记录 |
| **高性能**   | TCP 长连接复用，单次写入开销低            |
| **高可用**   | Kafka 故障时自动降级，恢复后自动补发      |
| **可扩展**   | 自定义 topic 直通 Kafka，无需服务端改代码 |
| **统一追踪** | 通过 TraceId 串联所有日志类型             |

### 1.3 支持的数据类型

| 类型                   | 用途                     | 采集策略                    |
| ---------------------- | ------------------------ | --------------------------- |
| **inbound HTTP 日志**  | app → 后端接口监控、排障 | 全量                        |
| **outbound HTTP 日志** | 后端 → 三方调用监控      | 全量                        |
| **事件埋点**           | 用户行为分析、业务转化   | 全量                        |
| **RPC 调用日志**       | 服务间调用监控、依赖拓扑 | 错误/超时全量 + 正常采样    |
| **应用日志**           | 业务 Debug、错误追踪     | ERROR 全量，INFO/DEBUG 采样 |

---

## 二、整体架构

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                         业务服务集群                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Service A   │  │ Service B   │  │ Service C   │              │
│  │ LogTrack    │  │ LogTrack    │  │ LogTrack    │              │
│  │ Client      │  │ Client      │  │ Client      │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                     │
│         └────────────────┼────────────────┘                     │
│                          │ TCP 长连接                           │
└──────────────────────────┼──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LogTrack Gateway                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ TCP Server (连接管理、读取消息)                         │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                    │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │ Message Queue & Worker Pool                             │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                    │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │ Message Router (按 topic 路由)                          │    │
│  └─────┬────────┬────────┬────────┬────────┬───────────────┘    │
│        │        │        │        │        │                    │
│        ▼        ▼        ▼        ▼        ▼                    │
│   ┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐            │
│   │Inbound ││Outbound││ Event  ││  RPC   ││  App   │            │
│   │HTTP    ││HTTP    ││Handler ││Handler ││Handler │            │
│   └───┬────┘└───┬────┘└───┬────┘└───┬────┘└───┬────┘            │
│       │         │         │         │         │                 │
│       └─────────┴─────────┴─────────┴─────────┘                 │
│                            │                                    │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │ Writer Manager (Kafka + 降级文件)                       │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │     Kafka Cluster       │
              │  ┌─────┐ ┌─────┐ ┌─────┐│
              │  │ T1  │ │ T2  │ │ T3  ││
              │  └─────┘ └─────┘ └─────┘│
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │    ClickHouse / ES      │
              │    (数据分析存储)       │
              └─────────────────────────┘
```

### 2.2 数据流向

1. 业务服务调用 SDK 记录日志
2. SDK 通过 TCP 长连接发送
3. Gateway 接收消息，按 topic 路由：命中已注册 handler 走对应 Handler，未命中走 Passthrough Handler
4. Handler（或 Passthrough）调用 Writer Manager 写入
5. Writer Manager 判断 Kafka 状态：
   - 健康 → 写入 Kafka
   - 不健康 → 写入本地降级文件
6. 后台协程定期检查 Kafka 健康，恢复后补发降级文件

---

## 三、通讯协议与字段定义

帧格式、信封、各 topic 的 data schema、event_name 命名约定、SDK 字段注入方式独立维护，见 [`PROTOCOL.md`](./PROTOCOL.md)。

本 README 后续只描述服务端实现、客户端实现、部署、监控、风险等设计层面内容，不再重复 schema。

---

## 四、服务端设计

### 4.1 模块职责

| 模块                      | 职责                                                                  |
| ------------------------- | --------------------------------------------------------------------- |
| **TCP Server**            | 管理长连接，读取消息，连接数限制                                      |
| **Message Queue**         | 缓冲接收到的消息，削峰填谷                                            |
| **Worker Pool**           | 消费队列，并行处理消息                                                |
| **Message Router**        | 解析 topic 字段，命中 handler 走 handler，否则走通用 passthrough      |
| **Inbound HTTP Handler**  | 校验 inbound HTTP 日志格式（topic=inbound-http-logs，app→后端，量大） |
| **Outbound HTTP Handler** | 校验 outbound HTTP 日志格式（topic=outbound-http-logs，后端→三方）    |
| **Event Handler**         | 校验事件埋点（topic=event-tracks）                                    |
| **RPC Handler**           | 校验 RPC 日志，识别慢调用/错误（topic=rpc-calls）                     |
| **App Handler**           | 按日志级别分流处理（topic=app-logs）                                  |
| **Passthrough Handler**   | 未命中已注册 handler 时使用，不做校验，整封写入对应 topic             |
| **Writer Manager**        | 管理写入 Kafka 和降级文件                                             |
| **Kafka Writer**          | 批量异步写入 Kafka                                                    |
| **Fallback Writer**       | 本地文件写入（Kafka 故障时）                                          |

### 4.2 降级与恢复机制

**降级触发条件**：
- Kafka 连接失败
- Kafka 写入超时
- Kafka 返回错误

**降级行为**：
- 消息写入本地文件（按日期/大小滚动）
- 记录降级指标，触发告警

**恢复行为**：
- 后台协程定期检测 Kafka 健康
- 恢复后读取降级文件，逐条补发
- 补发成功则删除或标记已发
- 补发失败则保留，等待下次重试

### 4.3 Kafka Topic 规划

内置 topic：

| Topic                | 对应内置 handler      | 分区策略         | 保留时间 |
| -------------------- | --------------------- | ---------------- | -------- |
| `inbound-http-logs`  | Inbound HTTP Handler  | 按 path hash     | 7 天     |
| `outbound-http-logs` | Outbound HTTP Handler | 按 provider hash | 7 天     |
| `event-tracks`       | Event Handler         | 按 user_id hash  | 30 天    |
| `rpc-calls`          | RPC Handler           | 按 method hash   | 7 天     |
| `app-logs`           | App Handler           | 按 service hash  | 15 天    |

**自定义 topic**：客户端可发送任意 `topic` 值，未命中内置 handler 的消息走 Passthrough Handler，整封 JSON 写入名为该 topic 的 Kafka topic。业务方需提前在 Kafka 创建 topic（含分区/保留时间等配置），LogTrack 不会自动建 topic；topic 不存在时按 Kafka 写入失败处理，触发降级到 fallback 文件。

### 4.4 配置项

**服务端配置**：

| 配置项                       | 建议值         | 说明                                 |
| ---------------------------- | -------------- | ------------------------------------ |
| server.address               | :9583          | TCP 监听地址                         |
| server.max_connections       | 10000          | 最大连接数                           |
| server.queue_size            | 50000          | 消息队列大小                         |
| server.worker_count          | 100            | Worker 数量                          |
| kafka.brokers                | ["kafka:9092"] | Kafka 集群地址                       |
| kafka.batch_size             | 100            | 批量发送数量                         |
| kafka.batch_timeout          | 5s             | 批量发送间隔                         |
| fallback.data_dir            | /data/logtrack | 降级文件目录                         |
| fallback.max_file_size       | 100MB          | 单文件最大大小                       |
| fallback.max_files           | 10             | 最大文件数量                         |
| shutdown.timeout             | 10s            | 优雅停机总超时                       |
| shutdown.conn_read_timeout   | 5s             | 关 listener 后已有连接的最长读取时间 |
| shutdown.kafka_flush_timeout | 3s             | 停机阶段 Kafka flush 单步超时        |

### 4.5 优雅停机

收到 `SIGTERM` / `SIGINT` 后按顺序执行（总超时 10s）：

1. 关闭 TCP listener，不再接受新连接
2. 已建立的连接继续读取消息，直到客户端 EOF 或 5s 读超时（取先到者），然后服务端主动关闭
3. 等待 worker 排空消息队列（这一步会让健康路径下的消息正常入 Kafka producer 缓冲）
4. **Flush Kafka producer**：把已缓冲的批次发完，单步限时 3s
5. 第 4 步超时或失败的剩余消息**直接走 fallback 文件**，跳过 Kafka 健康判断（停机阶段不再重试 Kafka）
6. Flush 并关闭 fallback writer，确保文件落盘
7. 关闭 Kafka producer

总流程超过 10s 强制退出，未落盘的消息丢失并记录到服务端日志。

客户端为同步直发，无队列与缓冲，业务进程退出即结束，无独立停机流程。

---

## 五、客户端 SDK 设计

### 5.1 设计原则

- **极简 API**：一行代码完成日志记录
- **同步直发**：每次调用直接通过 TCP 长连接发送，不做本地缓冲、批量、重试
- **失败可见**：发送失败通过业务方注入的 `*slog.Logger` 输出到本地日志
- **多连接分片**：上限 `max_conns` 条 TCP 长连接（默认 4），按 `trace_id` 哈希分槽；shard 槽对象懒建，初始没有任何连接，命中时才 dial；空 `trace_id` 时全部落到 shard 0
- **按需重连**：连接断开后置空，下次命中该槽时重新 dial

### 5.2 核心组件

| 组件                     | 职责                                                     |
| ------------------------ | -------------------------------------------------------- |
| **Client**               | 主入口，持有 N 个 shard、生命周期管理                    |
| **shardConn**            | 单个分片：一条 TCP 连接 + 一把 mutex；nil 表示未建或已断 |
| **Inbound HTTP Logger**  | 封装 inbound HTTP 日志参数和发送                         |
| **Outbound HTTP Logger** | 封装 outbound HTTP 日志参数和发送                        |
| **Event Tracker**        | 封装事件埋点参数和发送                                   |
| **RPC Logger**           | 封装 RPC 日志参数和发送                                  |
| **App Logger**           | 封装应用日志参数和发送                                   |

### 5.3 发送流程

每次记录日志的执行路径：

1. 构造统一信封（见 PROTOCOL.md §1.2），自动注入 `service / host / timestamp / trace_id`
2. 序列化为 JSON
3. 选 shard：`idx = hash(trace_id) % max_conns`；`trace_id` 为空时 `idx=0`
4. 拿该 shard 的 mutex（串行化建连与写帧）
5. 若该 shard `conn == nil`：执行 `net.DialTimeout(addr, connect_timeout)`，失败则记 slog 返回
6. 设置 `write_timeout` 写入 `4 字节大端长度 + JSON body`
7. 写入失败 → 关连接、置 `conn = nil`、记 slog 返回（不重试，下次调用会重建）
8. 释放锁

**断连检测**：被动模式，仅在 Write 返回 error 或超时时识别。SDK 不读响应、不发心跳。

**并发模型**：每个 shard 内串行（mutex），shard 之间并行；由 `trace_id` 哈希自然散列。

发送是同步阻塞的，调用方需自行评估对业务路径的影响（建议放在异步上下文，如 RPC 拦截器、HTTP middleware 的 `defer` 中）。

### 5.4 发送失败处理

SDK 不依赖服务端 ACK，遇到以下情况统一通过业务方传入的 `*slog.Logger` 记录到本地日志，不做重试，不阻塞业务：

| 场景         | 级别       | stage 字段值 |
| ------------ | ---------- | ------------ |
| 序列化失败   | slog.Error | `serialize`  |
| dial 失败    | slog.Warn  | `dial`       |
| TCP 写入失败 | slog.Warn  | `write`      |
| 写入超时     | slog.Warn  | `write`      |

**失败日志字段**：

| 字段     | 说明                           |
| -------- | ------------------------------ |
| stage    | `serialize` / `dial` / `write` |
| topic    | 目标 topic                     |
| service  | 配置中的 ServiceName           |
| trace_id | 信封 trace_id（如有）          |
| shard    | 选中的 shard 索引（0 ~ N-1）   |
| size     | 序列化后字节数                 |
| err      | 具体错误                       |

slog 实例由业务方在 `Init` 时通过 `Config.Logger` 注入，未注入则使用 `slog.Default()`。

### 5.5 API 设计

API 分为 option 形式（底层）与 ctx 形式（便捷）两套，详见 [`PROTOCOL.md`](./PROTOCOL.md) §二「SDK 字段注入」。

```go
// 初始化
logtrack.Init(&Config{
    GatewayAddr: "log-track:9583",
    ServiceName: "loan-backend",
    Logger:      slog.Default(), // 可选，默认 slog.Default()
})

// 通用发送：投递到任意 topic
// 业务字段（app_id/country/bid 等）直接放进 data
logtrack.Send("custom-business-events", map[string]any{
    "app_id":  123,
    "country": "id",
    "biz_id":  "order_123",
    "stage":   "paid",
}, logtrack.WithTraceID(traceID))

// helpers 是 Send 的快捷封装，分别对应内置 topic
logtrack.InboundHTTP(&InboundHTTPLog{   // → topic=inbound-http-logs
    Method:         "GET",
    URL:            "/api/user/123",
    ClientIP:       "1.2.3.4",
    ResponseStatus: 200,
    DurationMs:     45,
})

logtrack.OutboundHTTP(&OutboundHTTPLog{ // → topic=outbound-http-logs
    Provider:       "stripe",
    Method:         "POST",
    URL:            "https://api.stripe.com/v1/charges",
    ResponseStatus: 200,
    DurationMs:     320,
})

logtrack.Event(&Event{    // → topic=event-tracks
    AppID:      req.AppID,
    Country:    req.Country,
    BID:        "mx01",
    Name:       "loan.apply_submitted",
    UserID:     789,
    Platform:   "android",
    AppVersion: "3.14.2",
    Properties: map[string]any{"loan_id": "L_10086", "amount": 5000000},
}, logtrack.WithTraceID(traceID))

logtrack.RPC(&RPCLog{     // → topic=rpc-calls
    Caller:     "api-gateway",
    Callee:     "user-service",
    Method:     "/user.User/Get",
    DurationMs: 45,
})

logtrack.Error("failed to query database", // → topic=app-logs
    logtrack.String("user_id", "123"),
    logtrack.String("sql", "SELECT ..."),
)
```

各 helper 的字段定义见 [`PROTOCOL.md`](./PROTOCOL.md) §三。

### 5.6 配置项

| 配置项          | 建议值 | 说明                                    |
| --------------- | ------ | --------------------------------------- |
| gateway_addr    | log-track:9583 | Gateway 地址（默认值，可通过 Config.GatewayAddr 覆盖） |
| max_conns       | 4      | 最多 TCP 连接数（shard 数量），按需懒建 |
| connect_timeout | 3s     | 建立 TCP 连接超时                       |
| write_timeout   | 1s     | 单次消息写入超时                        |
| logger          | nil    | slog.Logger，默认 Default               |

---

## 六、部署方案

### 6.1 服务端部署

| 项目     | 建议                         |
| -------- | ---------------------------- |
| 部署方式 | 独立进程，2-4 个实例         |
| 负载均衡 | L4 负载均衡器（NLB/HAProxy） |
| 资源需求 | 2C4G 支撑约 5-10w QPS        |
| 存储     | 降级文件需要持久化存储       |
| 容器化   | 支持 Docker/K8s Deployment   |

### 6.2 客户端集成

| 项目       | 建议               |
| ---------- | ------------------ |
| 集成方式   | Go mod 依赖        |
| 初始化位置 | main 函数          |
| 配置注入   | 环境变量或配置中心 |

### 6.3 整体拓扑

```
                    ┌─────────────┐
                    │   L4 LB     │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │ Gateway 1 │   │ Gateway 2 │   │ Gateway 3 │
    └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
          │               │               │
          └───────────────┼───────────────┘
                          │
                          ▼
                   ┌───────────┐
                   │  Kafka    │
                   └───────────┘
```

---

## 七、监控与告警

### 7.1 关键指标

| 指标                 | 说明                          | 告警阈值         |
| -------------------- | ----------------------------- | ---------------- |
| gateway.connections  | 当前 TCP 连接数               | > 80% 最大连接   |
| gateway.message_rate | 消息处理速率（按 topic 分组） | 突降 > 50%       |
| gateway.queue_size   | 消息队列深度                  | > 80% 容量       |
| kafka.write_latency  | Kafka 写入延迟                | > 1s             |
| kafka.write_errors   | Kafka 写入错误率              | > 1%             |
| fallback.file_count  | 降级文件数量                  | > 5              |
| fallback.file_age    | 降级文件未处理时长            | > 1h             |
| client.send_errors   | 客户端发送失败数              | > 0（ERROR级别） |

### 7.2 日志采样建议

| 类型           | 采样策略           |
| -------------- | ------------------ |
| inbound HTTP   | 全量（核心数据）   |
| outbound HTTP  | 全量               |
| 事件埋点       | 全量               |
| RPC 日志       | 错误全量 + 1% 采样 |
| 应用日志 ERROR | 全量               |
| 应用日志 INFO  | 1% 采样或关闭      |

---

## 八、风险与应对

| 风险             | 影响             | 应对措施                      |
| ---------------- | ---------------- | ----------------------------- |
| Gateway 单点故障 | 日志丢失         | 多实例 + L4 负载均衡          |
| Kafka 故障       | 日志无法写入     | 降级到本地文件，恢复后补发    |
| 网络分区         | 客户端无法发送   | 发送失败记录到 slog           |
| 磁盘写满         | 降级文件无法写入 | 监控告警 + 自动轮转删除       |
| 业务调用阻塞     | 同步发送拖慢业务 | write_timeout=1s 限制最长阻塞 |
| 消息量突增       | Gateway 过载     | 连接数限制 + 监控告警         |

---

## 九、License

本项目采用 [MIT License](./LICENSE)。
