# LogTrack 代码结构

Go 项目目录约定、module 划分、依赖关系。设计文档见 [`README.md`](./README.md)，协议字段见 [`PROTOCOL.md`](./PROTOCOL.md)。

---

## 一、Module 划分

仓库根 path：`github.com/dongfenghulian/log-track`

仓库内有两个独立 module：

| Module                                             | 路径               | 用途                       | 业务方是否依赖 |
| -------------------------------------------------- | ------------------ | -------------------------- | -------------- |
| `github.com/dongfenghulian/log-track`              | 仓库根             | Gateway（服务端）          | 否             |
| `github.com/dongfenghulian/log-track/pkg/logtrack` | `pkg/logtrack/`    | 客户端 SDK + 共享 envelope | 是             |

**双 module 的关键收益**：业务方 `go get github.com/dongfenghulian/log-track/pkg/logtrack` 时，**不会**拉到 server 端代码（`internal/`、`cmd/`）的间接依赖（kafka-go、prometheus 等），SDK 依赖纯净。

**envelope 归属**：`pkg/logtrack/envelope/` 在 SDK module 内。Server 反向依赖 SDK module 的 envelope 子包（server 本就要懂协议）。SDK 自给自足，无 `replace` 指令。

---

## 二、目录结构

```
log-track/
├── go.mod                          # module: github.com/dongfenghulian/log-track
├── go.sum
├── README.md
├── PROTOCOL.md
├── STRUCTURE.md                    # 本文件
│
├── cmd/
│   └── gateway/
│       └── main.go                 # gateway 启动入口；blank import internal/handler 触发自动注册
│
├── internal/                       # 仅 gateway 使用，业务方无法 import
│   ├── server/
│   │   ├── tcp.go                  # TCP listener、连接管理、最大连接数限制
│   │   ├── conn.go                 # 单连接读循环、frame 解码、入队
│   │   └── shutdown.go             # 优雅停机编排（SIGTERM → flush → exit）
│   ├── queue/
│   │   └── queue.go                # 内存队列 + worker pool
│   ├── router/
│   │   ├── router.go               # 全局 registry：map[topic]Handler，按 topic 分发
│   │   └── handler.go              # Handler interface 定义
│   ├── handler/
│   │   ├── handlers.go             # 聚合包：blank import 各 handler 子包，触发 init() 自动注册
│   │   ├── inbound_http/
│   │   │   └── handler.go          # init() 调 router.Register("inbound-http-logs", ...)
│   │   ├── outbound_http/
│   │   │   └── handler.go
│   │   ├── event/
│   │   │   └── handler.go
│   │   ├── rpc/
│   │   │   └── handler.go
│   │   ├── app_log/
│   │   │   └── handler.go
│   │   └── passthrough/
│   │       └── handler.go          # 未命中时的兜底（路由层默认调用，不进 registry）
│   ├── writer/
│   │   ├── manager.go              # Writer Manager：Kafka 健康时走 Kafka，否则走 fallback
│   │   ├── kafka.go                # segmentio/kafka-go 封装：批量、健康检查、停机 flush
│   │   └── fallback.go             # 本地文件滚动写入 + 后台补发协程
│   ├── config/
│   │   └── config.go               # 环境变量加载、默认值；统一前缀 LOG_TRACK_
│   └── metrics/
│       └── metrics.go              # Prometheus 指标
│
└── pkg/
    └── logtrack/                   # 独立 module: .../pkg/logtrack
        ├── go.mod
        ├── go.sum
        ├── client.go               # Client、shardConn、Init/Close
        ├── send.go                 # Send / SendCtx 通用入口
        ├── http.go                 # InboundHTTP / OutboundHTTP + struct 定义
        ├── event.go                # Event / EventCtx + Event struct
        ├── rpc.go                  # RPC + RPCLog struct
        ├── app_log.go              # Error / Warn / Info / Debug
        ├── option.go               # WithTraceID、ctx key、Option 类型
        ├── frame.go                # 4 字节大端长度前缀编码（与 server 对端共用约定）
        └── envelope/
            └── envelope.go         # Envelope struct、topic 常量
```

---

## 三、Handler 自动注册机制

### 3.1 注册流程

```go
// internal/router/router.go
package router

type Handler interface {
    Topic() string
    Handle(env *envelope.Envelope) error
}

var registry = map[string]Handler{}

func Register(topic string, h Handler) {
    if _, exists := registry[topic]; exists {
        panic("logtrack: duplicate handler for topic: " + topic)
    }
    registry[topic] = h
}

func Lookup(topic string) (Handler, bool) {
    h, ok := registry[topic]
    return h, ok
}
```

```go
// internal/handler/inbound_http/handler.go
package inbound_http

import (
    "github.com/dongfenghulian/log-track/internal/router"
    "github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
    router.Register(envelope.TopicInboundHTTPLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicInboundHTTPLogs }
func (h *Handler) Handle(env *envelope.Envelope) error { /* 转发 writer */ }
```

### 3.2 聚合包（关键）

```go
// internal/handler/handlers.go
package handler

import (
    _ "github.com/dongfenghulian/log-track/internal/handler/inbound_http"
    _ "github.com/dongfenghulian/log-track/internal/handler/outbound_http"
    _ "github.com/dongfenghulian/log-track/internal/handler/event"
    _ "github.com/dongfenghulian/log-track/internal/handler/rpc"
    _ "github.com/dongfenghulian/log-track/internal/handler/app_log"
)
```

```go
// cmd/gateway/main.go
import (
    _ "github.com/dongfenghulian/log-track/internal/handler"  // 触发所有 handler init()
    // ...
)
```

加新内置 handler 时只改 `internal/handler/handlers.go` 增加一行 blank import，main 不动。

---

## 四、配置（环境变量）

统一前缀 `LOG_TRACK_`，全部有默认值，启动时由 `internal/config` 读取。

### 4.1 服务端

| 环境变量                              | 默认值           | 说明                                 |
| ------------------------------------- | ---------------- | ------------------------------------ |
| `LOG_TRACK_SERVER_ADDRESS`            | `:9583`          | TCP 监听地址                         |
| `LOG_TRACK_SERVER_MAX_CONNECTIONS`    | `3000`           | 最大连接数                           |
| `LOG_TRACK_SERVER_QUEUE_SIZE`         | `5000`           | 普通消息队列大小                     |
| `LOG_TRACK_SERVER_WORKER_COUNT`       | `30`             | 普通队列 Worker 数量                 |
| `LOG_TRACK_SERVER_CRITICAL_QUEUE_SIZE`| `2000`           | critical 队列大小（event-tracks）     |
| `LOG_TRACK_SERVER_CRITICAL_WORKER_COUNT` | `10`          | critical 队列 Worker 数量             |
| `LOG_TRACK_SERVER_MAX_MESSAGE_SIZE`   | `10485760`       | 单消息上限（字节，10MB）             |
| `LOG_TRACK_KAFKA_BROKERS`             | `kafka:9092`     | Kafka 集群地址，多个用逗号分隔       |
| `LOG_TRACK_KAFKA_BATCH_SIZE`          | `100`            | 批量发送数量                         |
| `LOG_TRACK_KAFKA_BATCH_TIMEOUT`       | `10ms`           | 批量发送间隔                         |
| `LOG_TRACK_KAFKA_WRITE_TIMEOUT`       | `2s`             | Kafka 写入超时                       |
| `LOG_TRACK_FALLBACK_DATA_DIR`         | `/data/logtrack` | 降级文件目录                         |
| `LOG_TRACK_FALLBACK_MAX_FILE_SIZE`    | `104857600`      | 单文件最大字节数（100MB）            |
| `LOG_TRACK_FALLBACK_MAX_FILES`        | `10`             | 最大文件数量                         |
| `LOG_TRACK_SHUTDOWN_TIMEOUT`          | `5s`             | 优雅停机总超时                       |
| `LOG_TRACK_SHUTDOWN_CONN_READ_TIMEOUT`| `3s`             | 关 listener 后已有连接的最长读取时间 |
| `LOG_TRACK_SHUTDOWN_KAFKA_FLUSH_TIMEOUT` | `3s`          | 停机阶段 Kafka flush 单步超时        |

### 4.2 客户端 SDK

SDK 不直接读环境变量（业务方进程的环境不归 SDK 管），通过 `Config` struct 传入。但以下两个变量例外：

| 环境变量                       | 默认值 | 说明                                              |
| ------------------------------ | ------ | ------------------------------------------------- |
| `LOG_TRACK_HTTP_BODY_SIZE`     | `1024` | inbound/outbound HTTP body 截断阈值（字节）       |

业务方需要覆盖时设置环境变量即可，无需改代码。

---

## 五、第三方依赖

### 5.1 Server 端（root module）

| 依赖                                  | 用途                          |
| ------------------------------------- | ----------------------------- |
| `github.com/segmentio/kafka-go`       | Kafka 生产者                  |
| `github.com/prometheus/client_golang` | Prometheus 指标               |
| 标准库 `log/slog`                     | 服务端日志                    |
| 标准库 `net`                          | TCP listener                  |

### 5.2 SDK module

依赖最小化，只用标准库：

| 依赖                | 用途                  |
| ------------------- | --------------------- |
| `encoding/json`     | 信封序列化            |
| `encoding/binary`   | 帧长度前缀            |
| `log/slog`          | 失败日志              |
| `net`               | TCP 客户端            |
| `sync`              | shard mutex           |
| `hash/fnv` 或类似   | trace_id 哈希分槽     |

---

## 六、构建与运行

### 6.1 Gateway

```bash
go build -o bin/gateway ./cmd/gateway
LOG_TRACK_SERVER_ADDRESS=:9583 LOG_TRACK_KAFKA_BROKERS=kafka1:9092,kafka2:9092 ./bin/gateway
```

### 6.2 SDK 用法（业务方）

```bash
go get github.com/dongfenghulian/log-track/pkg/logtrack
```

```go
import "github.com/dongfenghulian/log-track/pkg/logtrack"

logtrack.Init(&logtrack.Config{
    GatewayAddr: "log-track:9583",
    ServiceName: "loan-backend",
})
defer logtrack.Close()

logtrack.InboundHTTP(&logtrack.InboundHTTPLog{
    Method:         "GET",
    URL:            "/api/user/123",
    ResponseStatus: 200,
    DurationMs:     45,
})
```

---

## 七、命名约定

- 包名：全小写、单词、不带下划线（标准 Go 风格）。例外：`inbound_http` / `outbound_http` / `app_log` 因为单词组合需要分隔，可保留下划线（包名只用于 import 路径，不是标识符）。
- topic 常量：`envelope.TopicInboundHTTPLogs / TopicOutboundHTTPLogs / TopicEventTracks / TopicRPCCalls / TopicAppLogs`，值与 PROTOCOL.md 一致（`inbound-http-logs` 等）。`TopicAppLogs` 是 SDK 发送时的逻辑 topic，gateway 内部按 level 分流为 `TopicAppLogsError / TopicAppLogsWarn / TopicAppLogsInfo / TopicAppLogsDebug` 之一写入 Kafka。
- 环境变量：`LOG_TRACK_<DOMAIN>_<NAME>`（全大写、下划线分隔）。
- 公开 struct：与 PROTOCOL.md 字段名 PascalCase 化（`AppID / DurationMs / RequestBody`）；JSON tag 用文档中的 snake_case。
