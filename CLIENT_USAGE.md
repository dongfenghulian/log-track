# LogTrack 客户端 SDK 使用文档

业务方接入指南。

- 协议字段定义：[`PROTOCOL.md`](./PROTOCOL.md)
- 整体架构：[`README.md`](./README.md)

---

## 一、安装

```bash
go get github.com/dongfenghulian/log-track/pkg/logtrack
```

SDK 是独立 module，**只依赖 Go 标准库**，不会拉 kafka/prometheus 等任何第三方依赖。

---

## 二、初始化

`main` 函数里调一次 `Init`，进程退出前调 `Close`。

```go
package main

import (
	"log/slog"
	"os"

	"github.com/dongfenghulian/log-track/pkg/logtrack"
)

func main() {
	if err := logtrack.Init(&logtrack.Config{
		GatewayAddr: os.Getenv("LOGTRACK_GATEWAY"), // 留空则用默认 "log-track:9583"
		ServiceName: "loan-backend",                  // 后端服务名
		Logger:      slog.Default(),                  // 可选;失败时通过它记录到本地
	}); err != nil {
		slog.Error("logtrack init failed", "err", err)
		os.Exit(1)
	}
	defer logtrack.Close()

	// ... 你的业务代码 ...
}
```

### Config 字段

| 字段             | 必填 | 默认值          | 说明                                                    |
| ---------------- | ---- | --------------- | ------------------------------------------------------- |
| `GatewayAddr`    | 否   | `log-track:9583`| LogTrack Gateway 的 `host:port`                         |
| `ServiceName`    | 是   | -               | 你的服务名，会写入信封 `service` 字段                   |
| `MaxConns`       | 否   | 4               | 每个连接池的 TCP 长连接数（event-tracks 和普通 topic 各一组，按 trace_id 哈希分槽，懒建） |
| `ConnectTimeout` | 否   | 3s              | 建立 TCP 连接超时                                       |
| `WriteTimeout`   | 否   | 1s              | 单次消息写入超时                                        |
| `FailureBackoff` | 否   | 5s              | 普通 topic 发送失败后的重连退避；event-tracks 会持续尝试 |
| `Logger`         | 否   | `slog.Default()`| 发送失败时的本地日志输出                                |

### 环境变量

| 名称                       | 默认 | 说明                                                                  |
| -------------------------- | ---- | --------------------------------------------------------------------- |
| `LOG_TRACK_HTTP_BODY_SIZE` | 1024 | inbound/outbound HTTP 请求/响应 body 大于此值时 SDK 端丢弃 body 字段 |

---

## 三、五个内置 helper

每个 helper 对应一个内置 topic。所有 helper 都是**同步直发**——调用立即返回，发送在调用线程内完成（最长 `WriteTimeout`）。

### 3.1 inbound HTTP（app → 后端接口）

```go
import "github.com/dongfenghulian/log-track/pkg/logtrack"

logtrack.InboundHTTP(&logtrack.InboundHTTPLog{
    AppID:          123,
    Country:        "id",
    BID:            "mx01",
    Method:         "POST",
    URL:            "/api/v1/loan/apply",
    DurationMs:     156,
    ClientIP:       "1.2.3.4",
    RequestBody:    requestBodyMap,
    RequestBodyType: "json",
    RequestBodySize: int64(len(rawRequestBytes)),
    ResponseStatus: 200,
    ResponseBody:   responseBodyMap,
    ResponseBodyType: "json",
    ResponseBodySize: int64(len(rawResponseBytes)),
}, logtrack.WithTraceID(traceID))
```

**典型用法（HTTP middleware）**：

```go
func LogTrackMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()

        // 包一层 ResponseWriter 才能拿到 status 和 body size。
        rec := &recorder{ResponseWriter: w, status: 200}

        // 记录请求 body。注意:有的框架已经消费过 body,你需要自己读出来再 reset。
        var reqBody []byte
        if r.Body != nil {
            reqBody, _ = io.ReadAll(r.Body)
            r.Body = io.NopCloser(bytes.NewReader(reqBody))
        }

        next.ServeHTTP(rec, r)

        logtrack.InboundHTTP(&logtrack.InboundHTTPLog{
            AppID:           getAppID(r),
            Country:         getCountry(r),
            BID:             getBID(r),
            Method:          r.Method,
            URL:             r.URL.String(),
            DurationMs:      time.Since(start).Milliseconds(),
            ClientIP:        r.RemoteAddr,
            RequestBody:     parseJSON(reqBody),
            RequestBodyType: "json",
            RequestBodySize: int64(len(reqBody)),
            ResponseStatus:  rec.status,
            ResponseBody:    parseJSON(rec.body),
            ResponseBodyType: "json",
            ResponseBodySize: int64(len(rec.body)),
        }, logtrack.WithTraceID(getTraceID(r)))
    })
}
```

> body 大于 `LOG_TRACK_HTTP_BODY_SIZE`（默认 1024）时 SDK 自动丢弃 body 字段，但保留 size。

### 3.2 outbound HTTP（后端 → 三方调用）

```go
logtrack.OutboundHTTP(&logtrack.OutboundHTTPLog{
    AppID:          123,
    Country:        "id",
    BID:            "mx01",
    Provider:       "stripe",        // 命名约定见 PROTOCOL.md §3.2.1
    Method:         "POST",
    URL:            "https://api.stripe.com/v1/charges",
    DurationMs:     320,
    RequestBody:    requestBody,
    ResponseStatus: 200,
    ResponseBody:   responseBody,
}, logtrack.WithTraceID(traceID))
```

**典型用法（HTTP client wrapper）**：

```go
func CallStripe(ctx context.Context, body []byte) ([]byte, error) {
    start := time.Now()
    req, _ := http.NewRequestWithContext(ctx, "POST", stripeURL, bytes.NewReader(body))
    req.Header.Set("Authorization", "Bearer "+stripeKey)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        logtrack.OutboundHTTP(&logtrack.OutboundHTTPLog{
            AppID:      getAppID(ctx),
            Country:    getCountry(ctx),
            BID:        getBID(ctx),
            Provider:   "stripe",
            Method:     "POST",
            URL:        stripeURL,
            DurationMs: time.Since(start).Milliseconds(),
            // ResponseStatus = 0 表示连接失败
        }, logtrack.WithTraceID(getTraceID(ctx)))
        return nil, err
    }
    defer resp.Body.Close()

    respBody, _ := io.ReadAll(resp.Body)
    logtrack.OutboundHTTP(&logtrack.OutboundHTTPLog{
        AppID:           getAppID(ctx),
        Country:         getCountry(ctx),
        BID:             getBID(ctx),
        Provider:        "stripe",
        Method:          "POST",
        URL:             stripeURL,
        DurationMs:      time.Since(start).Milliseconds(),
        RequestBody:     parseJSON(body),
        RequestBodyType: "json",
        RequestBodySize: int64(len(body)),
        ResponseStatus:  resp.StatusCode,
        ResponseBody:    parseJSON(respBody),
        ResponseBodyType: "json",
        ResponseBodySize: int64(len(respBody)),
    }, logtrack.WithTraceID(getTraceID(ctx)))

    return respBody, nil
}
```

### 3.3 Event 事件埋点

```go
logtrack.EventTrack(&logtrack.Event{
    BID:         "mx01",
    Country:     "id",
    AppID:       123,
    Name:        "loan.apply_submitted",   // 命名约定见 PROTOCOL.md §3.3.1
    UserID:      789,
    SessionUUID: "sess_xyz",
    DeviceUUID:  "device_abc",
    Platform:    "android",
    AppVersion:  "3.14.2",
    Properties: map[string]any{
        "loan_id":    "L_10086",
        "amount":     5000000,
        "currency":   "IDR",
        "product_id": "p_30days",
    },
}, logtrack.WithTraceID(traceID))
```

> 必填字段：`BID`、`Name`、`Platform`、`AppVersion`。

### 3.4 RPC 调用日志

```go
logtrack.RPC(&logtrack.RPCLog{
    AppID:        123,
    Country:      "id",
    BID:          "mx01",
    Caller:       "api-gateway",
    Callee:       "user-service",
    Method:       "/user.User/Get",
    DurationMs:   45,
    StatusCode:   0,
    RequestSize:  1024,
    ResponseSize: 2048,
}, logtrack.WithTraceID(traceID))
```

> RPC 不记录 request/response body。看 body 用分布式追踪（jaeger 等）。

### 3.5 应用日志

```go
logtrack.App(&logtrack.AppLog{
    AppID:    123,
    Country:  "id",
    BID:      "mx01",
    Level:    "ERROR",       // DEBUG/INFO/WARN/ERROR，必须是这 4 个之一
    Message:  "failed to disburse loan",
    File:     "loan/service.go",
    Line:     88,
    Function: "Disburse",
    Stack:    string(debug.Stack()),
    Fields: map[string]any{
        "loan_id":     "L_10086",
        "user_id":     789,
        "retry_count": 3,
    },
}, logtrack.WithTraceID(traceID))
```

> **按 level 自动分流**：客户端调用方式不变；gateway 端会根据 `Level` 把消息写入不同 Kafka topic：
> `ERROR` → `app-logs-error`，`WARN` → `app-logs-warn`，`INFO` → `app-logs-info`，`DEBUG` → `app-logs-debug`。
> 未知 level 会被拒绝丢弃。
> 业务方需提前在 Kafka 创建好这 4 个 topic（auto-create 开启的话会自动建）。

---

## 四、自定义 topic：`Send`

业务自己定义的 topic 不需要在 LogTrack 服务端注册——直接发就行，gateway 把整个信封透传到同名 Kafka topic。

**前提：业务方需提前在 Kafka 创建好对应 topic。**

```go
logtrack.Send("custom-business-events", map[string]any{
    "app_id":  123,
    "country": "id",
    "bid":     "mx01",
    "biz_id":  "order_123",
    "stage":   "paid",
}, logtrack.WithTraceID(traceID))
```

第一个参数 = Kafka topic 名。第二个参数 = 任意可序列化为 JSON 的 data，会作为信封的 `data` 字段。

---

## 五、Kafka 分区 key

每条消息进入 Kafka 时会带一个 partition key（通过 `Hash` balancer 路由到 partition）。SDK 的取值优先级：

1. **`WithPartitionKey(key)` option（最高优先级）**——业务方显式指定
2. **`WithTraceID(traceID)` option**——未指定 partition key 时回退
3. **空**——`Hash` balancer 随机/轮询分配

例：让同一用户的所有事件落在同一个 partition（消费时保证按时间顺序）：

```go
logtrack.EventTrack(&logtrack.Event{
    Name:       "loan.apply_submitted",
    UserID:     789,
    Platform:   "android",
    AppVersion: "3.14.2",
    BID:        "mx01",
}, logtrack.WithPartitionKey("user-789"), logtrack.WithTraceID(traceID))
```

例：自定义 topic 按业务 id 分区：

```go
logtrack.Send("order-events", data,
    logtrack.WithPartitionKey("order-"+orderID),
    logtrack.WithTraceID(traceID))
```

ctx 形式同样支持：

```go
ctx = logtrack.CtxWithPartitionKey(ctx, "user-789")
ctx = logtrack.CtxWithTraceID(ctx, traceID)

logtrack.EventCtx(ctx, &logtrack.Event{...})
```

---

## 六、ctx 模式

如果你的项目把 `trace_id` / partition key 注入了 ctx，用 `XxxCtx` 系列 helper 自动取出来：

```go
// 在请求入口（middleware）注入
ctx = logtrack.CtxWithTraceID(ctx, traceID)
ctx = logtrack.CtxWithPartitionKey(ctx, "user-789") // 可选

// 后续调用自动从 ctx 拿 trace_id 和 partition key
logtrack.SendCtx(ctx, "custom-topic", data)
logtrack.InboundHTTPCtx(ctx, &logtrack.InboundHTTPLog{...})
logtrack.OutboundHTTPCtx(ctx, &logtrack.OutboundHTTPLog{...})
logtrack.EventCtx(ctx, &logtrack.Event{...})
logtrack.RPCCtx(ctx, &logtrack.RPCLog{...})
logtrack.AppCtx(ctx, &logtrack.AppLog{...})
```

`app_id / country / bid` 等业务字段不走 ctx，仍然在 struct 字段里直接传——你的项目自己用 ctx 把它们带下来即可。

---

## 七、发送失败处理

SDK **不依赖 ACK，不重试**。任何失败都通过 `Config.Logger`（默认 `slog.Default()`）记录到本地日志，**不会阻塞业务**。

| 场景         | 级别       | 说明                                  |
| ------------ | ---------- | ------------------------------------- |
| 序列化失败   | slog.Error | 调用方传入了无法 JSON 序列化的 data    |
| TCP dial 失败 | slog.Warn  | gateway 不可达；下次调用时会重连     |
| TCP 写入失败 | slog.Warn  | gateway 关闭连接或网络异常           |
| 写入超时     | slog.Warn  | 1s 内未写完（超载或慢网络）          |

失败日志包含字段：`stage / topic / service / trace_id / shard / size / err`。

**失败的消息丢失**——这是设计决定（SDK 极简，不做本地缓冲）。SLA 关键的事件请走业务流程的常规可靠机制（DB / 消息队列），LogTrack 是观测系统不是业务系统。

---

## 八、连接模型与并发

- 默认 4 条 TCP 长连接，按 `trace_id` FNV 哈希分槽
- 同一 `trace_id` → 固定走同一条连接（保证同 trace 在 Kafka 单 partition 内顺序）
- 没有 `trace_id` 的调用都走 shard 0
- 每条连接懒建：业务首次命中该 shard 才 dial
- 每条连接断开后下次调用自动重连
- 每个 shard 有自己的 mutex；shard 之间并行

调用是**线程安全**的，可以从任意 goroutine 调用。

---

## 九、性能特征

- 每次调用阻塞约 = 序列化时间 + TCP 写入时间，典型 < 1ms
- 单连接吞吐约 5k-20k QPS（消息小、本地网络）
- 4 条连接合计 20k-80k QPS

如果业务路径上每次都要发日志，建议：

- 放在 RPC 拦截器 / HTTP middleware 末尾
- 或放在 `defer` 里
- 不要放在用户感知的关键耗时段

---

## 十、停机

业务进程退出前调一次 `logtrack.Close()`。它关闭所有 shard 连接。

```go
defer logtrack.Close()
```

SDK 没有"排空队列"的概念（因为不缓冲）。`Close` 之后再调用 helper 都是空操作（不发送、不报错）。

---

## 十一、完整示例

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack"
)

func main() {
	if err := logtrack.Init(&logtrack.Config{
		GatewayAddr: os.Getenv("LOGTRACK_GATEWAY"),
		ServiceName: "loan-backend",
	}); err != nil {
		slog.Error("logtrack init failed", "err", err)
		os.Exit(1)
	}
	defer logtrack.Close()

	mux := http.NewServeMux()
	mux.Handle("/loan/apply", trackInbound(http.HandlerFunc(applyLoan)))

	_ = http.ListenAndServe(":8080", mux)
}

func applyLoan(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 业务事件埋点
	logtrack.EventCtx(ctx, &logtrack.Event{
		BID:        "mx01",
		Country:    "id",
		AppID:      123,
		Name:       "loan.apply_submitted",
		Platform:   r.Header.Get("X-Platform"),
		AppVersion: r.Header.Get("X-App-Version"),
		Properties: map[string]any{"loan_id": "L_10086", "amount": 5000000},
	})

	// 调三方风控
	if err := callRiskProvider(ctx); err != nil {
		logtrack.AppCtx(ctx, &logtrack.AppLog{
			AppID:   123,
			BID:     "mx01",
			Level:   "ERROR",
			Message: "risk check failed",
			Fields:  map[string]any{"err": err.Error()},
		})
		http.Error(w, "internal error", 500)
		return
	}

	w.Write([]byte(`{"ok":true}`))
}

func callRiskProvider(ctx context.Context) error {
	start := time.Now()
	// ... 实际调三方 ...
	logtrack.OutboundHTTPCtx(ctx, &logtrack.OutboundHTTPLog{
		AppID:      123,
		BID:        "mx01",
		Provider:   "risk-shumei",
		Method:     "POST",
		URL:        "https://api.shumei.com/risk/check",
		DurationMs: time.Since(start).Milliseconds(),
		ResponseStatus: 200,
	})
	return nil
}

func trackInbound(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 简化:省略 body 捕获和 status 包装
		start := time.Now()
		next.ServeHTTP(w, r)
		logtrack.InboundHTTP(&logtrack.InboundHTTPLog{
			AppID:          123,
			BID:            "mx01",
			Method:         r.Method,
			URL:            r.URL.String(),
			DurationMs:     time.Since(start).Milliseconds(),
			ClientIP:       r.RemoteAddr,
			ResponseStatus: 200,
		}, logtrack.WithTraceID(r.Header.Get("X-Trace-Id")))
	})
}
```

---

## 十二、FAQ

**Q: SDK 会拉哪些第三方依赖？**
A: 零。SDK 只用 Go 标准库。`go.mod` 没有任何 `require` 行。

**Q: 写日志会不会拖慢我的接口？**
A: 同步写 TCP，典型 < 1ms。受 `WriteTimeout=1s` 上限保护，最坏情况业务接口多 1 秒。建议放在 middleware 末尾或 `defer` 里。

**Q: gateway 挂了我的服务会怎样？**
A: 单次调用最多阻塞 `WriteTimeout`（1s），失败后写 slog 警告，然后业务继续。下次调用会自动重连。

**Q: 重启 gateway 期间消息会丢吗？**
A: 是的。SDK 不缓冲。这部分日志会丢失，但业务不受影响。如果你的事件需要 0 丢失，请走 DB / 业务消息队列，而不是 LogTrack。

**Q: 同一 trace_id 的多条日志能保证顺序到达 Kafka 吗？**
A: 顺序到达 Gateway（同 trace 走同一条 TCP 连接，串行写）。Gateway → Kafka 也用 trace_id 做分区 key（同 partition 内有序）。所以是的，同 trace 在 Kafka 单 partition 内有序。

**Q: 多个进程能用同一个 ServiceName 吗？**
A: 可以。`service` + `host`（自动注入主机名）一起标识来源进程。

**Q: 我自己定义一个 topic 行吗？**
A: 行。在 Kafka 提前建好该 topic，然后用 `logtrack.Send("my-topic", data, ...)`。Gateway 会把整个信封写入这个 topic。
