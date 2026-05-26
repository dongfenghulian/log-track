# LogTrack 协议与字段定义

涵盖：传输帧、信封、各 topic 的 data schema、命名约定、SDK 字段注入方式。Kafka 物理配置（分区/保留时间）见主 README §4.3。

---

## 一、传输协议

### 1.1 帧格式

采用 **Length-Prefixed Frame**，解决 TCP 粘包问题：

```
+----------------+------------------+
|    4 bytes     |    N bytes       |
|  消息长度 (N)  |   消息体 (JSON)  |
+----------------+------------------+
```

- 长度字段：4 字节，大端序（Big Endian）
- 消息体：UTF-8 编码的 JSON 字符串
- 最大消息体：10MB（可配置）

### 1.2 信封

所有消息使用统一的信封结构：

```json
{
  "version": "1.0",
  "topic": "inbound-http-logs",
  "service": "loan-backend",
  "host": "pod-xxx",
  "timestamp": 1704067200000,
  "trace_id": "abc-123-def",
  "partition_key": "user-789",
  "data": { ... }
}
```

| 字段          | 类型   | 必填 | 说明                                                                       |
| ------------- | ------ | ---- | -------------------------------------------------------------------------- |
| version       | string | 是   | 协议版本号                                                                 |
| topic         | string | 是   | 目标 Kafka topic 名，命中内置 handler 时走 handler 处理逻辑                |
| service       | string | 是   | 后端服务名（进程标识）                                                     |
| host          | string | 是   | 来源主机名/Pod 名                                                          |
| timestamp     | int64  | 是   | 毫秒级时间戳                                                               |
| trace_id      | string | 否   | 链路追踪 ID                                                                |
| partition_key | string | 否   | Kafka 分区 key；未提供时 Gateway 回退到 trace_id；都没有则消息均匀分布   |
| data          | object | 是   | 具体业务数据                                                               |

`app_id`、`country` 等业务身份字段不在信封内，由各 topic 的 data 自行定义（见 §三）。

### 1.3 版本兼容

服务端仅接受 `version=1.0` 的消息，其他版本直接丢弃并记录到服务端日志，不返回错误（无 ACK 机制）。

### 1.4 Topic 路由

- 命中内置 handler（topic 为 `inbound-http-logs / outbound-http-logs / event-tracks / rpc-calls / app-logs`）：走 handler 校验后写入对应 topic
- 未命中：将完整信封 JSON 直接写入名为 `topic` 字段值的 Kafka topic，由业务方自行确保该 topic 已在 Kafka 创建

---

## 二、SDK 字段注入

SDK 只为信封字段提供注入工具，业务字段（`app_id` / `country` / `bid` / `user_id` ...）直接在 helper 的 struct 或 `Send` 的 data map 里传，不走 option。

**信封字段注入**：

| 字段      | 注入方式                                |
| --------- | --------------------------------------- |
| service   | `Init` 时 `Config.ServiceName` 配置一次 |
| host      | SDK 自动从系统读取                      |
| timestamp | SDK 调用时自动取当前毫秒                |
| version   | SDK 固定 `1.0`                          |
| topic     | helper 内部固定 / `Send` 第一参数       |
| trace_id  | 通过 `WithTraceID` option 或 ctx 注入   |

**option 形式**：

```go
logtrack.Event(&Event{
    AppID:      123,
    Country:    "id",
    BID:        "mx01",
    Name:       "loan.apply_submitted",
    UserID:     789,
    Platform:   "android",
    AppVersion: "3.14.2",
}, logtrack.WithTraceID(traceID))

logtrack.Send("event-tracks", map[string]any{
    "app_id":     123,
    "country":    "id",
    "bid":        "mx01",
    "event_name": "loan.apply_submitted",
}, logtrack.WithTraceID(traceID))
```

**ctx 版本**：从 ctx 自动取 `trace_id`，转调 option 形式。适合 middleware 一次写入、深层调用自动传递的场景。

```go
ctx = logtrack.WithTraceID(ctx, traceID)

logtrack.SendCtx(ctx, "event-tracks", data)
logtrack.EventCtx(ctx, &Event{...})
```

业务方需要把 `app_id / country / bid` 等沿调用链带下去时，用项目自身的 ctx key 即可，与 LogTrack SDK 解耦。

---

## 三、内置 topic 与 data schema

### 3.1 inbound-http-logs（app → 后端）

**用途**：接口监控、性能分析、排障。app 端请求量大，单独占 topic 便于横向扩 Kafka 分区与下游消费组。

**body 截断**：请求/响应 body 大于 `LOG_TRACK_HTTP_BODY_SIZE`（默认 1024 字节）时，SDK 直接丢弃 `request_body` / `response_body` 字段（仍保留 `*_body_size`），不发送到 Gateway。Gateway 不做二次校验。

**data 结构**：

```json
{
  "app_id": 123,
  "country": "id",
  "bid": "mx01",
  "method": "POST",
  "url": "https://api.example.com/v1/user/login",
  "duration_ms": 156,
  "client_ip": "1.2.3.4",
  "request_headers": {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
    "X-Request-Id": "abc-123-def",
    "Authorization": "Bearer xxx"
  },
  "request_body_type":"json",
  "request_body": {
    "username": "test@example.com",
    "password": "******"
  },
  "request_body_size": 64,
  "response_status": 200,
  "response_headers": {
    "Content-Type": "application/json"
  },
  "response_body": {
    "code": 0,
    "message": "success",
    "data": { "user_id": "12345", "token": "eyJhbGciOi..." }
  },
  "response_body_type":"json",
  "response_body_size": 256,
  "extra": {}
}
```

| 字段               | 类型   | 必填 | 说明                                                                                     |
| ------------------ | ------ | ---- | ---------------------------------------------------------------------------------------- |
| app_id             | int    | 否   | 业务身份；系统任务/未知来源时省略                                                        |
| country            | string | 否   | 国家代码（小写，如 `mx` / `id` / `bd`）                                                  |
| bid                | string | 否   | 业务线                                                                                   |
| method             | string | 是   | HTTP 方法                                                                                |
| url                | string | 是   | 请求 URL                                                                                 |
| duration_ms        | int64  | 是   | 请求耗时（毫秒）                                                                         |
| client_ip          | string | 否   | 发起客户端 IP                                                                            |
| request_headers    | any    | 否   | 请求头                                                                                   |
| request_body       | any    | 否   | 请求体；按请求头里 Content-Type 判断：json → JSON 对象；text/form → string；二进制不记录 |
| request_body_type  | string | 否   | 请求体类型；按请求头里 Content-Type 判断：json → JSON 对象；text/form → string；         |
| request_body_size  | int64  | 否   | 请求体大小（字节）                                                                       |
| response_status    | int    | 是   | HTTP 状态码                                                                              |
| response_headers   | object | 否   | 响应头                                                                                   |
| response_body      | any    | 否   | 响应体；按响应头里 Content-Type 判断，规则同 request_body                                |
| response_body_type | string | 否   | 响应体类型；按响应头里 Content-Type 判断，规则同 request_body_type                       |
| response_body_size | int64  | 否   | 响应体大小（字节）                                                                       |
| extra              | object | 否   | 扩展字段                                                                                 |

**采集策略**：全量。

---

### 3.2 outbound-http-logs（后端 → 三方）

**用途**：监控对接的三方服务（支付、风控、adjust 等）调用质量。

**body 截断**：同 inbound-http-logs。

**data 结构**：

```json
{
  "app_id": 123,
  "country": "id",
  "bid": "mx01",
  "provider": "stripe",
  "method": "POST",
  "url": "https://api.stripe.com/v1/charges",
  "duration_ms": 320,
  "request_headers": { "Authorization": "Bearer sk_xxx" },
  "request_body": { "amount": 5000, "currency": "usd" },
  "request_body_type":"json",
  "request_body_size": 64,
  "response_status": 200,
  "response_headers": { "Content-Type": "application/json" },
  "response_body": { "id": "ch_xxx", "status": "succeeded" },
  "response_body_type":"json",
  "response_body_size": 128,
  "extra": {}
}
```

| 字段               | 类型   | 必填 | 说明                                                                             |
| ------------------ | ------ | ---- | -------------------------------------------------------------------------------- |
| app_id             | int    | 否   | 业务身份；系统任务/未知来源时省略                                                |
| country            | string | 否   | 国家代码（小写，如 `mx` / `id` / `bd`）                                          |
| bid                | string | 否   | 业务线                                                                           |
| provider           | string | 是   | 三方名（命名约定见 §3.2.1）                                                      |
| method             | string | 是   | HTTP 方法                                                                        |
| url                | string | 是   | 请求 URL                                                                         |
| duration_ms        | int64  | 是   | 请求耗时（毫秒）                                                                 |
| request_headers    | any    | 否   | 请求头                                                                           |
| request_body       | any    | 否   | 请求体；规则同 inbound-http-logs                                                 |
| request_body_type  | string | 否   | 请求体类型；按请求头里 Content-Type 判断：json → JSON 对象；text/form → string； |
| request_body_size  | int64  | 否   | 请求体大小（字节）                                                               |
| response_status    | int    | 是   | HTTP 状态码                                                                      |
| response_headers   | object | 否   | 响应头                                                                           |
| response_body      | any    | 否   | 响应体；规则同 inbound-http-logs                                                 |
| response_body_type | string | 否   | 响应体类型；按响应头里 Content-Type 判断，规则同 request_body_type               |
| response_body_size | int64  | 否   | 响应体大小（字节）                                                               |
| extra              | object | 否   | 扩展字段                                                                         |

**采集策略**：全量。

#### 3.2.1 provider 命名约定

不维护枚举清单，业务方按以下规则填写：

- 全小写
- 多词用连字符 `-` 分隔
- 不带后缀（`stripe` 不是 `stripe-pay`、不是 `Stripe`、不是 `STRIPE`）
- 同一三方在所有项目里保持同名（建议团队内部维护一份对照清单）

示例：`stripe / adjust / risk-shumei / kyc-jumio / sms-twilio`

下游若发现拼写不一致，由 ETL / ClickHouse 视图统一规范化（`lower(provider)` + 别名映射），不在 SDK 层做。

---

### 3.3 event-tracks（事件埋点）

**用途**：用户行为分析、业务转化。

**data 结构**：

```json
{
  "bid": "mx01",
  "country": "id",
  "app_id": 123,
  "event_name": "loan.apply_submitted",
  "user_id": 789,
  "session_uuid": "sess_xyz",
  "device_uuid": "device_abc",
  "platform": "android",
  "app_version": "3.14.2",
  "properties": {
    "loan_id": "L_10086",
    "amount": 5000000,
    "currency": "IDR",
    "product_id": "p_30days"
  },
  "extra": {}
}
```

| 字段         | 类型   | 必填 | 说明                                                            |
| ------------ | ------ | ---- | --------------------------------------------------------------- |
| bid          | string | 是   | 业务线                                                          |
| country      | string | 否   | 国家代码（小写，如 `mx` / `id` / `bd`）                         |
| app_id       | int    | 否   | 业务身份。多 app 共用后端时由调用方按请求传入；纯后端任务可省略 |
| event_name   | string | 是   | 事件名称（命名约定见 §3.3.1）                                   |
| user_id      | int    | 否   | 用户 ID                                                         |
| session_uuid | string | 否   | 会话 ID                                                         |
| device_uuid  | string | 否   | 设备 ID                                                         |
| platform     | string | 是   | `ios` / `android` / `web` / `h5`                                |
| app_version  | string | 是   | 客户端版本号                                                    |
| properties   | object | 否   | 事件属性（约定见 §3.3.1）                                       |
| extra        | object | 否   | 扩展字段                                                        |

**采集策略**：全量。

#### 3.3.1 event_name 命名约定

格式 `domain.action`：全小写、多词用 `_` 分隔、动词用过去时表示完成态。

**`app.*`** 应用生命周期：

| event_name       | 时机     |
| ---------------- | -------- |
| `app.launch`     | 冷启动   |
| `app.foreground` | 切到前台 |
| `app.background` | 切到后台 |

**`auth.*`** 登录注册：

| event_name                | 时机           |
| ------------------------- | -------------- |
| `auth.register_submitted` | 提交注册       |
| `auth.otp_sent`           | 发送验证码     |
| `auth.otp_verified`       | 验证码校验通过 |
| `auth.login_succeeded`    | 登录成功       |
| `auth.login_failed`       | 登录失败       |

**`kyc.*`** 实名认证：

| event_name               | 时机         |
| ------------------------ | ------------ |
| `kyc.submitted`          | 提交资料     |
| `kyc.approved`           | 实名通过     |
| `kyc.rejected`           | 实名拒绝     |
| `kyc.face_verify_failed` | 人脸识别失败 |

**`loan.*`** 贷款流程：

| event_name                 | 时机          |
| -------------------------- | ------------- |
| `loan.apply_submitted`     | 提交贷款申请  |
| `loan.approved`            | 风控/审核通过 |
| `loan.rejected`            | 风控/审核拒绝 |
| `loan.disbursed`           | 放款成功      |
| `loan.disburse_failed`     | 放款失败      |
| `loan.repayment_scheduled` | 还款计划生成  |
| `loan.repayment_succeeded` | 还款成功      |
| `loan.repayment_failed`    | 还款失败      |

**`payment.*`** 通用支付（区别于贷款还款）：

| event_name             | 时机         |
| ---------------------- | ------------ |
| `payment.method_added` | 添加支付方式 |
| `payment.charged`      | 扣款成功     |
| `payment.refunded`     | 退款成功     |

**`push.*`** 推送：

| event_name      | 时机           |
| --------------- | -------------- |
| `push.received` | 客户端收到推送 |
| `push.opened`   | 用户点开推送   |

**`risk.*`** 风控（如业务侧需埋点）：

| event_name      | 时机         |
| --------------- | ------------ |
| `risk.rule_hit` | 命中风控规则 |
| `risk.blocked`  | 被风控阻断   |

**properties 常用 key**：

| 业务域      | 常用 properties keys                                                              |
| ----------- | --------------------------------------------------------------------------------- |
| `loan.*`    | `loan_id` / `amount` / `currency` / `product_id` / `risk_score` / `reject_reason` |
| `payment.*` | `payment_id` / `amount` / `currency` / `method` / `provider`                      |
| `auth.*`    | `method`（`phone` / `email` / `oauth-google` ...）/ `reason`                      |
| `kyc.*`     | `step` / `reject_reason`                                                          |
| `push.*`    | `notification_id` / `template_id`                                                 |

---

### 3.4 rpc-calls（RPC 调用日志）

**用途**：服务间调用监控、依赖拓扑、链路追踪。

**data 结构**：

```json
{
  "app_id": 123,
  "country": "id",
  "bid": "mx01",
  "caller": "api-gateway",
  "callee": "user-service",
  "method": "/user.User/Get",
  "duration_ms": 45,
  "status_code": 0,
  "error": "",
  "request_size": 1024,
  "response_size": 2048,
  "extra": {}
}
```

| 字段          | 类型   | 必填 | 说明                                    |
| ------------- | ------ | ---- | --------------------------------------- |
| app_id        | int    | 否   | 业务身份；系统任务/未知来源时省略       |
| country       | string | 否   | 国家代码（小写，如 `mx` / `id` / `bd`） |
| bid           | string | 否   | 业务线                                  |
| caller        | string | 是   | 调用方服务名                            |
| callee        | string | 是   | 被调方服务名                            |
| method        | string | 是   | 方法名                                  |
| duration_ms   | int64  | 是   | 耗时（毫秒）                            |
| status_code   | int    | 是   | 状态码（0=成功，非 0=失败）             |
| error         | string | 否   | 错误信息                                |
| request_size  | int64  | 否   | 请求体大小                              |
| response_size | int64  | 否   | 响应体大小                              |
| extra         | object | 否   | 扩展字段                                |

**采集策略**：

| 场景      | 策略     |
| --------- | -------- |
| 错误/超时 | 全量记录 |
| 正常调用  | 1% 采样  |

---

### 3.5 app-logs（应用日志）

**用途**：业务 Debug、错误追踪。

**data 结构**：

```json
{
  "app_id": 123,
  "country": "id",
  "bid": "mx01",
  "level": "ERROR",
  "message": "failed to disburse loan",
  "file": "loan/service.go",
  "line": 88,
  "function": "Disburse",
  "stack": "...",
  "fields": {
    "loan_id": "L_10086",
    "user_id": 789,
    "retry_count": 3
  }
}
```

| 字段     | 类型   | 必填 | 说明                                                        |
| -------- | ------ | ---- | ----------------------------------------------------------- |
| app_id   | int    | 否   | 业务身份；系统任务/未知来源时省略                           |
| country  | string | 否   | 国家代码（小写，如 `mx` / `id` / `bd`）                     |
| bid      | string | 否   | 业务线                                                      |
| level    | string | 是   | 日志级别（DEBUG/INFO/WARN/ERROR）                           |
| message  | string | 是   | 日志内容                                                    |
| file     | string | 否   | 源文件路径                                                  |
| line     | int    | 否   | 行号                                                        |
| function | string | 否   | 函数名                                                      |
| stack    | string | 否   | 堆栈（ERROR 时记录）                                        |
| fields   | object | 否   | 结构化调试字段（高基数字段，如 user_id / sql / loan_id 等） |

**采集策略**：

| 级别  | 策略                                   |
| ----- | -------------------------------------- |
| ERROR | 全量记录                               |
| WARN  | 全量或高采样（如调试时）               |
| INFO  | 1% 采样或关闭或者按需开启 （如调试时） |
| DEBUG | 按需开启（如调试时）                   |

---

