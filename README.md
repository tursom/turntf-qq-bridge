# turntf-QQ 个人镜像桥接器

`turntf-qq-bridge` 是一个独立的 Go 服务，作为 turntf 消息平台与 QQ（腾讯即时通讯平台）之间的**个人镜像桥接器**。它运行在用户侧，将 QQ 会话镜像到 turntf，并允许用户通过 turntf 客户端向 QQ 发送消息。

## 目录

- [1. 设计目标](#1-设计目标)
- [2. 架构总览](#2-架构总览)
- [3. 项目结构](#3-项目结构)
- [4. 快速开始](#4-快速开始)
- [5. 配置文件详解](#5-配置文件详解)
- [6. 消息信封规范](#6-消息信封规范)
- [7. 内部架构](#7-内部架构)
- [8. 会话绑定机制](#8-会话绑定机制)
- [9. 异步解耦与消息可靠性](#9-异步解耦与消息可靠性)
- [10. 错误处理与回执](#10-错误处理与回执)
- [11. 后端对比](#11-后端对比)
- [12. 数据库表结构](#12-数据库表结构)
- [13. 运维指南](#13-运维指南)
- [14. 测试](#14-测试)
- [15. 依赖](#15-依赖)

---

## 1. 设计目标

| 目标 | 说明 |
|------|------|
| **平台抽象** | 用户侧只看到平台名 `qq`，不暴露 NapCat、QQ 官方 Bot、adapter、backend 等内部实现细节 |
| **单桥接用户模型** | turntf 不创建镜像 channel，所有本地消息发给同一个桥接用户 |
| **客户端分组渲染** | 客户端按 `conversation_ref` 字段将消息分组，渲染为多个独立的 QQ 会话 |
| **异步解耦** | turntf → bridge 方向使用持久化消息 + 本地 SQLite outbox，生产者与桥接器独立运行 |
| **后端可替换** | 通过配置切换 NapCat（OneBot 协议）或 QQ 官方 Bot，无需修改代码 |
| **不泄露实现** | 错误回执只使用平台级错误码，失败信息不包含后端具体实现细节 |

---

## 2. 架构总览

```
┌──────────────┐                    ┌─────────────────────┐
│  turntf 服务  │ ◄──── 持久化 ────► │  turntf-qq-bridge   │
│              │     消息/游标       │                     │
└──────────────┘                    │  ┌───────────────┐  │
                                    │  │   Runtime      │  │
                                    │  │  (3 个并发循环) │  │
                                    │  ├───────────────┤  │
                                    │  │  QQGateway     │  │
                                    │  │  (接口)        │  │
                                    │  ├───────┬───────┤  │
                                    │  │NapCat │QQ Bot │  │
                                    │  │(OneBot)│(官方) │  │
                                    │  ├───────┴───────┤  │
                                    │  │   SQLite       │  │
                                    │  │  (Store)       │  │
                                    │  └───────────────┘  │
                                    └─────────┬───────────┘
                                              │
                                    ┌─────────┴───────────┐
                                    │    QQ 平台           │
                                    │  (群聊 / 私聊)       │
                                    └─────────────────────┘
```

**数据流方向**：

- **turntf → QQ（出站）**：用户通过 turntf 客户端发送消息 → turntf 持久化 → bridge 消费并写入 outbound_jobs → gateway 发送到 QQ
- **QQ → turntf（入站）**：QQ 事件 → gateway 接收 → 写入 inbound_events → 解析绑定 → 生成 turntf_delivery_jobs → 发送到 turntf 目标用户

---

## 3. 项目结构

```text
app/turntf-qq-bridge/
├── README.md                           # 本文档
├── go.mod                              # Go 模块定义
├── go.sum                              # 依赖校验
├── cmd/
│   └── turntf-qq-bridge/
│       └── main.go                     # 程序入口
├── configs/
│   └── config.example.yaml             # 配置示例
└── internal/
    └── qqbridge/
        ├── config.go                   # 配置加载与校验
        ├── errors.go                   # 错误类型与回执码
        ├── model.go                    # 消息信封数据结构
        ├── model_test.go               # 信封解析测试
        ├── gateway.go                  # QQGateway 接口定义
        ├── gateway_napcat.go           # NapCat 后端实现
        ├── gateway_napcat_test.go      # NapCat 集成测试
        ├── gateway_qqbot.go            # QQ 官方 Bot 后端实现
        ├── gateway_qqbot_test.go       # QQ Bot 单元测试
        ├── runtime.go                  # 核心运行时（3 个并发循环）
        ├── store.go                    # SQLite 持久化与任务队列
        └── store_test.go               # Store 集成测试
```

---

## 4. 快速开始

### 4.1 前置条件

- Go 1.26+
- 运行中的 turntf 服务实例
- 运行中的 NapCat 实例（推荐）或已注册的 QQ 官方 Bot

### 4.2 编译

```bash
cd app/turntf-qq-bridge
go build -o turntf-qq-bridge ./cmd/turntf-qq-bridge
```

### 4.3 配置校验

在正式启动前，先校验配置文件是否正确：

```bash
./turntf-qq-bridge -config ./configs/config.example.yaml -check-config
```

输出 `config ok` 表示配置有效。

### 4.4 启动服务

```bash
./turntf-qq-bridge -config ./configs/config.example.yaml
```

### 4.5 命令行参数

| 参数 | 简写 | 说明 |
|------|------|------|
| `-config` | `-c` | 指定 YAML 配置文件路径（必填） |
| `-check-config` | — | 仅校验配置，校验通过后退出 |

### 4.6 信号处理

桥接器监听 `SIGINT` 和 `SIGTERM` 信号，收到信号后执行优雅关闭：取消 context，等待三个并发循环退出。

### 4.7 Docker 部署

从 GitHub Container Registry 拉取预构建镜像：

```bash
docker pull ghcr.io/tursom/turntf-qq-bridge:latest
```

运行容器：

```bash
docker run -d \
  --name turntf-qq-bridge \
  -v $(pwd)/config.yaml:/etc/qq-bridge/config.yaml:ro \
  -v $(pwd)/data:/data \
  ghcr.io/tursom/turntf-qq-bridge:latest \
  -config /etc/qq-bridge/config.yaml
```

**挂载说明**：

| 挂载 | 用途 |
|------|------|
| `config.yaml` | 配置文件（只读），路径需与 `-config` 参数一致 |
| `data` 目录 | SQLite 数据库持久化目录，需与配置中的 `sqlite_path` 对应 |

**本地构建**：

```bash
docker build -t turntf-qq-bridge .
```

**Docker Compose 部署**：

```yaml
# docker-compose.yml
version: "3.8"

services:
  qq-bridge:
    image: ghcr.io/tursom/turntf-qq-bridge:latest
    container_name: turntf-qq-bridge
    restart: unless-stopped
    volumes:
      - ./config.yaml:/etc/qq-bridge/config.yaml:ro
      - ./data:/data
    command: -config /etc/qq-bridge/config.yaml
```

启动与停止：

```bash
# 后台启动
docker compose up -d

# 查看日志
docker compose logs -f

# 停止
docker compose down
```

---

## 5. 配置文件详解

配置文件为 YAML 格式，完整示例见 `configs/config.example.yaml`。

### 5.1 顶层结构

```yaml
storage:      # 本地存储配置
turntf:       # turntf 服务连接配置
backend:      # QQ 后端配置
```

### 5.2 `storage` — 存储配置

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `sqlite_path` | string | 是 | SQLite 数据库文件路径。相对路径相对于配置文件所在目录解析 |

```
storage:
  sqlite_path: "./qq-bridge.sqlite"
```

### 5.3 `turntf` — turntf 连接配置

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `base_url` | string | 是 | turntf 服务 HTTP 地址，如 `http://127.0.0.1:8080` |
| `bridge_user.node_id` | int64 | 是 | 桥接用户在 turntf 中的节点 ID，不能为 0 |
| `bridge_user.user_id` | int64 | 是 | 桥接用户在 turntf 中的用户 ID，不能为 0 |
| `bridge_user.password.source` | string | 是 | `plain`（明文）或 `hashed`（预哈希） |
| `bridge_user.password.value` | string | 是 | 密码值 |

```yaml
turntf:
  base_url: "http://127.0.0.1:8080"
  bridge_user:
    node_id: 4096
    user_id: 1200
    password:
      source: plain
      value: bridge-password
```

**密码模式说明**：

- `source: plain` — 明文密码，由 turntf-go SDK 在客户端侧进行哈希处理
- `source: hashed` — 预哈希密码，直接传递给服务端

### 5.4 `backend` — QQ 后端配置

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `kind` | string | 是 | 后端类型，取值 `napcat` 或 `qqbot` |

#### NapCat 后端（`kind: napcat`）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `ws_url` | string | 是 | NapCat 正向 WebSocket 地址，如 `ws://127.0.0.1:3001/` |
| `access_token` | string | 否 | NapCat access token，设置后会以 `Authorization: Bearer <token>` 头发送 |
| `self_id` | string | 否 | Bot 自身的 QQ 号，用于过滤自己发送的消息，防止回声 |

```yaml
backend:
  kind: napcat
  napcat:
    ws_url: "ws://127.0.0.1:3001/"
    access_token: ""
    self_id: "20000"
```

#### QQ 官方 Bot 后端（`kind: qqbot`）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `app_id` | string | 是 | QQ 开放平台 Bot AppID |
| `secret` | string | 条件 | Bot Secret（推荐），若同时存在 `token` 则优先使用 |
| `token` | string | 条件 | Bot Token（兼容别名），`secret` 不存在时使用 |
| `sandbox` | bool | 否 | 是否使用沙箱环境，默认 `false` |

```yaml
backend:
  kind: qqbot
  qqbot:
    app_id: "123456"
    secret: "your-app-secret"
    token: ""
    sandbox: false
```

> **兼容说明**：`token` 是 `secret` 的兼容别名。若两者同时存在，优先使用 `secret`。

---

## 6. 消息信封规范

桥接器使用统一的 `BridgeEnvelope` JSON 格式在 turntf 和 QQ 之间传递消息。

### 6.1 完整结构

```json
{
  "version": "v1alpha1",
  "kind": "chat",
  "conversation_ref": {
    "platform": "qq",
    "scene": "group",
    "chat_id": "123456",
    "thread_id": ""
  },
  "content": {
    "segments": [
      { "kind": "text", "text": "hello qq" }
    ]
  },
  "remote_sender": {
    "id": "10001",
    "nickname": "张三",
    "remark": "小三",
    "role": "owner",
    "avatar_url": "https://..."
  },
  "message_ref": {
    "id": "msg-001",
    "reply_to": "msg-000"
  },
  "metadata": {
    "platform": "qq",
    "scene": "group",
    "self_id": "20000",
    "time": 1710000000
  }
}
```

### 6.2 顶层字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `version` | string | 否 | 信封版本，固定 `v1alpha1`。为空时自动填充 |
| `kind` | string | 是 | 消息类型：`chat`（聊天）、`system`（系统）、`receipt`（回执） |
| `conversation_ref` | object | 是 | QQ 会话标识，详见 §6.3 |
| `content` | object | 是 | 消息内容，包含 segments 数组，详见 §6.4 |
| `remote_sender` | object | 否 | 远程发送者信息，**仅 QQ → turntf 方向**由桥接器填写 |
| `message_ref` | object | 否 | 消息关联标识，详见 §6.5 |
| `metadata` | object | 否 | 扩展元数据，平台相关信息 |

### 6.3 `conversation_ref` — 会话标识

只使用平台语义，不暴露后端实现：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `platform` | string | 否 | 平台标识，固定 `qq`。为空时默认填充为 `qq` |
| `scene` | string | 是 | 会话场景：`group`（群聊）或 `private`（私聊） |
| `chat_id` | string | 是 | QQ 侧会话 ID（群号或用户 QQ 号） |
| `thread_id` | string | 否 | 子话题 ID，首版可省略 |

示例：

```json
// 群聊
{ "platform": "qq", "scene": "group", "chat_id": "987654321" }

// 私聊
{ "platform": "qq", "scene": "private", "chat_id": "10001" }
```

### 6.4 `content.segments` — 消息段类型

桥接器定义了 8 种统一的段类型。不同后端的实际支持能力不同（详见 §11），若当前后端不支持某段类型，桥接器会回写 `kind=receipt` 的失败消息。

#### 6.4.1 `text` — 文本

```json
{ "kind": "text", "text": "你好，世界" }
```

| 字段 | 说明 |
|------|------|
| `text` | 文本内容（必填，不可空白） |

#### 6.4.2 `image` — 图片

```json
{ "kind": "image", "url": "https://example.com/image.png", "mime": "image/png" }
```

| 字段 | 说明 |
|------|------|
| `url` | 图片 URL（必填） |
| `mime` | MIME 类型（可选） |

#### 6.4.3 `audio` — 音频

```json
{ "kind": "audio", "url": "https://example.com/voice.amr", "mime": "audio/amr" }
```

| 字段 | 说明 |
|------|------|
| `url` | 音频 URL（必填） |
| `mime` | MIME 类型（可选） |

#### 6.4.4 `video` — 视频

```json
{ "kind": "video", "url": "https://example.com/video.mp4", "mime": "video/mp4" }
```

| 字段 | 说明 |
|------|------|
| `url` | 视频 URL（必填） |
| `mime` | MIME 类型（可选） |

#### 6.4.5 `file` — 文件

```json
{ "kind": "file", "url": "https://example.com/doc.pdf", "file_name": "文档.pdf", "mime": "application/pdf" }
```

| 字段 | 说明 |
|------|------|
| `url` | 文件 URL（必填） |
| `file_name` | 文件名（可选） |
| `mime` | MIME 类型（可选） |

#### 6.4.6 `reply` — 回复引用

```json
{ "kind": "reply", "message_id": "msg-000" }
```

| 字段 | 说明 |
|------|------|
| `message_id` | 被引用的消息 ID（必填） |

#### 6.4.7 `mention` — @提及

```json
{ "kind": "mention", "user_id": "10001" }
```

| 字段 | 说明 |
|------|------|
| `user_id` | 被 @ 的用户 QQ 号（必填） |

#### 6.4.8 `json` — 扩展 JSON

```json
{ "kind": "json", "payload": { "custom_key": "custom_value" } }
```

| 字段 | 说明 |
|------|------|
| `payload` | 任意合法 JSON 值（必填，不可为 null） |

### 6.5 `message_ref` — 消息关联

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 消息唯一标识 |
| `reply_to` | string | 被回复的消息 ID |

### 6.6 `remote_sender` — 远程发送者

仅在 **QQ → turntf** 方向由桥接器自动填写：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | QQ 号 |
| `nickname` | string | 昵称 |
| `remark` | string | 备注名（群名片） |
| `role` | string | 群角色（owner/admin/member） |
| `avatar_url` | string | 头像 URL |

### 6.7 `metadata` — 扩展元数据

键值对形式的扩展信息，当前包含：

| 键 | 说明 |
|------|------|
| `platform` | 固定 `qq` |
| `scene` | 会话场景 |
| `self_id` | Bot 自身的 QQ 号（NapCat） |
| `time` | QQ 消息时间戳 |
| `code` | 回执错误码（仅 receipt 消息） |

---

## 7. 内部架构

### 7.1 模块职责

```
cmd/turntf-qq-bridge/main.go
    │
    ├── qqbridge.LoadConfig()      → config.go    配置加载与校验
    └── qqbridge.Run()             → runtime.go    核心运行时
                                      │
            ┌─────────────────────────┼─────────────────────────┐
            │                         │                         │
      Store (store.go)         QQGateway (gateway.go)    turntf Client
      SQLite 持久化             接口 + 工厂函数            (turntf-go SDK)
      7 张表                        │
            ┌────────────────────────┤
            │                        │
    NapCatGateway              QQBotGateway
    (gateway_napcat.go)        (gateway_qqbot.go)
    OneBot/WebSocket           官方 Bot SDK/WebSocket
```

### 7.2 Runtime — 三个并发循环

`runtime.go` 中的 `Run()` 函数在连接 turntf 后启动三个 goroutine：

#### 循环 1：`runOutboundLoop`（turntf → QQ 出站）

```
┌─────────────────┐     ┌──────────────┐     ┌────────────────┐
│ claimOutboundJob │ ──► │ gateway.Send │ ──► │ markDelivered  │
│ (SQLite 行锁)    │     │ (NapCat/QQBot)│     │ / retry / fail │
└─────────────────┘     └──────────────┘     └────────────────┘
```

流程：
1. 从 `outbound_jobs` 表认领一个状态为 `pending` 且 `next_attempt_at_ms <= now` 的任务（`SELECT ... LIMIT 1` + `UPDATE status=processing` 在同一事务内完成，通过行锁防止并发重复认领）
2. 调用 `gateway.Send()` 发送消息到 QQ
3. 成功 → `MarkOutboundDelivered`，记录远端消息 ID
4. 可重试错误 → `RetryOutbound`，指数退避后重试
5. 不可重试错误 → `FailOutbound` + `QueueReceipt`（向用户回写失败回执）

#### 循环 2：`consumeGatewayEvents`（QQ → Store 入站）

```
┌──────────────────┐     ┌──────────────────────┐
│ gateway.Events() │ ──► │ store.EnqueueInbound  │
│ (channel 消费)    │     │ Event (事务性处理)     │
└──────────────────┘     └──────────────────────┘
```

流程：
1. 从 `gateway.Events()` channel 读取 QQ 入站事件
2. 调用 `store.EnqueueInboundEvent()` 在事务中：
   - 写入 `inbound_events` 表（去重，按 `gateway_message_id` 唯一约束）
   - 查找 `session_bindings` 表匹配绑定关系
   - 有绑定 → 生成 `turntf_delivery_jobs`，标记入站事件为 `delivered`
   - 无绑定 → 写入 `orphan_inbound_events`，标记入站事件为 `failed`

#### 循环 3：`runTurnTFDeliveryLoop`（Store → turntf 投递）

```
┌───────────────────────┐     ┌───────────────────┐     ┌──────────────┐
│ claimTurnTFDeliveryJob │ ──► │ client.SendMessage │ ──► │ markDelivered │
│ (SQLite 行锁)          │     │ (turntf API)       │     │ / retry/fail  │
└───────────────────────┘     └───────────────────┘     └──────────────┘
```

流程与出站循环对称：
1. 从 `turntf_delivery_jobs` 表认领任务
2. 将信封序列化为 JSON
3. 调用 turntf client 发送消息到目标用户
4. 成功后标记 delivered，失败后分类为重试或失败

### 7.3 Gateway 接口

```go
type QQGateway interface {
    Send(context.Context, outboundMessage) (gatewaySendResult, error)
    Events() <-chan GatewayInboundEvent
    Capabilities() gatewayCapabilities
    Close() error
}
```

`gatewayCapabilities` 声明了后端支持的 segment 类型集合。`Send()` 调用前会检查 content 中的所有 segment 是否在支持范围内，不支持的 segment 返回 `unsupported_content` 错误。

后端选择在 `gateway.go` 中通过工厂函数 `newGateway()` 完成，基于 `backend.kind` 配置项。

### 7.4 NapCat 后端实现细节

- **连接方式**：桥接器作为 WebSocket 客户端，主动连接 NapCat 正向 WS 地址
- **协议**：OneBot 兼容的 JSON 帧协议
- **发送**：构造 `{"action": "send_group_msg"|"send_private_msg", "params": {...}, "echo": "napcat-N"}` JSON，通过 `echo` 字段关联响应
- **接收**：读取 WS 帧，`echo` 非空 → 解析为发送响应；`post_type == "message"` → 解析为入站事件
- **回声过滤**：通过 `self_id` 过滤自己发送的消息，防止消息循环
- **重连**：指数退避重连，初始 1s，每次翻倍，最大 30s
- **段映射**（Bridge → NapCat）：text → text, image → image, audio → record, video → video, file → file, reply → reply, mention → at, json → json
- **segment 解析**：优先解析 NapCat `message` 数组格式，回退到 `raw_message` 纯文本

### 7.5 QQ 官方 Bot 后端实现细节

- **认证**：OAuth2 token 机制，使用 `botgo` SDK 的 `QQBotTokenSource`，自动刷新 token
- **连接**：通过 `botgo.SessionManager` 管理 WebSocket 连接，注册事件处理器：
  - `GroupATMessageEventHandler` — 群 @ 消息
  - `C2CMessageEventHandler` — 私聊消息
- **发送**：文本消息使用 `dto.MessageToCreate`，富媒体（图片/视频/音频）使用 `dto.RichMediaMessage`
- **API 超时**：10 秒
- **沙箱模式**：`sandbox: true` 时使用 `botgo.NewSandboxOpenAPI`
- **段映射**（Bridge → QQ Bot）：text → TextMsg, image/video/audio → RichMediaMessage, reply → MessageReference
- **限制**：不支持 `file`、`mention`、`json` 段；富媒体不支持 reply 引用；不支持多个富媒体同时发送

---

## 8. 会话绑定机制

本地个人镜像会话通过"**第一条消息**"自动建立，无需手动配置：

```
步骤 1: 本地用户发送 kind=chat 消息给桥接用户
        ┌──────────┐    BridgeEnvelope     ┌──────────┐
        │ 用户 A    │ ────────────────────► │ 桥接用户  │
        │ (node:1,  │   conversation_ref:   │ (node:N,  │
        │  user:100)│   {platform:qq,       │  user:B)  │
        └──────────┘    scene:group,        └──────────┘
                        chat_id:"123456"}

步骤 2: 桥接器从消息的 sender 字段识别本地用户 (node:1, user:100)

步骤 3: 写入 session_bindings 表：
        (bridge_node:N, bridge_user:B, local_node:1, local_user:100,
         conversation_key:"qq|group|123456|-")

步骤 4: 后续该 QQ 群 "123456" 的入站事件
        → 查找绑定 → 找到用户 A → 回流消息给用户 A
```

**关键特性**：

- 同一个 QQ 会话可以被多个本地用户绑定（例如一个家庭群镜像给多个家庭成员）
- 绑定以 `(bridge_user, local_user, conversation_key)` 为主键，支持 upsert 更新
- 入站事件如果没有找到任何绑定，会写入 `orphan_inbound_events` 表，等待未来的绑定建立

---

## 9. 异步解耦与消息可靠性

### 9.1 turntf → QQ（出站方向）：强异步

```
┌──────────┐  持久化消息   ┌──────────────┐  outbound_jobs  ┌──────────┐
│ turntf   │ ────────────► │ bridge 消费   │ ─────────────► │ QQ 平台  │
│ 生产者    │   离线不丢     │ (游标补消费)  │   本地队列      │          │
└──────────┘               └──────────────┘                └──────────┘
```

- 桥接器离线期间，turntf 生产者可以继续发送持久化消息
- 桥接器恢复后通过游标（cursor）补消费离线期间的消息
- `turntf_messages` 表存储原始消息体，`seen_messages` 表记录已消费的游标位置
- 所有出站消息写入 `outbound_jobs` 队列，保证不丢失

### 9.2 QQ → turntf（入站方向）：弱异步

```
┌──────────┐  WS 事件     ┌──────────────┐  turntf_delivery  ┌──────────┐
│ QQ 平台  │ ──────────► │ bridge 在线   │  _jobs           │ turntf   │
│          │   需要在线    │ 接收 + 落库   │ ────────────────► │ 目标用户  │
└──────────┘              └──────────────┘                   └──────────┘
```

- 桥接器进程需要在线才能接收 QQ 事件（NapCat 正向 WS 或 QQ Bot WebSocket）
- 收到事件后先写入 `inbound_events` 表持久化
- 再异步通过 `turntf_delivery_jobs` 队列投递给目标用户

### 9.3 重试策略

所有出站和投递任务都支持重试，指数退避策略：

| 尝试次数 | 退避时间 |
|----------|----------|
| 1 | 1 秒 |
| 2 | 2 秒 |
| 3 | 4 秒 |
| 4 | 8 秒 |
| 5 | 16 秒 |
| 6+ | 32 秒（封顶） |

重试由 `BridgeError.Retryable` 字段控制。可重试的错误会重置 job 状态为 `pending` 并设定 `next_attempt_at_ms`，不可重试的错误直接标记为 `failed`。

### 9.4 重复消息保护

- `turntf_messages` 使用 `INSERT OR IGNORE` + `(node_id, seq)` 主键防止重复处理
- `outbound_jobs` 使用 `UNIQUE(source_node_id, source_seq)` 防止重复入队
- `inbound_events` 使用 `UNIQUE(gateway_message_id)` 防止重复事件
- `turntf_delivery_jobs` 使用 `UNIQUE(job_key)` 防止重复投递

---

## 10. 错误处理与回执

### 10.1 平台级错误码

桥接器只使用 6 种平台级错误码，不暴露后端实现细节：

| 错误码 | 含义 | 典型场景 | 可重试 |
|--------|------|----------|--------|
| `platform_unavailable` | 平台不可用 | WS 断连、网关未连接、turntf 离线 | ✅ |
| `target_not_found` | 目标不存在 | QQ 群/用户不存在、HTTP 404 | ❌ |
| `permission_denied` | 权限不足 | 被禁言、HTTP 403、turntf 鉴权失败 | ❌ |
| `unsupported_content` | 不支持的内容 | segment 类型不支持、信封格式错误 | ❌ |
| `rate_limited` | 频率限制 | QQ 限流、HTTP 429 | ✅ |
| `delivery_failed` | 投递失败 | 后端返回非特定错误 | ❌ |

### 10.2 可重试 vs 不可重试

```go
type BridgeError struct {
    Code      ReceiptCode   // 平台级错误码
    Message   string        // 人类可读的错误描述
    Retryable bool          // 是否可重试
    Err       error         // 底层错误（不暴露给用户）
}
```

- **可重试错误**：platform_unavailable、rate_limited，以及未分类的未知错误
- **不可重试错误**：target_not_found、permission_denied、unsupported_content，以及已知的终端错误

### 10.3 错误回执格式

失败回执本身也是 `BridgeEnvelope`，`kind=receipt`：

```json
{
  "version": "v1alpha1",
  "kind": "receipt",
  "conversation_ref": {
    "platform": "qq",
    "scene": "group",
    "chat_id": "123456"
  },
  "content": {
    "segments": [
      { "kind": "text", "text": "platform_unavailable: napcat websocket is not connected" }
    ]
  },
  "message_ref": { "id": "original-msg-id" },
  "metadata": { "code": "platform_unavailable" }
}
```

- `content.segments[0].text` 包含人类可读的错误描述（不包含底层实现细节）
- `metadata.code` 包含机器可读的错误码
- `message_ref.id` 引用原始失败消息的 ID

### 10.4 turntf 发送错误分类

`classifyTurnTFSendError()` 将 turntf client 发送错误映射为 BridgeError：

| 原始错误 | 映射结果 |
|----------|----------|
| `turntf.ErrClosed` / `ErrDisconnected` / `ErrNotConnected` | `platform_unavailable`（可重试） |
| ServerError `not_found` | `target_not_found`（不可重试） |
| ServerError `forbidden` / `unauthorized` | `permission_denied`（不可重试） |
| 其他 ServerError | `delivery_failed`（可重试） |
| 包含 "unauthorized" 的错误 | `permission_denied`（不可重试） |
| 其他未知错误 | `platform_unavailable`（可重试） |

---

## 11. 后端对比

| 特性 | NapCat | QQ 官方 Bot |
|------|--------|-------------|
| **部署方式** | 用户自行部署 headless QQ 客户端 | 腾讯云托管 |
| **协议** | OneBot（正向 WebSocket） | QQ 官方 WebSocket |
| **认证** | access_token（可选） | OAuth2（AppID + Secret） |
| **稳定性** | 依赖第三方客户端稳定性 | 官方基础设施 |
| **沙箱环境** | 不支持 | 支持 |
| **text** | ✅ | ✅ |
| **image** | ✅ | ✅ |
| **audio** | ✅（record） | ✅（voice/audio） |
| **video** | ✅ | ✅ |
| **file** | ✅（含文件名） | ❌ |
| **reply** | ✅ | ✅（仅文本消息） |
| **mention** (@) | ✅（at） | ❌ |
| **json**（扩展） | ✅ | ❌ |
| **富媒体 + reply** | ✅ | ❌ |
| **多富媒体** | ✅ | ❌（仅一个） |
| **群消息** | ✅ | ✅（仅 @ 消息） |
| **私聊消息** | ✅ | ✅ |
| **富媒体消息类型** | 普通消息 | RichMedia 消息 |

### 选择建议

- **个人使用 / 功能完整性优先** → NapCat。支持所有 8 种 segment 类型，无功能限制
- **稳定性优先 / 不想维护 QQ 客户端** → QQ 官方 Bot。受限于官方 API 的能力

---

## 12. 数据库表结构

桥接器使用 SQLite 存储所有持久化状态。数据库使用 WAL 模式，`busy_timeout = 5000ms`。

### 12.1 `seen_messages` — 游标持久化

记录已处理的 turntf 消息游标，用于断线重连后补消费。

| 列 | 类型 | 说明 |
|------|------|------|
| `node_id` | INTEGER | 消息节点 ID |
| `seq` | INTEGER | 消息序列号 |
| PRIMARY KEY | | `(node_id, seq)` |

### 12.2 `turntf_messages` — 原始消息存储

保存从 turntf 接收到的原始消息体。

| 列 | 类型 | 说明 |
|------|------|------|
| `node_id` | INTEGER | 消息节点 ID |
| `seq` | INTEGER | 消息序列号 |
| `recipient_node_id` | INTEGER | 接收者节点 ID |
| `recipient_user_id` | INTEGER | 接收者用户 ID |
| `sender_node_id` | INTEGER | 发送者节点 ID |
| `sender_user_id` | INTEGER | 发送者用户 ID |
| `body` | BLOB | 消息体 JSON |
| `created_at_hlc` | TEXT | turntf HLC 时间戳 |
| `saved_at_ms` | INTEGER | 本地保存时间（毫秒） |
| PRIMARY KEY | | `(node_id, seq)` |

### 12.3 `session_bindings` — 会话绑定

存储桥接用户、本地用户和 QQ 会话之间的绑定关系。

| 列 | 类型 | 说明 |
|------|------|------|
| `bridge_node_id` | INTEGER | 桥接用户节点 ID |
| `bridge_user_id` | INTEGER | 桥接用户用户 ID |
| `local_node_id` | INTEGER | 本地用户节点 ID |
| `local_user_id` | INTEGER | 本地用户用户 ID |
| `conversation_key` | TEXT | 会话键（`platform\|scene\|chat_id\|thread_id`） |
| `conversation_json` | BLOB | 会话 JSON |
| `created_at_ms` | INTEGER | 创建时间（毫秒） |
| `updated_at_ms` | INTEGER | 更新时间（毫秒） |
| PRIMARY KEY | | `(bridge_node_id, bridge_user_id, local_node_id, local_user_id, conversation_key)` |

### 12.4 `outbound_jobs` — 出站任务队列

turntf → QQ 方向的消息发送任务。

| 列 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER | 自增主键 |
| `source_node_id` | INTEGER | 来源节点 ID |
| `source_seq` | INTEGER | 来源序列号 |
| `source_sender_node_id` | INTEGER | 来源发送者节点 ID |
| `source_sender_user_id` | INTEGER | 来源发送者用户 ID |
| `conversation_key` | TEXT | 会话键 |
| `envelope_json` | BLOB | 信封 JSON |
| `status` | TEXT | 状态：`pending` / `processing` / `delivered` / `failed` |
| `attempts` | INTEGER | 已尝试次数 |
| `next_attempt_at_ms` | INTEGER | 下次尝试时间（毫秒） |
| `remote_message_id` | TEXT | QQ 侧消息 ID |
| `last_error_code` | TEXT | 最后错误码 |
| `last_error_message` | TEXT | 最后错误信息 |
| `created_at_ms` | INTEGER | 创建时间 |
| `updated_at_ms` | INTEGER | 更新时间 |
| UNIQUE | | `(source_node_id, source_seq)` |

索引：`idx_outbound_jobs_pending ON (status, next_attempt_at_ms, id)`

### 12.5 `inbound_events` — 入站事件记录

QQ → turntf 方向接收的事件。

| 列 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER | 自增主键 |
| `gateway_message_id` | TEXT | 网关消息 ID（UNIQUE） |
| `conversation_key` | TEXT | 会话键 |
| `envelope_json` | BLOB | 信封 JSON |
| `status` | TEXT | 状态：`pending` / `delivered` / `failed`（无绑定时） |
| `created_at_ms` | INTEGER | 创建时间 |
| `updated_at_ms` | INTEGER | 更新时间 |

### 12.6 `orphan_inbound_events` — 孤儿入站事件

没有会话绑定的入站事件，等待未来的绑定建立。

| 列 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER | 自增主键 |
| `gateway_message_id` | TEXT | 网关消息 ID（UNIQUE） |
| `conversation_key` | TEXT | 会话键 |
| `envelope_json` | BLOB | 信封 JSON |
| `created_at_ms` | INTEGER | 创建时间 |

### 12.7 `turntf_delivery_jobs` — turntf 投递任务队列

QQ → turntf 方向的消息投递任务（包括聊天消息和回执消息）。

| 列 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER | 自增主键 |
| `job_key` | TEXT | 任务唯一键（UNIQUE），格式如 `inbound:<event_id>:<node>:<user>` |
| `kind` | TEXT | 任务类型：`inbound` 或 `receipt` |
| `target_local_node_id` | INTEGER | 目标本地用户节点 ID |
| `target_local_user_id` | INTEGER | 目标本地用户用户 ID |
| `envelope_json` | BLOB | 信封 JSON |
| `status` | TEXT | 状态：`pending` / `processing` / `delivered` / `failed` |
| `attempts` | INTEGER | 已尝试次数 |
| `next_attempt_at_ms` | INTEGER | 下次尝试时间 |
| `last_error_code` | TEXT | 最后错误码 |
| `last_error_message` | TEXT | 最后错误信息 |
| `created_at_ms` | INTEGER | 创建时间 |
| `updated_at_ms` | INTEGER | 更新时间 |

索引：`idx_turntf_delivery_jobs_pending ON (status, next_attempt_at_ms, id)`

---

## 13. 运维指南

### 13.1 多 QQ 身份部署

同一实例只连接一个 QQ 后端。如需多个 QQ 身份：

1. 部署多个桥接器实例
2. 每个实例使用独立的配置文件（不同的 SQLite 路径、不同的 NapCat WS 地址 / QQ Bot AppID）
3. 在 turntf 中注册多个桥接用户，每个实例使用不同的 `bridge_user`

### 13.2 SQLite 数据库维护

```bash
# 查看数据库状态
sqlite3 qq-bridge.sqlite "SELECT status, COUNT(*) FROM outbound_jobs GROUP BY status;"
sqlite3 qq-bridge.sqlite "SELECT status, COUNT(*) FROM turntf_delivery_jobs GROUP BY status;"

# 查看失败任务
sqlite3 qq-bridge.sqlite "SELECT id, last_error_code, last_error_message FROM outbound_jobs WHERE status='failed' ORDER BY updated_at_ms DESC LIMIT 10;"

# 查看孤儿事件（无绑定会话）
sqlite3 qq-bridge.sqlite "SELECT COUNT(*) FROM orphan_inbound_events;"

# 数据库大小
ls -lh qq-bridge.sqlite
```

### 13.3 日志

日志输出到 stderr，格式为 `yyyy/mm/dd HH:MM:SS qqbridge <message>`。关键日志事件：

- `qqbridge napcat connected` / `qqbridge qqbot websocket started` — 后端连接成功
- `qqbridge napcat dial failed` — NapCat 连接失败（会自动重连）
- `qqbridge turntf login ok` — turntf 登录成功
- `qqbridge observed turntf message` — 收到 turntf 消息
- `qqbridge claim outbound job failed` — 出站任务认领失败
- `qqbridge enqueue inbound event failed` — 入站事件入队失败

### 13.4 健康检查

桥接器没有内置的 HTTP 健康检查端点。可以通过以下方式间接判断健康状态：

- 桥接器进程存在且未退出
- 日志中持续有正常的消息处理日志
- SQLite 中 `outbound_jobs` 和 `turntf_delivery_jobs` 的 `failed` 状态任务没有持续增长

### 13.5 性能考量

- SQLite 使用 WAL 模式，支持读写并发
- `busy_timeout = 5000ms`，在并发较高时等待而非立即失败
- 出站和投递循环在没有任务时 sleep 500ms，避免空转
- 事件 channel 缓冲区为 64，高消息量时可能阻塞
- QQ Bot API 超时设为 10 秒

### 13.6 安全注意事项

- `access_token` 和 `secret` 等敏感信息存储在配置文件中，应设置适当的文件权限（如 `0600`）
- NapCat WS URL 中的 `access_token` 通过 HTTP Header `Authorization: Bearer` 发送，不使用 URL 参数
- 桥接器密码支持 `hashed` 模式，避免明文存储

---

## 14. 测试

### 14.1 运行测试

```bash
cd app/turntf-qq-bridge
go test ./internal/qqbridge/ -v
```

### 14.2 测试覆盖

| 文件 | 测试内容 |
|------|----------|
| `model_test.go` | 信封解析、规范化、平台无关序列化（验证无后端特定字段泄露）、默认平台填充 |
| `store_test.go` | 端到端消息持久化、会话绑定、出站任务创建、入站事件与绑定解析、无效消息回执队列 |
| `gateway_napcat_test.go` | 集成测试：内存 WebSocket 服务器模拟 NapCat — 发送入站事件验证解析，发送出站动作验证 NapCat action 结构 |
| `gateway_qqbot_test.go` | 单元测试：私聊消息发送、WS 管理器启动、C2C 事件规范化（文本 + 图片附件） |

所有测试使用 `t.Parallel()` 并行运行，数据库测试使用临时目录和独立 SQLite 实例。

---

## 15. 依赖

### 15.1 直接依赖

| 依赖 | 版本 | 用途 |
|------|------|------|
| `github.com/coder/websocket` | v1.8.14 | NapCat WebSocket 客户端 |
| `github.com/tencent-connect/botgo` | v0.2.1 | QQ 官方 Bot SDK |
| `github.com/tursom/turntf-go` | v0.0.0 (本地) | turntf 客户端 SDK（`replace ../../turntf-go`） |
| `gopkg.in/yaml.v3` | v3.0.1 | YAML 配置解析 |
| `modernc.org/sqlite` | v1.50.0 | 纯 Go SQLite 驱动（无 CGo 依赖） |

### 15.2 Go 版本

`go 1.26.1`

### 15.3 本地 replace

```go
replace github.com/tursom/turntf-go => ../../turntf-go
```

`turntf-go` SDK 通过本地路径替换引用，确保与主仓库同步开发。
