# hi-im-hubclient

hi-im 生态 **L2 Hub TCP 客户端库**（Go module）：纯 Go 实现 **bus wire v1**，对应必嗨 `lib/rtmq` Go Proxy；编译进 gateway / msgsvr 等进程，**不独立部署**。

**许可证**：Apache License 2.0

## 文档

| 文档 | 说明 |
|------|------|
| [doc/技术设计文档.md](doc/技术设计文档.md) | 主设计：状态机、API、goroutine 模型、与 hi-im-api 边界 |
| [doc/M1-实施清单.md](doc/M1-实施清单.md) | 生态 M2 开发任务与验收 |

## 职责（一句话）

连 **hi-im-core** Hub → AUTH / SUB / KPALIVE → **AsyncSend** 上行 / **RegisterHandler** 下行；**不含** IM 业务、Redis、WebSocket。

## 生态

- **[hi-im](https://github.com/sunchao1/hi-im)** — 主仓与档 C 总方案
- **[hi-im-api](https://github.com/sunchao1/hi-im-api)** — IM 48B 头、CMD、proto（L3 拼包）
- **[hi-im-core](https://github.com/sunchao1/hi-im-core)** — C++ Hub（运行时 TCP 对端）

## Module（规划）

```text
github.com/sunchao1/hi-im-hubclient
```

```go
import "github.com/sunchao1/hi-im-hubclient/pkg/hubclient"

cli, _ := hubclient.New(cfg)
cli.RegisterHandler(cmd, handler)
_ = cli.Start(ctx)
_ = cli.AsyncSend(bizCmd, destNid, payload)
```

## 状态

M1 实现已完成：`wire` golden 单测、`Client` API（AUTH→SUB→KPALIVE）、`//go:build integration` unicast 集成测试。

```bash
# 单元测试
go test ./...

# 集成测试（需 hi-im-hub 运行中）
go test -tags=integration ./test/integration/...
```
