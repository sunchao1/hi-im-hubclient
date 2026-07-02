# hi-im-hubclient 文档

> **hi-im-hubclient** 是 [hi-im](https://github.com/sunchao1/hi-im) 生态的 **L2 Hub TCP 客户端库**（Go module），对应必嗨 `lib/rtmq` Go 部分；**不独立部署**。  
> **许可证**：Apache License 2.0（见仓库根目录 `LICENSE`）

---

## 阅读顺序

| 顺序 | 文档 | 内容 |
|------|------|------|
| 1 | [技术设计文档.md](技术设计文档.md) | 定位、状态机、goroutine 模型、API、与 hi-im-api / hi-im-core 边界 |
| 2 | [M1-实施清单.md](M1-实施清单.md) | 生态 M2 文件级任务（wire 单测 + unicast 集成） |

---

## 生态位置

```text
hi-im-core（Hub）     ← bus wire v1 服务端
hi-im-api             ← IM 48B 头 / CMD / proto（拼 payload）
hi-im-hubclient（本库）← TCP 传输：AUTH / SUB / AsyncSend / handler
hi-im-gateway / …     ← 业务（import api + hubclient）
```

线协议细节：[hi-im-core/doc/协议规范-bus-wire-v1.md](https://github.com/sunchao1/hi-im-core/blob/main/doc/协议规范-bus-wire-v1.md)

全栈方案：[hi-im/doc/hi-im-档C技术方案设计.md](https://github.com/sunchao1/hi-im/blob/main/doc/hi-im-档C技术方案设计.md) §4.5～§4.7、§7.3～§7.4。

---

## 模块路径

```text
github.com/sunchao1/hi-im-hubclient
```

```go
import "github.com/sunchao1/hi-im-hubclient/pkg/hubclient"
```
