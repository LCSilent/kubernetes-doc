# Kubernetes 调度器源码解析

深入解析 Kubernetes 1.36 调度器的核心架构、实现机制和扩展方式。

## 目录结构

```
kube-scheduler/
├── doc/                              # 文档目录
│   ├── 01-introduction.md            # 第1章：调度器基础概念介绍
│   ├── 02-scheduling-queue.md        # 第2章：调度队列实现解析
│   ├── 03-cache-snapshot.md         # 第3章：调度缓存与快照机制
│   ├── 04-core-source.md            # 第4章：调度器核心源码解析
│   ├── 05-dynamic-resource-allocation.md  # 第5章：Kubernetes 1.36 DRA 新特性
│   └── 06-plugin-development.md     # 第6章：调度器插件开发指南
├── images/                          # 图片目录
│   └── *.svg                        # 架构图和流程图
└── README.md
```

## 文章索引

### 核心章节

1. **[第1章：调度器基础概念介绍](./doc/01-introduction.md)**
   - 了解调度器的定位、职责和在 Kubernetes 架构中的位置
   - 掌握调度器的基本工作流程

2. **[第2章：调度队列实现解析](./doc/02-scheduling-queue.md)**
   - 深入理解 PriorityQueue、ActiveQ 和 BackoffQ 的实现
   - 掌握 Pod 在不同队列间的转换机制
   - 了解 QueueingHint 优化（1.32+）

3. **[第3章：调度缓存与快照机制](./doc/03-cache-snapshot.md)**
   - 深入分析 SchedulerCache 的数据结构
   - 掌握 NodeInfo 和 LRU 缓存机制
   - 了解 Assumed Pod 生命周期管理

4. **[第4章：调度器核心源码解析](./doc/04-core-source.md)**
   - 深入分析调度周期的实现
   - 掌握调度框架和插件机制
   - 理解调度器与 API Server 的交互

5. **[第5章：Dynamic Resource Allocation (DRA) 深度解析](./doc/05-dynamic-resource-allocation.md)**
   - 掌握 Kubernetes 1.36 DRA 新特性
   - 深入分析调度器 DRA 插件和 kubelet DRA Manager
   - 了解 DRA 与 CSI 结合的潜在方向

6. **[第6章：调度器插件开发指南](./doc/06-plugin-development.md)**
   - 学习如何编写自定义调度插件
   - 掌握插件的生命周期管理
   - 了解生产环境插件开发最佳实践

## 快速导航

- [第1章：调度器基础概念介绍](./doc/01-introduction.md)
- [第2章：调度队列实现解析](./doc/02-scheduling-queue.md)
- [第3章：调度缓存与快照机制](./doc/03-cache-snapshot.md)
- [第4章：调度器核心源码解析](./doc/04-core-source.md)
- [第5章：Dynamic Resource Allocation (DRA) 深度解析](./doc/05-dynamic-resource-allocation.md)
- [第6章：调度器插件开发指南](./doc/06-plugin-development.md)

## 架构图

本文档包含多个架构图，图片存放在 `images/` 目录中：

| 图表名称 | 对应章节 | 文件位置 |
|----------|----------|----------|
| PriorityQueue 三层架构图 | 第2章 | [scheduling-queue-architecture.svg](./images/scheduling-queue-architecture.svg) |
| Pod 生命周期图 | 第2章 | [pod-lifecycle.svg](./images/pod-lifecycle.svg) |
| ActiveQ 内部结构图 | 第2章 | [activeq-structure.svg](./images/activeq-structure.svg) |
| 指数退避算法图 | 第2章 | [exponential-backoff.svg](./images/exponential-backoff.svg) |
| QueueingHint 优化图 | 第2章 | [queueinghint-optimization.svg](./images/queueinghint-optimization.svg) |
| Scheduler Cache 深度架构图 | 第3章 | [cache-deep-architecture.svg](./images/cache-deep-architecture.svg) |
| LRU 链表机制图 | 第3章 | [lru-linked-list.svg](./images/lru-linked-list.svg) |
| NodeInfo 数据结构图 | 第3章 | [nodeinfo-structure.svg](./images/nodeinfo-structure.svg) |
| 缓存更新流程图 | 第3章 | [cache-update-flow.svg](./images/cache-update-flow.svg) |
| Assumed Pod 生命周期图 | 第3章 | [assumed-pod-lifecycle.svg](./images/assumed-pod-lifecycle.svg) |
| DRA 架构图 | 第5章 | [dra-architecture.svg](./images/dra-architecture.svg) |
| 插件开发完整生命周期图 | 第6章 | [plugin-development-lifecycle.svg](./images/plugin-development-lifecycle.svg) |

## 阅读顺序

1. **第1章：调度器基础概念介绍** - 了解调度器的定位和职责
2. **第2章：调度队列实现解析** - 深入理解 Pod 调度顺序管理
3. **第3章：调度缓存与快照机制** - 掌握节点状态管理
4. **第4章：调度器核心源码解析** - 深入分析调度周期实现
5. **第5章：Kubernetes 1.36 DRA 新特性** - 了解动态资源分配
6. **第6章：调度器插件开发指南** - 学习如何扩展调度器

## 目标读者

- Kubernetes 开发者
- SRE 和平台工程师
- 云原生技术爱好者
- 需要深入理解调度器的架构师

## 版本说明

本文档基于 **Kubernetes 1.36** 版本源码编写。
