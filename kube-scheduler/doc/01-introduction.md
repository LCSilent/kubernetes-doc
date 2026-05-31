# 第1章：调度器基础概念介绍

## 1.1 调度器在 Kubernetes 控制平面中的定位

`kube-scheduler` 是 Kubernetes 控制平面的核心组件之一，**负责为每个待调度的 Pod 选择最合适的运行节点**。这一职责看似简单，实则涉及复杂的资源优化、策略约束和权衡决策。

### 调度器的本质

从架构角度看，调度器是一个**纯决策组件**——它只负责"决定 Pod 应该运行在哪里"，**不负责启动任何容器**。容器启动由目标节点上的 Kubelet 完成。这种职责分离是 Kubernetes 设计哲学的重要体现：控制平面做决策，工作节点执行决策。

调度决策是一个**多维度权衡**的过程。考虑以下因素：

- **资源匹配**：Pod 申请的资源（CPU、内存）是否能在节点上满足
- **硬件亲和**：Pod 是否需要运行在特定硬件上（如 GPU 节点）
- **软件约束**：节点操作系统版本、内核参数等是否满足要求
- **拓扑约束**：Pod 是否需要与其他 Pod 分布在不同可用区
- **数据局部性**：Pod 访问的数据是否在本地存储
- **干扰隔离**：高负载 Pod 是否会干扰其他工作负载
- **业务策略**：部门预算、项目配额等业务层面约束

### Kubernetes 集群架构全景

![Kubernetes 集群组件架构](https://aka.doubaocdn.com/s/Xq961wVn0k)

上图展示了 Kubernetes 集群的完整组件视图。可以看到，控制平面（Control Plane）负责集群管理决策，工作节点（Worker Node）负责运行 Pod。调度器运行在控制平面，通过 API Server 与集群状态交互，最终影响工作节点上的 Pod 部署。

### 调度器在控制平面中的位置

![kube-scheduler 组件架构](https://aka.doubaocdn.com/s/Hfg11wVn5M)

如上图所示，kube-scheduler 对 Pod、Node 等对象进行了 list/watch，根据 Informer 将未调度的 Pod 放入待调度 Pod 队列，并根据 Informer 构建调度器 Cache（用于快速获取需要的 Node 等对象）。`sched.scheduleOne` 方法为 kube-scheduler 组件调度 Pod 的核心处理逻辑所在，从未调度 Pod 队列中取出一个 Pod，经过预选与优选算法，最终选出一个最优 Node。

这种架构设计有几点值得深入理解：

1. **调度器与 API Server 的关系**：调度器不是独立运作的，它依赖 API Server 获取集群状态（节点列表、Pod 状态），并将调度决策（绑定）写回 API Server。所有状态变更都经过 API Server，确保了 etcd 作为单一真实数据源的地位。

2. **调度器与控制器的区别**：控制器（如 Deployment Controller）负责维护期望状态，通过创建/删除 Pod 来响应事件。调度器负责决定这些 Pod 具体运行在哪里。两者配合完成"期望状态→具体部署"的完整链路。

3. **调度器的高可用**：在生产环境中，通常运行多个调度器实例，通过 Leader Election 确保只有一个实例实际执行调度，避免决策冲突。

## 1.2 调度器与其他组件的交互

### 核心交互流程

调度器与 API Server 的交互是理解调度器工作原理的关键。这不是一个简单的请求-响应模式，而是一个**持续 Watch + 决策 + Bind 的循环**。

![调度工作流](https://aka.doubaocdn.com/s/leLZ1wVnIO)

如上图所示，调度工作流分为三个主要阶段：

**Phase I. API 服务器接收请求并保存状态**
1. 用户请求创建新 Pod（通过 kubectl 等工具）
2. API 服务器接收并验证请求，将 Pod 状态保存到 etcd（从 new 变为 pending）
3. API 服务器向外部系统发送确认响应

**Phase II. kube-scheduler 执行调度**
4. kube-scheduler 持续监控未分配的 Pod（通过 watch 功能），检测到新 Pod 被添加
5. kube-scheduler 根据 Pod 的要求（CPU、内存、策略等）决定 Pod 将在哪个节点上运行，并创建 Pod-Node 绑定。之后将 Pod-Node 绑定结果通知 API 服务器
6. kube-scheduler 通过 API 服务器将 Pod 状态更新到 etcd

**Phase III. kubelet 在目标节点上执行**
7. 分配给该节点的 kubelet 监控到属于自己的新 Pod
8. kubelet 从 API 服务器获取 Pod 的详细信息
9. kubelet 基于 Pod 配置拉取容器镜像
10. kubelet 使用容器运行时创建并启动容器
11. kubelet 定期将 Pod 运行状态上报给 API 服务器

**Watch 机制**：调度器通过 `Watch` API 监听 API Server 上的 Pod 变更事件。当有新的 `Pending` 状态 Pod（`pod.spec.nodeName` 为空）创建时，API Server 通知调度器。这是一个**推模式**而非轮询，确保调度器能及时响应。

**Bind 机制**：调度器做出调度决策后，通过 `Bind` API 将 Pod 的 `nodeName` 字段更新为目标节点。这个操作是幂等的——如果绑定失败（节点不可达、资源耗尽等），Pod 会重新进入调度队列等待重试。

### 与 Kubelet 的间接协作

调度器不直接与 Kubelet 通信。这种间接性是 Kubernetes **解耦设计**的典型体现：

![Pod 启动流程](https://aka.doubaocdn.com/s/KOx11wVnIP)

上图展示了 Pod 从创建到启动的完整流程：

1. 用户通过 `kubectl run` 提交 Pod 创建请求
2. 请求发送到 API Server，API Server 验证请求并将 Pod 数据保存到 etcd（此时节点尚未分配）
3. Scheduler 监控到未分配节点的 Pod，选择最优节点进行分配
4. kubelet 监控到分配到自身节点的 Pod，拉取容器镜像并启动
5. Pod 最终成功启动运行

这种设计有几个优势：

1. **状态一致性**：所有状态变更都经过 API Server，不会出现调度器和 Kubelet 看到的状态不一致
2. **可审计性**：所有操作都有 API Server 作为中介，便于审计和调试
3. **松耦合**：调度器和 Kubelet 可以独立演进，不需要协调版本
4. **可扩展性**：可以插入 Webhook、Admission Controller 等组件拦截任何操作

### 调度决策的触发时机

调度器在以下情况会为 Pod 进行调度决策：

| 触发场景 | 说明 |
|---------|------|
| 新 Pod 创建 | `kubectl apply` 或其他控制器创建新 Pod |
| 节点加入集群 | 新节点 Ready 后，等待其上的资源可以被使用 |
| Pod 驱逐完成 | 被高优先级 Pod 驱逐的低优先级 Pod 重新入队 |
| 节点资源释放 | 之前因资源不足无法调度的 Pod 现在可以调度 |
| 调度失败重试 | Pod 从 UnschedulableQ 重新入队 |
| 抢占成功 | 抢占者 Pod 获得资源后，被抢佔的 Pod 重新调度 |

## 1.3 调度器的整体工作流程

### 两阶段调度算法

从算法角度，调度器采用经典的 **Filter + Score 两阶段算法**。这是理解调度器核心逻辑的关键框架。

![调度概念](https://aka.doubaocdn.com/s/m6O81wVnIO)

Kubernetes 调度是指"验证 Pod 在哪个节点上是可行的"的过程。调度的目的是通过**资源使用优化、保持高可用性（HA）、满足用户定义的约束条件**等方式来"最大化集群效率"。

- **Filtering**：确定要查看节点的哪些资源作为标准
- **Scoring**：根据该标准赋予权重，并据此打分
- **Binding**：将选定的 Feasible 节点和 Pod 进行分配

整个调度分为调度（Scheduling）和绑定（Binding）两大阶段，每个阶段又分为多个步骤（stage），每个步骤里面包含了若干个扩展点（Extension Points）。

**Filter 阶段**（过滤）：这一阶段判断"能不能"，即节点是否满足 Pod 运行的基本要求。任何一项检查失败，该节点就被排除。Filter 是**减法**，逐步缩小候选范围。

**Score 阶段**（评分）：这一阶段判断"哪个好"，即在通过过滤的节点中，哪个是最优选择。Score 是**加法**，综合多项指标得出最终排名。

这种设计的优势在于**计算效率**：先快速排除明显不行的节点，再对少量候选节点进行精细评估。如果直接对所有节点评分，计算复杂度会大幅增加。

### 调度周期详解

在 Kubernetes 1.19 及之后的版本中，调度器引入了**调度框架（Scheduling Framework）**，将调度周期拆分为多个扩展点：

![Scheduling Framework 调度上下文](https://aka.doubaocdn.com/s/hbMp1wVn0k)

如上图所示，一次完整的调度尝试（Scheduling Attempt）分为两个阶段：

**1. 调度周期（Scheduling Cycle）**：
- 从调度队列取出 Pod
- 依次执行：PreFilter → Filter → PostFilter → Reserve
- 选择最优节点
- 进入绑定周期

**2. 绑定周期（Binding Cycle）**：
- 执行 PreBind 插件（如 VolumeBinding）
- 执行 Bind 插件
- 更新 API Server 上的 Pod 状态
- 等待绑定结果
- 如果成功：Reserve 资源
- 如果失败：释放 Reserve，进入重试

两个周期的分离设计允许**绑定期并发执行**。当一个 Pod 在等待 Volume 挂载时，后续的 Pod 可以开始新的调度周期，提高了整体吞吐量。

### 调度失败的处理

调度不是总能成功。当没有可行节点时，调度器会：

1. **记录失败原因**：在 Pod 的 `.status.reason` 中记录具体原因（如 `Unschedulable: Insufficient memory`）
2. **进入 Backoff**：Pod 进入退避队列，延迟重试（避免频繁重试浪费资源）
3. **触发抢占**：如果启用了抢占功能，尝试驱逐低优先级 Pod 为高优先级 Pod 腾出空间
4. **集群事件**：生成 `Warning` 事件，可在 `kubectl describe pod` 中查看

理解失败原因是排查调度问题的关键：

```bash
kubectl describe pod <pod-name> | grep -A 10 "Events:"
```

## 1.4 关键组件概览

### 核心组件关系

![调度框架与插件体系](https://aka.doubaocdn.com/s/8dpG1wVn5M)

如上图所示，kube-scheduler 核心组件包括：

- **Scheduler**：调度主循环，协调各组件
- **Cache**：维护节点状态快照，减少 API 调用
- **Queue**：管理待调度 Pod 的优先级队列
- **Framework**：插件化调度逻辑的运行时
- **Plugins**：具体调度策略的实现

### 各组件职责

| 组件 | 源码位置 | 核心职责 |
|------|----------|----------|
| **Scheduler** | `pkg/scheduler/scheduler.go` | 调度主循环，协调各组件 |
| **Cache** | `pkg/scheduler/backend/cache/` | 维护节点状态快照，减少 API 调用 |
| **Queue** | `pkg/scheduler/backend/queue/` | 管理待调度 Pod 的优先级队列 |
| **Framework** | `pkg/scheduler/framework/` | 插件化调度逻辑的运行时 |
| **Plugins** | `pkg/scheduler/framework/plugins/` | 具体调度策略的实现 |

## 1.5 调度器配置

### 调度器配置文件结构

Kubernetes 1.19+ 使用 `KubeSchedulerConfiguration` 来配置调度器行为：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1beta3
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: /etc/kubernetes/scheduler.conf
leaderElection:
  leaderElect: true
  leaseDuration: 15s
  renewDeadline: 10s
  resourceLock: leases
  resourceName: kube-scheduler
  retryPeriod: 5s
profiles:
  - schedulerName: default-scheduler
    plugins:
      filter:
        enabled:
          - name: NodeResourcesFit
          - name: NodeAffinity
          - name: PodTopologySpread
      score:
        enabled:
          - name: NodeResourcesBalancedAllocation
          - name: ImageLocality
```

### 关键配置项解析

1. **Leader Election**：多实例部署时的选主机制，确保只有一个调度器实际工作
2. **Profiles**：支持配置多个调度器实例，每个实例可以有独立的插件配置
3. **Plugin 启用顺序**：Filter 和 Score 阶段插件的执行顺序会影响调度结果
4. **百分比 vs 分数**：某些插件的评分策略可选择使用百分比或原始分数

## 1.6 总结

本章建立了对 Kubernetes 调度器的基础认知框架：

1. **定位认知**：调度器是控制平面的决策组件，负责"在哪运行"，不负责"怎么运行"
2. **交互模式**：通过 Watch + Bind 与 API Server 协作，与 Kubelet 间接通信
3. **核心算法**：Filter + Score 两阶段，先排除再排序
4. **架构理解**：Scheduler、Cache、Queue、Framework 四大组件各司其职

这些概念将在后续章节中逐一深入展开。下一章我们将分析**调度队列**的实现，理解 Pod 是如何被组织、排序和管理的。

## 思考题

1. 如果调度器宕机，新创建的 Pod 会怎样？
2. 为什么调度决策不在 Kubelet 端做，而要在控制平面集中决策？
3. Filter 和 Score 的边界在哪里？某些场景下这个边界是否模糊？
4. 调度器如何保证多个实例间的决策一致性？