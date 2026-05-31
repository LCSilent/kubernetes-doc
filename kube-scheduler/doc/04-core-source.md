
# 第4章：调度框架深层架构与性能优化

**源码版本：** Kubernetes 1.36  
**分析深度：** 源码级解析  
**最后更新：** 2026-05-30

---

## 4.1 调度框架设计哲学

### 4.1.1 从调度器到调度框架的演进

Kubernetes调度器的架构经历了三代重要演进：

- **v1.0 - v1.11 (第一代):** 硬编码的调度逻辑
- **v1.12 - v1.17 (第二代):** 扩展器(Extender)模式
- **v1.18+ (第三代):** 调度框架(Scheduling Framework)

调度框架的设计目标是：
1. **可扩展性：** 让自定义调度逻辑更易于开发和维护
2. **性能优化：** 减少调度周期的时间开销
3. **灵活性：** 支持多种调度策略的组合
4. **可观测性：** 提供清晰的插件执行流程

### 4.1.2 frameworkImpl：调度框架的心脏

[framework.go](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/runtime/framework.go#L58) 中的 `frameworkImpl` 结构体是调度框架的核心：

```go
type frameworkImpl struct {
    // 扩展点插件集合
    queueSortPlugins    []fwk.QueueSortPlugin
    preFilterPlugins    []fwk.PreFilterPlugin
    filterPlugins       []fwk.FilterPlugin
    postFilterPlugins   []fwk.PostFilterPlugin
    preScorePlugins     []fwk.PreScorePlugin
    scorePlugins        []fwk.ScorePlugin
    reservePlugins      []fwk.ReservePlugin
    permitPlugins       []fwk.PermitPlugin
    preBindPlugins      []fwk.PreBindPlugin
    bindPlugins         []fwk.BindPlugin
    postBindPlugins     []fwk.PostBindPlugin
    
    // 批处理优化
    batch *OpportunisticBatch
    
    // 并发控制
    parallelizer fwk.Parallelizer
    
    // 状态管理
    waitingPods *waitingPodsMap
    podsInPreBind *podsInPreBindMap
    
    // ... 其他核心字段
}
```

---

## 4.2 核心扩展点深层解析

### 4.2.1 PreFilter：调度决策的"预处理器"

**源码位置：** [interface.go](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/interface.go#L204)

```go
// RunPreFilterPlugins不仅返回状态，还返回：
// 1. PreFilterResult - 可以影响后续的节点评估范围
// 2. 拒绝节点的插件集合
RunPreFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod) 
    (*fwk.PreFilterResult, *fwk.Status, sets.Set[string])
```

**PreFilter的核心价值：**
- 一次性计算Pod所需的所有资源
- 快速过滤掉不可能满足的节点
- 为Filter阶段准备计算上下文

**NodeResourcesFit PreFilter 实现示例：**

```go
func (pl *Fit) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
    podRequest := computePodResourceRequest(pod) // 一次性计算资源
    state.Write(preFilterStateKey, podRequest)   // 存入CycleState供后续使用
    return nil, nil
}
```

### 4.2.2 Filter："是或否"的决策点

Filter阶段是调度过程中**最耗时**的阶段，因为需要对每个节点执行所有Filter插件。

**框架实现的优化策略：**
1. **并行过滤：** 使用Parallelizer同时评估多个节点
2. **快速失败：** 任一插件拒绝，立即停止对该节点的评估
3. **节点采样：** 大规模集群中不评估所有节点

```go
// frameworkImpl.RunFilterPlugins源码结构
func (f *frameworkImpl) RunFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
    for _, pl := range f.filterPlugins {
        status := pl.Filter(ctx, state, pod, nodeInfo)
        if !status.IsSuccess() {
            return status // 快速失败：任一插件拒绝即停止
        }
    }
    return nil
}
```

### 4.2.3 PostFilter：抢占机制的"发动机"

PostFilter是处理"无节点可选"情况的扩展点，最核心的实现是DefaultPreemption。

**DefaultPreemption 核心流程：**
1. 选择一组"受害者"(低优先级Pod)
2. 评估驱逐受害者后当前Pod能否调度
3. 选择最佳的驱逐方案
4. 更新Pod的NominatedNodeName

### 4.2.4 Score：精细的节点排序

Score插件为每个通过Filter的节点打分（0-100）。

**分数计算的关键点：**
1. 每个Score插件独立打分
2. NormalizeScore插件统一评分范围
3. 最终分数 = Σ(plugin_score × plugin_weight)

---

## 4.3 性能优化的关键技术

### 4.3.1 OpportunisticBatch：机会主义批处理 (Kubernetes 1.36新特性)

**源码位置：** [batch.go](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/runtime/batch.go#L31)

这是Kubernetes 1.36引入的最重要的性能优化特性！

```go
type OpportunisticBatch struct {
    state     *batchState      // 缓存的调度结果
    lastCycle schedulingCycle  // 上一个调度周期的信息
    handle    fwk.Handle
}
```

**批处理调度的工作原理：**

```
Pod1 (signature=S) → 调度 → 节点排序 [Node1, Node2, Node3, ...]
                                                     ↓  缓存到state
Pod2 (signature=S) → 检测到可以复用 → 直接从state取Node1
                                                     ↓  继续从state取下一个
Pod3 (signature=S) → 继续复用 → 直接从state取Node2
```

**核心优化逻辑：** [GetNodeHint](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/runtime/batch.go#L63)

```go
func (b *OpportunisticBatch) GetNodeHint(ctx context.Context, pod *v1.Pod, signature fwk.PodSignature, state fwk.CycleState, cycleCount int64) string {
    // 1. 检查是否有可复用的缓存状态
    if !b.batchStateCompatible(ctx, pod, signature, cycleCount, state, nodeInfos) {
        return "" // 不可复用，返回空hint
    }
    
    // 2. 从缓存的排序列表中弹出最佳节点
    hint = b.state.sortedNodes.Pop()
    return hint
}
```

**批处理缓存的失效条件：**
- 间隔超过500ms (maxBatchAge)
- Pod签名不匹配
- 上一个节点还能容纳当前Pod (资源充足)
- 上一个调度周期失败
- 有其他Profile的Pod插入调度

**批处理的性能提升：**
- 避免重复的Filter和Score计算
- 大规模Pod部署场景下提升50%+的调度速度
- 特别适合DaemonSet、Job等批量调度场景

### 4.3.2 CycleState：状态传递的艺术

CycleState是调度框架中非常精妙的设计，它解决了两个问题：
1. 避免在扩展点间重复计算
2. 提供插件间协作的机制

**源码：** [cycle_state.go](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/cycle_state.go)

```go
type CycleState struct {
    state sync.Map // key是StateKey，value是StateData
}

// 每个StateData需要实现Clone()，因为某些场景需要克隆状态
type StateData interface {
    Clone() StateData
}
```

**使用模式示例：**

```go
// PreFilter阶段：计算一次，存入CycleState
func (pl *Plugin) PreFilter(...) {
    expensiveData := computeExpensiveData(pod)
    state.Write(MyStateKey, expensiveData)
}

// Filter阶段：直接从CycleState读取，避免重复计算
func (pl *Plugin) Filter(...) {
    expensiveData, _ := state.Read(MyStateKey)
    use(expensiveData)
}
```

### 4.3.3 并行化调度：Parallelizer

调度框架使用并行化来加速Filter阶段：

```go
// 并行过滤实现的核心思想
func findNodesThatFitPod() {
    numWorkers := parallelizer.Parallelism()
    resultChan := make(chan NodeInfo)
    
    for i := 0; i &lt; numWorkers; i++ {
        go func() {
            for node := range nodeChan {
                if fits(node, pod) {
                    resultChan &lt;- node
                }
            }
        }()
    }
    // ... 收集结果
}
```

---

## 4.4 调度扩展的深度实战

### 4.4.1 开发一个完整的调度插件

让我们通过一个实际的例子来展示如何开发调度插件："NodeGroupAffinity"。

**步骤1：定义插件结构**

```go
type NodeGroupAffinity struct {
    handle framework.Handle
}
```

**步骤2：实现PreFilter和Filter扩展点**

```go
// PreFilter：解析Pod注解中定义的节点组偏好
func (pl *NodeGroupAffinity) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
    preferredGroups := getPreferredNodeGroups(pod)
    state.Write(nodeGroupAffinityKey, preferredGroups)
    return nil, nil
}

// Filter：过滤不满足节点组的节点
func (pl *NodeGroupAffinity) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
    preferredGroups, _ := state.Read(nodeGroupAffinityKey)
    nodeGroups := getNodeGroups(nodeInfo.Node())
    
    if !hasOverlap(preferredGroups, nodeGroups) {
        return framework.NewStatus(framework.Unschedulable, "Node not in preferred group")
    }
    return nil
}

// Score：优先选择更匹配的节点组
func (pl *NodeGroupAffinity) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
    preferredGroups, _ := state.Read(nodeGroupAffinityKey)
    nodeInfo, _ := handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
    nodeGroups := getNodeGroups(nodeInfo.Node())
    
    score := calculateMatchScore(preferredGroups, nodeGroups)
    return score, nil
}
```

**步骤3：注册插件到Registry**

```go
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
    return &amp;NodeGroupAffinity{handle: h}, nil
}

func init() {
    registry.Register(PluginName, New)
}
```

### 4.4.2 插件配置与启用

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: my-scheduler
  plugins:
    preFilter:
      enabled:
      - name: NodeResourcesFit
      - name: NodeGroupAffinity  # 启用我们的自定义插件
    filter:
      enabled:
      - name: NodeResourcesFit
      - name: TaintToleration
      - name: NodeGroupAffinity  # 启用我们的自定义插件
    score:
      enabled:
      - name: NodeResourcesBalancedAllocation
        weight: 1
      - name: NodeGroupAffinity
        weight: 5  # 给我们的插件更高权重
```

---

## 4.5 核心调度插件源码级分析

### 4.5.1 NodeResourcesFit：资源调度的基石

**源码：** [fit.go](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/plugins/noderesources/fit.go)

```go
func fitsRequest(podRequest *preFilterState, nodeInfo framework.NodeInfo) []InsufficientResource {
    insufficient := make([]InsufficientResource, 0)
    
    // 1. 检查Pod数量限制
    allowedPodNumber := nodeInfo.GetAllocatable().GetAllowedPodNumber()
    if len(nodeInfo.GetPods()) + 1 &gt; allowedPodNumber {
        insufficient = append(insufficient, InsufficientResource{
            ResourceName: v1.ResourcePods,
            Reason: "Too many pods",
        })
    }
    
    // 2. 检查CPU
    if podRequest.MilliCPU &gt; (allocatable.MilliCPU - requested.MilliCPU) {
        insufficient = append(insufficient, InsufficientResource{
            ResourceName: v1.ResourceCPU,
            Reason: "Insufficient cpu",
        })
    }
    
    // 3. 检查Memory
    // ...
    
    // 4. 检查扩展资源(Extended Resources)
    for rName, rQuant := range podRequest.ScalarResources {
        if rQuant &gt; (allocatableScalar[rName] - requestedScalar[rName]) {
            insufficient = append(insufficient, InsufficientResource{
                ResourceName: rName,
                Reason: fmt.Sprintf("Insufficient %v", rName),
            })
        }
    }
    
    return insufficient
}
```

**Score策略的三种模式：**

1. **LeastAllocated（默认）：** 偏好资源利用率低的节点
   ```
   Score = (1 - used/allocatable) × 10
   ```

2. **MostAllocated：** 偏好资源利用率高的节点（装箱优化）
   ```
   Score = (used/allocatable) × 10
   ```

3. **RequestedToCapacityRatio：** 自定义评分函数

### 4.5.2 TaintToleration：污点与容忍

**源码：** [taint_toleration.go](file:///d:/code/gopath/src/kubernetes-doc/kubernetes-release-1.36/pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go)

```go
func (pl *TaintToleration) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
    node := nodeInfo.Node()
    
    // 过滤出需要检查的污点（NoSchedule或NoExecute）
    filterPredicate := func(t *v1.Taint) bool {
        return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
    }
    
    if !TolerationsTolerateTaintsWithFilter(pod.Spec.Tolerations, node.Spec.Taints, filterPredicate) {
        return framework.NewStatus(framework.Unschedulable, "node has taints that the pod doesn't tolerate")
    }
    return nil
}
```

### 4.5.3 PodTopologySpread：拓扑分布控制

这个插件实现了Pod在拓扑域（如zone、rack等）中的均匀分布。

---

## 4.6 调度框架的可观测性

### 4.6.1 Metrics：性能监控

调度框架暴露了丰富的Prometheus指标：

```go
// 调度算法总耗时
metrics.SchedulingAlgorithmDuration.Observe(...)

// 绑定操作耗时
metrics.BindingDuration.Observe(...)

// 批处理调度统计
metrics.GetNodeHintDuration.Observe(...)
metrics.BatchAttemptStats.WithLabelValues(profile, hintStatus).Inc()
metrics.BatchCacheFlushed.WithLabelValues(profile, reason).Inc()
```

### 4.6.2 Events：事件记录

```go
eventRecorder.Eventf(pod, v1.EventTypeNormal, "Scheduled", 
    "Successfully assigned %v/%v to %v", pod.Namespace, pod.Name, node)
```

---

## 4.7 调度框架的演进与未来

### 4.7.1 关键版本的重要改进

| 版本 | 重要特性 |
|------|----------|
| 1.18 | 调度框架GA |
| 1.20 | PodTopologySpread稳定 |
| 1.23 | Dynamic Resource Allocation (Alpha) |
| 1.36 | Opportunistic Batch Scheduling (Alpha) |
| 未来 | 更多DRA增强、AI驱动的调度 |

### 4.7.2 未来方向

- **AI驱动的调度预测：** 使用机器学习预测工作负载需求
- **更精细的DRA支持：** 更强大的动态资源分配能力
- **更智能的批处理：** 更复杂的批处理调度优化
- **调度器微服务化：** 支持更多形式的扩展

---

## 4.8 总结

本章深入分析了Kubernetes调度框架的内部机制：

1. **架构设计：** frameworkImpl作为核心协调所有扩展点
2. **性能优化：**
   - OpportunisticBatch批处理调度（1.36新特性）
   - CycleState避免重复计算
   - Parallelizer并行节点过滤
   - 节点采样提升大规模集群的性能
3. **插件开发：** 完整的插件开发流程和最佳实践
4. **核心插件：** NodeResourcesFit、TaintToleration等的源码级分析
5. **可观测性：** Metrics和Event机制

调度框架的设计展示了Kubernetes社区卓越的软件工程实践，它既保持了向后兼容，又为未来的创新提供了无限可能。
