# DEP2P PubSub

DEP2P PubSub 是一个基于 libp2p 的分布式发布订阅系统,提供了灵活的消息发布和订阅功能。除了传统的发布订阅模式外,还创新性地提供了请求-响应模式。

## 主要特性

### 1. 多种发布订阅模式
- GossipSub: 基于 gossip 协议的高效消息传播
- FloodSub: 简单的消息洪泛机制
- RandomSub: 随机选择节点进行消息传播

### 2. 创新的请求-响应模式 (PublishWithReply)

除了传统的单向消息发布,本系统创新性地提供了 PublishWithReply 功能,支持发送消息并等待响应:

```go
// 发送消息并等待响应
reply, err := topic.PublishWithReply(ctx, message)
// 指定目标节点发送消息并等待响应
reply, err := topic.PublishWithReply(ctx, message, targetPeerIDs...)
```

### 3. 与传统 PubSub 的对比:

| 特性 | 传统 PubSub | DEP2P PubSub |
|------|------------|--------------|
| 消息流向 | 单向(发布->订阅) | 双向(支持请求-响应) |
| 响应处理 | 需要额外实现 | 内置支持 |
| 目标指定 | 广播给所有订阅者 | 可指定特定节点 |

### 3. 高级特性

- 消息追踪: 支持消息全链路追踪
- 节点发现: 自动发现网络中的其他节点
- 状态监控: 实时监控节点状态和网络质量
- 安全机制: 支持消息签名和验证

## 使用示例

### 基本发布订阅:

```go
// 创建主题
topic, err := ps.Join("test-topic")
// 订阅消息
sub, err := topic.Subscribe()
msg, err := sub.Next(ctx)
// 发布消息
err = topic.Publish(ctx, []byte("Hello World"))
```

### 请求-响应模式:

```go
// 发送方:
reply, err := topic.PublishWithReply(ctx, []byte("Request"))
fmt.Printf("收到响应: %s\n", reply)

// 接收方:
sub, err := topic.Subscribe()
msg, err := sub.Next(ctx)

// 发送响应
replyMsg := append([]byte("Reply: "), msg.Data...)
err = topic.Publish(ctx, replyMsg, WithMessageMetadata(
    msg.GetMetadata().GetMessageID(), 
    pb.MessageMetadata_RESPONSE,
))
```

## 性能优化

- 支持消息缓存和批处理
- 动态调整心跳间隔
- 智能节点评分系统
- 自适应的网络质量监控

## 配置选项

提供丰富的配置选项以适应不同场景:

```go
options := &Options{
    SignMessages:     true,  // 启用消息签名
    ValidateMessages: true,  // 启用消息验证
    MaxMessageSize:   1024 * 1024, // 最大消息大小
    HeartbeatInterval: 500 * time.Millisecond,
    PubSubMode:       GossipSub, // 使用 GossipSub 模式
}
```

## 注意事项

1. 合理配置消息大小限制
2. 根据网络规模调整心跳间隔
3. 在大规模网络中建议启用消息签名和验证
4. 注意处理超时和错误情况

## 订阅响应模式详解

### 1. 工作原理

订阅响应模式基于以下核心机制：

- **消息ID追踪**: 每个请求消息都有唯一的 UUID，用于关联请求和响应
- **消息类型标记**: 通过 MessageMetadata 区分请求（REQUEST）和响应（RESPONSE）
- **响应通道管理**: 为每个请求维护一个专用的响应通道
- **超时控制**: 内置超时机制，避免无限等待响应
- **重试机制**: 支持自动重试失败的请求

### 2. 高级用法

#### 2.1 定向请求响应
```go
// 向特定节点发送请求
reply, err := topic.PublishWithReply(ctx, data, targetPeerID1, targetPeerID2)

// 批量处理多个响应
replies := make([][]byte, 0)
for i := 0; i < expectedResponses; i++ {
    reply, err := topic.PublishWithReply(ctx, data)
    if err == nil {
        replies = append(replies, reply)
    }
}
```

#### 2.2 超时和重试控制
```go
// 使用带超时的上下文
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// 带重试的请求发送
reply, err := topic.PublishWithReply(ctx, data)
if err == context.DeadlineExceeded {
    // 处理超时情况
}
```

#### 2.3 响应处理模式
```go
// 接收方处理请求并响应
sub, err := topic.Subscribe()
msg, err := sub.Next(ctx)

// 检查消息类型
if msg.GetMetadata().GetType() == pb.MessageMetadata_REQUEST {
    // 处理请求并发送响应
    response := processRequest(msg.Data)
    err = topic.Publish(ctx, response, WithMessageMetadata(
        msg.GetMetadata().GetMessageID(),
        pb.MessageMetadata_RESPONSE,
    ))
}
```

### 3. 性能优化建议

#### 3.1 网络优化
- **连接池管理**: 维护活跃连接，减少建立连接的开销
- **消息压缩**: 对大型消息进行压缩传输
- **批量处理**: 合并多个小请求，减少网络往返

#### 3.2 超时设置
```go
const (
    MinTimeout = 1 * time.Second
    MaxTimeout = 30 * time.Second
    DefaultTimeout = 5 * time.Second
)

// 根据网络状况动态调整超时时间
timeout := calculateDynamicTimeout(networkLatency)
ctx, cancel := context.WithTimeout(context.Background(), timeout)
```

#### 3.3 错误处理策略
```go
// 实现渐进式重试
for attempt := 1; attempt <= maxRetries; attempt++ {
    reply, err := topic.PublishWithReply(ctx, data)
    if err == nil {
        break
    }
    // 指数退避
    backoff := time.Duration(attempt * attempt) * baseDelay
    time.Sleep(backoff)
}
```

### 4. 应用场景

#### 4.1 分布式服务发现
- 节点能力查询
- 服务状态检查
- 资源可用性探测

#### 4.2 分布式协调
- 领导者选举
- 配置同步
- 状态协商

#### 4.3 数据同步
- 增量数据同步
- 状态验证
- 数据一致性检查

### 5. 监控和调试

#### 5.1 内置指标
- 请求响应延迟
- 成功/失败率
- 重试次数
- 超时统计

#### 5.2 日志追踪
```go
// 启用详细日志
options := []PubOpt{
    WithMessageTracing(true),
    WithPerformanceMetrics(true),
}
```

### 6. 安全考虑

#### 6.1 消息安全
- 支持消息签名验证
- 可选的消息加密
- 防重放攻击保护

#### 6.2 访问控制
- 基于节点ID的权限控制
- 请求频率限制
- 消息大小限制

### 7. 最佳实践

1. **错误处理**
   - 始终设置合理的超时时间
   - 实现优雅的降级策略
   - 处理所有可能的错误情况

2. **性能优化**
   - 适当的缓存策略
   - 批量处理请求
   - 合理的重试策略

3. **资源管理**
   - 及时清理过期的响应通道
   - 控制并发请求数量
   - 监控资源使用情况

4. **测试建议**
   - 进行压力测试
   - 模拟网络延迟和故障
   - 验证错误恢复机制

## 贡献指南

欢迎提交 Issue 和 Pull Request。在提交代码前,请确保:

1. 通过所有测试用例
2. 遵循代码规范
3. 更新相关文档
4. 添加必要的测试用例