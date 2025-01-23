// package pubsub 提供了发布订阅功能的实现
package pubsub

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/dep2p/go-dep2p/core/host"
	"github.com/dep2p/go-dep2p/core/network"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/p2p/protocol/ping"
)

// NodeStatus 表示节点的状态
type NodeStatus int

const (
	Online     NodeStatus = iota // 节点在线
	Suspicious                   // 节点可疑
	Offline                      // 节点离线
)

// StatusChange 表示节点状态的变化
type StatusChange struct {
	PeerID            peer.ID       // 节点ID
	OldStatus         NodeStatus    // 旧状态
	NewStatus         NodeStatus    // 新状态
	Timestamp         time.Time     // 状态变化时间戳
	Score             float64       // 节点评分
	ConnectionQuality float64       // 连接质量
	CheckInterval     time.Duration // 检查间隔
	FailedAttempts    int           // 连续失败尝试次数
	NetworkLatency    time.Duration // 网络延迟
}

// Node 表示一个节点及其状态息
type Node struct {
	ID                  peer.ID         // 节点ID
	Status              NodeStatus      // 当前状态
	LastSeen            time.Time       // 最后一次看到节点的时间
	FailedAttempts      int             // 连续失败尝试次数
	Score               float64         // 节点评分
	History             []StatusChange  // 状态变化历史
	CheckInterval       time.Duration   // 检查间隔
	SuccessiveSuccesses int             // 连续成功次数
	LastCheckTime       time.Time       // 上次检查时间
	ConnectionQuality   float64         // 连接质量
	NetworkCondition    []time.Duration // 网络延迟历史
}

// NodeStatusTracker 用于跟踪和管理节点状态
type NodeStatusTracker struct {
	host                 host.Host           // libp2p主机
	nodes                map[peer.ID]*Node   // 节点映射
	defaultCheckInterval time.Duration       // 默认检查间隔
	offlineThreshold     int                 // 离线阈值
	pingTimeout          time.Duration       // ping超时时间
	pingService          *ping.PingService   // ping服务
	mu                   sync.RWMutex        // 读写锁
	ctx                  context.Context     // 上下文
	cancel               context.CancelFunc  // 取消函数
	statusChanges        chan StatusChange   // 态变化通道
	subscribers          []chan StatusChange // 订阅者列表
}

// NewNodeStatusTracker 创建一个新的 NodeStatusTracker 实例
// 参数:
//   - h: libp2p主机
//   - defaultCheckInterval: 默认检查间隔
//   - offlineThreshold: 离线阈值
//   - pingTimeout: ping超时时间
//
// 返回:
//   - *NodeStatusTracker: 新创建的NodeStatusTracker实例
func NewNodeStatusTracker(h host.Host, defaultCheckInterval time.Duration, offlineThreshold int, pingTimeout time.Duration) *NodeStatusTracker {
	// 创建一个带有取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 返回一个新的NodeStatusTracker实例
	return &NodeStatusTracker{
		host:                 h,
		nodes:                make(map[peer.ID]*Node),
		defaultCheckInterval: defaultCheckInterval,
		offlineThreshold:     offlineThreshold,
		pingTimeout:          pingTimeout,
		pingService:          ping.NewPingService(h),
		ctx:                  ctx,
		cancel:               cancel,
		statusChanges:        make(chan StatusChange, 100),
	}
}

// Start 启动节点状态跟踪器
func (nst *NodeStatusTracker) Start() {
	go nst.checkNodesRoutine()    // 启动节点检查例程
	go nst.processStatusChanges() // 启动状态变化处理例程
}

// Stop 停止节点状跟踪器
func (nst *NodeStatusTracker) Stop() {
	nst.cancel() // 调用取消函数停止所有goroutine
}

// checkNodesRoutine 定期检查所有节点的状态
func (nst *NodeStatusTracker) checkNodesRoutine() {
	for {
		select {
		case <-nst.ctx.Done():
			return // 如果上下文被取消，退出循环
		default:
			nst.checkAllNodes()                  // 检查所有节点
			time.Sleep(nst.defaultCheckInterval) // 等待默认检查间隔
		}
	}
}

// checkAllNodes 检查所有已知节点的状态
func (nst *NodeStatusTracker) checkAllNodes() {
	// 获取读锁以安全地读取节点列表
	nst.mu.RLock()
	peers := make([]peer.ID, 0, len(nst.nodes))
	for pid := range nst.nodes {
		peers = append(peers, pid)
	}
	nst.mu.RUnlock()

	// 遍历所有节点并检查它们的状态
	for _, pid := range peers {
		nst.checkNode(pid) // 检查每个节点
	}
}

// checkNode 检查单个节点的状态 - 非阻塞主入口
func (nst *NodeStatusTracker) checkNode(pid peer.ID) {
	// 1. 快速路径检查 - 使用读锁
	nst.mu.RLock()
	node, exists := nst.nodes[pid]
	if exists {
		// 跳过频繁检查
		if time.Since(node.LastCheckTime) < node.CheckInterval {
			nst.mu.RUnlock()
			return
		}
		// 在线且活跃的节点降低检查频率
		if node.Status == Online && time.Since(node.LastSeen) < 5*time.Second {
			nst.mu.RUnlock()
			return
		}
	}
	nst.mu.RUnlock()

	// 2. 快速连接检查 - 无需加锁
	if nst.host.Network().Connectedness(pid) == network.Connected {
		nst.handleQuickSuccess(pid)
		return
	}

	// 3. 异步执行完整检查
	go nst.asyncFullCheck(pid)
}

// handleQuickSuccess 处理快速路径成功的情况
func (nst *NodeStatusTracker) handleQuickSuccess(pid peer.ID) {
	nst.mu.Lock()
	defer nst.mu.Unlock()

	node, exists := nst.nodes[pid]
	if !exists {
		// 新节点初始化 - 给予较高的初始信任度
		node = &Node{
			ID:                pid,
			Status:            Online,
			LastSeen:          time.Now(),
			Score:             0.8,
			CheckInterval:     nst.defaultCheckInterval * 2,
			ConnectionQuality: 0.8,
			NetworkCondition:  make([]time.Duration, 0),
			History:           make([]StatusChange, 0),
		}
		nst.nodes[pid] = node
		return
	}

	// 更新基本状态
	node.LastSeen = time.Now()
	node.LastCheckTime = time.Now()
	if node.Status != Online {
		nst.updateNodeStatus(node, Online)
	}
}

// asyncFullCheck 执行完整的异步节点检查
func (nst *NodeStatusTracker) asyncFullCheck(pid peer.ID) {
	const (
		quickCheckTimeout = 500 * time.Millisecond
		fullCheckTimeout  = 3 * time.Second
	)

	// 1. 创建检查上下文
	ctx, cancel := context.WithTimeout(context.Background(), fullCheckTimeout)
	defer cancel()

	// 2. 并行执行多种检查
	results := make(chan checkResult, 4)

	// 2.1 直连检查
	go func() {
		results <- checkResult{
			name:    "direct",
			success: nst.host.Network().Connectedness(pid) == network.Connected,
		}
	}()

	// 2.2 Ping 检查
	go func() {
		start := time.Now()
		success := nst.pingNode(pid)
		results <- checkResult{
			name:    "ping",
			success: success,
			latency: time.Since(start),
		}
	}()

	// 2.3 DHT 查找（如果可用）
	go func() {
		results <- checkResult{
			name:    "dht",
			success: nst.checkDHT(pid),
		}
	}()

	// 3. 收集检查结果
	success := false
	var bestLatency time.Duration
	checkCount := 0

	for checkCount < 3 {
		select {
		case result := <-results:
			checkCount++
			if result.success {
				success = true
				if result.latency > 0 {
					bestLatency = result.latency
				}
				// 一旦发现成功，快速更新状态
				nst.updateNodeState(pid, true, bestLatency)
				return
			}
		case <-ctx.Done():
			// 超时处理
			nst.updateNodeState(pid, false, 0)
			return
		}
	}

	// 所有检查都失败
	if !success {
		nst.updateNodeState(pid, false, 0)
	}
}

// checkResult 表示单个检查的结果
type checkResult struct {
	name    string
	success bool
	latency time.Duration
}

// updateNodeState 更新节点状态并调整检查间隔
// 参数:
//   - pid: 节点ID
//   - isConnected: 是否连接成功
//   - latency: 网络延迟
func (nst *NodeStatusTracker) updateNodeState(pid peer.ID, isConnected bool, latency time.Duration) {
	nst.mu.Lock()
	defer nst.mu.Unlock()

	// 定义状态转换和评分的关键阈值
	const (
		minScore               = 0.4             // 最低评分阈值
		recoveryThreshold      = 2               // 状态恢复所需的连续成功次数
		maxFailureThreshold    = 5               // 状态降级的失败阈值
		networkJitterTolerance = 5 * time.Second // 网络抖动容忍时间
	)

	// 获取节点信息
	node, exists := nst.nodes[pid]
	if !exists {
		return
	}

	now := time.Now()
	oldStatus := node.Status

	if isConnected {
		// 处理连接成功的情况
		node.LastSeen = now
		node.SuccessiveSuccesses++
		// 快速减少失败计数，给予节点恢复机会
		node.FailedAttempts = int(float64(node.FailedAttempts) * 0.3)

		// 根据连续成功次数动态调整评分增长
		scoreIncrease := 0.1 * (1 + float64(node.SuccessiveSuccesses)/20)
		node.Score = math.Min(1.0, node.Score+scoreIncrease)

		// 动态调整检查间隔
		if node.Score > 0.8 {
			node.CheckInterval = nst.defaultCheckInterval * 2 // 高分节点降低检查频率
		} else {
			node.CheckInterval = nst.defaultCheckInterval // 其他情况使用默认间隔
		}

		// 状态恢复逻辑
		if node.Status != Online &&
			(node.SuccessiveSuccesses >= recoveryThreshold ||
				time.Since(node.LastSeen) < networkJitterTolerance) {
			nst.updateNodeStatus(node, Online)
		}
	} else {
		// 处理连接失败的情况
		if time.Since(node.LastSeen) < networkJitterTolerance {
			return // 跳过临时性失败
		}

		node.FailedAttempts++
		node.SuccessiveSuccesses = 0

		// 根据当前评分计算惩罚力度
		penaltyFactor := math.Max(0.7, float64(node.Score))
		actualDecrement := 0.05 * penaltyFactor
		node.Score = math.Max(minScore, node.Score-actualDecrement)

		// 缩短检查间隔以更快发现状态改善
		node.CheckInterval = nst.defaultCheckInterval / 2

		// 渐进式状态降级
		if node.FailedAttempts >= maxFailureThreshold {
			if node.Status == Online {
				nst.updateNodeStatus(node, Suspicious)
			} else if node.Status == Suspicious &&
				node.FailedAttempts >= maxFailureThreshold*2 {
				nst.updateNodeStatus(node, Offline)
				go nst.performDeepCheck(pid) // 触发深度检查
			}
		}
	}

	// 记录状态变化
	if oldStatus != node.Status {
		nst.recordStatusChange(StatusChange{
			PeerID:            pid,
			OldStatus:         oldStatus,
			NewStatus:         node.Status,
			Timestamp:         now,
			Score:             node.Score,
			ConnectionQuality: node.ConnectionQuality,
			CheckInterval:     node.CheckInterval,
			FailedAttempts:    node.FailedAttempts,
			NetworkLatency:    latency,
		})
	}
}

// updateNodeStatus 更新节点状态
// 参数:
//   - node: 要更新的节点
//   - status: 新的状态
func (nst *NodeStatusTracker) updateNodeStatus(node *Node, status NodeStatus) {
	node.Status = status
	if status == Online {
		node.LastSeen = time.Now()
		node.FailedAttempts = 0
		node.Score = min(node.Score+0.1, 1.0) // 增加节点评分，最高为1.0
	} else {
		node.Score = max(node.Score-0.2, 0.0) // 减少节点评分，最低为0.0
	}
}

// pingNode 尝试 ping 指定的节点
// 参数:
//   - pid: 要ping的节点ID
//
// 返回:
//   - bool: ping是否成功
func (nst *NodeStatusTracker) pingNode(pid peer.ID) bool {
	// 创建一个带有超时的上下文
	ctx, cancel := context.WithTimeout(nst.ctx, nst.pingTimeout)
	defer cancel()

	// 执行ping操作
	result := nst.pingService.Ping(ctx, pid)
	select {
	case <-result:
		return true // ping成功
	case <-ctx.Done():
		return false // ping超时
	}
}

// recordStatusChange 记录状态变化并通知订阅者
// 参数:
//   - change: 状态变化信息
func (nst *NodeStatusTracker) recordStatusChange(change StatusChange) {
	select {
	case nst.statusChanges <- change:
		// 成功将变化发送到通道
	default:
		// 如果通道已满，记录日志但不阻塞
		logger.Warnf("状态变化通道已满，丢弃变化：%v", change)
	}
}

// processStatusChanges 处理状态变化并通知订阅者
func (nst *NodeStatusTracker) processStatusChanges() {
	for {
		select {
		case change := <-nst.statusChanges:
			nst.notifySubscribers(change) // 通知所有订阅者
		case <-nst.ctx.Done():
			return // 如果上下文被取消，退出循环
		}
	}
}

// notifySubscribers 通知所有订阅者状态变化
// 参数:
//   - change: 状态变化信息
func (nst *NodeStatusTracker) notifySubscribers(change StatusChange) {
	for _, sub := range nst.subscribers {
		select {
		case sub <- change:
			// 成功发送状态变化到订阅者
		default:
			// 如果订阅者的通道已满，跳过但不阻塞
		}
	}
}

// SubscribeStatusChanges 订阅状态变化事件
// 返回:
//   - chan StatusChange: 状态变化通道
func (nst *NodeStatusTracker) SubscribeStatusChanges() chan StatusChange {
	ch := make(chan StatusChange, 10)
	nst.mu.Lock()
	nst.subscribers = append(nst.subscribers, ch)
	nst.mu.Unlock()
	return ch
}

// UnsubscribeStatusChanges 取消订阅状态变化事件
// 参数:
//   - ch: 要取消订阅的通道
func (nst *NodeStatusTracker) UnsubscribeStatusChanges(ch chan StatusChange) {
	nst.mu.Lock()
	for i, sub := range nst.subscribers {
		if sub == ch {
			// 从订阅者列表中移除指定的通道
			nst.subscribers = append(nst.subscribers[:i], nst.subscribers[i+1:]...)
			break
		}
	}
	nst.mu.Unlock()
}

// UpdateNodeStatus 允许外部组件更新节点状态
// 参数:
//   - pid: 要更新的节点ID
//   - status: 新的状态
func (nst *NodeStatusTracker) UpdateNodeStatus(pid peer.ID, status NodeStatus) {
	nst.mu.Lock()
	defer nst.mu.Unlock()

	// 获取或创建节点
	node, exists := nst.nodes[pid]
	if !exists {
		node = &Node{ID: pid, CheckInterval: nst.defaultCheckInterval}
		nst.nodes[pid] = node
	}

	oldStatus := node.Status
	nst.updateNodeStatus(node, status)

	// 如果状态发生变化，记录变化
	if oldStatus != status {
		nst.recordStatusChange(StatusChange{
			PeerID:    pid,
			OldStatus: oldStatus,
			NewStatus: status,
			Timestamp: time.Now(),
		})
	}
}

// min 返回两个浮点数中的较小值
// 参数:
//   - a, b: 要比较的两个浮点数
//
// 返回:
//   - float64: 较小的值
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// max 返回两个浮点数中的较大值
// 参数:
//   - a, b: 要比较的两个浮点数
//
// 返回:
//   - float64: 较大的值
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// performDeepCheck 执行深度节点检查
func (nst *NodeStatusTracker) performDeepCheck(pid peer.ID) {
	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// 尝试多种方式检查节点
	checks := []struct {
		name  string
		check func() bool
	}{
		{"直接连接", func() bool {
			return nst.host.Network().Connectedness(pid) == network.Connected
		}},
		{"Ping检查", func() bool {
			return nst.pingNode(pid)
		}},
		{"DHT查找", func() bool {
			// 如果实现了DHT，可以尝试通过DHT查找节点
			return false // 待实现
		}},
	}

	for _, check := range checks {
		select {
		case <-ctx.Done():
			return
		default:
			if check.check() {
				// 如果任何检查成功，更新节点状态
				nst.mu.Lock()
				if node, exists := nst.nodes[pid]; exists {
					nst.updateNodeStatus(node, Suspicious)         // 降级到可疑而不是直接在线
					node.FailedAttempts = nst.offlineThreshold - 1 // 给予恢复机会
				}
				nst.mu.Unlock()
				return
			}
		}
	}
}

// checkDHT 通过 DHT 检查节点是否可达
func (nst *NodeStatusTracker) checkDHT(pid peer.ID) bool {
	// 创建超时上下文
	ctx, cancel := context.WithTimeout(nst.ctx, 5*time.Second)
	defer cancel()

	// 尝试从 DHT 查找节点
	if dht, ok := nst.host.Peerstore().(interface {
		FindPeer(context.Context, peer.ID) (peer.AddrInfo, error)
	}); ok {
		_, err := dht.FindPeer(ctx, pid)
		return err == nil
	}

	// 如果没有 DHT 功能，返回 false
	return false
}
