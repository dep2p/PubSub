// 作用：管理对等节点的连接和访问控制。
// 功能：根据对等节点的行为和评分，决定是否允许与其通信。

package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/dep2p/pubsub/logger"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	manet "github.com/multiformats/go-multiaddr/net"
)

// 默认参数定义
var (
	DefaultPeerGaterRetainStats     = 6 * time.Hour                        // 保留统计数据的默认时间
	DefaultPeerGaterQuiet           = time.Minute                          // 安静期的默认时间
	DefaultPeerGaterDuplicateWeight = 0.125                                // 重复消息的权重
	DefaultPeerGaterIgnoreWeight    = 1.0                                  // 忽略消息的权重
	DefaultPeerGaterRejectWeight    = 16.0                                 // 拒绝消息的权重
	DefaultPeerGaterThreshold       = 0.33                                 // 门控阈值
	DefaultPeerGaterGlobalDecay     = ScoreParameterDecay(2 * time.Minute) // 全局衰减参数
	DefaultPeerGaterSourceDecay     = ScoreParameterDecay(time.Hour)       // 来源衰减参数
)

// PeerGaterParams 包含控制 peer gater 操作的参数
type PeerGaterParams struct {
	Threshold            float64            // 当被限流/验证消息的比例超过此阈值时，启用 gater
	GlobalDecay          float64            // 全局计数器衰减参数
	SourceDecay          float64            // 每 IP 计数器衰减参数
	DecayInterval        time.Duration      // 衰减间隔
	DecayToZero          float64            // 计数器归零阈值
	RetainStats          time.Duration      // 保留统计数据的时间
	Quiet                time.Duration      // 安静期时间
	DuplicateWeight      float64            // 重复消息的权重
	IgnoreWeight         float64            // 忽略消息的权重
	RejectWeight         float64            // 拒绝消息的权重
	TopicDeliveryWeights map[string]float64 // 优先主题的传递权重
}

// validate 验证参数是否合法
// 返回值:error，如果参数不合法则返回错误信息
func (p *PeerGaterParams) validate() error {
	if p.Threshold <= 0 {
		logger.Warnf("Threshold 无效: %f", p.Threshold) // Threshold 无效
		return fmt.Errorf("Threshold 无效: %f", p.Threshold)
	}
	if p.GlobalDecay <= 0 || p.GlobalDecay >= 1 {
		logger.Warnf("GlobalDecay 无效: %f", p.GlobalDecay) // GlobalDecay 无效
		return fmt.Errorf("GlobalDecay 无效: %f", p.GlobalDecay)
	}
	if p.SourceDecay <= 0 || p.SourceDecay >= 1 {
		logger.Warnf("SourceDecay 无效: %f", p.SourceDecay) // SourceDecay 无效
		return fmt.Errorf("SourceDecay 无效: %f", p.SourceDecay)
	}
	if p.DecayInterval < time.Second {
		logger.Warnf("DecayInterval 无效: %s", p.DecayInterval) // DecayInterval 无效
		return fmt.Errorf("DecayInterval 无效: %s", p.DecayInterval)
	}
	if p.DecayToZero <= 0 || p.DecayToZero >= 1 {
		logger.Warnf("DecayToZero 无效: %f", p.DecayToZero) // DecayToZero 无效
		return fmt.Errorf("DecayToZero 无效: %f", p.DecayToZero)
	}
	if p.Quiet < time.Second {
		logger.Warnf("Quiet 无效: %s", p.Quiet) // Quiet 无效
		return fmt.Errorf("Quiet 无效: %s", p.Quiet)
	}
	if p.DuplicateWeight <= 0 {
		logger.Warnf("DuplicateWeight 无效: %f", p.DuplicateWeight) // DuplicateWeight 无效
		return fmt.Errorf("DuplicateWeight 无效: %f", p.DuplicateWeight)
	}
	if p.IgnoreWeight < 1 {
		logger.Warnf("IgnoreWeight 无效: %f", p.IgnoreWeight) // IgnoreWeight 无效
		return fmt.Errorf("IgnoreWeight 无效: %f", p.IgnoreWeight)
	}
	if p.RejectWeight < 1 {
		logger.Warnf("RejectWeight 无效: %f", p.RejectWeight) // RejectWeight 无效
		return fmt.Errorf("RejectWeight 无效: %f", p.RejectWeight)
	}

	return nil
}

// WithTopicDeliveryWeights 设置优先主题的传递权重
// 参数:
//   - w: 主题权重的映射
//
// 返回值:
//   - *PeerGaterParams: 返回更新后的参数
func (p *PeerGaterParams) WithTopicDeliveryWeights(w map[string]float64) *PeerGaterParams {
	p.TopicDeliveryWeights = w
	return p
}

// NewPeerGaterParams 创建新的 PeerGaterParams 结构，使用指定的阈值和衰减参数以及默认值
// 参数:
//   - threshold: 门控阈值
//   - globalDecay: 全局衰减参数
//   - sourceDecay: 来源衰减参数
//
// 返回值:
//   - *PeerGaterParams: 返回新创建的参数结构
func NewPeerGaterParams(threshold, globalDecay, sourceDecay float64) *PeerGaterParams {
	return &PeerGaterParams{
		Threshold:       threshold,
		GlobalDecay:     globalDecay,
		SourceDecay:     sourceDecay,
		DecayToZero:     DefaultDecayToZero,
		DecayInterval:   DefaultDecayInterval,
		RetainStats:     DefaultPeerGaterRetainStats,
		Quiet:           DefaultPeerGaterQuiet,
		DuplicateWeight: DefaultPeerGaterDuplicateWeight,
		IgnoreWeight:    DefaultPeerGaterIgnoreWeight,
		RejectWeight:    DefaultPeerGaterRejectWeight,
	}
}

// DefaultPeerGaterParams 创建使用默认值的 PeerGaterParams 结构
// 返回值:
//   - *PeerGaterParams: 返回使用默认值的参数结构
func DefaultPeerGaterParams() *PeerGaterParams {
	return NewPeerGaterParams(DefaultPeerGaterThreshold, DefaultPeerGaterGlobalDecay, DefaultPeerGaterSourceDecay)
}

// peerGater 对象
type peerGater struct {
	sync.Mutex // 互斥锁，确保并发安全

	host host.Host // 本地主机

	params *PeerGaterParams // gater 参数

	validate, throttle float64 // 验证和限制计数器

	lastThrottle time.Time // 上次验证限制的时间

	peerStats map[peer.ID]*peerGaterStats // 每个 peer.ID 的统计数据
	ipStats   map[string]*peerGaterStats  // 每个 IP 的统计数据

	getIP func(peer.ID) string // 单元测试使用的获取 IP 方法
}

// peerGaterStats 包含每个 peer.ID 的统计数据
type peerGaterStats struct {
	connected int       // 连接的 peer.ID 数量
	expire    time.Time // 统计数据的过期时间

	deliver, duplicate, ignore, reject float64 // 各类消息的计数器
}

// WithPeerGater 是一个 gossipsub 路由器选项，用于启用反应性验证队列管理
// 参数:
//   - params: PeerGater 的参数
//
// 返回值:
//   - Option: PubSub 的选项函数
func WithPeerGater(params *PeerGaterParams) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter) // 将 PubSub 的路由器类型转换为 GossipSubRouter
		if !ok {                           // 如果转换失败，返回错误信息
			logger.Warnf("pubsub 路由器不是 gossipsub")
			return fmt.Errorf("pubsub 路由器不是 gossipsub")
		}

		err := params.validate() // 验证参数的有效性
		if err != nil {          // 如果验证失败，返回错误信息
			logger.Warnf("peer gater 参数验证失败: %v", err)
			return err
		}

		gs.gate = newPeerGater(ps.ctx, ps.host, params) // 创建新的 peerGater 实例，并赋值给 GossipSubRouter 的 gate 字段

		// 挂钩 tracer
		if ps.tracer != nil { // 如果已经存在 tracer
			ps.tracer.raw = append(ps.tracer.raw, gs.gate) // 将新的 gate 添加到 tracer 的 raw 列表中
		} else { // 如果不存在 tracer
			ps.tracer = &pubsubTracer{ // 创建新的 pubsubTracer 实例，并赋值给 PubSub 的 tracer 字段
				raw:   []RawTracer{gs.gate}, // 将新的 gate 添加到 raw 列表中
				pid:   ps.host.ID(),         // 设置 tracer 的 pid 为本地主机的 ID
				idGen: ps.idGen,             // 设置 tracer 的 idGen
			}
		}

		return nil // 返回 nil 表示成功
	}
}

// newPeerGater 创建一个新的 peerGater 实例
// 参数:
//   - ctx: 上下文
//   - host: 本地主机
//   - params: PeerGater 的参数
//
// 返回值:
//   - *peerGater: 返回新创建的 peerGater 实例
func newPeerGater(ctx context.Context, host host.Host, params *PeerGaterParams) *peerGater {
	pg := &peerGater{
		params:    params,                            // 设置 peerGater 的参数
		peerStats: make(map[peer.ID]*peerGaterStats), // 初始化 peerStats 为一个空的 map
		ipStats:   make(map[string]*peerGaterStats),  // 初始化 ipStats 为一个空的 map
		host:      host,                              // 设置本地主机
	}
	go pg.background(ctx) // 启动后台 goroutine 进行统计数据的衰减
	return pg             // 返回新创建的 peerGater 实例
}

// background 在后台运行，定期衰减统计数据
// 参数:
//   - ctx: 上下文
func (pg *peerGater) background(ctx context.Context) {
	tick := time.NewTicker(pg.params.DecayInterval) // 每个 DecayInterval 时间间隔触发一次

	defer tick.Stop() // 停止 ticker

	for {
		select {
		case <-tick.C:
			pg.decayStats() // 衰减统计数据
		case <-ctx.Done():
			return // 上下文取消时退出
		}
	}
}

// decayStats 衰减统计数据
func (pg *peerGater) decayStats() {
	pg.Lock()         // 加锁
	defer pg.Unlock() // 解锁

	// 衰减 validate 和 throttle 计数器
	pg.validate *= pg.params.GlobalDecay     // 对 validate 计数器进行全局衰减
	if pg.validate < pg.params.DecayToZero { // 如果 validate 计数器小于衰减到零的阈值
		pg.validate = 0 // 将 validate 计数器设置为零
	}

	pg.throttle *= pg.params.GlobalDecay     // 对 throttle 计数器进行全局衰减
	if pg.throttle < pg.params.DecayToZero { // 如果 throttle 计数器小于衰减到零的阈值
		pg.throttle = 0 // 将 throttle 计数器设置为零
	}

	now := time.Now()                // 获取当前时间
	for ip, st := range pg.ipStats { // 遍历 ipStats map
		if st.connected > 0 { // 如果统计数据中的 connected 计数器大于零
			// 衰减 deliver, duplicate, ignore, reject 计数器
			st.deliver *= pg.params.SourceDecay     // 对 deliver 计数器进行源衰减
			if st.deliver < pg.params.DecayToZero { // 如果 deliver 计数器小于衰减到零的阈值
				st.deliver = 0 // 将 deliver 计数器设置为零
			}

			st.duplicate *= pg.params.SourceDecay     // 对 duplicate 计数器进行源衰减
			if st.duplicate < pg.params.DecayToZero { // 如果 duplicate 计数器小于衰减到零的阈值
				st.duplicate = 0 // 将 duplicate 计数器设置为零
			}

			st.ignore *= pg.params.SourceDecay     // 对 ignore 计数器进行源衰减
			if st.ignore < pg.params.DecayToZero { // 如果 ignore 计数器小于衰减到零的阈值
				st.ignore = 0 // 将 ignore 计数器设置为零
			}

			st.reject *= pg.params.SourceDecay     // 对 reject 计数器进行源衰减
			if st.reject < pg.params.DecayToZero { // 如果 reject 计数器小于衰减到零的阈值
				st.reject = 0 // 将 reject 计数器设置为零
			}
		} else if st.expire.Before(now) { // 如果统计数据已经过期
			delete(pg.ipStats, ip) // 删除过期的统计数据
		}
	}
}

// getPeerStats 获取或创建指定 peer.ID 的统计数据
// 参数:
//   - p: peer.ID
//
// 返回值:
//   - *peerGaterStats: 返回对应的统计数据
func (pg *peerGater) getPeerStats(p peer.ID) *peerGaterStats {
	st, ok := pg.peerStats[p] // 从 peerStats map 中获取指定 peer.ID 的统计数据
	if !ok {                  // 如果不存在
		st = pg.getIPStats(p) // 调用 getIPStats 获取 IP 统计数据
		pg.peerStats[p] = st  // 将新的统计数据添加到 peerStats map 中
	}
	return st // 返回统计数据
}

// getIPStats 获取或创建指定 IP 的统计数据
// 参数:
//   - p: peer.ID
//
// 返回值:
//   - *peerGaterStats: 返回对应的统计数据
func (pg *peerGater) getIPStats(p peer.ID) *peerGaterStats {
	ip := pg.getPeerIP(p)    // 获取指定 peer.ID 的 IP 地址
	st, ok := pg.ipStats[ip] // 从 ipStats map 中获取对应 IP 的统计数据
	if !ok {                 // 如果不存在
		st = &peerGaterStats{} // 创建一个新的 peerGaterStats 实例
		pg.ipStats[ip] = st    // 将新的统计数据添加到 ipStats map 中
	}
	return st // 返回统计数据
}

// getPeerIP 获取指定 peer.ID 的 IP 地址
// 参数:
//   - p: peer.ID
//
// 返回值:
//   - string: 返回对应的 IP 地址
func (pg *peerGater) getPeerIP(p peer.ID) string {
	if pg.getIP != nil { // 如果自定义的 getIP 函数存在
		return pg.getIP(p) // 使用自定义的 getIP 函数获取 IP 地址
	}

	// connToIP 是一个辅助函数，用于从连接中提取 IP 地址
	connToIP := func(c network.Conn) string {
		remote := c.RemoteMultiaddr() // 获取远程的 Multiaddr
		ip, err := manet.ToIP(remote) // 将 Multiaddr 转换为 IP 地址
		if err != nil {               // 如果转换失败
			logger.Warnf("确定远程节点的 IP 失败: %s: %s", remote, err) // 记录警告日志
			return "<unknown>"                                 // 返回未知 IP
		}
		return ip.String() // 返回 IP 地址字符串
	}

	conns := pg.host.Network().ConnsToPeer(p) // 获取与指定 peer.ID 相关的所有连接
	switch len(conns) {                       // 根据连接的数量进行处理
	case 0: // 如果没有连接
		return "<unknown>" // 返回未知 IP
	case 1: // 如果只有一个连接
		return connToIP(conns[0]) // 使用该连接获取 IP 地址
	default: // 如果有多个连接
		// 有多个连接 -- 按流的数量排序，使用流最多的那个连接；跟踪每个 peer 的多个 IP 是个噩梦，所以选择最好的一个。
		streams := make(map[string]int) // 创建一个 map 用于存储每个连接的流数量
		for _, c := range conns {       // 遍历所有连接
			if c.Stat().Limited { // 如果连接是瞬态连接
				// 忽略瞬态连接
				continue
			}
			streams[c.ID()] = len(c.GetStreams()) // 将连接 ID 和对应的流数量存储到 map 中
		}
		// 按照流的数量对连接进行排序
		sort.Slice(conns, func(i, j int) bool {
			return streams[conns[i].ID()] > streams[conns[j].ID()]
		})
		return connToIP(conns[0]) // 使用流最多的那个连接获取 IP 地址
	}
}

// AcceptFrom 判断是否接受来自指定 peer.ID 的消息
// 参数:
//   - p: peer.ID
//
// 返回值:
//   - AcceptStatus: 接受状态
func (pg *peerGater) AcceptFrom(p peer.ID) AcceptStatus {
	if pg == nil {
		return AcceptAll
	}

	pg.Lock()
	defer pg.Unlock()

	// 检查安静期；如果验证队列在安静期内没有被限流，则关闭断路器并接受消息。
	if time.Since(pg.lastThrottle) > pg.params.Quiet {
		return AcceptAll
	}

	// 没有限流事件 -- 或者它们已经衰减；接受消息。
	if pg.throttle == 0 {
		return AcceptAll
	}

	// 检查限流/验证比率；如果低于阈值则接受消息。
	if pg.validate != 0 && pg.throttle/pg.validate < pg.params.Threshold {
		return AcceptAll
	}

	st := pg.getPeerStats(p)

	// 计算 peer 的传输质量；分母是消息计数器的加权混合
	total := st.deliver + pg.params.DuplicateWeight*st.duplicate + pg.params.IgnoreWeight*st.ignore + pg.params.RejectWeight*st.reject
	if total == 0 {
		return AcceptAll
	}

	// 根据 peer 的传输质量做出随机决策。
	// 概率偏置通过在传递计数器中加 1 实现，这样我们不会在第一次负面事件中无条件限流；它还确保 peer 总是有机会被接受；这不是一个黑洞/黑名单。
	threshold := (1 + st.deliver) / (1 + total)
	if rand.Float64() < threshold {
		return AcceptAll
	}

	logger.Debugf("限流节点 %s 阈值 %f", p, threshold)
	return AcceptControl
}

// -- RawTracer 接口方法
var _ RawTracer = (*peerGater)(nil)

// tracer 接口
// AddPeer 添加 peer.ID
// 参数:
//   - p: peer.ID
//   - proto: 协议 ID
func (pg *peerGater) AddPeer(p peer.ID, proto protocol.ID) {
	pg.Lock()         // 加锁，防止并发修改
	defer pg.Unlock() // 确保在函数结束时解锁

	st := pg.getPeerStats(p) // 获取或创建指定 peer.ID 的统计数据
	st.connected++           // 增加 connected 计数器
}

// RemovePeer 移除 peer.ID
// 参数:
//   - p: peer.ID
func (pg *peerGater) RemovePeer(p peer.ID) {
	pg.Lock()         // 加锁，防止并发修改
	defer pg.Unlock() // 确保在函数结束时解锁

	st := pg.getPeerStats(p)                          // 获取或创建指定 peer.ID 的统计数据
	st.connected--                                    // 减少 connected 计数器
	st.expire = time.Now().Add(pg.params.RetainStats) // 设置过期时间为当前时间加上 RetainStats 参数

	delete(pg.peerStats, p) // 从 peerStats map 中删除指定的 peer.ID
}

// Join 加入主题
// 参数:
//   - topic: 主题
func (pg *peerGater) Join(topic string) {}

// Leave 离开主题
// 参数:
//   - topic: 主题
func (pg *peerGater) Leave(topic string) {}

// Graft 加入 peer.ID 到主题
// 参数:
//   - p: peer.ID
//   - topic: 主题
func (pg *peerGater) Graft(p peer.ID, topic string) {}

// Prune 从主题中移除 peer.ID
// 参数:
//   - p: peer.ID
//   - topic: 主题
func (pg *peerGater) Prune(p peer.ID, topic string) {}

// ValidateMessage 验证消息
// 参数:
//   - msg: 消息
func (pg *peerGater) ValidateMessage(msg *Message) {
	pg.Lock()         // 加锁，确保在操作 pg 时没有其他 goroutine 并发访问
	defer pg.Unlock() // 在函数返回前解锁，确保锁在函数结束时被释放

	pg.validate++ // 增加 validate 计数器，记录已验证的消息数量
}

// DeliverMessage 传递消息
// 参数:
//   - msg: 消息
func (pg *peerGater) DeliverMessage(msg *Message) {
	pg.Lock()         // 加锁，确保在操作 pg 时没有其他 goroutine 并发访问
	defer pg.Unlock() // 在函数返回前解锁，确保锁在函数结束时被释放

	st := pg.getPeerStats(msg.ReceivedFrom) // 获取消息发送者的统计信息

	topic := msg.GetTopic()                         // 获取消息的主题
	weight := pg.params.TopicDeliveryWeights[topic] // 获取主题的传递权重

	if weight == 0 { // 如果权重为 0，则设置默认权重为 1
		weight = 1
	}

	st.deliver += weight // 增加消息发送者的 deliver 计数器，按照权重累加
}

// RejectMessage 拒绝消息
// 参数:
//   - msg: 消息
//   - reason: 拒绝原因
func (pg *peerGater) RejectMessage(msg *Message, reason string) {
	pg.Lock()         // 加锁，确保在操作 pg 时没有其他 goroutine 并发访问
	defer pg.Unlock() // 在函数返回前解锁，确保锁在函数结束时被释放

	switch reason { // 根据拒绝原因执行不同的操作
	case RejectValidationQueueFull: // 如果拒绝原因是队列已满
		fallthrough // 继续执行下一个 case 的代码
	case RejectValidationThrottled: // 如果拒绝原因是被限流
		pg.lastThrottle = time.Now() // 记录最后一次限流的时间
		pg.throttle++                // 增加 throttle 计数器

	case RejectValidationIgnored: // 如果拒绝原因是被忽略
		st := pg.getPeerStats(msg.ReceivedFrom) // 获取消息发送者的统计信息
		st.ignore++                             // 增加 ignore 计数器

	default: // 其他拒绝原因
		st := pg.getPeerStats(msg.ReceivedFrom) // 获取消息发送者的统计信息
		st.reject++                             // 增加 reject 计数器
	}
}

// DuplicateMessage 处理重复消息
// 参数:
//   - msg: 消息
func (pg *peerGater) DuplicateMessage(msg *Message) {
	pg.Lock()         // 加锁，确保在操作 pg 时没有其他 goroutine 并发访问
	defer pg.Unlock() // 在函数返回前解锁，确保锁在函数结束时被释放

	st := pg.getPeerStats(msg.ReceivedFrom) // 获取消息发送者的统计信息
	st.duplicate++                          // 增加 duplicate 计数器
}

// ThrottlePeer 限流 peer
// 参数:
//   - p: peer.ID
func (pg *peerGater) ThrottlePeer(p peer.ID) {}

// RecvRPC 接收 RPC
// 参数:
//   - rpc: RPC
func (pg *peerGater) RecvRPC(rpc *RPC) {}

// SendRPC 发送 RPC
// 参数:
//   - rpc: RPC
//   - p: peer.ID
func (pg *peerGater) SendRPC(rpc *RPC, p peer.ID) {}

// DropRPC 丢弃 RPC
// 参数:
//   - rpc: RPC
//   - p: peer.ID
func (pg *peerGater) DropRPC(rpc *RPC, p peer.ID) {}

// UndeliverableMessage 处理不可传递的消息
// 参数:
//   - msg: 消息
func (pg *peerGater) UndeliverableMessage(msg *Message) {}
