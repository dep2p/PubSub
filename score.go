// 作用：实现对等节点评分。
// 功能：根据对等节点的行为对其进行评分，决定与其通信的优先级和限制。

package pubsub

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dep2p/go-dep2p/core/host"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/core/protocol"

	manet "github.com/dep2p/go-dep2p/multiformats/multiaddr/net"
)

// peerStats 包含对等节点的统计信息
type peerStats struct {
	connected        bool                   // 对等节点是否当前连接
	expire           time.Time              // 断开连接的对等节点的分数统计信息过期时间
	topics           map[string]*topicStats // 每个主题的统计信息
	ips              []string               // IP 跟踪信息，存储为字符串以便处理
	ipWhitelist      map[string]bool        // IP 白名单缓存
	behaviourPenalty float64                // 行为模式处罚（由路由器应用）
}

// topicStats 包含主题的统计信息
type topicStats struct {
	inMesh                      bool          // 对等节点是否在 mesh 中
	graftTime                   time.Time     // 对等节点（最后一次）GRAFT 的时间，仅在 mesh 中时有效
	meshTime                    time.Duration // 在 mesh 中的时间（在刷新/衰减时更新以避免在每次分数调用时调用 gettimeofday）
	firstMessageDeliveries      float64       // 首次消息传递
	meshMessageDeliveries       float64       // mesh 消息传递
	meshMessageDeliveriesActive bool          // 对等节点是否在 mesh 中足够长时间以激活 mesh 消息传递
	meshFailurePenalty          float64       // 粘性 mesh 速率失败处罚计数器
	invalidMessageDeliveries    float64       // 无效消息传递计数器
}

// peerScore 包含用于计算对等节点分数的参数和统计信息
type peerScore struct {
	sync.Mutex

	params     *PeerScoreParams                // 分数参数
	peerStats  map[peer.ID]*peerStats          // 每个对等节点的统计信息
	peerIPs    map[string]map[peer.ID]struct{} // IP 同位跟踪，映射 IP 到对等节点集合
	deliveries *messageDeliveries              // 消息传递跟踪
	idGen      *msgIDGenerator                 // 消息 ID 生成器
	host       host.Host                       // 主机

	inspect       PeerScoreInspectFn         // 调试检查函数
	inspectEx     ExtendedPeerScoreInspectFn // 扩展调试检查函数
	inspectPeriod time.Duration              // 检查周期
}

// 实现 RawTracer 接口
var _ RawTracer = (*peerScore)(nil)

// messageDeliveries 包含消息传递的跟踪信息
type messageDeliveries struct {
	seenMsgTTL time.Duration              // 记住消息传递的时间
	records    map[string]*deliveryRecord // 消息传递记录
	head       *deliveryEntry             // 清理旧传递记录的队列头
	tail       *deliveryEntry             // 清理旧传递记录的队列尾
}

// deliveryRecord 包含单个消息的传递记录
type deliveryRecord struct {
	status    int                  // 消息状态
	firstSeen time.Time            // 首次看到消息的时间
	validated time.Time            // 消息验证时间
	peers     map[peer.ID]struct{} // 参与传递的对等节点
}

// deliveryEntry 包含单个消息的传递队列条目
type deliveryEntry struct {
	id     string         // 消息 ID
	expire time.Time      // 传递记录过期时间
	next   *deliveryEntry // 队列中的下一个条目
}

// 消息传递记录状态常量
const (
	deliveryUnknown   = iota // 未知消息状态
	deliveryValid            // 已验证消息
	deliveryInvalid          // 无效消息
	deliveryIgnored          // 被验证器忽略的消息
	deliveryThrottled        // 无法确定是否有效，因为验证被限制
)

type (
	// PeerScoreInspectFn 定义对等节点分数检查函数类型
	PeerScoreInspectFn = func(map[peer.ID]float64)
	// ExtendedPeerScoreInspectFn 定义扩展的对等节点分数检查函数类型
	ExtendedPeerScoreInspectFn = func(map[peer.ID]*PeerScoreSnapshot)
)

// PeerScoreSnapshot 包含对等节点分数快照
type PeerScoreSnapshot struct {
	Score              float64                        // 对等节点分数
	Topics             map[string]*TopicScoreSnapshot // 每个主题的分数快照
	AppSpecificScore   float64                        // 应用程序特定的分数
	IPColocationFactor float64                        // IP 同位因素
	BehaviourPenalty   float64                        // 行为模式处罚
}

// TopicScoreSnapshot 包含主题分数快照
type TopicScoreSnapshot struct {
	TimeInMesh               time.Duration // 在 mesh 中的时间
	FirstMessageDeliveries   float64       // 首次消息传递
	MeshMessageDeliveries    float64       // mesh 消息传递
	InvalidMessageDeliveries float64       // 无效消息传递
}

// WithPeerScoreInspect 是一个 gossipsub 路由器选项，用于启用对等节点分数调试。
// 启用此选项后，将定期调用提供的函数，以便应用程序检查或转储已连接对等节点的分数。
// 提供的函数可以具有以下两种签名之一：
//   - PeerScoreInspectFn，接受对等节点 ID 到分数的映射。
//   - ExtendedPeerScoreInspectFn，接受对等节点 ID 到 PeerScoreSnapshots 的映射，
//     并允许检查单个分数组件以调试对等节点评分。
//
// 此选项必须在 WithPeerScore 选项之后传递。
func WithPeerScoreInspect(inspect interface{}, period time.Duration) Option {
	return func(ps *PubSub) error {
		// 检查路由器是否为 gossipsub 类型
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			logger.Warnf("pubsub 路由器不是 gossipsub") // pubsub 路由器不是 gossipsub
			return fmt.Errorf("pubsub 路由器不是 gossipsub")
		}

		// 检查是否已启用对等节点评分
		if gs.score == nil {
			logger.Warnf("未启用对等节点评分") // 未启用对等节点评分
			return fmt.Errorf("未启用对等节点评分")
		}

		// 检查是否已有分数检查器
		if gs.score.inspect != nil || gs.score.inspectEx != nil {
			logger.Warnf("重复的对等节点分数检查器") // 重复的对等节点分数检查器
			return fmt.Errorf("重复的对等节点分数检查器")
		}

		// 根据传入的检查器类型设置相应的检查器
		switch i := inspect.(type) {
		case PeerScoreInspectFn:
			gs.score.inspect = i
		case ExtendedPeerScoreInspectFn:
			gs.score.inspectEx = i
		default:
			logger.Warnf("未知的对等节点分数检查器类型: %v", inspect) // 未知的对等节点分数检查器类型
			return fmt.Errorf("未知的对等节点分数检查器类型: %v", inspect)
		}

		// 设置检查周期
		gs.score.inspectPeriod = period

		return nil
	}
}

// newPeerScore 创建新的 peerScore 实例
// 参数:
//   - params: *PeerScoreParams，分数参数
//
// 返回值:
//   - *peerScore，新创建的 peerScore 实例
func newPeerScore(params *PeerScoreParams) *peerScore {
	seenMsgTTL := params.SeenMsgTTL
	if seenMsgTTL == 0 {
		seenMsgTTL = TimeCacheDuration
	}
	return &peerScore{
		params:     params,
		peerStats:  make(map[peer.ID]*peerStats),
		peerIPs:    make(map[string]map[peer.ID]struct{}),
		deliveries: &messageDeliveries{seenMsgTTL: seenMsgTTL, records: make(map[string]*deliveryRecord)},
		idGen:      newMsgIdGenerator(),
	}
}

// SetTopicScoreParams 设置新的主题分数参数。
// 如果该主题之前有参数，并且新参数降低了交付上限，则分数计数器会相应地重新计算。
// 参数:
//   - topic: string，主题
//   - p: *TopicScoreParams，新的主题分数参数
//
// 返回值:
//   - error，错误信息
func (ps *peerScore) SetTopicScoreParams(topic string, p *TopicScoreParams) error {
	ps.Lock()
	defer ps.Unlock()

	old, exist := ps.params.Topics[topic]
	ps.params.Topics[topic] = p

	if !exist {
		return nil
	}

	// 检查计数器上限是否降低，如果是，则重新计算计数器
	recap := false
	if p.FirstMessageDeliveriesCap < old.FirstMessageDeliveriesCap {
		recap = true
	}
	if p.MeshMessageDeliveriesCap < old.MeshMessageDeliveriesCap {
		recap = true
	}
	if !recap {
		return nil
	}

	// 重新计算该主题的计数器
	for _, pstats := range ps.peerStats {
		tstats, ok := pstats.topics[topic]
		if !ok {
			continue
		}

		if tstats.firstMessageDeliveries > p.FirstMessageDeliveriesCap {
			tstats.firstMessageDeliveries = p.FirstMessageDeliveriesCap
		}

		if tstats.meshMessageDeliveries > p.MeshMessageDeliveriesCap {
			tstats.meshMessageDeliveries = p.MeshMessageDeliveriesCap
		}
	}

	return nil
}

// Start 启动 peerScore
// 参数:
//   - gs: *GossipSubRouter，GossipSub 路由器
func (ps *peerScore) Start(gs *GossipSubRouter) {
	if ps == nil {
		return
	}

	ps.idGen = gs.p.idGen
	ps.host = gs.p.host
	go ps.background(gs.p.ctx)
}

// Score 计算给定对等节点的分数
// 参数:
//   - p: peer.ID，对等节点 ID
//
// 返回值:
//   - float64，对等节点分数
func (ps *peerScore) Score(p peer.ID) float64 {
	if ps == nil {
		return 0
	}

	ps.Lock()
	defer ps.Unlock()

	return ps.score(p)
}

// score 内部方法，用于计算给定对等节点的分数
// 参数:
//   - p: peer.ID，对等节点 ID
//
// 返回值:
//   - float64，对等节点分数
func (ps *peerScore) score(p peer.ID) float64 {
	pstats, ok := ps.peerStats[p]
	if !ok {
		return 0
	}

	var score float64

	// 计算主题分数
	for topic, tstats := range pstats.topics {
		// 获取主题参数
		topicParams, ok := ps.params.Topics[topic]
		if !ok {
			continue
		}

		var topicScore float64

		// P1: Mesh 中的时间
		if tstats.inMesh {
			p1 := float64(tstats.meshTime / topicParams.TimeInMeshQuantum)
			if p1 > topicParams.TimeInMeshCap {
				p1 = topicParams.TimeInMeshCap
			}
			topicScore += p1 * topicParams.TimeInMeshWeight
		}

		// P2: 首次消息传递
		p2 := tstats.firstMessageDeliveries
		topicScore += p2 * topicParams.FirstMessageDeliveriesWeight

		// P3: Mesh 消息传递
		if tstats.meshMessageDeliveriesActive {
			if tstats.meshMessageDeliveries < topicParams.MeshMessageDeliveriesThreshold {
				deficit := topicParams.MeshMessageDeliveriesThreshold - tstats.meshMessageDeliveries
				p3 := deficit * deficit
				topicScore += p3 * topicParams.MeshMessageDeliveriesWeight
			}
		}

		// P3b: Mesh 失败惩罚
		p3b := tstats.meshFailurePenalty
		topicScore += p3b * topicParams.MeshFailurePenaltyWeight

		// P4: 无效消息
		p4 := (tstats.invalidMessageDeliveries * tstats.invalidMessageDeliveries)
		topicScore += p4 * topicParams.InvalidMessageDeliveriesWeight

		// 更新分数，混合主题权重
		score += topicScore * topicParams.TopicWeight
	}

	// 应用主题分数上限（如果有）
	if ps.params.TopicScoreCap > 0 && score > ps.params.TopicScoreCap {
		score = ps.params.TopicScoreCap
	}

	// P5: 应用程序特定分数
	p5 := ps.params.AppSpecificScore(p)
	score += p5 * ps.params.AppSpecificWeight

	// P6: IP 合作因素
	p6 := ps.ipColocationFactor(p)
	score += p6 * ps.params.IPColocationFactorWeight

	// P7: 行为模式惩罚
	if pstats.behaviourPenalty > ps.params.BehaviourPenaltyThreshold {
		excess := pstats.behaviourPenalty - ps.params.BehaviourPenaltyThreshold
		p7 := excess * excess
		score += p7 * ps.params.BehaviourPenaltyWeight
	}

	return score
}

// ipColocationFactor 计算给定对等节点的 IP 合作因素分数
// 参数:
//   - p: peer.ID，对等节点 ID
//
// 返回值:
//   - float64，对等节点的 IP 合作因素分数
func (ps *peerScore) ipColocationFactor(p peer.ID) float64 {
	pstats, ok := ps.peerStats[p]
	if !ok {
		// 如果找不到对等节点的统计信息，返回 0
		return 0
	}

	var result float64
loop:
	for _, ip := range pstats.ips {
		// 检查 IP 白名单
		if len(ps.params.IPColocationFactorWhitelist) > 0 {
			if pstats.ipWhitelist == nil {
				pstats.ipWhitelist = make(map[string]bool)
			}

			whitelisted, ok := pstats.ipWhitelist[ip]
			if !ok {
				// 解析 IP 地址
				ipObj := net.ParseIP(ip)
				for _, ipNet := range ps.params.IPColocationFactorWhitelist {
					if ipNet.Contains(ipObj) {
						// 如果 IP 在白名单中，标记并跳过
						pstats.ipWhitelist[ip] = true
						continue loop
					}
				}

				pstats.ipWhitelist[ip] = false
			}

			if whitelisted {
				// 如果 IP 在白名单中，跳过
				continue loop
			}
		}

		// 计算超过阈值的 IP 数量
		peersInIP := len(ps.peerIPs[ip])
		if peersInIP > ps.params.IPColocationFactorThreshold {
			// 超过阈值的部分进行二次方计算并加到结果中
			surpluss := float64(peersInIP - ps.params.IPColocationFactorThreshold)
			result += surpluss * surpluss
		}
	}

	return result
}

// AddPenalty 增加对等节点的行为模式惩罚分数
// 参数:
//   - p: peer.ID，对等节点 ID
//   - count: int，惩罚计数
func (ps *peerScore) AddPenalty(p peer.ID, count int) {
	if ps == nil {
		return
	}

	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 增加惩罚分数
	pstats.behaviourPenalty += float64(count)
}

// background 处理定期维护任务
// 参数:
//   - ctx: context.Context，上下文
func (ps *peerScore) background(ctx context.Context) {
	// 定期刷新分数
	refreshScores := time.NewTicker(ps.params.DecayInterval)
	defer refreshScores.Stop()

	// 定期刷新 IP 信息
	refreshIPs := time.NewTicker(time.Minute)
	defer refreshIPs.Stop()

	// 定期清理交付记录
	gcDeliveryRecords := time.NewTicker(time.Minute)
	defer gcDeliveryRecords.Stop()

	var inspectScores <-chan time.Time
	if ps.inspect != nil || ps.inspectEx != nil {
		// 设置分数检查周期
		ticker := time.NewTicker(ps.inspectPeriod)
		defer ticker.Stop()
		defer ps.inspectScores()
		inspectScores = ticker.C
	}

	for {
		select {
		case <-refreshScores.C:
			// 刷新分数
			ps.refreshScores()

		case <-refreshIPs.C:
			// 刷新 IP 信息
			ps.refreshIPs()

		case <-gcDeliveryRecords.C:
			// 清理交付记录
			ps.gcDeliveryRecords()

		case <-inspectScores:
			// 检查分数
			ps.inspectScores()

		case <-ctx.Done():
			// 退出
			return
		}
	}
}

// inspectScores 将所有跟踪的分数导入检查函数
func (ps *peerScore) inspectScores() {
	if ps.inspect != nil {
		ps.inspectScoresSimple()
	}
	if ps.inspectEx != nil {
		ps.inspectScoresExtended()
	}
}

// inspectScoresSimple 简单检查对等节点分数，并调用用户提供的检查函数
func (ps *peerScore) inspectScoresSimple() {
	ps.Lock()
	scores := make(map[peer.ID]float64, len(ps.peerStats))
	for p := range ps.peerStats {
		scores[p] = ps.score(p) // 获取对等节点分数
	}
	ps.Unlock()

	// 由于这是一个用户注入的函数，可能会执行 I/O 操作，不想阻塞 scorer 的后台循环，因此在单独的 goroutine 中启动
	go ps.inspect(scores)
}

// inspectScoresExtended 扩展检查对等节点分数，并调用用户提供的检查函数
func (ps *peerScore) inspectScoresExtended() {
	ps.Lock()
	scores := make(map[peer.ID]*PeerScoreSnapshot, len(ps.peerStats))
	for p, pstats := range ps.peerStats {
		pss := new(PeerScoreSnapshot)
		pss.Score = ps.score(p) // 获取对等节点分数
		if len(pstats.topics) > 0 {
			pss.Topics = make(map[string]*TopicScoreSnapshot, len(pstats.topics))
			for t, ts := range pstats.topics {
				tss := &TopicScoreSnapshot{
					FirstMessageDeliveries:   ts.firstMessageDeliveries,   // 第一次消息交付数
					MeshMessageDeliveries:    ts.meshMessageDeliveries,    // 网状消息交付数
					InvalidMessageDeliveries: ts.invalidMessageDeliveries, // 无效消息交付数
				}
				if ts.inMesh {
					tss.TimeInMesh = ts.meshTime // 网状时间
				}
				pss.Topics[t] = tss
			}
		}
		pss.AppSpecificScore = ps.params.AppSpecificScore(p) // 应用特定分数
		pss.IPColocationFactor = ps.ipColocationFactor(p)    // IP 合作因素
		pss.BehaviourPenalty = pstats.behaviourPenalty       // 行为惩罚
		scores[p] = pss
	}
	ps.Unlock()

	// 由于这是一个用户注入的函数，可能会执行 I/O 操作，不想阻塞 scorer 的后台循环，因此在单独的 goroutine 中启动
	go ps.inspectEx(scores)
}

// refreshScores 刷新分数并在到期后清除断开连接的对等节点的分数记录
func (ps *peerScore) refreshScores() {
	ps.Lock()
	defer ps.Unlock()

	now := time.Now()
	for p, pstats := range ps.peerStats {
		if !pstats.connected {
			// 检查保留期是否已过期
			if now.After(pstats.expire) {
				// 已过期，清除记录并清理 IP 跟踪
				ps.removeIPs(p, pstats.ips)
				delete(ps.peerStats, p)
			}
			// 对断开连接的对等节点不进行衰减处理，避免负分对等节点通过断开重连来重置分数
			continue
		}

		for topic, tstats := range pstats.topics {
			topicParams, ok := ps.params.Topics[topic]
			if !ok {
				continue
			}

			// 衰减计数器
			tstats.firstMessageDeliveries *= topicParams.FirstMessageDeliveriesDecay
			if tstats.firstMessageDeliveries < ps.params.DecayToZero {
				tstats.firstMessageDeliveries = 0
			}
			tstats.meshMessageDeliveries *= topicParams.MeshMessageDeliveriesDecay
			if tstats.meshMessageDeliveries < ps.params.DecayToZero {
				tstats.meshMessageDeliveries = 0
			}
			tstats.meshFailurePenalty *= topicParams.MeshFailurePenaltyDecay
			if tstats.meshFailurePenalty < ps.params.DecayToZero {
				tstats.meshFailurePenalty = 0
			}
			tstats.invalidMessageDeliveries *= topicParams.InvalidMessageDeliveriesDecay
			if tstats.invalidMessageDeliveries < ps.params.DecayToZero {
				tstats.invalidMessageDeliveries = 0
			}
			// 更新网状时间并激活网状消息交付参数
			if tstats.inMesh {
				tstats.meshTime = now.Sub(tstats.graftTime)
				if tstats.meshTime > topicParams.MeshMessageDeliveriesActivation {
					tstats.meshMessageDeliveriesActive = true
				}
			}
		}

		// 衰减 P7 计数器
		pstats.behaviourPenalty *= ps.params.BehaviourPenaltyDecay
		if pstats.behaviourPenalty < ps.params.DecayToZero {
			pstats.behaviourPenalty = 0
		}
	}
}

// refreshIPs 刷新正在跟踪的对等节点的 IP
func (ps *peerScore) refreshIPs() {
	ps.Lock()
	defer ps.Unlock()

	// 对等节点的 IP 可能会更改，因此定期刷新
	for p, pstats := range ps.peerStats {
		if pstats.connected {
			ips := ps.getIPs(p)
			ps.setIPs(p, ips, pstats.ips)
			pstats.ips = ips
		}
	}
}

// gcDeliveryRecords 清理交付记录
func (ps *peerScore) gcDeliveryRecords() {
	ps.Lock()
	defer ps.Unlock()

	ps.deliveries.gc()
}

// tracer interface

// AddPeer 添加新节点到评分系统。
// 参数:
// - p: 节点 ID。
// - proto: 协议 ID。
func (ps *peerScore) AddPeer(p peer.ID, proto protocol.ID) {
	ps.Lock()
	defer ps.Unlock()

	// 如果节点不存在，初始化节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		pstats = &peerStats{topics: make(map[string]*topicStats)}
		ps.peerStats[p] = pstats
	}

	// 标记节点为已连接
	pstats.connected = true
	ips := ps.getIPs(p)
	ps.setIPs(p, ips, pstats.ips)
	pstats.ips = ips
}

// RemovePeer 移除节点。
// 参数:
// - p: 节点 ID。
func (ps *peerScore) RemovePeer(p peer.ID) {
	ps.Lock()
	defer ps.Unlock()

	// 获取节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 如果节点评分为正值，移除节点信息
	if ps.score(p) > 0 {
		ps.removeIPs(p, pstats.ips)
		delete(ps.peerStats, p)
		return
	}

	// 重置首次消息交付计数并应用网格交付惩罚
	for topic, tstats := range pstats.topics {
		tstats.firstMessageDeliveries = 0

		threshold := ps.params.Topics[topic].MeshMessageDeliveriesThreshold
		if tstats.inMesh && tstats.meshMessageDeliveriesActive && tstats.meshMessageDeliveries < threshold {
			deficit := threshold - tstats.meshMessageDeliveries
			tstats.meshFailurePenalty += deficit * deficit
		}

		tstats.inMesh = false
	}

	// 标记节点为已断开并设置过期时间
	pstats.connected = false
	pstats.expire = time.Now().Add(ps.params.RetainScore)
}

// Join 加入主题
// 参数:
// - topic: 主题名称。
func (ps *peerScore) Join(topic string) {}

// Leave 离开主题
// 参数:
// - topic: 主题名称。
func (ps *peerScore) Leave(topic string) {}

// Graft 将节点添加到网格中。
// 参数:
// - p: 节点 ID。
// - topic: 主题名称。
func (ps *peerScore) Graft(p peer.ID, topic string) {
	ps.Lock()
	defer ps.Unlock()

	// 获取节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 获取主题统计信息
	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	// 标记节点在网格中并更新统计信息
	tstats.inMesh = true
	tstats.graftTime = time.Now()
	tstats.meshTime = 0
	tstats.meshMessageDeliveriesActive = false
}

// Prune 从网格中移除节点。
// 参数:
// - p: 节点 ID。
// - topic: 主题名称。
func (ps *peerScore) Prune(p peer.ID, topic string) {
	ps.Lock()
	defer ps.Unlock()

	// 获取节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 获取主题统计信息
	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	// 计算网格消息交付失败惩罚
	threshold := ps.params.Topics[topic].MeshMessageDeliveriesThreshold
	if tstats.meshMessageDeliveriesActive && tstats.meshMessageDeliveries < threshold {
		deficit := threshold - tstats.meshMessageDeliveries
		tstats.meshFailurePenalty += deficit * deficit
	}

	// 标记节点不在网格中
	tstats.inMesh = false
}

// ValidateMessage 验证消息时调用。
// 参数:
// - msg: 要验证的消息。
func (ps *peerScore) ValidateMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	// 创建记录以跟踪消息在验证管道中的时间
	_ = ps.deliveries.getRecord(ps.idGen.ID(msg))
}

// DeliverMessage 消息交付时调用。
// 参数:
// - msg: 要交付的消息。
func (ps *peerScore) DeliverMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	ps.markFirstMessageDelivery(msg.ReceivedFrom, msg)

	drec := ps.deliveries.getRecord(ps.idGen.ID(msg))

	// 检查是否为首次交付跟踪
	if drec.status != deliveryUnknown {
		logger.Debugf("意外的交付跟踪: 消息来自 %s 首次看到 %s 前和交付状态 %d", msg.ReceivedFrom, time.Since(drec.firstSeen), drec.status)
		return
	}

	// 标记消息为有效并奖励已转发给我们的网格节点
	drec.status = deliveryValid
	drec.validated = time.Now()
	for p := range drec.peers {
		if p != msg.ReceivedFrom {
			ps.markDuplicateMessageDelivery(p, msg, time.Time{})
		}
	}
}

// RejectMessage 消息拒绝时调用。
// 参数:
// - msg: 被拒绝的消息。
// - reason: 拒绝原因。
func (ps *peerScore) RejectMessage(msg *Message, reason string) {
	ps.Lock()
	defer ps.Unlock()

	switch reason {
	// 这些消息被认为是无效的，需要惩罚发送这些消息的节点
	case RejectMissingSignature, RejectInvalidSignature, RejectUnexpectedSignature, RejectUnexpectedAuthInfo, RejectSelfOrigin:
		ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)
		return

	// 这些消息被忽略，无需进一步处理
	case RejectBlacklstedPeer, RejectBlacklistedSource:
		return

	// 这些消息在进入验证管道前被拒绝，不知道是否有效，因此忽略
	case RejectValidationQueueFull:
		return
	}

	drec := ps.deliveries.getRecord(ps.idGen.ID(msg))

	// 检查是否为首次拒绝跟踪
	if drec.status != deliveryUnknown {
		logger.Debugf("意外的拒绝跟踪: 消息来自 %s 首次看到 %s 前和交付状态 %d", msg.ReceivedFrom, time.Since(drec.firstSeen), drec.status)
		return
	}

	switch reason {
	case RejectValidationThrottled:
		// 由于验证节流而被拒绝，不惩罚转发该消息的节点
		drec.status = deliveryThrottled
		drec.peers = nil
		return
	case RejectValidationIgnored:
		// 验证器指示忽略该消息但不惩罚节点
		drec.status = deliveryIgnored
		drec.peers = nil
		return
	}

	// 标记消息为无效并惩罚已转发的节点
	drec.status = deliveryInvalid

	ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)
	for p := range drec.peers {
		ps.markInvalidMessageDelivery(p, msg)
	}

	drec.peers = nil
}

// DuplicateMessage 处理重复消息。
// 参数:
// - msg: 接收到的重复消息。
func (ps *peerScore) DuplicateMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	// 获取消息交付记录
	drec := ps.deliveries.getRecord(ps.idGen.ID(msg))

	// 检查该节点是否已经发送了该消息的重复消息
	_, ok := drec.peers[msg.ReceivedFrom]
	if ok {
		// 已经看到这个重复消息，直接返回
		return
	}

	switch drec.status {
	case deliveryUnknown:
		// 消息正在验证中，跟踪该节点交付并等待交付/拒绝通知
		drec.peers[msg.ReceivedFrom] = struct{}{}

	case deliveryValid:
		// 标记节点交付时间，只计入一次重复交付
		drec.peers[msg.ReceivedFrom] = struct{}{}
		ps.markDuplicateMessageDelivery(msg.ReceivedFrom, msg, drec.validated)

	case deliveryInvalid:
		// 不再跟踪交付时间
		ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)

	case deliveryThrottled:
		// 消息被节流，不执行任何操作（不知道消息是否有效）
	case deliveryIgnored:
		// 消息被忽略，不执行任何操作
	}
}

// ThrottlePeer 对节点进行节流。
// 参数:
// - p: 节点 ID。
func (ps *peerScore) ThrottlePeer(p peer.ID) {}

// RecvRPC 处理接收到的 RPC 消息。
// 参数:
// - rpc: 接收到的 RPC 消息。
func (ps *peerScore) RecvRPC(rpc *RPC) {}

// SendRPC 处理发送的 RPC 消息。
// 参数:
// - rpc: 要发送的 RPC 消息。
// - p: 接收节点 ID。
func (ps *peerScore) SendRPC(rpc *RPC, p peer.ID) {}

// DropRPC 处理被丢弃的 RPC 消息。
// 参数:
// - rpc: 被丢弃的 RPC 消息。
// - p: 发送节点 ID。
func (ps *peerScore) DropRPC(rpc *RPC, p peer.ID) {}

// UndeliverableMessage 处理不可交付的消息。
// 参数:
// - msg: 无法交付的消息。
func (ps *peerScore) UndeliverableMessage(msg *Message) {}

// message delivery records
// getRecord 获取消息的交付记录。
// 参数:
// - id: 消息 ID。
// 返回值:
// - 消息交付记录。
func (d *messageDeliveries) getRecord(id string) *deliveryRecord {
	// 检查记录是否存在
	rec, ok := d.records[id]
	if ok {
		return rec
	}

	now := time.Now()

	// 创建新的记录
	rec = &deliveryRecord{peers: make(map[peer.ID]struct{}), firstSeen: now}
	d.records[id] = rec

	// 创建新的交付条目并添加到链表中
	entry := &deliveryEntry{id: id, expire: now.Add(d.seenMsgTTL)}
	if d.tail != nil {
		d.tail.next = entry
		d.tail = entry
	} else {
		d.head = entry
		d.tail = entry
	}

	return rec
}

// gc 执行垃圾回收，删除过期的消息交付记录。
func (d *messageDeliveries) gc() {
	if d.head == nil {
		return
	}

	now := time.Now()
	for d.head != nil && now.After(d.head.expire) {
		delete(d.records, d.head.id)
		d.head = d.head.next
	}

	if d.head == nil {
		d.tail = nil
	}
}

// getTopicStats 获取或初始化主题统计信息。
// 参数:
// - topic: 主题名称。
// - params: 评分参数。
// 返回值:
// - 主题统计信息。
// - 是否成功获取。
func (pstats *peerStats) getTopicStats(topic string, params *PeerScoreParams) (*topicStats, bool) {
	// 检查是否已存在
	tstats, ok := pstats.topics[topic]
	if ok {
		return tstats, true
	}

	// 检查主题是否被评分
	_, scoredTopic := params.Topics[topic]
	if !scoredTopic {
		return nil, false
	}

	// 初始化新的主题统计信息
	tstats = &topicStats{}
	pstats.topics[topic] = tstats

	return tstats, true
}

// markInvalidMessageDelivery 增加无效消息交付计数器。
// 参数:
// - p: 节点 ID。
// - msg: 无效消息。
func (ps *peerScore) markInvalidMessageDelivery(p peer.ID, msg *Message) {
	// 获取节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 获取主题统计信息
	topic := msg.GetTopic()
	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	// 增加无效消息交付计数器
	tstats.invalidMessageDeliveries += 1
}

// markFirstMessageDelivery 增加首次消息交付计数器。
// 参数:
// - p: 节点 ID。
// - msg: 首次交付的消息。
func (ps *peerScore) markFirstMessageDelivery(p peer.ID, msg *Message) {
	// 获取节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 获取主题统计信息
	topic := msg.GetTopic()
	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	// 增加首次消息交付计数器并检查上限
	cap := ps.params.Topics[topic].FirstMessageDeliveriesCap
	tstats.firstMessageDeliveries += 1
	if tstats.firstMessageDeliveries > cap {
		tstats.firstMessageDeliveries = cap
	}

	// 如果节点不在网格中，返回
	if !tstats.inMesh {
		return
	}

	// 增加网格消息交付计数器并检查上限
	cap = ps.params.Topics[topic].MeshMessageDeliveriesCap
	tstats.meshMessageDeliveries += 1
	if tstats.meshMessageDeliveries > cap {
		tstats.meshMessageDeliveries = cap
	}
}

// markDuplicateMessageDelivery 增加网格消息交付计数器。
// 参数:
// - p: 节点 ID。
// - msg: 重复交付的消息。
// - validated: 验证时间。
func (ps *peerScore) markDuplicateMessageDelivery(p peer.ID, msg *Message, validated time.Time) {
	// 获取节点统计信息
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// 获取主题统计信息
	topic := msg.GetTopic()
	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	// 如果节点不在网格中，返回
	if !tstats.inMesh {
		return
	}

	tparams := ps.params.Topics[topic]

	// 检查网格交付窗口
	if !validated.IsZero() && time.Since(validated) > tparams.MeshMessageDeliveriesWindow {
		return
	}

	// 增加网格消息交付计数器并检查上限
	cap := tparams.MeshMessageDeliveriesCap
	tstats.meshMessageDeliveries += 1
	if tstats.meshMessageDeliveries > cap {
		tstats.meshMessageDeliveries = cap
	}
}

// getIPs 获取节点的当前 IP 地址。
// 参数:
// - p: 节点 ID。
// 返回值:
// - IP 地址列表。
func (ps *peerScore) getIPs(p peer.ID) []string {
	if ps.host == nil {
		return nil
	}

	conns := ps.host.Network().ConnsToPeer(p)
	res := make([]string, 0, 1)
	for _, c := range conns {
		if c.Stat().Limited {
			// 忽略临时连接
			continue
		}

		remote := c.RemoteMultiaddr()
		ip, err := manet.ToIP(remote)
		if err != nil {
			continue
		}

		// 忽略回环地址，回环地址用于单元测试
		if ip.IsLoopback() {
			continue
		}

		if len(ip.To4()) == 4 {
			// IPv4 地址
			ip4 := ip.String()
			res = append(res, ip4)
		} else {
			// IPv6 地址，添加实际地址和 /64 子网
			ip6 := ip.String()
			res = append(res, ip6)

			ip6mask := ip.Mask(net.CIDRMask(64, 128)).String()
			res = append(res, ip6mask)
		}
	}

	return res
}

// setIPs 添加新 IP 地址并移除过期的 IP 地址。
// 参数:
// - p: 节点 ID。
// - newips: 新 IP 地址列表。
// - oldips: 旧 IP 地址列表。
func (ps *peerScore) setIPs(p peer.ID, newips, oldips []string) {
addNewIPs:
	for _, ip := range newips {
		for _, xip := range oldips {
			if ip == xip {
				continue addNewIPs
			}
		}

		peers, ok := ps.peerIPs[ip]
		if !ok {
			peers = make(map[peer.ID]struct{})
			ps.peerIPs[ip] = peers
		}
		peers[p] = struct{}{}
	}

removeOldIPs:
	for _, ip := range oldips {
		for _, xip := range newips {
			if ip == xip {
				continue removeOldIPs
			}
		}

		peers, ok := ps.peerIPs[ip]
		if !ok {
			continue
		}
		delete(peers, p)
		if len(peers) == 0 {
			delete(ps.peerIPs, ip)
		}
	}
}

// removeIPs 移除节点的 IP 地址。
// 参数:
// - p: 节点 ID。
// - ips: 要移除的 IP 地址列表。
func (ps *peerScore) removeIPs(p peer.ID, ips []string) {
	for _, ip := range ips {
		peers, ok := ps.peerIPs[ip]
		if !ok {
			continue
		}

		delete(peers, p)
		if len(peers) == 0 {
			delete(ps.peerIPs, ip)
		}
	}
}
