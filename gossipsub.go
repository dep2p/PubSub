// 作用：实现GossipSub协议。
// 功能：GossipSub是一种改进的消息传递协议，利用gossip协议减少网络负载和提高消息传播效率。

package pubsub

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/dep2p/pubsub/logger"
	pb "github.com/dep2p/pubsub/pb"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

const (
	// GossipSubID_v10 是 GossipSub 协议的版本 1.0.0 的协议 ID。
	// 它与 GossipSubID_v11 一起发布以实现向后兼容。
	GossipSubID_v10 = protocol.ID("/meshsub/1.0.0")

	// GossipSubID_v11 是 GossipSub 协议的版本 1.1.0 的协议 ID。
	// 参见规范了解 v1.1.0 与 v1.0.0 的详细比较：
	// https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md
	GossipSubID_v11 = protocol.ID("/meshsub/1.1.0")
)

// 定义 gossipsub 的默认参数。
var (
	// D 设置 GossipSub 主题网格的最佳度。
	GossipSubD = 6
	// Dlo 设置我们在 GossipSub 主题网格中保持的对等节点的下限。
	GossipSubDlo = 5
	// Dhi 设置我们在 GossipSub 主题网格中保持的对等节点的上限。
	GossipSubDhi = 12
	// Dscore 影响在由于过度订阅而修剪网格时如何选择对等节点。
	GossipSubDscore = 4
	// Dout 设置在主题网格中维护的出站连接的配额。
	GossipSubDout = 2
	// HistoryLength 控制用于 gossip 的消息缓存的大小。
	GossipSubHistoryLength = 5
	// HistoryGossip 控制我们将在 IHAVE gossip 消息中广告的缓存消息 ID 的数量。
	GossipSubHistoryGossip = 3
	// Dlazy 影响我们在每个心跳期间将 gossip 发送到的对等节点数量。
	GossipSubDlazy = 6
	// GossipFactor 影响我们在每个心跳期间将 gossip 发送到的对等节点数量。
	GossipSubGossipFactor = 0.25
	// GossipRetransmission 控制在开始忽略对等节点之前允许对等节点通过 IWANT gossip 请求相同消息 ID 的次数。
	GossipSubGossipRetransmission = 3
	// HeartbeatInitialDelay 是路由器初始化后心跳计时器开始之前的短暂延迟。
	GossipSubHeartbeatInitialDelay = 100 * time.Millisecond
	// HeartbeatInterval 控制心跳之间的时间。
	GossipSubHeartbeatInterval = 1 * time.Second
	// FanoutTTL 控制我们跟踪 fanout 状态的时间。
	GossipSubFanoutTTL = 60 * time.Second
	// PrunePeers 控制修剪对等节点交换中的对等节点数量。
	GossipSubPrunePeers = 16
	// PruneBackoff 控制修剪对等节点的回退时间。
	GossipSubPruneBackoff = time.Minute
	// UnsubscribeBackoff 控制取消订阅主题时使用的回退时间。
	GossipSubUnsubscribeBackoff = 10 * time.Second
	// Connectors 控制通过 PX 获取的对等节点的活动连接尝试数量。
	GossipSubConnectors = 8
	// MaxPendingConnections 设置通过 px 尝试的对等节点的最大挂起连接数。
	GossipSubMaxPendingConnections = 128
	// ConnectionTimeout 控制连接尝试的超时时间。
	GossipSubConnectionTimeout = 30 * time.Second
	// DirectConnectTicks 是尝试重新连接当前未连接的直接对等节点的心跳滴答次数。
	GossipSubDirectConnectTicks uint64 = 300
	// DirectConnectInitialDelay 是在打开与直接对等节点的连接之前的初始延迟。
	GossipSubDirectConnectInitialDelay = time.Second
	// OpportunisticGraftTicks 是尝试通过机会性移植改善网格的心跳滴答次数。
	GossipSubOpportunisticGraftTicks uint64 = 60
	// OpportunisticGraftPeers 是机会性移植的对等节点数量。
	GossipSubOpportunisticGraftPeers = 2
	// GraftFloodThreshold 是在最后一次修剪后经过的时间内 GRAFT 提供的额外分数惩罚。
	GossipSubGraftFloodThreshold = 10 * time.Second
	// MaxIHaveLength 是包含在 IHAVE 消息中的最大消息数量。
	GossipSubMaxIHaveLength = 5000
	// MaxIHaveMessages 是在心跳期间从对等节点接受的最大 IHAVE 消息数量。
	GossipSubMaxIHaveMessages = 10
	// IWantFollowupTime 是在 IHAVE 广告之后通过 IWANT 请求消息的等待时间。
	GossipSubIWantFollowupTime = 3 * time.Second
)

// GossipSubParams 定义了所有 gossipsub 特定的参数。
type GossipSubParams struct {
	// overlay parameters. 叠加网络相关参数

	// D 设置 GossipSub 主题网格的理想度数。例如，如果 D == 6，
	// 每个节点希望在他们订阅的每个主题中保持大约六个节点在他们的网格中。
	// D 应该设置在 Dlo 和 Dhi 之间的某个值。
	D int

	// Dlo 设置 GossipSub 主题网格中保持的最少节点数。
	// 如果我们拥有的节点少于 Dlo，我们将在下一个心跳中尝试添加更多节点到网格中。
	Dlo int

	// Dhi 设置 GossipSub 主题网格中保持的最多节点数。
	// 如果我们拥有的节点多于 Dhi，我们将在下一个心跳中选择一些节点从网格中移除。
	Dhi int

	// Dscore 影响由于过度订阅而修剪网格时选择保留哪些节点。
	// 保留的节点中至少 Dscore 个将是高分节点，而其余的节点将随机选择。
	Dscore int

	// Dout 设置在主题网格中保持的出站连接的配额。
	// 当网格因过度订阅而被修剪时，我们确保至少与 Dout 个幸存节点保持出站连接。
	// 这可以防止 sybil 攻击者通过大量的入站连接压倒我们的网格。
	//
	// Dout 必须设置在 Dlo 之下，并且不能超过 D / 2。
	Dout int

	// gossip parameters. Gossip 相关参数

	// HistoryLength 控制用于 gossip 的消息缓存的大小。
	// 消息缓存将在 HistoryLength 个心跳内记住消息。
	HistoryLength int

	// HistoryGossip 控制我们将在 IHAVE gossip 消息中通告的缓存消息 ID 的数量。
	// 当被要求提供我们看到的消息 ID 时，我们将只返回来自最近 HistoryGossip 次心跳的那些消息。
	// HistoryGossip 和 HistoryLength 之间的差距使我们避免广告即将过期的消息。
	//
	// HistoryGossip 必须小于或等于 HistoryLength 以避免运行时 panic。
	HistoryGossip int

	// Dlazy 影响我们在每次心跳时将 gossip 发送给多少节点。
	// 我们将至少向 Dlazy 个网格外部的节点发送 gossip。实际的数量可能更多，
	// 取决于 GossipFactor 和我们连接的节点数。
	Dlazy int

	// GossipFactor 影响我们在每次心跳时将 gossip 发送给多少节点。
	// 我们将 gossip 发送给 GossipFactor * (非网格节点的总数)，或者 Dlazy，
	// 以较大者为准。
	GossipFactor float64

	// GossipRetransmission 控制在开始忽略节点之前，我们允许节点通过 IWANT gossip 请求相同消息 ID 的次数。
	// 这样设计是为了防止节点通过请求消息浪费我们的资源。
	GossipRetransmission int

	// heartbeat interval. 心跳间隔

	// HeartbeatInitialDelay 是在路由器初始化后，心跳定时器开始之前的短暂延迟。
	HeartbeatInitialDelay time.Duration

	// HeartbeatInterval 控制心跳之间的时间间隔。
	HeartbeatInterval time.Duration

	// SlowHeartbeatWarning 是心跳处理时间超过该阈值时触发警告的持续时间；这表明节点可能过载。
	SlowHeartbeatWarning float64

	// FanoutTTL 控制我们保持 fanout 状态的时间。如果自上次我们发布到一个我们没有订阅的主题以来，
	// 已经过了 FanoutTTL，我们将删除该主题的 fanout 映射。
	FanoutTTL time.Duration

	// PrunePeers 控制在 prune Peer eXchange 中包含的节点数量。
	// 当我们修剪一个符合 PX 条件的节点（得分良好等）时，我们会尝试向他们发送我们知道的最多 PrunePeers 个节点的签名节点记录。
	PrunePeers int

	// PruneBackoff 控制被修剪节点的退避时间。这是节点在被修剪后重新尝试加入我们网格之前必须等待的时间。
	// 当修剪节点时，我们会向他们发送我们设置的 PruneBackoff 值，以便他们知道最短等待时间。
	// 运行旧版本的节点可能不会发送退避时间，所以如果我们收到没有退避时间的 prune 消息，
	// 我们将在重新加入前等待至少 PruneBackoff 时间。
	PruneBackoff time.Duration

	// UnsubscribeBackoff 控制取消订阅主题时使用的退避时间。
	// 节点在此期间不应重新订阅该主题。
	UnsubscribeBackoff time.Duration

	// Connectors 控制通过 PX 获取的节点的活动连接尝试数量。
	Connectors int

	// MaxPendingConnections 设置通过 PX 尝试连接的节点的最大挂起连接数。
	MaxPendingConnections int

	// ConnectionTimeout 控制连接尝试的超时时间。
	ConnectionTimeout time.Duration

	// DirectConnectTicks 是尝试重新连接当前未连接的直接节点的心跳刻度数。
	DirectConnectTicks uint64

	// DirectConnectInitialDelay 是开始与直接节点建立连接之前的初始延迟。
	DirectConnectInitialDelay time.Duration

	// OpportunisticGraftTicks 是通过机会性 grafting 改善网格的心跳刻度数。
	// 每当经过 OpportunisticGraftTicks 次心跳时，我们将尝试选择一些高分网格节点来替换低分的节点，
	// 如果我们网格节点的中位数得分低于阈值（见 https://godoc.org/bpfs#PeerScoreThresholds）。
	OpportunisticGraftTicks uint64

	// OpportunisticGraftPeers 是要机会性 graft 的节点数。
	OpportunisticGraftPeers int

	// 如果在上次 PRUNE 之后 GraftFloodThreshold 时间内收到 GRAFT，
	// 则对节点应用额外的得分惩罚，通过 P7 来实现。
	GraftFloodThreshold time.Duration

	// MaxIHaveLength 是 IHAVE 消息中包含的最大消息数量。
	// 还控制在一个心跳内，我们从节点接受和请求的 IHAVE ids 的最大数量，以防止 IHAVE 泛滥。
	// 如果您的系统在 HistoryGossip 心跳中推送了超过 5000 条消息，您应调整该值；
	// 默认情况下，这意味着 1666 条消息/秒。
	MaxIHaveLength int

	// MaxIHaveMessages 是在一个心跳内从节点接受的 IHAVE 消息的最大数量。
	MaxIHaveMessages int

	// IWantFollowupTime 是在 IHAVE 广告后等待通过 IWANT 请求的消息的时间。
	// 如果在此窗口内未收到消息，则声明违反承诺，路由器可能会应用行为惩罚。
	IWantFollowupTime time.Duration
}

// NewGossipSub 返回一个新的使用默认 GossipSubRouter 作为路由器的 PubSub 对象。
// 参数:
//   - ctx: 上下文
//   - h: 主机
//   - opts: 选项
//
// 返回值:
//   - *PubSub: PubSub 对象
//   - error: 错误信息
func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := DefaultGossipSubRouter(h)
	opts = append(opts, WithRawTracer(rt.tagTracer))
	return NewGossipSubWithRouter(ctx, h, rt, opts...)
}

// NewGossipSubWithRouter 返回一个使用给定路由器的新的 PubSub 对象。
// 参数:
//   - ctx: 上下文
//   - h: 主机
//   - rt: 路由器
//   - opts: 选项
//
// 返回值:
//   - *PubSub: PubSub 对象
//   - error: 错误信息
func NewGossipSubWithRouter(ctx context.Context, h host.Host, rt PubSubRouter, opts ...Option) (*PubSub, error) {
	return NewPubSub(ctx, h, rt, opts...)
}

// DefaultGossipSubRouter 返回一个带有默认参数的新的 GossipSubRouter。
// 参数:
//   - h: 主机
//
// 返回值:
//   - *GossipSubRouter: GossipSubRouter 对象
func DefaultGossipSubRouter(h host.Host) *GossipSubRouter {
	params := DefaultGossipSubParams()
	return &GossipSubRouter{
		peers:     make(map[peer.ID]protocol.ID),
		mesh:      make(map[string]map[peer.ID]struct{}),
		fanout:    make(map[string]map[peer.ID]struct{}),
		lastpub:   make(map[string]int64),
		gossip:    make(map[peer.ID][]*pb.ControlIHave),
		control:   make(map[peer.ID]*pb.ControlMessage),
		backoff:   make(map[string]map[peer.ID]time.Time),
		peerhave:  make(map[peer.ID]int),
		iasked:    make(map[peer.ID]int),
		outbound:  make(map[peer.ID]bool),
		connect:   make(chan connectInfo, params.MaxPendingConnections),
		cab:       pstoremem.NewAddrBook(),
		mcache:    NewMessageCache(params.HistoryGossip, params.HistoryLength),
		protos:    GossipSubDefaultProtocols,
		feature:   GossipSubDefaultFeatures,
		tagTracer: newTagTracer(h.ConnManager()),
		params:    params,
	}
}

// DefaultGossipSubParams 返回默认的 gossipsub 参数。
// 返回值:
//   - GossipSubParams: gossipsub 参数
func DefaultGossipSubParams() GossipSubParams {
	return GossipSubParams{
		D:                         GossipSubD,
		Dlo:                       GossipSubDlo,
		Dhi:                       GossipSubDhi,
		Dscore:                    GossipSubDscore,
		Dout:                      GossipSubDout,
		HistoryLength:             GossipSubHistoryLength,
		HistoryGossip:             GossipSubHistoryGossip,
		Dlazy:                     GossipSubDlazy,
		GossipFactor:              GossipSubGossipFactor,
		GossipRetransmission:      GossipSubGossipRetransmission,
		HeartbeatInitialDelay:     GossipSubHeartbeatInitialDelay,
		HeartbeatInterval:         GossipSubHeartbeatInterval,
		FanoutTTL:                 GossipSubFanoutTTL,
		PrunePeers:                GossipSubPrunePeers,
		PruneBackoff:              GossipSubPruneBackoff,
		UnsubscribeBackoff:        GossipSubUnsubscribeBackoff,
		Connectors:                GossipSubConnectors,
		MaxPendingConnections:     GossipSubMaxPendingConnections,
		ConnectionTimeout:         GossipSubConnectionTimeout,
		DirectConnectTicks:        GossipSubDirectConnectTicks,
		DirectConnectInitialDelay: GossipSubDirectConnectInitialDelay,
		OpportunisticGraftTicks:   GossipSubOpportunisticGraftTicks,
		OpportunisticGraftPeers:   GossipSubOpportunisticGraftPeers,
		GraftFloodThreshold:       GossipSubGraftFloodThreshold,
		MaxIHaveLength:            GossipSubMaxIHaveLength,
		MaxIHaveMessages:          GossipSubMaxIHaveMessages,
		IWantFollowupTime:         GossipSubIWantFollowupTime,
		SlowHeartbeatWarning:      0.1,
	}
}

// WithPeerScore 是一个 gossipsub 路由器选项，用于启用对等节点评分。
// 参数:
//   - params: *PeerScoreParams 类型，表示对等节点评分参数。
//   - thresholds: *PeerScoreThresholds 类型，表示对等节点评分阈值。
//
// 返回值:
//   - Option: 返回一个 Option 类型的函数，用于配置 gossipsub 路由器。
func WithPeerScore(params *PeerScoreParams, thresholds *PeerScoreThresholds) Option {
	return func(ps *PubSub) error { // 返回的函数，接收 *PubSub 类型参数并返回 error。
		gs, ok := ps.rt.(*GossipSubRouter) // 类型断言，检查路由器是否为 GossipSubRouter 类型。
		if !ok {                           // 如果断言失败，表示路由器不是 gossipsub 类型。
			logger.Warnf("pubsub 路由器不是 gossipsub")      // 返回错误，说明当前路由器不是 gossipsub。
			return fmt.Errorf("pubsub 路由器不是 gossipsub") // 返回错误，说明当前路由器不是 gossipsub。
		}

		// 检查评分参数的合法性
		err := params.validate() // 调用 validate 方法验证 PeerScoreParams 的合法性。
		if err != nil {          // 如果验证失败，返回错误。
			logger.Warnf("peer score 参数验证失败: %v", err) // 返回错误信息。
			return err                                 // 返回错误信息。
		}

		// 检查阈值的合法性
		err = thresholds.validate() // 调用 validate 方法验证 PeerScoreThresholds 的合法性。
		if err != nil {             // 如果验证失败，返回错误。
			logger.Warnf("peer score 阈值验证失败: %v", err) // 返回错误信息。
			return err                                 // 返回错误信息。
		}

		gs.score = newPeerScore(params)                                         // 创建新的对等节点评分实例，并赋值给路由器的 score 字段。
		gs.gossipThreshold = thresholds.GossipThreshold                         // 设置 gossip 阈值。
		gs.publishThreshold = thresholds.PublishThreshold                       // 设置发布阈值。
		gs.graylistThreshold = thresholds.GraylistThreshold                     // 设置灰名单阈值。
		gs.acceptPXThreshold = thresholds.AcceptPXThreshold                     // 设置接受 PX 阈值。
		gs.opportunisticGraftThreshold = thresholds.OpportunisticGraftThreshold // 设置机会性 graft 阈值。

		gs.gossipTracer = newGossipTracer() // 创建新的 gossip 追踪器实例。

		// 挂钩追踪器
		if ps.tracer != nil { // 如果 pubsub 中已有追踪器，则添加新的追踪器。
			ps.tracer.raw = append(ps.tracer.raw, gs.score, gs.gossipTracer) // 将 score 和 gossipTracer 添加到追踪器列表中。
		} else { // 如果没有现有的追踪器。
			ps.tracer = &pubsubTracer{ // 创建一个新的 pubsubTracer 实例。
				raw:   []RawTracer{gs.score, gs.gossipTracer}, // 初始化追踪器列表。
				pid:   ps.host.ID(),                           // 设置主机 ID。
				idGen: ps.idGen,                               // 设置 ID 生成器。
			}
		}

		return nil // 返回 nil，表示没有错误。
	}
}

// WithFloodPublish 是一个 gossipsub 路由器选项，用于启用洪水发布。
// 参数:
//   - floodPublish: bool 类型，表示是否启用洪水发布。
//
// 返回值:
//   - Option: 返回一个 Option 类型的函数，用于配置 gossipsub 路由器。
func WithFloodPublish(floodPublish bool) Option {
	return func(ps *PubSub) error { // 返回的函数，接收 *PubSub 类型参数并返回 error。
		gs, ok := ps.rt.(*GossipSubRouter) // 类型断言，检查路由器是否为 GossipSubRouter 类型。
		if !ok {                           // 如果断言失败，表示路由器不是 gossipsub 类型。
			logger.Warnf("发布订阅路由器不是 gossipsub 类型")      // 返回错误，说明当前路由器不是 gossipsub。
			return fmt.Errorf("发布订阅路由器不是 gossipsub 类型") // 返回错误，说明当前路由器不是 gossipsub。
		}

		gs.floodPublish = floodPublish // 设置 floodPublish 字段，根据参数决定是否启用洪水发布。

		return nil // 返回 nil，表示没有错误。
	}
}

// WithPeerExchange 是一个 gossipsub 路由器选项，用于在 PRUNE 上启用对等节点交换。
// 参数:
//   - doPX: bool 类型，表示是否启用对等节点交换。
//
// 返回值:
//   - Option: 返回一个 Option 类型的函数，用于配置 gossipsub 路由器。
func WithPeerExchange(doPX bool) Option {
	return func(ps *PubSub) error { // 返回的函数，接收 *PubSub 类型参数并返回 error。
		gs, ok := ps.rt.(*GossipSubRouter) // 类型断言，检查路由器是否为 GossipSubRouter 类型。
		if !ok {                           // 如果断言失败，表示路由器不是 gossipsub 类型。
			logger.Warnf("发布订阅路由器不是 gossipsub 类型")      // 返回错误，说明当前路由器不是 gossipsub。
			return fmt.Errorf("发布订阅路由器不是 gossipsub 类型") // 返回错误，说明当前路由器不是 gossipsub。
		}

		gs.doPX = doPX // 设置 doPX 字段，根据参数决定是否启用对等节点交换。

		return nil // 返回 nil，表示没有错误。
	}
}

// WithDirectPeers 是一个 gossipsub 路由器选项，用于指定具有直接对等关系的对等节点。
// 参数:
//   - pis: []peer.AddrInfo 类型，表示对等节点的信息列表。
//
// 返回值:
//   - Option: 返回一个 Option 类型的函数，用于配置 gossipsub 路由器。
func WithDirectPeers(pis []peer.AddrInfo) Option {
	return func(ps *PubSub) error { // 返回的函数，接收 *PubSub 类型参数并返回 error。
		gs, ok := ps.rt.(*GossipSubRouter) // 类型断言，检查路由器是否为 GossipSubRouter 类型。
		if !ok {                           // 如果断言失败，表示路由器不是 gossipsub 类型。
			logger.Warnf("发布订阅路由器不是 gossipsub 类型")      // 返回错误，说明当前路由器不是 gossipsub。
			return fmt.Errorf("发布订阅路由器不是 gossipsub 类型") // 返回错误，说明当前路由器不是 gossipsub。
		}

		direct := make(map[peer.ID]struct{}) // 创建一个空的 map，用于存储直接对等节点的 ID。
		for _, pi := range pis {             // 遍历传入的对等节点信息列表。
			direct[pi.ID] = struct{}{}                                                // 将对等节点的 ID 添加到 map 中。
			ps.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL) // 将对等节点的地址添加到 peerstore 中，地址的 TTL 为永久。
		}

		gs.direct = direct // 将直接对等节点的 map 赋值给 gossipsub 路由器的 direct 字段。

		if gs.tagTracer != nil { // 如果路由器中已有 tagTracer，则更新其 direct 字段。
			gs.tagTracer.direct = direct // 将直接对等节点的 map 赋值给 tagTracer 的 direct 字段。
		}

		return nil // 返回 nil，表示没有错误。
	}
}

// WithDirectConnectTicks 是一个 gossipsub 路由器选项，用于设置尝试重新连接当前未连接的直接对等节点的心跳滴答次数。
// 参数:
//   - t: uint64 类型，表示心跳滴答次数。
//
// 返回值:
//   - Option: 返回一个 Option 类型的函数，用于配置 gossipsub 路由器。
func WithDirectConnectTicks(t uint64) Option {
	return func(ps *PubSub) error { // 返回的函数，接收 *PubSub 类型参数并返回 error。
		gs, ok := ps.rt.(*GossipSubRouter) // 类型断言，检查路由器是否为 GossipSubRouter 类型。
		if !ok {                           // 如果断言失败，表示路由器不是 gossipsub 类型。
			logger.Warnf("发布订阅路由器不是 gossipsub 类型")      // 返回错误，说明当前路由器不是 gossipsub。
			return fmt.Errorf("发布订阅路由器不是 gossipsub 类型") // 返回错误，说明当前路由器不是 gossipsub。
		}
		gs.params.DirectConnectTicks = t // 将传入的心跳滴答次数赋值给 gossipsub 路由器的 DirectConnectTicks 参数。
		return nil                       // 返回 nil，表示没有错误。
	}
}

// WithGossipSubParams 是一个 gossipsub 路由器选项，允许在实例化 gossipsub 路由器时设置自定义配置。
// 参数:
//   - cfg: GossipSubParams 类型，表示 gossipsub 参数配置。
//
// 返回值:
//   - Option: 返回一个 Option 类型的函数，用于配置 gossipsub 路由器。
func WithGossipSubParams(cfg GossipSubParams) Option {
	return func(ps *PubSub) error { // 返回的函数，接收 *PubSub 类型参数并返回 error。
		gs, ok := ps.rt.(*GossipSubRouter) // 类型断言，检查路由器是否为 GossipSubRouter 类型。
		if !ok {                           // 如果断言失败，表示路由器不是 gossipsub 类型。
			logger.Warnf("发布订阅路由器不是 gossipsub 类型")      // 返回错误，说明当前路由器不是 gossipsub。
			return fmt.Errorf("发布订阅路由器不是 gossipsub 类型") // 返回错误，说明当前路由器不是 gossipsub。
		}
		// 覆盖当前配置和路由器中的相关变量。
		gs.params = cfg                                                   // 使用传入的 GossipSubParams 配置覆盖当前路由器的参数。
		gs.connect = make(chan connectInfo, cfg.MaxPendingConnections)    // 根据配置的 MaxPendingConnections 创建一个新的连接通道。
		gs.mcache = NewMessageCache(cfg.HistoryGossip, cfg.HistoryLength) // 根据配置的 HistoryGossip 和 HistoryLength 创建一个新的消息缓存实例。

		return nil // 返回 nil，表示没有错误。
	}
}

// GossipSubRouter 是一个实现 gossipsub 协议的路由器。
// 对于我们加入的每个主题，我们维护一个消息流过的覆盖层；这是 mesh map。
// 对于我们发布但没有加入的每个主题，我们维护一个对等节点列表，用于在覆盖层中注入我们的消息；这是 fanout map。
// 如果我们没有发布任何消息到 fanout 主题的 fanout 对等节点列表在 GossipSubFanoutTTL 之后将过期。
type GossipSubRouter struct {
	p        *PubSub                          // PubSub 实例
	peers    map[peer.ID]protocol.ID          // 对等节点协议
	direct   map[peer.ID]struct{}             // 直接对等节点
	mesh     map[string]map[peer.ID]struct{}  // 主题网格
	fanout   map[string]map[peer.ID]struct{}  // 主题 fanout
	lastpub  map[string]int64                 // fanout 主题的最后发布时间
	gossip   map[peer.ID][]*pb.ControlIHave   // 挂起的 gossip
	control  map[peer.ID]*pb.ControlMessage   // 挂起的控制消息
	peerhave map[peer.ID]int                  // 在最后一个心跳中从对等节点接收到的 IHAVE 数量
	iasked   map[peer.ID]int                  // 在最后一个心跳中我们从对等节点请求的消息数量
	outbound map[peer.ID]bool                 // 连接方向缓存，标记具有出站连接的对等节点
	backoff  map[string]map[peer.ID]time.Time // 修剪回退
	connect  chan connectInfo                 // px 连接请求
	cab      peerstore.AddrBook               // 地址簿

	protos  []protocol.ID        // 协议列表
	feature GossipSubFeatureTest // 特性测试函数

	mcache       *MessageCache // 消息缓存
	tracer       *pubsubTracer // 跟踪器
	score        *peerScore    // 对等节点评分
	gossipTracer *gossipTracer // gossip 跟踪器
	tagTracer    *tagTracer    // 标签跟踪器
	gate         *peerGater    // 对等节点门控

	// gossipsub 参数配置
	params GossipSubParams

	// 是否启用对等节点交换；应在引导节点和其他连接良好/受信任的节点中启用。
	doPX bool

	// 接受 PX 的阈值；应为正值，并限于引导节点和受信任节点可达到的分数
	acceptPXThreshold float64

	// 发出/接受 gossip 的对等节点评分阈值
	// 如果对等节点评分低于此阈值，我们不会发出或接受来自对等节点的 gossip。
	// 当没有评分时，此值为 0。
	gossipThreshold float64

	// 洪水发布评分阈值；当使用洪水发布时，我们仅向评分 >= 阈值的对等节点发布消息
	// 或者对等节点是 fanout 或 floodsub 对等节点。
	publishThreshold float64

	// 对等节点评分的阈值，在此之前我们会将对等节点列入灰名单并默默地忽略其 RPC。
	graylistThreshold float64

	// 中位对等节点评分的阈值，在此之前会触发机会性移植。
	opportunisticGraftThreshold float64

	// 是否使用洪水发布
	floodPublish bool

	// 从开始的心跳滴答数；这允许我们摊销一些资源清理操作，例如回退清理。
	heartbeatTicks uint64
}

// connectInfo 是连接信息结构体。
type connectInfo struct {
	p   peer.ID          // 对等节点 ID
	spr *record.Envelope // 记录信封
}

// Protocols 返回协议列表。
// 返回值:
//   - []protocol.ID: 协议列表
func (gs *GossipSubRouter) Protocols() []protocol.ID {
	return gs.protos
}

// Attach 将 GossipSubRouter 附加到 PubSub 实例。
// 参数:
//   - p: PubSub 实例
func (gs *GossipSubRouter) Attach(p *PubSub) {
	gs.p = p
	gs.tracer = p.tracer

	// 启动评分
	gs.score.Start(gs)

	// 启动 gossip 跟踪
	gs.gossipTracer.Start(gs)

	// 启动 connmgr 标签跟踪器
	gs.tagTracer.Start(gs)

	// 开始使用与 PubSub 相同的消息 ID 函数来缓存消息。
	gs.mcache.SetMsgIdFn(p.idGen.ID)

	// 启动心跳
	go gs.heartbeatTimer()

	// 启动 PX 连接器
	for i := 0; i < gs.params.Connectors; i++ {
		go gs.connector()
	}

	// 管理地址簿
	go gs.manageAddrBook()

	// 连接直接对等节点
	if len(gs.direct) > 0 {
		go func() {
			if gs.params.DirectConnectInitialDelay > 0 {
				time.Sleep(gs.params.DirectConnectInitialDelay)
			}
			for p := range gs.direct {
				gs.connect <- connectInfo{p: p}
			}
		}()
	}
}

// manageAddrBook 管理地址簿。
func (gs *GossipSubRouter) manageAddrBook() {
	sub, err := gs.p.host.EventBus().Subscribe([]interface{}{ // 订阅与节点身份识别和连接状态变化相关的事件。
		&event.EvtPeerIdentificationCompleted{}, // 节点身份识别完成事件。
		&event.EvtPeerConnectednessChanged{},    // 节点连接状态变化事件。
	})
	if err != nil { // 如果订阅失败，记录错误日志并返回。
		logger.Errorf("订阅节点身份识别事件失败: %v", err) // 记录订阅事件失败的错误信息。
		return
	}
	defer sub.Close() // 在函数返回时关闭订阅，以释放资源。

	for { // 无限循环，用于处理事件。
		select {
		case <-gs.p.ctx.Done(): // 检查上下文是否已取消，如果是则进行清理操作。
			cabCloser, ok := gs.cab.(io.Closer) // 检查地址簿是否实现了 io.Closer 接口。
			if ok {                             // 如果实现了 io.Closer 接口，则尝试关闭地址簿。
				errClose := cabCloser.Close() // 调用 Close 方法关闭地址簿。
				if errClose != nil {          // 如果关闭失败，记录警告日志。
					logger.Warnf("关闭地址簿失败: %v", errClose) // 记录关闭地址簿失败的警告信息。
				}
			}
			return // 返回，结束函数执行。
		case ev := <-sub.Out(): // 从事件订阅中获取事件。
			switch ev := ev.(type) { // 使用类型断言检查事件类型，并分别处理。
			case event.EvtPeerIdentificationCompleted: // 如果事件是 EvtPeerIdentificationCompleted 类型。
				if ev.SignedPeerRecord != nil { // 如果事件中包含已签名的对等节点记录。
					cab, ok := peerstore.GetCertifiedAddrBook(gs.cab) // 获取认证地址簿（CertifiedAddrBook）。
					if ok {                                           // 如果获取成功，处理签名的对等节点记录。
						ttl := peerstore.RecentlyConnectedAddrTTL                            // 设置 TTL 为最近连接的 TTL。
						if gs.p.host.Network().Connectedness(ev.Peer) == network.Connected { // 如果节点当前已连接，使用连接的 TTL。
							ttl = peerstore.ConnectedAddrTTL
						}
						_, err := cab.ConsumePeerRecord(ev.SignedPeerRecord, ttl) // 消费签名的对等节点记录，并设置相应的 TTL。
						if err != nil {                                           // 如果消费失败，记录警告日志。
							logger.Warnf("消费签名对等节点记录失败: %v", err) // 记录消费签名对等节点记录失败的警告信息。
						}
					}
				}
			case event.EvtPeerConnectednessChanged: // 如果事件是 EvtPeerConnectednessChanged 类型。
				if ev.Connectedness != network.Connected { // 如果节点不再处于连接状态。
					gs.cab.UpdateAddrs(ev.Peer, peerstore.ConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL) // 更新节点地址的 TTL，将其从连接的 TTL 更新为最近连接的 TTL。
				}
			}
		}
	}
}

// AddPeer 添加对等节点。
// 参数:
//   - p: peer.ID 类型，表示对等节点的 ID。
//   - proto: protocol.ID 类型，表示协议 ID。
func (gs *GossipSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	logger.Debugf("对等节点上线: %s 使用 %s", p, proto) // 记录添加新对等节点的调试信息。
	gs.tracer.AddPeer(p, proto)                 // 调用 tracer 的 AddPeer 方法，记录对等节点的添加信息。
	gs.peers[p] = proto                         // 将对等节点和协议 ID 添加到 peers 映射中。

	// 追踪连接方向
	outbound := false                           // 初始化 outbound 变量，表示连接方向是否为出站。
	conns := gs.p.host.Network().ConnsToPeer(p) // 获取与指定对等节点的所有连接。
loop: // 标签，用于在找到符合条件的连接时跳出循环。
	for _, c := range conns { // 遍历所有连接。
		stat := c.Stat() // 获取连接状态。

		if stat.Limited { // 如果连接受限，跳过该连接。
			continue
		}

		if stat.Direction == network.DirOutbound { // 如果连接方向为出站。
			// 仅在连接具有 pubsub 流时计算连接
			for _, s := range c.GetStreams() { // 遍历连接中的所有流。
				if s.Protocol() == proto { // 如果流的协议与指定协议匹配。
					outbound = true // 设置 outbound 为 true，表示此连接为出站连接。
					break loop      // 跳出循环，不再检查其他连接。
				}
			}
		}
	}
	gs.outbound[p] = outbound // 将连接方向信息记录到 outbound 映射中。
}

// RemovePeer 移除对等节点。
// 参数:
//   - p: peer.ID 类型，表示对等节点的 ID。
func (gs *GossipSubRouter) RemovePeer(p peer.ID) {
	logger.Debugf("对等节点下线: %s", p)  // 记录移除对等节点的调试信息。
	gs.tracer.RemovePeer(p)         // 调用 tracer 的 RemovePeer 方法，记录对等节点的移除信息。
	delete(gs.peers, p)             // 从 peers 映射中删除对等节点。
	for _, peers := range gs.mesh { // 遍历所有 mesh 主题的对等节点集合。
		delete(peers, p) // 从每个主题的对等节点集合中删除指定对等节点。
	}
	for _, peers := range gs.fanout { // 遍历所有 fanout 主题的对等节点集合。
		delete(peers, p) // 从每个主题的对等节点集合中删除指定对等节点。
	}
	delete(gs.gossip, p)   // 从 gossip 映射中删除指定对等节点。
	delete(gs.control, p)  // 从 control 映射中删除指定对等节点。
	delete(gs.outbound, p) // 从 outbound 映射中删除指定对等节点。
}

// EnoughPeers 检查主题是否有足够的对等节点。
// 参数:
//   - topic: string 类型，表示主题名称。
//   - suggested: int 类型，表示建议的对等节点数量。
//
// 返回值:
//   - bool: 返回布尔值，表示是否有足够的对等节点。
func (gs *GossipSubRouter) EnoughPeers(topic string, suggested int) bool {
	// 检查主题中的所有对等节点
	tmap, ok := gs.p.topics[topic] // 从 topics 映射中获取指定主题的对等节点集合。
	if !ok {                       // 如果主题不存在，返回 false。
		return false
	}

	fsPeers, gsPeers := 0, 0 // 初始化 floodsub 和 gossipsub 对等节点计数器。
	// floodsub 对等节点
	for p := range tmap { // 遍历主题中的所有对等节点。
		if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) { // 如果节点不支持 gossipsub mesh 特性，则计为 floodsub 节点。
			fsPeers++
		}
	}

	// gossipsub 对等节点
	gsPeers = len(gs.mesh[topic]) // 获取 gossipsub 节点的数量。

	if suggested == 0 { // 如果没有指定建议的对等节点数量，则使用配置中的 Dlo 值。
		suggested = gs.params.Dlo
	}

	// 如果 floodsub 节点数加 gossipsub 节点数大于或等于建议数，或 gossipsub 节点数大于或等于 Dhi，则返回 true。
	if fsPeers+gsPeers >= suggested || gsPeers >= gs.params.Dhi {
		return true
	}

	return false // 否则返回 false，表示没有足够的对等节点。
}

// AcceptFrom 检查是否接受来自对等节点的消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点的 ID。
//
// 返回值:
//   - AcceptStatus: 返回接受状态，表示是否接受消息。
func (gs *GossipSubRouter) AcceptFrom(p peer.ID) AcceptStatus {
	_, direct := gs.direct[p] // 检查对等节点是否为直接连接的节点。
	if direct {               // 如果是直接连接的节点，接受所有消息。
		return AcceptAll
	}

	if gs.score.Score(p) < gs.graylistThreshold { // 如果对等节点的评分低于灰名单阈值，不接受任何消息。
		return AcceptNone
	}

	return gs.gate.AcceptFrom(p) // 否则，根据 gate 的判断决定是否接受消息。
}

// HandleRPC 处理 RPC 消息。
// 参数:
//   - rpc: *RPC 类型，表示要处理的 RPC 消息。
func (gs *GossipSubRouter) HandleRPC(rpc *RPC) {
	ctl := rpc.GetControl() // 获取 RPC 消息中的控制消息。
	if ctl == nil {         // 如果控制消息为空，直接返回。
		return
	}

	iwant := gs.handleIHave(rpc.from, ctl) // 处理 IHAVE 控制消息，并获取 IWANT 控制消息列表。
	ihave := gs.handleIWant(rpc.from, ctl) // 处理 IWANT 控制消息，并获取消息列表。
	prune := gs.handleGraft(rpc.from, ctl) // 处理 GRAFT 控制消息，并获取 PRUNE 控制消息列表。
	gs.handlePrune(rpc.from, ctl)          // 处理 PRUNE 控制消息。

	if len(iwant) == 0 && len(ihave) == 0 && len(prune) == 0 { // 如果没有需要发送的控制消息，直接返回。
		return
	}

	out := rpcWithControl(ihave, nil, iwant, nil, prune) // 构造带有控制消息的 RPC 消息。
	gs.sendRPC(rpc.from, out)                            // 发送 RPC 消息到发送者。
}

// handleIHave 处理 IHAVE 控制消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点的 ID。
//   - ctl: *pb.ControlMessage 类型，表示控制消息。
//
// 返回值:
//   - []*pb.ControlIWant: 返回 IWANT 控制消息列表。
func (gs *GossipSubRouter) handleIHave(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlIWant {
	// 忽略评分低于 gossip 阈值的对等节点的 IHAVE gossip
	score := gs.score.Score(p)      // 获取对等节点的评分。
	if score < gs.gossipThreshold { // 如果评分低于 gossip 阈值，忽略此对等节点的 IHAVE 消息。
		logger.Debugf("IHAVE: 忽略评分低于阈值的对等节点 %s [score = %f]", p, score) // 记录忽略消息的调试信息。
		return nil                                                      // 返回空列表。
	}

	// IHAVE flood 保护
	gs.peerhave[p]++                                 // 增加此对等节点的 IHAVE 消息计数。
	if gs.peerhave[p] > gs.params.MaxIHaveMessages { // 如果对等节点的 IHAVE 消息计数超过了限制，忽略此消息。
		logger.Debugf("IHAVE: 对等节点 %s 在心跳间隔内广告了太多消息 (%d); 忽略", p, gs.peerhave[p]) // 记录忽略消息的调试信息。
		return nil                                                                // 返回空列表。
	}
	if gs.iasked[p] >= gs.params.MaxIHaveLength { // 如果已请求的消息数量超过了限制，忽略此消息。
		logger.Debugf("IHAVE: 对等节点 %s 已经广告了太多消息 (%d); 忽略", p, gs.iasked[p]) // 记录忽略消息的调试信息。
		return nil                                                          // 返回空列表。
	}

	iwant := make(map[string]struct{})     // 创建一个空的 map，用于存储请求的消息 ID。
	for _, ihave := range ctl.GetIhave() { // 遍历 IHAVE 控制消息中的消息 ID 列表。
		topic := ihave.GetTopicID() // 获取消息所属的主题 ID。
		_, ok := gs.mesh[topic]     // 检查主题是否在 mesh 中。
		if !ok {                    // 如果主题不在 mesh 中，跳过此消息。
			continue
		}

		if !gs.p.peerFilter(p, topic) { // 如果对等节点不符合主题的过滤条件，跳过此消息。
			continue
		}

	checkIwantMsgsLoop: // 标签，用于在遇到限制时跳出消息循环。
		for msgIdx, mid := range ihave.GetMessageIDs() { // 遍历消息 ID 列表。
			// 防止远程对等节点在单个 IHAVE 消息中发送过多消息 ID
			if msgIdx >= gs.params.MaxIHaveLength { // 如果消息数量超过限制，忽略剩余消息。
				logger.Debugf("IHAVE: 对等节点 %s 在主题 %s 上发送了太多消息 (%d); 忽略剩余消息", p, topic, len(ihave.MessageIDs)) // 记录忽略消息的调试信息。
				break checkIwantMsgsLoop                                                                      // 跳出消息循环。
			}

			if gs.p.seenMessage(mid) { // 如果消息 ID 已被看到，跳过此消息。
				continue
			}
			iwant[mid] = struct{}{} // 将消息 ID 添加到请求列表中。
		}
	}

	if len(iwant) == 0 { // 如果没有需要请求的消息 ID，返回空列表。
		return nil
	}

	iask := len(iwant)                                // 获取需要请求的消息数量。
	if iask+gs.iasked[p] > gs.params.MaxIHaveLength { // 如果请求的消息数量超过了限制，截断请求列表。
		iask = gs.params.MaxIHaveLength - gs.iasked[p]
	}

	logger.Debugf("IHAVE: 向对等节点 %s 请求 %d 条消息中的 %d 条", p, len(iwant), iask) // 记录请求消息的调试信息。

	iwantlst := make([]string, 0, len(iwant)) // 创建一个字符串切片，用于存储请求的消息 ID。
	for mid := range iwant {                  // 将消息 ID 添加到列表中。
		iwantlst = append(iwantlst, mid)
	}

	// 随机顺序请求
	shuffleStrings(iwantlst) // 随机打乱请求列表中的消息 ID 顺序。

	// 截断到我们实际请求的消息并更新 iasked 计数器
	iwantlst = iwantlst[:iask] // 截断请求列表到实际请求的消息数量，并更新已请求计数器。
	gs.iasked[p] += iask       // 更新已请求消息的计数器。

	gs.gossipTracer.AddPromise(p, iwantlst) // 将请求的消息 ID 添加到 gossip 追踪器的承诺列表中。

	return []*pb.ControlIWant{{MessageIDs: iwantlst}} // 返回构造的 IWANT 控制消息列表。
}

// handleIWant 处理 IWANT 控制消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点的 ID。
//   - ctl: *pb.ControlMessage 类型，表示控制消息。
//
// 返回值:
//   - []*pb.Message: 返回消息列表。
func (gs *GossipSubRouter) handleIWant(p peer.ID, ctl *pb.ControlMessage) []*pb.Message {
	// 不响应评分低于 gossip 阈值的对等节点的 IWANT 请求
	score := gs.score.Score(p)      // 获取对等节点的评分。
	if score < gs.gossipThreshold { // 如果评分低于 gossip 阈值，忽略此对等节点的 IWANT 请求。
		logger.Debugf("IWANT: 忽略评分低于阈值的对等节点 %s [score = %f]", p, score) // 记录忽略请求的调试信息。
		return nil                                                      // 返回空消息列表。
	}

	ihave := make(map[string]*pb.Message)  // 创建一个空的 map，用于存储需要发送的消息。
	for _, iwant := range ctl.GetIwant() { // 遍历 IWANT 控制消息中的消息 ID 列表。
		for _, mid := range iwant.GetMessageIDs() { // 遍历每个消息 ID。
			msg, count, ok := gs.mcache.GetForPeer(mid, p) // 从消息缓存中获取对应的消息及其请求计数。
			if !ok {                                       // 如果消息不在缓存中，跳过此消息。
				continue
			}

			if !gs.p.peerFilter(p, msg.GetTopic()) { // 如果对等节点不符合主题的过滤条件，跳过此消息。
				continue
			}

			if count > gs.params.GossipRetransmission { // 如果消息已被请求次数超过了限制，忽略此请求。
				logger.Debugf("IWANT: 对等节点 %s 请求消息 %s 次数过多; 忽略请求", p, mid) // 记录忽略请求的调试信息。
				continue
			}

			ihave[mid] = msg.Message // 将消息添加到需要发送的消息列表中。
		}
	}

	if len(ihave) == 0 { // 如果没有需要发送的消息，返回空消息列表。
		return nil
	}

	logger.Debugf("IWANT: 向对等节点 %s 发送 %d 条消息", p, len(ihave)) // 记录发送消息的调试信息。

	msgs := make([]*pb.Message, 0, len(ihave)) // 创建一个消息切片，用于存储将要发送的消息。
	for _, msg := range ihave {                // 将消息添加到消息列表中。
		msgs = append(msgs, msg)
	}

	return msgs // 返回消息列表。
}

// handleGraft 处理 GRAFT 控制消息。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - ctl: *pb.ControlMessage 类型，控制消息。
//
// 返回值:
//   - []*pb.ControlPrune: 返回 PRUNE 控制消息列表。
func (gs *GossipSubRouter) handleGraft(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlPrune {
	var prune []string // 定义一个字符串切片，用于存储需要 PRUNE 的主题。

	doPX := gs.doPX            // 获取当前 PX（Peer Exchange，对等节点交换）的状态。
	score := gs.score.Score(p) // 获取对等节点的评分。
	now := time.Now()          // 获取当前时间。

	for _, graft := range ctl.GetGraft() { // 遍历所有 GRAFT 控制消息。
		topic := graft.GetTopicID() // 获取 GRAFT 消息中的主题 ID。

		if !gs.p.peerFilter(p, topic) { // 检查对等节点是否通过了主题的过滤器。
			continue // 如果未通过过滤器，跳过此节点。
		}

		peers, ok := gs.mesh[topic] // 获取该主题的网格对等节点列表。
		if !ok {                    // 如果主题不在网格中。
			// 在未知主题时不进行 PX，以避免泄露我们的对等节点。
			doPX = false // 禁用 PX。
			// 防 spam 硬化：忽略未知主题的 GRAFT。
			continue // 跳过此节点。
		}

		// 检查是否已在网格中；如果是则不做任何操作（可能存在并发 grafting）。
		_, inMesh := peers[p] // 检查对等节点是否已在网格中。
		if inMesh {           // 如果对等节点已在网格中。
			continue // 跳过此节点。
		}

		// 我们不会对直接对等节点进行 GRAFT；如果发生这种情况，请大声抱怨。
		_, direct := gs.direct[p] // 检查对等节点是否为直接对等节点。
		if direct {               // 如果对等节点是直接对等节点。
			logger.Warnf("GRAFT: 忽略来自直接对等节点 %s 的请求", p) // 记录警告信息，忽略来自直接对等节点的 GRAFT 请求。
			// 这可能是非对称配置的错误；发送 PRUNE。
			prune = append(prune, topic) // 将主题添加到 PRUNE 列表中。
			// 但不进行 PX。
			doPX = false // 禁用 PX。
			continue     // 跳过此节点。
		}

		// 确保我们没有回避该对等节点。
		expire, backoff := gs.backoff[topic][p] // 获取该主题下对等节点的回退时间。
		if backoff && now.Before(expire) {      // 如果对等节点处于回退期，并且当前时间在回退时间之前。
			logger.Debugf("GRAFT: 忽略回退的对等节点 %s", p) // 记录调试信息，忽略此对等节点的 GRAFT 请求。
			// 添加行为惩罚。
			gs.score.AddPenalty(p, 1) // 对该对等节点添加惩罚。
			// 不进行 PX。
			doPX = false // 禁用 PX。
			// 检查 flood 截止点——GRAFT 是否来得太快？
			floodCutoff := expire.Add(gs.params.GraftFloodThreshold - gs.params.PruneBackoff) // 计算 flood 截止时间。
			if now.Before(floodCutoff) {                                                      // 如果当前时间在 flood 截止时间之前。
				// 额外惩罚。
				gs.score.AddPenalty(p, 1) // 对该对等节点添加额外的惩罚。
			}
			// 刷新回退。
			gs.addBackoff(p, topic, false) // 刷新回退时间。
			prune = append(prune, topic)   // 将主题添加到 PRUNE 列表中。
			continue                       // 跳过此节点。
		}

		// 检查评分。
		if score < 0 { // 如果对等节点的评分为负。
			// 我们不会 GRAFT 评分为负的对等节点。
			logger.Debugf("GRAFT: 忽略评分低于 0 的对等节点 %s [score = %f, topic = %s]", p, score, topic) // 记录调试信息，忽略此对等节点的 GRAFT 请求。
			// 我们确实发送 PRUNE，因为这是协议正确性的问题。
			prune = append(prune, topic) // 将主题添加到 PRUNE 列表中。
			// 但我们不会 PX 给它们。
			doPX = false // 禁用 PX。
			// 添加/刷新回退，以便即使评分回升我们也不会过早重新 GRAFT。
			gs.addBackoff(p, topic, false) // 添加或刷新回退时间。
			continue                       // 跳过此节点。
		}

		// 检查网格对等节点数量；如果已达到（或超过） Dhi，我们仅接受出站连接的对等节点的 GRAFT；
		// 这是一个防御检查，以限制潜在的网格接管攻击和爱情轰炸的结合。
		if len(peers) >= gs.params.Dhi && !gs.outbound[p] { // 如果网格中对等节点数量达到或超过 Dhi，并且该节点不是出站连接。
			prune = append(prune, topic)   // 将主题添加到 PRUNE 列表中。
			gs.addBackoff(p, topic, false) // 添加或刷新回退时间。
			continue                       // 跳过此节点。
		}

		logger.Debugf("GRAFT: 从对等节点 %s 添加网格链接到主题 %s", p, topic) // 记录调试信息，添加对等节点到网格中。
		gs.tracer.Graft(p, topic)                               // 记录 GRAFT 操作。
		peers[p] = struct{}{}                                   // 将对等节点添加到网格中。
	}

	if len(prune) == 0 { // 如果没有需要 PRUNE 的主题。
		return nil // 返回空列表。
	}

	cprune := make([]*pb.ControlPrune, 0, len(prune)) // 创建一个空的 PRUNE 控制消息列表。
	for _, topic := range prune {                     // 遍历所有需要 PRUNE 的主题。
		cprune = append(cprune, gs.makePrune(p, topic, doPX, false)) // 为每个主题创建 PRUNE 控制消息并添加到列表中。
	}

	return cprune // 返回 PRUNE 控制消息列表。
}

// handlePrune 处理 PRUNE 控制消息。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - ctl: *pb.ControlMessage 类型，控制消息。
func (gs *GossipSubRouter) handlePrune(p peer.ID, ctl *pb.ControlMessage) {
	score := gs.score.Score(p) // 获取对等节点的评分。

	for _, prune := range ctl.GetPrune() { // 遍历所有 PRUNE 控制消息。
		topic := prune.GetTopicID() // 获取 PRUNE 消息中的主题 ID。
		peers, ok := gs.mesh[topic] // 获取该主题的网格对等节点列表。
		if !ok {                    // 如果主题不在网格中。
			continue // 跳过此节点。
		}

		logger.Debugf("PRUNE: 从网格中移除对等节点 %s 到主题 %s", p, topic) // 记录调试信息，从网格中移除对等节点。
		gs.tracer.Prune(p, topic)                              // 记录 PRUNE 操作。
		delete(peers, p)                                       // 从网格中删除对等节点。
		// 对等节点是否指定了回退时间？如果是，请遵守。
		backoff := prune.GetBackoff() // 获取 PRUNE 消息中的回退时间。
		if backoff > 0 {              // 如果回退时间大于 0。
			gs.doAddBackoff(p, topic, time.Duration(backoff)*time.Second) // 添加指定的回退时间。
		} else {
			gs.addBackoff(p, topic, false) // 添加默认的回退时间。
		}

		px := prune.GetPeers() // 获取 PRUNE 消息中的对等节点列表。
		if len(px) > 0 {       // 如果对等节点列表不为空。
			// 忽略评分不足的对等节点的 PX。
			if score < gs.acceptPXThreshold { // 如果对等节点的评分低于接受 PX 的阈值。
				logger.Debugf("PRUNE: 忽略评分不足的对等节点 %s 的 PX [score = %f, topic = %s]", p, score, topic) // 记录调试信息，忽略该对等节点的 PX。
				continue                                                                              // 跳过此节点。
			}

			gs.pxConnect(px) // 连接 PX 对等节点。
		}
	}
}

// addBackoff 添加回退。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - topic: string 类型，主题名称。
//   - isUnsubscribe: bool 类型，表示是否取消订阅。
func (gs *GossipSubRouter) addBackoff(p peer.ID, topic string, isUnsubscribe bool) {
	backoff := gs.params.PruneBackoff // 获取默认的 PRUNE 回退时间。
	if isUnsubscribe {                // 如果取消订阅。
		backoff = gs.params.UnsubscribeBackoff // 使用取消订阅的回退时间。
	}
	gs.doAddBackoff(p, topic, backoff) // 添加回退时间。
}

// doAddBackoff 执行添加回退。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - topic: string 类型，主题名称。
//   - interval: time.Duration 类型，回退时间间隔。
func (gs *GossipSubRouter) doAddBackoff(p peer.ID, topic string, interval time.Duration) {
	backoff, ok := gs.backoff[topic] // 获取该主题下的回退映射。
	if !ok {                         // 如果该主题没有回退映射。
		backoff = make(map[peer.ID]time.Time) // 创建一个新的回退映射。
		gs.backoff[topic] = backoff           // 将新的回退映射添加到回退列表中。
	}
	expire := time.Now().Add(interval) // 计算回退的过期时间。
	if backoff[p].Before(expire) {     // 如果当前回退时间早于新的过期时间。
		backoff[p] = expire // 更新回退时间为新的过期时间。
	}
}

// pxConnect 连接对等节点。
// 参数:
//   - peers: []*pb.PeerInfo 类型，对等节点信息列表。
func (gs *GossipSubRouter) pxConnect(peers []*pb.PeerInfo) {
	if len(peers) > gs.params.PrunePeers { // 如果对等节点数量超过了 PRUNE 阈值。
		shufflePeerInfo(peers)               // 随机打乱对等节点列表。
		peers = peers[:gs.params.PrunePeers] // 截断对等节点列表到 PRUNE 阈值。
	}

	toconnect := make([]connectInfo, 0, len(peers)) // 创建一个空的连接信息列表。

	for _, pi := range peers { // 遍历所有对等节点信息。
		p := peer.ID(pi.PeerID) // 获取对等节点的 ID。

		_, connected := gs.peers[p] // 检查对等节点是否已连接。
		if connected {              // 如果对等节点已连接。
			continue // 跳过此节点。
		}

		var spr *record.Envelope        // 定义一个变量，用于存储签名对等节点记录。
		if pi.SignedPeerRecord != nil { // 如果对等节点发送了签名记录。
			// 对等节点发送了签名记录；确保其有效。
			envelope, r, err := record.ConsumeEnvelope(pi.SignedPeerRecord, peer.PeerRecordEnvelopeDomain) // 解析签名记录。
			if err != nil {                                                                                // 如果解析失败。
				logger.Warnf("通过 px 获取的对等节点记录解析失败: %s", err) // 记录警告信息，忽略此对等节点。
				continue                                     // 跳过此节点。
			}
			rec, ok := r.(*peer.PeerRecord) // 类型断言，检查记录是否为 PeerRecord 类型。
			if !ok {                        // 如果记录不是 PeerRecord 类型。
				logger.Warnf("通过 px 获取的对等节点记录解析失败: 记录的 payload 不是 PeerRecord") // 记录警告信息，忽略此对等节点。
				continue                                                       // 跳过此节点。
			}
			if rec.PeerID != p { // 如果记录中的对等节点 ID 与预期的不匹配。
				logger.Warnf("通过 px 获取的对等节点记录解析失败: 记录的对等节点 ID %s 与预期的不匹配 %s", rec.PeerID, p) // 记录警告信息，忽略此对等节点。
				continue                                                                     // 跳过此节点。
			}
			spr = envelope // 将有效的签名记录存储到 spr 变量中。
		}

		toconnect = append(toconnect, connectInfo{p, spr}) // 将对等节点和签名记录添加到连接信息列表中。
	}

	if len(toconnect) == 0 { // 如果没有需要连接的对等节点。
		return // 返回，结束函数执行。
	}

	for _, ci := range toconnect { // 遍历所有需要连接的对等节点。
		select {
		case gs.connect <- ci: // 将连接信息发送到连接通道中。
		default: // 如果连接通道已满。
			logger.Debugf("忽略对等节点的连接尝试; 太多等待连接") // 记录调试信息，忽略连接请求。
		}
	}
}

// connector 执行连接逻辑。
func (gs *GossipSubRouter) connector() {
	for {
		select {
		case ci := <-gs.connect: // 从连接通道中获取连接信息。
			if gs.p.host.Network().Connectedness(ci.p) == network.Connected { // 检查对等节点是否已连接。
				continue // 如果对等节点已连接，跳过此节点。
			}

			logger.Debugf("开始连接对等节点 %s", ci.p)                // 记录调试信息，开始连接对等节点。
			cab, ok := peerstore.GetCertifiedAddrBook(gs.cab) // 获取认证地址簿。
			if ok && ci.spr != nil {                          // 如果获取认证地址簿成功，并且签名记录不为空。
				_, err := cab.ConsumePeerRecord(ci.spr, peerstore.TempAddrTTL) // 消费签名记录，并设置临时 TTL。
				if err != nil {                                                // 如果消费失败。
					logger.Debugf("处理对等节点记录失败: %s", err) // 记录调试信息，忽略此错误。
				}
			}

			ctx, cancel := context.WithTimeout(gs.p.ctx, gs.params.ConnectionTimeout)         // 创建带超时的上下文，用于连接操作。
			err := gs.p.host.Connect(ctx, peer.AddrInfo{ID: ci.p, Addrs: gs.cab.Addrs(ci.p)}) // 连接对等节点。
			cancel()                                                                          // 取消上下文。
			if err != nil {                                                                   // 如果连接失败。
				logger.Debugf("连接对等节点 %s 失败: %s", ci.p, err) // 记录调试信息，忽略此错误。
			}

		case <-gs.p.ctx.Done(): // 检查上下文是否已取消。
			return // 如果上下文已取消，结束函数执行。
		}
	}
}

// Publish 发布消息。
// 参数:
//   - msg: *Message 类型，表示要发布的消息。
func (gs *GossipSubRouter) Publish(msg *Message) {
	gs.mcache.Put(msg) // 将消息放入消息缓存中。

	from := msg.ReceivedFrom // 获取消息的发送者。
	topic := msg.GetTopic()  // 获取消息所属的主题。

	tosend := make(map[peer.ID]struct{}) // 创建一个空的对等节点集合，用于存储需要发送消息的对等节点。

	// 主题中有对等节点吗？
	tmap, ok := gs.p.topics[topic] // 获取该主题的对等节点集合。
	if !ok {                       // 如果主题中没有对等节点。
		return // 返回，结束函数执行。
	}

	if gs.floodPublish && from == gs.p.host.ID() { // 如果启用了洪水发布，并且消息发送者是自己。
		for p := range tmap { // 遍历主题中的所有对等节点。
			_, direct := gs.direct[p]                               // 检查对等节点是否为直接对等节点。
			if direct || gs.score.Score(p) >= gs.publishThreshold { // 如果是直接对等节点，或对等节点评分高于发布阈值。
				tosend[p] = struct{}{} // 将对等节点添加到需要发送的集合中。
			}
		}
	} else {
		// 直接对等节点
		for p := range gs.direct { // 遍历所有直接对等节点。
			_, inTopic := tmap[p] // 检查对等节点是否在主题中。
			if inTopic {          // 如果对等节点在主题中。
				tosend[p] = struct{}{} // 将对等节点添加到需要发送的集合中。
			}
		}

		// floodsub 对等节点
		for p := range tmap { // 遍历主题中的所有对等节点。
			if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) && gs.score.Score(p) >= gs.publishThreshold { // 如果对等节点不支持 gossipsub mesh 特性，并且评分高于发布阈值。
				tosend[p] = struct{}{} // 将对等节点添加到需要发送的集合中。
			}
		}

		// gossipsub 对等节点
		gmap, ok := gs.mesh[topic] // 获取该主题的网格对等节点集合。
		if !ok {                   // 如果主题不在网格中。
			// 我们不在该主题的网格中，使用 fanout 对等节点。
			gmap, ok = gs.fanout[topic] // 获取该主题的 fanout 对等节点集合。
			if !ok || len(gmap) == 0 {  // 如果 fanout 对等节点集合为空。
				// 我们没有任何对等节点，选择一些评分高于发布阈值的对等节点。
				peers := gs.getPeers(topic, gs.params.D, func(p peer.ID) bool { // 从对等节点列表中选择一些符合条件的节点。
					_, direct := gs.direct[p]                                  // 检查对等节点是否为直接对等节点。
					return !direct && gs.score.Score(p) >= gs.publishThreshold // 返回是否符合条件。
				})

				if len(peers) > 0 { // 如果找到了一些符合条件的对等节点。
					gmap = peerListToMap(peers) // 将对等节点列表转换为映射。
					gs.fanout[topic] = gmap     // 将映射存储到 fanout 对等节点集合中。
				}
			}
			gs.lastpub[topic] = time.Now().UnixNano() // 记录最后一次发布的时间。
		}

		for p := range gmap { // 遍历网格对等节点集合。
			tosend[p] = struct{}{} // 将对等节点添加到需要发送的集合中。
		}
	}

	out := rpcWithMessages(msg.Message) // 构造包含消息的 RPC 消息。
	for pid := range tosend {           // 遍历需要发送消息的对等节点集合。
		if pid == from || pid == peer.ID(msg.GetFrom()) { // 如果对等节点是消息的发送者。
			continue // 跳过此节点。
		}

		gs.sendRPC(pid, out) // 发送 RPC 消息到对等节点。
	}
}

// Join 加入主题。
// 参数:
//   - topic: string 类型，表示主题名称。
func (gs *GossipSubRouter) Join(topic string) {
	gmap, ok := gs.mesh[topic] // 获取该主题的网格对等节点集合。
	if ok {                    // 如果该主题已经在网格中。
		return // 返回，结束函数执行。
	}

	logger.Debugf("加入主题 %s", topic) // 记录调试信息，加入主题。
	gs.tracer.Join(topic)           // 记录加入操作。

	gmap, ok = gs.fanout[topic] // 获取该主题的 fanout 对等节点集合。
	if ok {                     // 如果 fanout 对等节点集合不为空。
		backoff := gs.backoff[topic] // 获取该主题的回退映射。
		// 这些对等节点的评分高于发布阈值，可能为负值，因此删除评分为负的对等节点。
		for p := range gmap { // 遍历 fanout 对等节点集合。
			_, doBackOff := backoff[p]              // 检查对等节点是否处于回退期。
			if gs.score.Score(p) < 0 || doBackOff { // 如果对等节点评分为负或处于回退期。
				delete(gmap, p) // 将对等节点从集合中删除。
			}
		}

		if len(gmap) < gs.params.D { // 如果 fanout 对等节点数量少于 D。
			// 我们需要更多的对等节点；急切地进行，因为这会在下一个心跳中修复。
			more := gs.getPeers(topic, gs.params.D-len(gmap), func(p peer.ID) bool { // 获取更多符合条件的对等节点。
				// 过滤掉当前对等节点、直接对等节点、我们正在回避的对等节点以及评分为负的对等节点。
				_, inMesh := gmap[p]                                              // 检查对等节点是否已在 fanout 集合中。
				_, direct := gs.direct[p]                                         // 检查对等节点是否为直接对等节点。
				_, doBackOff := backoff[p]                                        // 检查对等节点是否处于回退期。
				return !inMesh && !direct && !doBackOff && gs.score.Score(p) >= 0 // 返回是否符合条件。
			})
			for _, p := range more { // 遍历找到的对等节点。
				gmap[p] = struct{}{} // 将对等节点添加到集合中。
			}
		}
		gs.mesh[topic] = gmap     // 将 fanout 集合转换为网格集合。
		delete(gs.fanout, topic)  // 从 fanout 集合中删除该主题。
		delete(gs.lastpub, topic) // 删除最后发布时间记录。
	} else {
		backoff := gs.backoff[topic]                                    // 获取该主题的回退映射。
		peers := gs.getPeers(topic, gs.params.D, func(p peer.ID) bool { // 获取符合条件的对等节点列表。
			// 过滤掉直接对等节点、我们正在回避的对等节点和评分为负的对等节点。
			_, direct := gs.direct[p]                              // 检查对等节点是否为直接对等节点。
			_, doBackOff := backoff[p]                             // 检查对等节点是否处于回退期。
			return !direct && !doBackOff && gs.score.Score(p) >= 0 // 返回是否符合条件。
		})
		gmap = peerListToMap(peers) // 将对等节点列表转换为映射。
		gs.mesh[topic] = gmap       // 将映射添加到网格集合中。
	}

	for p := range gmap { // 遍历网格对等节点集合。
		logger.Debugf("添加网格链接到对等节点 %s 到主题 %s", p, topic) // 记录调试信息，添加对等节点到网格中。
		gs.tracer.Graft(p, topic)                        // 记录 GRAFT 操作。
		gs.sendGraft(p, topic)                           // 发送 GRAFT 消息。
	}
}

// Leave 离开主题。
// 参数:
//   - topic: string 类型，表示主题名称。
func (gs *GossipSubRouter) Leave(topic string) {
	gmap, ok := gs.mesh[topic] // 获取该主题的网格对等节点集合。
	if !ok {                   // 如果该主题不在网格中。
		return // 返回，结束函数执行。
	}

	logger.Debugf("离开主题 %s", topic) // 记录调试信息，离开主题。
	gs.tracer.Leave(topic)          // 记录离开操作。

	delete(gs.mesh, topic) // 从网格集合中删除该主题。

	for p := range gmap { // 遍历网格对等节点集合。
		logger.Debugf("从网格中移除对等节点 %s 到主题 %s", p, topic) // 记录调试信息，从网格中移除对等节点。
		gs.tracer.Prune(p, topic)                       // 记录 PRUNE 操作。
		gs.sendPrune(p, topic, true)                    // 发送 PRUNE 消息。
		// 添加回退，以防我们在回退期结束前重新加入该主题时急切地重新 GRAFT 该对等节点。
		gs.addBackoff(p, topic, true) // 添加回退时间。
	}
}

// sendGraft 发送 GRAFT 消息。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - topic: string 类型，主题名称。
func (gs *GossipSubRouter) sendGraft(p peer.ID, topic string) {
	graft := []*pb.ControlGraft{{TopicID: topic}}    // 创建一个 GRAFT 控制消息。
	out := rpcWithControl(nil, nil, nil, graft, nil) // 构造包含 GRAFT 消息的 RPC 消息。
	gs.sendRPC(p, out)                               // 发送 RPC 消息到对等节点。
}

// sendPrune 发送 PRUNE 消息。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - topic: string 类型，主题名称。
//   - isUnsubscribe: bool 类型，表示是否取消订阅。
func (gs *GossipSubRouter) sendPrune(p peer.ID, topic string, isUnsubscribe bool) {
	prune := []*pb.ControlPrune{gs.makePrune(p, topic, gs.doPX, isUnsubscribe)} // 创建一个 PRUNE 控制消息。
	out := rpcWithControl(nil, nil, nil, nil, prune)                            // 构造包含 PRUNE 消息的 RPC 消息。
	gs.sendRPC(p, out)                                                          // 发送 RPC 消息到对等节点。
}

// sendRPC 发送 RPC 消息。
// 参数:
//   - p: peer.ID 类型，对等节点 ID。
//   - out: *RPC 类型，表示 RPC 消息。
func (gs *GossipSubRouter) sendRPC(p peer.ID, out *RPC) {
	// 我们拥有 RPC 吗？
	own := false // 初始化标志，表示是否拥有 RPC。

	// 捎带控制消息重试。
	ctl, ok := gs.control[p] // 获取该对等节点的控制消息。
	if ok {                  // 如果存在控制消息。
		out = copyRPC(out)               // 复制 RPC 消息。
		own = true                       // 设置标志，表示拥有 RPC。
		gs.piggybackControl(p, out, ctl) // 捎带控制消息。
		delete(gs.control, p)            // 从控制消息映射中删除该对等节点的控制消息。
	}

	// 捎带 gossip。
	ihave, ok := gs.gossip[p] // 获取该对等节点的 gossip 消息。
	if ok {                   // 如果存在 gossip 消息。
		if !own { // 如果尚未拥有 RPC。
			out = copyRPC(out) // 复制 RPC 消息。
			own = true         // 设置标志，表示拥有 RPC。
		}
		gs.piggybackGossip(out, ihave) // 捎带 gossip 消息。
		delete(gs.gossip, p)           // 从 gossip 消息映射中删除该对等节点的 gossip 消息。
	}

	mch, ok := gs.p.peers[p] // 获取该对等节点的 RPC 消息通道。
	if !ok {                 // 如果通道不存在。
		return // 返回，结束函数执行。
	}

	// 如果我们低于最大消息大小，继续发送。
	if out.Size() < gs.p.maxMessageSize { // 如果 RPC 消息的大小小于最大消息大小。
		gs.doSendRPC(out, p, mch) // 发送 RPC 消息到对等节点。
		return                    // 返回，结束函数执行。
	}

	// 可能将 RPC 拆分为多个低于最大消息大小的 RPC。
	outRPCs := appendOrMergeRPC(nil, gs.p.maxMessageSize, *out) // 将 RPC 消息拆分为多个小于最大消息大小的消息。
	for _, rpc := range outRPCs {                               // 遍历拆分后的 RPC 消息。
		if rpc.Size() > gs.p.maxMessageSize { // 如果拆分后的 RPC 消息仍然大于最大消息大小。
			// 这仅在单个消息/控制大于 maxMessageSize 时发生。
			gs.doDropRPC(out, p, fmt.Sprintf("丢弃超大消息. 大小: %d, 限制: %d. (超过 %d 字节)", rpc.Size(), gs.p.maxMessageSize, rpc.Size()-gs.p.maxMessageSize)) // 丢弃超大消息，并记录调试信息。
			continue                                                                                                                                 // 跳过此消息。
		}
		gs.doSendRPC(rpc, p, mch) // 发送拆分后的 RPC 消息到对等节点。
	}
}

// doDropRPC 丢弃 RPC 消息。
// 参数:
//   - rpc: *RPC 类型，表示 RPC 消息。
//   - p: peer.ID 类型，对等节点 ID。
//   - reason: string 类型，表示丢弃原因。
func (gs *GossipSubRouter) doDropRPC(rpc *RPC, p peer.ID, reason string) {
	logger.Debugf("丢弃消息到对等节点 %s: %s", p, reason) // 记录调试信息，丢弃消息并说明原因。
	gs.tracer.DropRPC(rpc, p)                    // 记录丢弃 RPC 消息的操作。
	// 推送需要重试的控制消息。
	ctl := rpc.GetControl() // 获取 RPC 消息中的控制消息。
	if ctl != nil {         // 如果控制消息不为空。
		gs.pushControl(p, ctl) // 将控制消息推送到需要重试的消息列表中。
	}
}

// doSendRPC 发送 RPC 消息。
// 参数:
//   - rpc: *RPC 类型，表示 RPC 消息。
//   - p: peer.ID 类型，对等节点 ID。
//   - mch: chan *RPC 类型，表示 RPC 消息通道。
func (gs *GossipSubRouter) doSendRPC(rpc *RPC, p peer.ID, mch chan *RPC) {
	select {
	case mch <- rpc: // 将 RPC 消息发送到对等节点的消息通道中。
		gs.tracer.SendRPC(rpc, p) // 记录发送 RPC 消息的操作。
	default: // 如果消息通道已满。
		gs.doDropRPC(rpc, p, "队列已满") // 丢弃消息并说明原因。
	}
}

// appendOrMergeRPC 将给定的 RPC 附加到切片中，如果可能，合并它们。
// 如果任何元素太大而无法放入单个 RPC，它将被拆分为多个 RPC。
// 如果 RPC 太大且无法进一步拆分（例如消息数据大于 RPC 限制），则它将作为超大 RPC 返回。
// 调用者应过滤掉超大 RPC。
// 参数:
//   - slice: []*RPC 类型，表示要附加到的 RPC 切片。
//   - limit: int 类型，表示 RPC 的大小限制。
//   - elems: 可变参数 RPC，表示要合并的 RPC 元素。
//
// 返回值:
//   - []*RPC: 返回合并后的 RPC 切片。
func appendOrMergeRPC(slice []*RPC, limit int, elems ...RPC) []*RPC {
	if len(elems) == 0 { // 如果没有要合并的元素，直接返回当前的 RPC 切片。
		return slice
	}

	if len(slice) == 0 && len(elems) == 1 && elems[0].Size() < limit { // 如果当前切片为空，且只有一个元素且其大小小于限制。
		// 快速路径：无需合并且只有一个元素。
		return append(slice, &elems[0]) // 直接将该元素添加到切片中并返回。
	}

	out := slice       // 创建一个新的 RPC 切片，用于存储合并后的 RPC。
	if len(out) == 0 { // 如果当前切片为空。
		out = append(out, &RPC{RPC: pb.RPC{}}) // 添加一个空的 RPC 到切片中。
		out[0].from = elems[0].from            // 设置第一个元素的来源。
	}

	for _, elem := range elems { // 遍历所有要合并的 RPC 元素。
		lastRPC := out[len(out)-1] // 获取切片中的最后一个 RPC。

		// 合并/附加发布消息。
		for _, msg := range elem.GetPublish() { // 遍历所有发布消息。
			if lastRPC.Publish = append(lastRPC.Publish, msg); lastRPC.Size() > limit { // 尝试将消息添加到最后一个 RPC 中，并检查是否超过限制。
				lastRPC.Publish = lastRPC.Publish[:len(lastRPC.Publish)-1] // 如果超过限制，将消息从最后一个 RPC 中移除。
				lastRPC = &RPC{RPC: pb.RPC{}, from: elem.from}             // 创建一个新的 RPC。
				lastRPC.Publish = append(lastRPC.Publish, msg)             // 将消息添加到新的 RPC 中。
				out = append(out, lastRPC)                                 // 将新的 RPC 添加到切片中。
			}
		}

		// 合并/附加订阅。
		for _, sub := range elem.GetSubscriptions() { // 遍历所有订阅。
			if lastRPC.Subscriptions = append(lastRPC.Subscriptions, sub); lastRPC.Size() > limit { // 尝试将订阅添加到最后一个 RPC 中，并检查是否超过限制。
				lastRPC.Subscriptions = lastRPC.Subscriptions[:len(lastRPC.Subscriptions)-1] // 如果超过限制，将订阅从最后一个 RPC 中移除。
				lastRPC = &RPC{RPC: pb.RPC{}, from: elem.from}                               // 创建一个新的 RPC。
				lastRPC.Subscriptions = append(lastRPC.Subscriptions, sub)                   // 将订阅添加到新的 RPC 中。
				out = append(out, lastRPC)                                                   // 将新的 RPC 添加到切片中。
			}
		}

		// 合并/附加控制消息。
		if ctl := elem.GetControl(); ctl != nil { // 获取控制消息，如果存在控制消息。
			if lastRPC.Control == nil { // 如果最后一个 RPC 没有控制消息。
				lastRPC.Control = &pb.ControlMessage{} // 初始化一个新的控制消息。
				if lastRPC.Size() > limit {            // 如果控制消息导致 RPC 超过限制。
					lastRPC.Control = nil                                                       // 将控制消息置空。
					lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}, from: elem.from} // 创建一个新的 RPC，并附加控制消息。
					out = append(out, lastRPC)                                                  // 将新的 RPC 添加到切片中。
				}
			}

			for _, graft := range ctl.GetGraft() { // 遍历所有 GRAFT 消息。
				if lastRPC.Control.Graft = append(lastRPC.Control.Graft, graft); lastRPC.Size() > limit { // 尝试将 GRAFT 消息添加到最后一个 RPC 中，并检查是否超过限制。
					lastRPC.Control.Graft = lastRPC.Control.Graft[:len(lastRPC.Control.Graft)-1] // 如果超过限制，将 GRAFT 消息从最后一个 RPC 中移除。
					lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}, from: elem.from}  // 创建一个新的 RPC，并附加控制消息。
					lastRPC.Control.Graft = append(lastRPC.Control.Graft, graft)                 // 将 GRAFT 消息添加到新的 RPC 中。
					out = append(out, lastRPC)                                                   // 将新的 RPC 添加到切片中。
				}
			}

			for _, prune := range ctl.GetPrune() { // 遍历所有 PRUNE 消息。
				if lastRPC.Control.Prune = append(lastRPC.Control.Prune, prune); lastRPC.Size() > limit { // 尝试将 PRUNE 消息添加到最后一个 RPC 中，并检查是否超过限制。
					lastRPC.Control.Prune = lastRPC.Control.Prune[:len(lastRPC.Control.Prune)-1] // 如果超过限制，将 PRUNE 消息从最后一个 RPC 中移除。
					lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}, from: elem.from}  // 创建一个新的 RPC，并附加控制消息。
					lastRPC.Control.Prune = append(lastRPC.Control.Prune, prune)                 // 将 PRUNE 消息添加到新的 RPC 中。
					out = append(out, lastRPC)                                                   // 将新的 RPC 添加到切片中。
				}
			}

			for _, iwant := range ctl.GetIwant() { // 遍历所有 IWANT 消息。
				if len(lastRPC.Control.Iwant) == 0 { // 如果没有 IWANT 消息。
					// 初始化一个 IWANT。
					// 对于 IWANT，我们不需要超过一个，因为这里没有主题 ID。
					newIWant := &pb.ControlIWant{}                                                               // 创建一个新的 IWANT 控制消息。
					if lastRPC.Control.Iwant = append(lastRPC.Control.Iwant, newIWant); lastRPC.Size() > limit { // 尝试将 IWANT 消息添加到最后一个 RPC 中，并检查是否超过限制。
						lastRPC.Control.Iwant = lastRPC.Control.Iwant[:len(lastRPC.Control.Iwant)-1] // 如果超过限制，将 IWANT 消息从最后一个 RPC 中移除。
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Iwant: []*pb.ControlIWant{newIWant},
						}}, from: elem.from} // 创建一个新的 RPC，并附加控制消息。
						out = append(out, lastRPC) // 将新的 RPC 添加到切片中。
					}
				}
				for _, msgID := range iwant.GetMessageIDs() { // 遍历所有消息 ID。
					if lastRPC.Control.Iwant[0].MessageIDs = append(lastRPC.Control.Iwant[0].MessageIDs, msgID); lastRPC.Size() > limit { // 尝试将消息 ID 添加到 IWANT 控制消息中，并检查是否超过限制。
						lastRPC.Control.Iwant[0].MessageIDs = lastRPC.Control.Iwant[0].MessageIDs[:len(lastRPC.Control.Iwant[0].MessageIDs)-1] // 如果超过限制，将消息 ID 从 IWANT 控制消息中移除。
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Iwant: []*pb.ControlIWant{{MessageIDs: []string{msgID}}},
						}}, from: elem.from} // 创建一个新的 RPC，并附加控制消息。
						out = append(out, lastRPC) // 将新的 RPC 添加到切片中。
					}
				}
			}

			for _, ihave := range ctl.GetIhave() { // 遍历所有 IHAVE 消息。
				if len(lastRPC.Control.Ihave) == 0 ||
					lastRPC.Control.Ihave[len(lastRPC.Control.Ihave)-1].TopicID != ihave.TopicID { // 如果引用了新的主题 ID。
					// 如果我们引用一个新的主题 ID，则开始新的 IHAVE。
					newIhave := &pb.ControlIHave{TopicID: ihave.TopicID}                                         // 创建一个新的 IHAVE 控制消息。
					if lastRPC.Control.Ihave = append(lastRPC.Control.Ihave, newIhave); lastRPC.Size() > limit { // 尝试将 IHAVE 消息添加到最后一个 RPC 中，并检查是否超过限制。
						lastRPC.Control.Ihave = lastRPC.Control.Ihave[:len(lastRPC.Control.Ihave)-1] // 如果超过限制，将 IHAVE 消息从最后一个 RPC 中移除。
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Ihave: []*pb.ControlIHave{newIhave},
						}}, from: elem.from} // 创建一个新的 RPC，并附加控制消息。
						out = append(out, lastRPC) // 将新的 RPC 添加到切片中。
					}
				}
				for _, msgID := range ihave.GetMessageIDs() { // 遍历所有消息 ID。
					lastIHave := lastRPC.Control.Ihave[len(lastRPC.Control.Ihave)-1]                        // 获取最后一个 IHAVE 控制消息。
					if lastIHave.MessageIDs = append(lastIHave.MessageIDs, msgID); lastRPC.Size() > limit { // 尝试将消息 ID 添加到 IHAVE 控制消息中，并检查是否超过限制。
						lastIHave.MessageIDs = lastIHave.MessageIDs[:len(lastIHave.MessageIDs)-1] // 如果超过限制，将消息 ID 从 IHAVE 控制消息中移除。
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Ihave: []*pb.ControlIHave{{TopicID: ihave.TopicID, MessageIDs: []string{msgID}}},
						}}, from: elem.from} // 创建一个新的 RPC，并附加控制消息。
						out = append(out, lastRPC) // 将新的 RPC 添加到切片中。
					}
				}
			}
		}
	}

	return out // 返回合并后的 RPC 切片。
}

// heartbeatTimer 启动心跳计时器。
func (gs *GossipSubRouter) heartbeatTimer() {
	time.Sleep(gs.params.HeartbeatInitialDelay) // 延迟心跳开始时间。
	select {
	case gs.p.eval <- gs.heartbeat: // 将心跳操作发送到评估通道。
	case <-gs.p.ctx.Done(): // 检查上下文是否已取消。
		return // 如果上下文已取消，返回结束函数。
	}

	ticker := time.NewTicker(gs.params.HeartbeatInterval) // 创建一个定时器，用于每隔指定的心跳间隔触发一次。
	defer ticker.Stop()                                   // 在函数返回时停止定时器。

	for {
		select {
		case <-ticker.C: // 每当定时器触发。
			select {
			case gs.p.eval <- gs.heartbeat: // 将心跳操作发送到评估通道。
			case <-gs.p.ctx.Done(): // 检查上下文是否已取消。
				return // 如果上下文已取消，返回结束函数。
			}
		case <-gs.p.ctx.Done(): // 检查上下文是否已取消。
			return // 如果上下文已取消，返回结束函数。
		}
	}
}

// heartbeat 执行心跳逻辑。
func (gs *GossipSubRouter) heartbeat() {
	start := time.Now() // 记录心跳开始时间。
	defer func() {
		if gs.params.SlowHeartbeatWarning > 0 { // 检查是否设置了慢心跳警告时间。
			slowWarning := time.Duration(gs.params.SlowHeartbeatWarning * float64(gs.params.HeartbeatInterval)) // 计算慢心跳警告时间。
			if dt := time.Since(start); dt > slowWarning {                                                      // 检查心跳执行时间是否超过警告阈值。
				logger.Warnf("心跳过慢: %v", dt) // 记录心跳过慢的警告。
			}
		}
	}()

	gs.heartbeatTicks++ // 增加心跳计数。

	tograft := make(map[peer.ID][]string) // 创建一个映射，用于存储需要 GRAFT 的对等节点和主题。
	toprune := make(map[peer.ID][]string) // 创建一个映射，用于存储需要 PRUNE 的对等节点和主题。
	noPX := make(map[peer.ID]bool)        // 创建一个映射，用于标记不进行 PX 的对等节点。

	// 清理过期的回退。
	gs.clearBackoff()

	// 清理 iasked 计数器。
	gs.clearIHaveCounters()

	// 应用 IWANT 请求惩罚。
	gs.applyIwantPenalties()

	// 确保直接对等节点已连接。
	gs.directConnect()

	// 在整个心跳期间缓存评分。
	scores := make(map[peer.ID]float64) // 创建一个映射，用于缓存对等节点的评分。
	score := func(p peer.ID) float64 {
		s, ok := scores[p]
		if !ok {
			s = gs.score.Score(p) // 获取对等节点的评分。
			scores[p] = s         // 将评分缓存到映射中。
		}
		return s // 返回评分。
	}

	// 维护我们加入的主题的网格。
	for topic, peers := range gs.mesh {
		prunePeer := func(p peer.ID) {
			gs.tracer.Prune(p, topic)          // 记录 PRUNE 操作。
			delete(peers, p)                   // 从网格中移除该对等节点。
			gs.addBackoff(p, topic, false)     // 为该对等节点添加回退时间。
			topics := toprune[p]               // 获取该对等节点的 PRUNE 主题列表。
			toprune[p] = append(topics, topic) // 将主题添加到 PRUNE 列表中。
		}

		graftPeer := func(p peer.ID) {
			logger.Debugf("添加网格链接到对等节点 %s 到主题 %s", p, topic) // 记录调试信息，添加该对等节点到网格中。
			gs.tracer.Graft(p, topic)                        // 记录 GRAFT 操作。
			peers[p] = struct{}{}                            // 将该对等节点添加到网格中。
			topics := tograft[p]                             // 获取该对等节点的 GRAFT 主题列表。
			tograft[p] = append(topics, topic)               // 将主题添加到 GRAFT 列表中。
		}

		// 删除所有评分为负的对等节点，不进行 PX。
		for p := range peers {
			if score(p) < 0 { // 如果对等节点评分为负。
				logger.Debugf("PRUNE 对等节点 %s 评分低于 0 [评分 = %f, 主题 = %s]", p, score(p), topic) // 记录调试信息，PRUNE 该对等节点。
				prunePeer(p)                                                                 // 执行 PRUNE 操作。
				noPX[p] = true                                                               // 标记该对等节点不进行 PX。
			}
		}

		// 我们有足够的对等节点吗？
		if l := len(peers); l < gs.params.Dlo { // 如果网格中的对等节点少于下限。
			backoff := gs.backoff[topic] // 获取该主题的回退映射。
			ineed := gs.params.D - l     // 计算需要添加的对等节点数量。
			plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
				// 过滤掉当前对等节点和直接对等节点、我们正在回避的对等节点以及评分为负的对等节点。
				_, inMesh := peers[p]
				_, doBackoff := backoff[p]
				_, direct := gs.direct[p]
				return !inMesh && !doBackoff && !direct && score(p) >= 0
			})

			for _, p := range plst {
				graftPeer(p) // 执行 GRAFT 操作。
			}
		}

		// 我们有太多的对等节点吗？
		if len(peers) > gs.params.Dhi { // 如果网格中的对等节点多于上限。
			plst := peerMapToList(peers) // 将对等节点映射转换为列表。

			// 按评分排序（但首先为我们不使用评分的情况打乱）。
			shufflePeers(plst) // 打乱对等节点列表的顺序。
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) > score(plst[j]) // 按评分从高到低排序。
			})

			// 我们保留前 D_score 名评分最高的对等节点，剩下的随机选择，最多保留 D 名。
			// 在保持 D_out 对等节点在网格中的前提下（如果我们有那么多的话）。
			shufflePeers(plst[gs.params.Dscore:]) // 打乱剩余的对等节点列表。

			// 计算我们保留的出站对等节点。
			outbound := 0
			for _, p := range plst[:gs.params.D] { // 遍历前 D 个对等节点。
				if gs.outbound[p] { // 如果该对等节点为出站连接。
					outbound++
				}
			}

			// 如果少于 D_out，从随机选择中冒出一些出站对等节点。
			if outbound < gs.params.Dout {
				rotate := func(i int) {
					// 向右旋转 plst 并将第 i 个对等节点放在前面。
					p := plst[i]
					for j := i; j > 0; j-- {
						plst[j] = plst[j-1]
					}
					plst[0] = p
				}

				// 首先将选择中的所有出站对等节点冒到前面。
				if outbound > 0 {
					ihave := outbound
					for i := 1; i < gs.params.D && ihave > 0; i++ {
						p := plst[i]
						if gs.outbound[p] {
							rotate(i) // 将出站对等节点旋转到前面。
							ihave--
						}
					}
				}

				// 现在将选择之外的足够出站对等节点冒到前面。
				ineed := gs.params.Dout - outbound
				for i := gs.params.D; i < len(plst) && ineed > 0; i++ {
					p := plst[i]
					if gs.outbound[p] {
						rotate(i) // 将出站对等节点旋转到前面。
						ineed--
					}
				}
			}

			// 修剪多余的对等节点。
			for _, p := range plst[gs.params.D:] { // 遍历多余的对等节点。
				logger.Debugf("从网格中移除对等节点 %s 到主题 %s", p, topic) // 记录调试信息，PRUNE 该对等节点。
				prunePeer(p)                                    // 执行 PRUNE 操作。
			}
		}

		// 我们有足够的出站对等节点吗？
		if len(peers) >= gs.params.Dlo {
			// 计算我们有多少出站对等节点。
			outbound := 0
			for p := range peers {
				if gs.outbound[p] {
					outbound++
				}
			}

			// 如果少于 D_out，选择一些具有出站连接的对等节点并进行 graft。
			if outbound < gs.params.Dout {
				ineed := gs.params.Dout - outbound
				backoff := gs.backoff[topic]
				plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
					// 过滤掉当前对等节点和直接对等节点、我们正在回避的对等节点以及评分为负的对等节点。
					_, inMesh := peers[p]
					_, doBackoff := backoff[p]
					_, direct := gs.direct[p]
					return !inMesh && !doBackoff && !direct && gs.outbound[p] && score(p) >= 0
				})

				for _, p := range plst {
					graftPeer(p) // 执行 GRAFT 操作。
				}
			}
		}

		// 我们是否应该尝试通过机会性 grafting 改善网格？
		if gs.heartbeatTicks%gs.params.OpportunisticGraftTicks == 0 && len(peers) > 1 {
			// 机会性 grafting 的工作原理如下：我们检查网格中对等节点的中位评分；
			// 如果该评分低于 opportunisticGraftThreshold，我们随机选择一些评分高于中位数的对等节点。
			// 目的是通过引入可能一直在向我们 gossip 的高评分对等节点来（慢慢地）改进表现不佳的网格。
			// 这使我们能够摆脱与差劲的对等节点困在一起的粘性情况，并且还可以从优秀对等节点的 churn 中恢复过来。

			// 现在计算网格中对等节点的中位评分。
			plst := peerMapToList(peers)
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) < score(plst[j])
			})
			medianIndex := len(peers) / 2
			medianScore := scores[plst[medianIndex]]

			// 如果中位评分低于阈值，选择一个更好的对等节点（如果有）并进行 GRAFT。
			if medianScore < gs.opportunisticGraftThreshold {
				backoff := gs.backoff[topic]
				plst = gs.getPeers(topic, gs.params.OpportunisticGraftPeers, func(p peer.ID) bool {
					_, inMesh := peers[p]
					_, doBackoff := backoff[p]
					_, direct := gs.direct[p]
					return !inMesh && !doBackoff && !direct && score(p) > medianScore
				})

				for _, p := range plst {
					logger.Debugf("机会性 GRAFT 对等节点 %s 到主题 %s", p, topic) // 记录调试信息，GRAFT 该对等节点。
					graftPeer(p)                                        // 执行 GRAFT 操作。
				}
			}
		}

		// 第二个参数是排除在 gossip 之外的网格对等节点。我们已经向他们推送消息，所以发出 IHAVEs 是多余的。
		gs.emitGossip(topic, peers) // 发送 gossip 消息。
	}

	// 过期没有发布一段时间的主题的 fanout。
	now := time.Now().UnixNano()
	for topic, lastpub := range gs.lastpub {
		if lastpub+int64(gs.params.FanoutTTL) < now {
			delete(gs.fanout, topic)
			delete(gs.lastpub, topic)
		}
	}

	// 维护我们正在发布但未加入的主题的 fanout。
	for topic, peers := range gs.fanout {
		// 检查我们的对等节点是否仍在主题中并且评分高于发布阈值。
		for p := range peers {
			_, ok := gs.p.topics[topic][p]
			if !ok || score(p) < gs.publishThreshold {
				delete(peers, p)
			}
		}

		// 我们需要更多的对等节点吗？
		if len(peers) < gs.params.D {
			ineed := gs.params.D - len(peers)
			plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
				// 过滤掉当前对等节点和直接对等节点，以及评分高于发布阈值的对等节点。
				_, inFanout := peers[p]
				_, direct := gs.direct[p]
				return !inFanout && !direct && score(p) >= gs.publishThreshold
			})

			for _, p := range plst {
				peers[p] = struct{}{}
			}
		}

		// 第二个参数是排除在 gossip 之外的 fanout 对等节点。我们已经向他们推送消息，所以发出 IHAVEs 是多余的。
		gs.emitGossip(topic, peers) // 发送 gossip 消息。
	}

	// 发送合并的 GRAFT/PRUNE 消息（将捎带 gossip）。
	gs.sendGraftPrune(tograft, toprune, noPX)

	// 刷新所有未捎带的挂起 gossip。
	gs.flush()

	// 推进消息历史窗口。
	gs.mcache.Shift()
}

// clearIHaveCounters 清理 IHAVE 计数器。
func (gs *GossipSubRouter) clearIHaveCounters() {
	if len(gs.peerhave) > 0 { // 如果 peerhave 映射不为空。
		// 丢弃旧映射并创建新映射。
		gs.peerhave = make(map[peer.ID]int) // 清空并重新初始化 peerhave 映射。
	}

	if len(gs.iasked) > 0 { // 如果 iasked 映射不为空。
		// 丢弃旧映射并创建新映射。
		gs.iasked = make(map[peer.ID]int) // 清空并重新初始化 iasked 映射。
	}
}

// applyIwantPenalties 应用 IWANT 请求惩罚。
func (gs *GossipSubRouter) applyIwantPenalties() {
	for p, count := range gs.gossipTracer.GetBrokenPromises() { // 遍历所有未遵守 IWANT 请求的对等节点。
		logger.Infof("对等节点 %s 未遵守 %d 次 IWANT 请求; 添加惩罚", p, count) // 记录信息，说明哪个对等节点未遵守 IWANT 请求以及对应的次数。
		gs.score.AddPenalty(p, count)                             // 根据未遵守的次数为该对等节点添加惩罚分。
	}
}

// clearBackoff 清理回退。
func (gs *GossipSubRouter) clearBackoff() {
	// 我们每 15 个心跳才清理一次，以避免过多地迭代映射。
	if gs.heartbeatTicks%15 != 0 { // 如果当前心跳次数不是 15 的倍数，则跳过清理。
		return
	}

	now := time.Now()                        // 获取当前时间。
	for topic, backoff := range gs.backoff { // 遍历所有主题的回退映射。
		for p, expire := range backoff { // 遍历每个主题下所有对等节点的回退时间。
			// 添加一些缓冲时间。
			if expire.Add(2 * GossipSubHeartbeatInterval).Before(now) { // 如果回退时间加上缓冲时间早于当前时间。
				delete(backoff, p) // 从回退映射中删除该对等节点。
			}
		}
		if len(backoff) == 0 { // 如果该主题下的回退映射为空。
			delete(gs.backoff, topic) // 删除该主题的回退映射。
		}
	}
}

// directConnect 直接连接。
func (gs *GossipSubRouter) directConnect() {
	// 我们每几个心跳才执行一次此操作，以允许挂起的连接完成并考虑重启/停机时间。
	if gs.heartbeatTicks%gs.params.DirectConnectTicks != 0 { // 如果当前心跳次数不是 DirectConnectTicks 的倍数，则跳过连接操作。
		return
	}

	var toconnect []peer.ID    // 初始化一个对等节点 ID 列表，用于存储需要连接的对等节点。
	for p := range gs.direct { // 遍历所有直接对等节点。
		_, connected := gs.peers[p] // 检查该对等节点是否已连接。
		if !connected {             // 如果未连接。
			toconnect = append(toconnect, p) // 将该对等节点添加到待连接列表中。
		}
	}

	if len(toconnect) > 0 { // 如果有待连接的对等节点。
		go func() { // 启动一个新的 goroutine 进行连接操作。
			for _, p := range toconnect { // 遍历所有待连接的对等节点。
				gs.connect <- connectInfo{p: p} // 将连接信息发送到连接通道中，执行连接操作。
			}
		}()
	}
}

// sendGraftPrune 发送 GRAFT 和 PRUNE 消息。
// 参数:
//   - tograft: map[peer.ID][]string 类型，表示需要 GRAFT 的对等节点和主题。
//   - toprune: map[peer.ID][]string 类型，表示需要 PRUNE 的对等节点和主题。
//   - noPX: map[peer.ID]bool 类型，表示不进行 PX 的对等节点。
func (gs *GossipSubRouter) sendGraftPrune(tograft, toprune map[peer.ID][]string, noPX map[peer.ID]bool) {
	for p, topics := range tograft { // 遍历所有需要 GRAFT 的对等节点及其对应的主题。
		graft := make([]*pb.ControlGraft, 0, len(topics)) // 初始化一个 GRAFT 消息列表。
		for _, topic := range topics {                    // 遍历每个对等节点的所有主题。
			// 复制主题字符串，因为
			// 这里的字符串引用在每次切片迭代时都会变化。
			copiedID := topic                                          // 复制主题字符串。
			graft = append(graft, &pb.ControlGraft{TopicID: copiedID}) // 为该主题创建 GRAFT 消息并添加到列表中。
		}

		var prune []*pb.ControlPrune // 初始化一个 PRUNE 消息列表。
		pruning, ok := toprune[p]    // 获取该对等节点的 PRUNE 主题列表。
		if ok {                      // 如果该对等节点需要 PRUNE。
			delete(toprune, p)                                // 从 PRUNE 列表中删除该对等节点。
			prune = make([]*pb.ControlPrune, 0, len(pruning)) // 为该对等节点初始化 PRUNE 消息列表。
			for _, topic := range pruning {                   // 遍历该对等节点的所有 PRUNE 主题。
				prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p], false)) // 创建 PRUNE 消息并添加到列表中。
			}
		}

		out := rpcWithControl(nil, nil, nil, graft, prune) // 创建包含 GRAFT 和 PRUNE 消息的 RPC。
		gs.sendRPC(p, out)                                 // 发送 RPC 消息到对等节点。
	}

	for p, topics := range toprune { // 遍历所有需要 PRUNE 的对等节点及其对应的主题。
		prune := make([]*pb.ControlPrune, 0, len(topics)) // 初始化一个 PRUNE 消息列表。
		for _, topic := range topics {                    // 遍历每个对等节点的所有主题。
			prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p], false)) // 为该主题创建 PRUNE 消息并添加到列表中。
		}

		out := rpcWithControl(nil, nil, nil, nil, prune) // 创建包含 PRUNE 消息的 RPC。
		gs.sendRPC(p, out)                               // 发送 RPC 消息到对等节点。
	}
}

// emitGossip 发出 IHAVE gossip，通告该主题的消息缓存窗口中的项目。
// 参数:
//   - topic: string 类型，表示主题名称。
//   - exclude: map[peer.ID]struct{} 类型，表示排除的对等节点。
func (gs *GossipSubRouter) emitGossip(topic string, exclude map[peer.ID]struct{}) {
	mids := gs.mcache.GetGossipIDs(topic) // 获取该主题的消息 ID 列表。
	if len(mids) == 0 {                   // 如果没有消息 ID。
		return // 直接返回，不进行 gossip。
	}

	// 打乱顺序以随机发出。
	shuffleStrings(mids) // 打乱消息 ID 的顺序。

	// 如果我们发出的 mids 超过 GossipSubMaxIHaveLength，则截断列表。
	if len(mids) > gs.params.MaxIHaveLength { // 如果消息 ID 的数量超过最大限制。
		// 我们在每个对等节点上进行截断。
		logger.Debugf("gossip 消息太多; 将截断 IHAVE 列表 (%d 条消息)", len(mids)) // 记录调试信息，说明将对消息 ID 列表进行截断。
	}

	// 将 gossip 发送给高于阈值的 GossipFactor 对等节点，最少为 D_lazy。
	// 首先，我们收集高于 gossipThreshold 且不在排除集中的对等节点，
	// 然后从该集合中随机选择。
	// 我们还排除直接对等节点，因为没有理由向他们发出 gossip。
	peers := make([]peer.ID, 0, len(gs.p.topics[topic])) // 初始化一个对等节点列表。
	for p := range gs.p.topics[topic] {                  // 遍历该主题下的所有对等节点。
		_, inExclude := exclude[p] // 检查该对等节点是否在排除列表中。
		_, direct := gs.direct[p]  // 检查该对等节点是否为直接对等节点。
		if !inExclude && !direct && gs.feature(GossipSubFeatureMesh, gs.peers[p]) && gs.score.Score(p) >= gs.gossipThreshold {
			peers = append(peers, p) // 如果该对等节点满足条件，则将其添加到列表中。
		}
	}

	target := gs.params.Dlazy                                   // 获取 D_lazy 参数的值。
	factor := int(gs.params.GossipFactor * float64(len(peers))) // 计算需要发送 gossip 的对等节点数量。
	if factor > target {                                        // 如果计算出的数量大于 D_lazy。
		target = factor // 使用计算出的数量。
	}

	if target > len(peers) { // 如果目标数量大于可用的对等节点数量。
		target = len(peers) // 使用可用的对等节点数量。
	} else {
		shufflePeers(peers) // 否则，打乱对等节点的顺序。
	}
	peers = peers[:target] // 选择目标数量的对等节点。

	// 向选定的对等节点发出 IHAVE gossip。
	for _, p := range peers { // 遍历所有选定的对等节点。
		peerMids := mids                          // 获取消息 ID 列表。
		if len(mids) > gs.params.MaxIHaveLength { // 如果消息 ID 的数量超过最大限制。
			// 我们为每个对等节点进行此操作，以便为每个对等节点发出不同的集合。
			// 我们系统中有足够的冗余，当我们进行截断时，这将显著增加消息覆盖率。
			peerMids = make([]string, gs.params.MaxIHaveLength) // 为该对等节点创建一个截断后的消息 ID 列表。
			shuffleStrings(mids)                                // 再次打乱消息 ID 的顺序。
			copy(peerMids, mids)                                // 复制消息 ID 到截断后的列表中。
		}
		gs.enqueueGossip(p, &pb.ControlIHave{TopicID: topic, MessageIDs: peerMids}) // 将 IHAVE 消息排入 gossip 队列。
	}
}

// flush 刷新所有未捎带的挂起 gossip。
func (gs *GossipSubRouter) flush() {
	// 首先发送 gossip，这也将捎带挂起的控制消息。
	for p, ihave := range gs.gossip { // 遍历所有对等节点及其对应的 gossip 消息。
		delete(gs.gossip, p)                             // 从 gossip 映射中删除该对等节点。
		out := rpcWithControl(nil, ihave, nil, nil, nil) // 创建一个包含 IHAVE 控制消息的 RPC。
		gs.sendRPC(p, out)                               // 发送 RPC 消息到对应的对等节点。
	}

	// 发送未与 gossip 合并的其余控制消息。
	for p, ctl := range gs.control { // 遍历所有对等节点及其对应的控制消息。
		delete(gs.control, p)                                      // 从 control 映射中删除该对等节点。
		out := rpcWithControl(nil, nil, nil, ctl.Graft, ctl.Prune) // 创建一个包含 GRAFT 和 PRUNE 控制消息的 RPC。
		gs.sendRPC(p, out)                                         // 发送 RPC 消息到对应的对等节点。
	}
}

// enqueueGossip 将 IHAVE 控制消息排入 gossip 队列。
// 参数:
//   - p: peer.ID 类型，表示对等节点 ID。
//   - ihave: *pb.ControlIHave 类型，表示 IHAVE 控制消息。
func (gs *GossipSubRouter) enqueueGossip(p peer.ID, ihave *pb.ControlIHave) {
	gossip := gs.gossip[p]         // 获取该对等节点对应的 gossip 消息列表。
	gossip = append(gossip, ihave) // 将新的 IHAVE 消息追加到列表中。
	gs.gossip[p] = gossip          // 将更新后的列表存回 gossip 映射中。
}

// piggybackGossip 捎带 gossip 消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点 ID。
//   - out: *RPC 类型，表示 RPC 消息。
//   - ihave: []*pb.ControlIHave 类型，表示 IHAVE 控制消息列表。
func (gs *GossipSubRouter) piggybackGossip(out *RPC, ihave []*pb.ControlIHave) {
	ctl := out.GetControl() // 获取 RPC 消息中的控制消息。
	if ctl == nil {         // 如果控制消息不存在。
		ctl = &pb.ControlMessage{} // 创建一个新的控制消息。
		out.Control = ctl          // 将新的控制消息存入 RPC 中。
	}

	ctl.Ihave = ihave // 将 IHAVE 消息列表添加到控制消息中。
}

// pushControl 推送控制消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点 ID。
//   - ctl: *pb.ControlMessage 类型，表示控制消息。
func (gs *GossipSubRouter) pushControl(p peer.ID, ctl *pb.ControlMessage) {
	// 从控制消息中删除 IHAVE 和 IWANT，gossip 不重试这些消息。
	ctl.Ihave = nil                           // 删除 IHAVE 消息。
	ctl.Iwant = nil                           // 删除 IWANT 消息。
	if ctl.Graft != nil || ctl.Prune != nil { // 如果控制消息中包含 GRAFT 或 PRUNE 消息。
		gs.control[p] = ctl // 将控制消息存入 control 映射中，以便稍后发送。
	}
}

// piggybackControl 捎带控制消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点 ID。
//   - out: *RPC 类型，表示 RPC 消息。
//   - ctl: *pb.ControlMessage 类型，表示控制消息。
func (gs *GossipSubRouter) piggybackControl(p peer.ID, out *RPC, ctl *pb.ControlMessage) {
	// 首先检查控制消息是否过时。
	var tograft []*pb.ControlGraft // 初始化一个 GRAFT 消息列表。
	var toprune []*pb.ControlPrune // 初始化一个 PRUNE 消息列表。

	for _, graft := range ctl.GetGraft() { // 遍历所有 GRAFT 消息。
		topic := graft.GetTopicID() // 获取主题 ID。
		peers, ok := gs.mesh[topic] // 获取该主题的网格对等节点。
		if !ok {                    // 如果该主题不在网格中。
			continue // 跳过该 GRAFT 消息。
		}
		_, ok = peers[p] // 检查该对等节点是否在网格中。
		if ok {          // 如果在网格中。
			tograft = append(tograft, graft) // 将 GRAFT 消息添加到列表中。
		}
	}

	for _, prune := range ctl.GetPrune() { // 遍历所有 PRUNE 消息。
		topic := prune.GetTopicID() // 获取主题 ID。
		peers, ok := gs.mesh[topic] // 获取该主题的网格对等节点。
		if !ok {                    // 如果该主题不在网格中。
			toprune = append(toprune, prune) // 将 PRUNE 消息添加到列表中。
			continue                         // 跳过检查。
		}
		_, ok = peers[p] // 检查该对等节点是否在网格中。
		if !ok {         // 如果不在网格中。
			toprune = append(toprune, prune) // 将 PRUNE 消息添加到列表中。
		}
	}

	if len(tograft) == 0 && len(toprune) == 0 { // 如果没有需要捎带的消息。
		return // 直接返回。
	}

	xctl := out.Control // 获取 RPC 中的控制消息。
	if xctl == nil {    // 如果控制消息不存在。
		xctl = &pb.ControlMessage{} // 创建一个新的控制消息。
		out.Control = xctl          // 将新的控制消息存入 RPC 中。
	}

	if len(tograft) > 0 { // 如果有 GRAFT 消息需要捎带。
		xctl.Graft = append(xctl.Graft, tograft...) // 将 GRAFT 消息追加到控制消息中。
	}
	if len(toprune) > 0 { // 如果有 PRUNE 消息需要捎带。
		xctl.Prune = append(xctl.Prune, toprune...) // 将 PRUNE 消息追加到控制消息中。
	}
}

// makePrune 创建 PRUNE 控制消息。
// 参数:
//   - p: peer.ID 类型，表示对等节点 ID。
//   - topic: string 类型，表示主题名称。
//   - doPX: bool 类型，表示是否进行 PX。
//   - isUnsubscribe: bool 类型，表示是否取消订阅。
//
// 返回值:
//   - *pb.ControlPrune: PRUNE 控制消息。
func (gs *GossipSubRouter) makePrune(p peer.ID, topic string, doPX bool, isUnsubscribe bool) *pb.ControlPrune {
	if !gs.feature(GossipSubFeaturePX, gs.peers[p]) { // 如果对等节点不支持 PX 功能。
		// GossipSub v1.0 -- 无对等交换，无法解析的对等节点。
		return &pb.ControlPrune{TopicID: topic} // 仅返回一个包含主题 ID 的 PRUNE 消息。
	}

	backoff := uint64(gs.params.PruneBackoff / time.Second) // 获取 PRUNE 回退时间（秒）。
	if isUnsubscribe {                                      // 如果这是取消订阅操作。
		backoff = uint64(gs.params.UnsubscribeBackoff / time.Second) // 使用取消订阅的回退时间。
	}

	var px []*pb.PeerInfo // 初始化 PX 对等节点列表。
	if doPX {             // 如果需要进行 PX。
		// 选择对等交换的对等节点。
		peers := gs.getPeers(topic, gs.params.PrunePeers, func(xp peer.ID) bool { // 选择评分大于或等于 0 的对等节点。
			return p != xp && gs.score.Score(xp) >= 0
		})

		cab, ok := peerstore.GetCertifiedAddrBook(gs.cab) // 获取经过认证的地址簿。
		px = make([]*pb.PeerInfo, 0, len(peers))          // 初始化 PX 对等节点信息列表。
		for _, p := range peers {                         // 遍历所有选择的对等节点。
			// 查看我们是否有签名的对等记录，如果没有，只发送对等 ID
			// 并让被修剪的对等节点在 DHT 中找到它们——我们无法通过 px 信任未签名的地址记录。
			var recordBytes []byte // 初始化对等节点记录的字节数组。
			if ok {                // 如果地址簿存在。
				spr := cab.GetPeerRecord(p) // 获取该对等节点的签名记录。
				var err error
				if spr != nil { // 如果签名记录存在。
					recordBytes, err = spr.Marshal() // 将签名记录序列化为字节数组。
					if err != nil {                  // 如果序列化失败。
						logger.Warnf("序列化签名对等节点记录失败: %s", err) // 记录警告信息。
					}
				}
			}
			px = append(px, &pb.PeerInfo{PeerID: []byte(p), SignedPeerRecord: recordBytes}) // 将对等节点信息添加到 PX 列表中。
		}
	}

	return &pb.ControlPrune{TopicID: topic, Peers: px, Backoff: backoff} // 创建并返回 PRUNE 消息。
}

// getPeers 获取对等节点。
// 参数:
//   - topic: string 类型，表示主题名称。
//   - count: int 类型，表示需要获取的对等节点数量。
//   - filter: func(peer.ID) bool 类型，表示过滤函数。
//
// 返回值:
//   - []peer.ID: 对等节点 ID 列表。
func (gs *GossipSubRouter) getPeers(topic string, count int, filter func(peer.ID) bool) []peer.ID {
	tmap, ok := gs.p.topics[topic] // 获取该主题下的所有对等节点。
	if !ok {                       // 如果该主题没有对等节点。
		return nil // 返回空列表。
	}

	peers := make([]peer.ID, 0, len(tmap)) // 初始化对等节点列表。
	for p := range tmap {                  // 遍历所有对等节点。
		if gs.feature(GossipSubFeatureMesh, gs.peers[p]) && filter(p) && gs.p.peerFilter(p, topic) {
			peers = append(peers, p) // 如果对等节点满足条件，则将其添加到列表中。
		}
	}

	shufflePeers(peers) // 打乱对等节点列表的顺序。

	if count > 0 && len(peers) > count { // 如果需要获取的对等节点数量少于可用对等节点数量。
		peers = peers[:count] // 截取所需数量的对等节点。
	}

	return peers // 返回对等节点列表。
}

// WithDefaultTagTracer 返回 GossipSubRouter 的标签跟踪器作为 PubSub 选项。
// 这对于 GossipSubRouter 在外部实例化并作为依赖项注入 GossipSub 构造函数的情况很有用。
// 这允许标签跟踪器也作为 PubSub 选项依赖项注入 GossipSub 构造函数。
func (gs *GossipSubRouter) WithDefaultTagTracer() Option {
	return WithRawTracer(gs.tagTracer) // 返回一个包含标签跟踪器的 PubSub 选项。
}

// peerListToMap 将对等节点列表转换为映射。
// 参数:
//   - peers: []peer.ID 类型，表示对等节点 ID 列表。
//
// 返回值:
//   - map[peer.ID]struct{}: 对等节点映射。
func peerListToMap(peers []peer.ID) map[peer.ID]struct{} {
	pmap := make(map[peer.ID]struct{}) // 初始化对等节点映射。
	for _, p := range peers {          // 遍历对等节点列表。
		pmap[p] = struct{}{} // 将每个对等节点添加到映射中。
	}
	return pmap // 返回对等节点映射。
}

// peerMapToList 将对等节点映射转换为列表。
// 参数:
//   - peers: map[peer.ID]struct{} 类型，表示对等节点映射。
//
// 返回值:
//   - []peer.ID: 对等节点 ID 列表。
func peerMapToList(peers map[peer.ID]struct{}) []peer.ID {
	plst := make([]peer.ID, 0, len(peers)) // 初始化对等节点列表。
	for p := range peers {                 // 遍历对等节点映射。
		plst = append(plst, p) // 将每个对等节点添加到列表中。
	}
	return plst // 返回对等节点列表。
}

// shufflePeers 打乱对等节点列表的顺序。
// 参数:
//   - peers: []peer.ID 类型，表示对等节点 ID 列表。
func shufflePeers(peers []peer.ID) {
	for i := range peers { // 遍历对等节点列表。
		j := rand.Intn(i + 1)                   // 生成一个随机索引。
		peers[i], peers[j] = peers[j], peers[i] // 交换当前对等节点和随机索引处的对等节点。
	}
}

// shufflePeerInfo 打乱对等节点信息列表的顺序。
// 参数:
//   - peers: []*pb.PeerInfo 类型，表示对等节点信息列表。
func shufflePeerInfo(peers []*pb.PeerInfo) {
	for i := range peers { // 遍历对等节点信息列表。
		j := rand.Intn(i + 1)                   // 生成一个随机索引。
		peers[i], peers[j] = peers[j], peers[i] // 交换当前对等节点信息和随机索引处的对等节点信息。
	}
}

// shuffleStrings 打乱字符串列表的顺序。
// 参数:
//   - lst: []string 类型，表示字符串列表。
func shuffleStrings(lst []string) {
	for i := range lst { // 遍历字符串列表。
		j := rand.Intn(i + 1)           // 生成一个随机索引。
		lst[i], lst[j] = lst[j], lst[i] // 交换当前字符串和随机索引处的字符串。
	}
}
