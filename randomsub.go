// 作用：实现RandomSub协议。
// 功能：RandomSub是一种简单的消息传递协议，随机选择一部分对等节点进行消息传递。

package pubsub

import (
	"context"
	"math"

	"github.com/dep2p/pubsub/logger"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	// RandomSubID 是 RandomSub 路由器使用的协议 ID
	RandomSubID = protocol.ID("/randomsub/1.0.0")
)

var (
	// RandomSubD 是 RandomSub 路由器使用的最小对等节点数
	RandomSubD = 6
)

// NewRandomSub 返回一个使用 RandomSubRouter 作为路由器的新 PubSub 对象
// 参数:
//   - ctx: 上下文
//   - h: 主机
//   - size: 网络大小
//   - opts: 选项
//
// 返回值:
//   - *PubSub: PubSub 对象
//   - error: 错误
func NewRandomSub(ctx context.Context, h host.Host, size int, opts ...Option) (*PubSub, error) {
	rt := &RandomSubRouter{
		size:  size,
		peers: make(map[peer.ID]protocol.ID),
	}
	return NewPubSub(ctx, h, rt, opts...)
}

// RandomSubRouter 是一个实现随机传播策略的路由器。
// 对于每条消息，它选择网络大小平方根个对等节点，最少为 RandomSubD，并将消息转发给它们。
type RandomSubRouter struct {
	p      *PubSub                 // PubSub 对象
	peers  map[peer.ID]protocol.ID // 对等节点映射
	size   int                     // 网络大小
	tracer *pubsubTracer           // 跟踪器
}

// Protocols 返回路由器支持的协议列表
// 返回值:
//   - []protocol.ID: 协议列表
func (rs *RandomSubRouter) Protocols() []protocol.ID {
	return []protocol.ID{RandomSubID, FloodSubID}
}

// Attach 将路由器附加到一个初始化的 PubSub 实例
// 参数:
//   - p: PubSub 对象
func (rs *RandomSubRouter) Attach(p *PubSub) {
	rs.p = p
	rs.tracer = p.tracer
}

// AddPeer 通知路由器一个新的对等节点已经连接
// 参数:
//   - p: 对等节点 ID
//   - proto: 协议 ID
func (rs *RandomSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	rs.tracer.AddPeer(p, proto)
	rs.peers[p] = proto
}

// RemovePeer 通知路由器一个对等节点已经断开连接
// 参数:
//   - p: 对等节点 ID
func (rs *RandomSubRouter) RemovePeer(p peer.ID) {
	rs.tracer.RemovePeer(p)
	delete(rs.peers, p)
}

// EnoughPeers 返回路由器是否需要更多对等节点才能准备好发布新记录
// 参数:
//   - topic: 主题
//   - suggested: 建议的对等节点数
//
// 返回值:
//   - bool: 是否有足够的对等节点
func (rs *RandomSubRouter) EnoughPeers(topic string, suggested int) bool {
	// 检查主题中的所有对等节点
	tmap, ok := rs.p.topics[topic]
	if !ok {
		// 如果主题不存在，返回 false
		return false
	}

	// 初始化 floodsub 和 randomsub 对等节点计数器
	fsPeers := 0
	rsPeers := 0

	// 统计 floodsub 和 randomsub 对等节点
	for p := range tmap {
		switch rs.peers[p] {
		case FloodSubID:
			// 如果是 FloodSub 节点，增加 fsPeers 计数器
			fsPeers++
		case RandomSubID:
			// 如果是 RandomSub 节点，增加 rsPeers 计数器
			rsPeers++
		}
	}

	// 如果建议的对等节点数为 0，使用默认值 RandomSubD
	if suggested == 0 {
		suggested = RandomSubD
	}

	// 如果 floodsub 和 randomsub 对等节点总数大于或等于建议的对等节点数，返回 true
	if fsPeers+rsPeers >= suggested {
		return true
	}

	// 如果 randomsub 对等节点数大于或等于 RandomSubD，返回 true
	if rsPeers >= RandomSubD {
		return true
	}

	// 否则，返回 false
	return false
}

// AcceptFrom 在处理控制信息或将消息推送到验证管道之前，对每个传入消息调用此方法
// 参数:
//   - peer.ID: 对等节点 ID
//
// 返回值:
//   - AcceptStatus: 接受状态
func (rs *RandomSubRouter) AcceptFrom(peer.ID) AcceptStatus {
	return AcceptAll
}

// HandleRPC 处理控制消息
// 参数:
//   - rpc: RPC 对象
func (rs *RandomSubRouter) HandleRPC(rpc *RPC) {}

// Publish 发布一条已验证的新消息
// 参数:
//   - msg: 消息对象
func (rs *RandomSubRouter) Publish(msg *Message) {
	// 获取消息的发送者
	from := msg.ReceivedFrom

	// 初始化两个空的映射，一个用于存储要发送消息的节点，另一个用于存储随机选取的节点
	tosend := make(map[peer.ID]struct{})
	rspeers := make(map[peer.ID]struct{})
	// 获取消息的来源节点
	src := peer.ID(msg.GetFrom())

	// 获取消息的主题
	topic := msg.GetTopic()
	// 查找该主题对应的节点映射
	tmap, ok := rs.p.topics[topic]
	if !ok {
		// 如果主题不存在，直接返回
		return
	}

	// 遍历该主题对应的节点映射
	for p := range tmap {
		// 如果节点是消息的发送者或来源节点，跳过该节点
		if p == from || p == src {
			continue
		}

		// 如果节点是 FloodSub 节点，添加到 tosend 映射中
		if rs.peers[p] == FloodSubID {
			tosend[p] = struct{}{}
		} else {
			// 否则，添加到 rspeers 映射中
			rspeers[p] = struct{}{}
		}
	}

	// 如果随机选取的节点数量超过预定值
	if len(rspeers) > RandomSubD {
		// 目标值设定为 RandomSubD
		target := RandomSubD
		// 计算平方根，如果大于目标值，则更新目标值
		sqrt := int(math.Ceil(math.Sqrt(float64(rs.size))))
		if sqrt > target {
			target = sqrt
		}
		// 如果目标值超过随机选取的节点数量，更新目标值为随机选取的节点数量
		if target > len(rspeers) {
			target = len(rspeers)
		}
		// 将随机选取的节点映射转换为列表并打乱顺序
		xpeers := peerMapToList(rspeers)
		shufflePeers(xpeers)
		// 选择目标值数量的节点
		xpeers = xpeers[:target]
		// 将选中的节点添加到 tosend 映射中
		for _, p := range xpeers {
			tosend[p] = struct{}{}
		}
	} else {
		// 如果随机选取的节点数量不超过预定值，全部添加到 tosend 映射中
		for p := range rspeers {
			tosend[p] = struct{}{}
		}
	}

	// 创建包含消息的 RPC 对象
	out := rpcWithMessages(msg.Message)
	// 遍历要发送消息的节点
	for p := range tosend {
		// 获取节点对应的消息通道
		mch, ok := rs.p.peers[p]
		if !ok {
			// 如果节点不存在，跳过该节点
			continue
		}

		// 尝试向节点发送消息
		select {
		case mch <- out:
			// 如果发送成功，记录发送操作
			rs.tracer.SendRPC(out, p)
		default:
			// 如果消息队列满了，记录丢弃操作
			logger.Warnf("丢弃发送到对等节点 %s 的消息: 队列已满", p)
			rs.tracer.DropRPC(out, p)
		}
	}
}

// Join 通知路由器我们想要接收和转发主题中的消息
// 参数:
//   - topic: 主题
func (rs *RandomSubRouter) Join(topic string) {
	rs.tracer.Join(topic)
}

// Leave 通知路由器我们不再对主题感兴趣
// 参数:
//   - topic: 主题
func (rs *RandomSubRouter) Leave(topic string) {
	rs.tracer.Leave(topic)
}
