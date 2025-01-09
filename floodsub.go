// 作用：实现FloodSub协议。
// 功能：FloodSub是一种简单的消息传递协议，消息会被广播到所有订阅该主题的对等节点。

package pubsub

import (
	"context"

	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"
)

// 常量定义
const (
	FloodSubID              = protocol.ID("/floodsub/1.0.0") // FloodSub协议ID
	FloodSubTopicSearchSize = 5                              // FloodSub主题搜索大小
)

// NewFloodsubWithProtocols 返回一个新的启用floodsub的PubSub对象，使用指定的协议
// 参数:
//   - ctx: 上下文
//   - h: 主机
//   - ps: 协议列表
//   - opts: 选项
//
// 返回值:
//   - *PubSub: 创建的PubSub实例
//   - error: 如果创建失败，返回错误
func NewFloodsubWithProtocols(ctx context.Context, h host.Host, ps []protocol.ID, opts ...Option) (*PubSub, error) {
	rt := &FloodSubRouter{
		protocols: ps, // 使用指定的协议初始化FloodSubRouter
	}
	return NewPubSub(ctx, h, rt, opts...) // 创建并返回新的PubSub实例
}

// NewFloodSub 返回一个使用FloodSubRouter的新PubSub对象
// 参数:
//   - ctx: 上下文
//   - h: 主机
//   - opts: 选项
//
// 返回值:
//   - *PubSub: 创建的PubSub实例
//   - error: 如果创建失败，返回错误
func NewFloodSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	return NewFloodsubWithProtocols(ctx, h, []protocol.ID{FloodSubID}, opts...) // 调用NewFloodsubWithProtocols创建PubSub实例
}

// FloodSubRouter 结构体定义了FloodSub路由器
type FloodSubRouter struct {
	p         *PubSub       // 关联的PubSub实例
	protocols []protocol.ID // 支持的协议列表
	tracer    *pubsubTracer // 追踪器
}

// Protocols 返回FloodSubRouter支持的协议列表
// 返回值:
//   - []protocol.ID: 支持的协议列表
func (fs *FloodSubRouter) Protocols() []protocol.ID {
	return fs.protocols
}

// Attach 将FloodSubRouter附加到PubSub实例
// 参数:
//   - p: PubSub实例
func (fs *FloodSubRouter) Attach(p *PubSub) {
	fs.p = p             // 设置关联的PubSub实例
	fs.tracer = p.tracer // 设置追踪器
}

// AddPeer 添加对等节点到FloodSubRouter
// 参数:
//   - p: 对等节点ID
//   - proto: 协议ID
func (fs *FloodSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	fs.tracer.AddPeer(p, proto) // 在追踪器中添加对等节点
}

// RemovePeer 从FloodSubRouter中移除对等节点
// 参数:
//   - p: 对等节点ID
func (fs *FloodSubRouter) RemovePeer(p peer.ID) {
	fs.tracer.RemovePeer(p) // 在追踪器中移除对等节点
}

// EnoughPeers 检查是否有足够的对等节点来支持特定主题。
// 参数:
//   - topic: 主题名称
//   - suggested: 建议的对等节点数量
//
// 返回值:
//   - bool: 是否有足够的对等节点支持该主题
func (fs *FloodSubRouter) EnoughPeers(topic string, suggested int) bool {
	// 检查主题的所有对等节点映射
	tmap, ok := fs.p.topics[topic]
	if !ok {
		return false
	}

	// 如果建议的对等节点数量为零，则使用默认的搜索大小
	if suggested == 0 {
		suggested = FloodSubTopicSearchSize
	}

	// 如果该主题的对等节点数量大于或等于建议的数量，返回 true
	if len(tmap) >= suggested {
		return true
	}

	return false
}

// AcceptFrom 决定是否接受来自特定对等节点的消息
// 参数:
//   - peer: 对等节点ID
//
// 返回值:
//   - AcceptStatus: 接受状态
func (fs *FloodSubRouter) AcceptFrom(peer.ID) AcceptStatus {
	return AcceptAll // 接受所有消息
}

// HandleRPC 处理接收到的RPC消息
// 参数:
//   - rpc: RPC消息
func (fs *FloodSubRouter) HandleRPC(rpc *RPC) {}

// Publish 发布消息到主题
// 参数:
//   - msg: 消息
func (fs *FloodSubRouter) Publish(msg *Message) {
	from := msg.ReceivedFrom // 获取消息发送者
	topic := msg.GetTopic()  // 获取消息主题

	out := rpcWithMessages(msg.Message) // 将消息打包成RPC

	// 遍历订阅了该主题的对等节点
	for pid := range fs.p.topics[topic] {
		// 如果节点是消息发送者或消息来源节点，跳过该节点
		if pid == from || pid == peer.ID(msg.GetFrom()) {
			continue
		}

		// 获取对等节点的消息通道
		mch, ok := fs.p.peers[pid]
		if !ok {
			// 如果对等节点不存在，跳过该节点
			continue
		}

		// 尝试向对等节点发送消息
		select {
		case mch <- out: // 发送消息到对等节点
			fs.tracer.SendRPC(out, pid) // 追踪发送的RPC消息
		default:
			// 如果消息队列已满，丢弃消息
			logger.Infof("丢弃消息到对等节点 %s: 队列已满", pid) // 队列已满，丢弃消息
			fs.tracer.DropRPC(out, pid)             // 追踪丢弃的RPC消息
		}
	}
}

// Join 加入主题
// 参数:
//   - topic: 主题
func (fs *FloodSubRouter) Join(topic string) {
	fs.tracer.Join(topic) // 追踪加入的主题
}

// Leave 离开主题
// 参数:
//   - topic: 主题
func (fs *FloodSubRouter) Leave(topic string) {
	fs.tracer.Leave(topic) // 追踪离开的主题
}
