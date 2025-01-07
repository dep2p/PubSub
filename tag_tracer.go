// 作用：标签追踪。
// 功能：实现对标签的追踪和管理，用于调试和分析pubsub系统中的标签传播。

package pubsub

import (
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var (
	// GossipSubConnTagBumpMessageDelivery 表示每次一个对等节点首次在一个主题中传递消息时，添加到连接管理器标签中的量。
	// 每次对等节点首次在一个主题中传递消息时，我们将此标签增加该量，直到最大值 GossipSubConnTagMessageDeliveryCap。
	// 注意，传递标签会随着时间衰减，在每个 GossipSubConnTagDecayInterval 期间减少 GossipSubConnTagDecayAmount。
	GossipSubConnTagBumpMessageDelivery = 1

	// GossipSubConnTagDecayInterval 是连接管理器标签衰减的时间间隔。
	GossipSubConnTagDecayInterval = 10 * time.Minute

	// GossipSubConnTagDecayAmount 是在每个衰减间隔期间从衰减标签值中减去的量。
	GossipSubConnTagDecayAmount = 1

	// GossipSubConnTagMessageDeliveryCap 是用于跟踪消息传递的连接管理器标签的最大值。
	GossipSubConnTagMessageDeliveryCap = 15
)

// tagTracer 是一个内部跟踪器，它根据对等节点的行为对对等节点连接应用连接管理器标签。
// 我们出于以下原因对对等节点的连接进行标记：
//   - 直接连接的对等节点被标记为 GossipSubConnTagValueDirectPeer（默认值为 1000）。
//   - 网状网络中的对等节点被标记为 GossipSubConnTagValueMeshPeer（默认值为 20）。
//     如果一个对等节点在多个主题网中，他们将分别标记每个主题。
//   - 每当我们收到一条消息时，我们会为第一个传递消息的对等节点增加一个传递标签。
//     传递标签有一个最大值 GossipSubConnTagMessageDeliveryCap，并且以每个 GossipSubConnTagDecayInterval 衰减 GossipSubConnTagDecayAmount。
type tagTracer struct {
	sync.RWMutex

	cmgr     connmgr.ConnManager            // 连接管理器
	idGen    *msgIDGenerator                // 消息 ID 生成器
	decayer  connmgr.Decayer                // 衰减器
	decaying map[string]connmgr.DecayingTag // 衰减标签的映射
	direct   map[peer.ID]struct{}           // 直接对等节点的集合

	// 消息 ID 映射到在消息完成验证之前传递消息但不是第一个传递的对等节点集合
	nearFirst map[string]map[peer.ID]struct{}
}

// newTagTracer 创建一个新的 tagTracer。
// 参数:
// - cmgr: connmgr.ConnManager 连接管理器
// 返回值:
// - *tagTracer: 创建的 tagTracer 实例
func newTagTracer(cmgr connmgr.ConnManager) *tagTracer {
	decayer, ok := connmgr.SupportsDecay(cmgr) // 检查连接管理器是否支持衰减标签
	if !ok {
		logger.Debugf("连接管理器不支持衰减标签，传递标签将不会被应用")
	}
	return &tagTracer{
		cmgr:      cmgr,
		idGen:     newMsgIdGenerator(),
		decayer:   decayer,
		decaying:  make(map[string]connmgr.DecayingTag),
		nearFirst: make(map[string]map[peer.ID]struct{}),
	}
}

// Start 启动 tagTracer。
// 参数:
// - gs: *GossipSubRouter
func (t *tagTracer) Start(gs *GossipSubRouter) {
	if t == nil {
		return
	}

	t.idGen = gs.p.idGen
	t.direct = gs.direct
}

// tagPeerIfDirect 如果对等节点是直接对等节点，则标记它。
// 参数:
// - p: peer.ID
func (t *tagTracer) tagPeerIfDirect(p peer.ID) {
	if t.direct == nil {
		return
	}

	_, direct := t.direct[p]
	if direct {
		t.cmgr.Protect(p, "pubsub:<direct>")
	}
}

// tagMeshPeer 标记网状网络中的对等节点。
// 参数:
// - p: peer.ID
// - topic: string
func (t *tagTracer) tagMeshPeer(p peer.ID, topic string) {
	tag := topicTag(topic)
	t.cmgr.Protect(p, tag)
}

// untagMeshPeer 取消标记网状网络中的对等节点。
// 参数:
// - p: peer.ID
// - topic: string
func (t *tagTracer) untagMeshPeer(p peer.ID, topic string) {
	tag := topicTag(topic)
	t.cmgr.Unprotect(p, tag)
}

// topicTag 返回用于标记主题的标签字符串。
// 参数:
// - topic: string
// 返回值:
// - string: 标签字符串
func topicTag(topic string) string {
	return fmt.Sprintf("pubsub:%s", topic)
}

// addDeliveryTag 添加传递标签。
// 参数:
// - topic: string
func (t *tagTracer) addDeliveryTag(topic string) {
	if t.decayer == nil {
		return
	}

	name := fmt.Sprintf("pubsub-deliveries:%s", topic)
	t.Lock()
	defer t.Unlock()
	tag, err := t.decayer.RegisterDecayingTag(
		name,
		GossipSubConnTagDecayInterval,
		connmgr.DecayFixed(GossipSubConnTagDecayAmount),
		connmgr.BumpSumBounded(0, GossipSubConnTagMessageDeliveryCap))

	if err != nil {
		logger.Warnf("无法创建衰减传递标签: %s", err)
		return
	}
	t.decaying[topic] = tag
}

// removeDeliveryTag 移除传递标签。
// 参数:
// - topic: string
func (t *tagTracer) removeDeliveryTag(topic string) {
	t.Lock()
	defer t.Unlock()
	tag, ok := t.decaying[topic]
	if !ok {
		return
	}
	err := tag.Close()
	if err != nil {
		logger.Warnf("关闭衰减连接管理器标签失败: %s", err)
	}
	delete(t.decaying, topic)
}

// bumpDeliveryTag 增加传递标签的值。
// 参数:
// - p: peer.ID
// - topic: string
// 返回值:
// - error: 错误信息，如果有的话
func (t *tagTracer) bumpDeliveryTag(p peer.ID, topic string) error {
	t.RLock()
	defer t.RUnlock()

	tag, ok := t.decaying[topic]
	if !ok {
		logger.Warnf("没有为主题 %s 注册衰减标签", topic)
		return fmt.Errorf("没有为主题 %s 注册衰减标签", topic)
	}
	return tag.Bump(p, GossipSubConnTagBumpMessageDelivery)
}

// bumpTagsForMessage 增加消息的传递标签。
// 参数:
// - p: peer.ID
// - msg: *Message
func (t *tagTracer) bumpTagsForMessage(p peer.ID, msg *Message) {
	topic := msg.GetTopic()
	err := t.bumpDeliveryTag(p, topic)
	if err != nil {
		logger.Warnf("增加传递标签失败: %s", err)
	}
}

// nearFirstPeers 返回在消息验证时传递消息的对等节点。
// 参数:
// - msg: *Message
// 返回值:
// - []peer.ID: 对等节点列表
func (t *tagTracer) nearFirstPeers(msg *Message) []peer.ID {
	t.Lock()
	defer t.Unlock()
	peersMap, ok := t.nearFirst[t.idGen.ID(msg)]
	if !ok {
		return nil
	}
	peers := make([]peer.ID, 0, len(peersMap))
	for p := range peersMap {
		peers = append(peers, p)
	}
	return peers
}

// AddPeer 添加对等节点。
// 参数:
// - p: peer.ID
// - proto: protocol.ID
func (t *tagTracer) AddPeer(p peer.ID, proto protocol.ID) {
	t.tagPeerIfDirect(p)
}

// Join 加入主题。
// 参数:
// - topic: string
func (t *tagTracer) Join(topic string) {
	t.addDeliveryTag(topic)
}

// DeliverMessage 传递消息。
// 参数:
// - msg: *Message
func (t *tagTracer) DeliverMessage(msg *Message) {
	nearFirst := t.nearFirstPeers(msg)

	t.bumpTagsForMessage(msg.ReceivedFrom, msg)
	for _, p := range nearFirst {
		t.bumpTagsForMessage(p, msg)
	}

	// 删除该消息的传递状态
	t.Lock()
	delete(t.nearFirst, t.idGen.ID(msg))
	t.Unlock()
}

// Leave 离开主题。
// 参数:
// - topic: string
func (t *tagTracer) Leave(topic string) {
	t.removeDeliveryTag(topic)
}

// Graft 将对等节点添加到网状网络中。
// 参数:
// - p: peer.ID
// - topic: string
func (t *tagTracer) Graft(p peer.ID, topic string) {
	t.tagMeshPeer(p, topic)
}

// Prune 将对等节点从网状网络中移除。
// 参数:
// - p: peer.ID
// - topic: string
func (t *tagTracer) Prune(p peer.ID, topic string) {
	t.untagMeshPeer(p, topic)
}

// ValidateMessage 验证消息。
// 参数:
// - msg: *Message
func (t *tagTracer) ValidateMessage(msg *Message) {
	t.Lock()
	defer t.Unlock()

	// 创建映射以开始跟踪在验证时传递消息的对等节点
	id := t.idGen.ID(msg)
	if _, exists := t.nearFirst[id]; exists {
		return
	}
	t.nearFirst[id] = make(map[peer.ID]struct{})
}

// DuplicateMessage 处理重复消息。
// 参数:
// - msg: *Message
func (t *tagTracer) DuplicateMessage(msg *Message) {
	t.Lock()
	defer t.Unlock()

	id := t.idGen.ID(msg)
	peers, ok := t.nearFirst[id]
	if !ok {
		return
	}
	peers[msg.ReceivedFrom] = struct{}{}
}

// RejectMessage 处理被拒绝的消息。
// 参数:
// - msg: *Message
// - reason: string 拒绝的原因
func (t *tagTracer) RejectMessage(msg *Message, reason string) {
	t.Lock()
	defer t.Unlock()

	// 我们希望删除通过验证管道的消息的近首次传递跟踪。
	// 其他拒绝原因（缺少签名等）跳过验证队列，因此我们不希望删除状态，以防消息仍在验证中。
	switch reason {
	case RejectValidationThrottled:
		fallthrough
	case RejectValidationIgnored:
		fallthrough
	case RejectValidationFailed:
		delete(t.nearFirst, t.idGen.ID(msg))
	}
}

// RemovePeer 是移除对等节点的方法。
// 参数:
// - peer.ID: 对等节点 ID
func (t *tagTracer) RemovePeer(peer.ID) {}

// ThrottlePeer 是限流对等节点的方法。
// 参数:
// - p: peer.ID 对等节点 ID
func (t *tagTracer) ThrottlePeer(p peer.ID) {}

// RecvRPC 是接收 RPC 的方法。
// 参数:
// - rpc: *RPC RPC 消息
func (t *tagTracer) RecvRPC(rpc *RPC) {}

// SendRPC 是发送 RPC 的方法。
// 参数:
// - rpc: *RPC RPC 消息
// - p: peer.ID 对等节点 ID
func (t *tagTracer) SendRPC(rpc *RPC, p peer.ID) {}

// DropRPC 是丢弃 RPC 的方法。
// 参数:
// - rpc: *RPC RPC 消息
// - p: peer.ID 对等节点 ID
func (t *tagTracer) DropRPC(rpc *RPC, p peer.ID) {}

// UndeliverableMessage 是处理不可传递消息的方法。
// 参数:
// - msg: *Message 消息
func (t *tagTracer) UndeliverableMessage(msg *Message) {}
