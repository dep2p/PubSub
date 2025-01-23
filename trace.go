// 作用：消息追踪。
// 功能：记录和跟踪消息的传播路径，帮助调试和分析消息传递过程。

package pubsub

import (
	"time"

	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/core/protocol"

	pb "github.com/dep2p/pubsub/pb"
)

// EventTracer 是一个通用的事件追踪器接口。
// 这是一个高级追踪接口，它传递由 pb/trace.proto 中定义的追踪事件。
type EventTracer interface {
	// Trace 方法用于记录一个追踪事件。
	Trace(evt *pb.TraceEvent)
}

// RawTracer 是一个低级追踪接口，允许应用程序追踪 pubsub 子系统的内部操作。
// 请注意，追踪器是同步调用的，这意味着应用程序的追踪器必须注意不要阻塞或修改参数。
// 警告：此接口不是固定的，可能会根据系统需求添加新方法。
type RawTracer interface {
	// AddPeer 当一个新对等节点被添加时调用。
	AddPeer(p peer.ID, proto protocol.ID)
	// RemovePeer 当一个对等节点被移除时调用。
	RemovePeer(p peer.ID)
	// Join 当加入一个新主题时调用。
	Join(topic string)
	// Leave 当放弃一个主题时调用。
	Leave(topic string)
	// Graft 当一个新对等节点被添加到网格时调用（gossipsub）。
	Graft(p peer.ID, topic string)
	// Prune 当一个对等节点被从网格中移除时调用（gossipsub）。
	Prune(p peer.ID, topic string)
	// ValidateMessage 当消息首次进入验证管道时调用。
	ValidateMessage(msg *Message)
	// DeliverMessage 当消息被传递时调用。
	DeliverMessage(msg *Message)
	// RejectMessage 当消息被拒绝或忽略时调用。
	// 参数 reason 是一个命名字符串 Reject*。
	RejectMessage(msg *Message, reason string)
	// DuplicateMessage 当重复消息被丢弃时调用。
	DuplicateMessage(msg *Message)
	// ThrottlePeer 当一个对等节点被对等节点限制器限制时调用。
	ThrottlePeer(p peer.ID)
	// RecvRPC 当接收到一个传入的 RPC 时调用。
	RecvRPC(rpc *RPC)
	// SendRPC 当发送一个 RPC 时调用。
	SendRPC(rpc *RPC, p peer.ID)
	// DropRPC 当一个出站 RPC 被丢弃时调用，通常是因为队列已满。
	DropRPC(rpc *RPC, p peer.ID)
	// UndeliverableMessage 当 Subscribe 的消费者未能足够快地读取消息且压力释放机制触发丢弃消息时调用。
	UndeliverableMessage(msg *Message)
}

// pubsubTracer 结构体，用于管理追踪器。
type pubsubTracer struct {
	tracer EventTracer     // 事件追踪器
	raw    []RawTracer     // 低级追踪器数组
	pid    peer.ID         // 节点 ID
	idGen  *msgIDGenerator // 消息 ID 生成器
}

// PublishMessage 方法记录发布消息的事件。
func (t *pubsubTracer) PublishMessage(msg *Message) {
	if t == nil {
		return
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_PUBLISH_MESSAGE,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		PublishMessage: &pb.TraceEvent_PublishMessage{
			MessageID: []byte(t.idGen.ID(msg)),
			Topic:     msg.Message.Topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// ValidateMessage 方法记录验证消息的事件。
func (t *pubsubTracer) ValidateMessage(msg *Message) {
	if t == nil {
		return
	}

	if msg.ReceivedFrom != t.pid {
		for _, tr := range t.raw {
			tr.ValidateMessage(msg) // 调用所有低级追踪器的 ValidateMessage 方法
		}
	}
}

// RejectMessage 方法记录拒绝消息的事件。
// 参数:
//   - msg: 被拒绝的消息
//   - reason: 拒绝的原因
func (t *pubsubTracer) RejectMessage(msg *Message, reason string) {
	if t == nil {
		return
	}

	if msg.ReceivedFrom != t.pid {
		for _, tr := range t.raw {
			tr.RejectMessage(msg, reason) // 调用所有低级追踪器的 RejectMessage 方法
		}
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_REJECT_MESSAGE,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		RejectMessage: &pb.TraceEvent_RejectMessage{
			MessageID:    []byte(t.idGen.ID(msg)),
			ReceivedFrom: []byte(msg.ReceivedFrom),
			Reason:       reason,
			Topic:        msg.Topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// DuplicateMessage 方法记录重复消息的事件。
// 参数:
//   - msg: 重复的消息
func (t *pubsubTracer) DuplicateMessage(msg *Message) {
	if t == nil {
		return
	}

	if msg.ReceivedFrom != t.pid {
		for _, tr := range t.raw {
			tr.DuplicateMessage(msg) // 调用所有低级追踪器的 DuplicateMessage 方法
		}
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_DUPLICATE_MESSAGE,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		DuplicateMessage: &pb.TraceEvent_DuplicateMessage{
			MessageID:    []byte(t.idGen.ID(msg)),
			ReceivedFrom: []byte(msg.ReceivedFrom),
			Topic:        msg.Topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// DeliverMessage 方法记录传递消息的事件。
// 参数:
//   - msg: 被传递的消息
func (t *pubsubTracer) DeliverMessage(msg *Message) {
	if t == nil {
		return
	}

	if msg.ReceivedFrom != t.pid {
		for _, tr := range t.raw {
			tr.DeliverMessage(msg) // 调用所有低级追踪器的 DeliverMessage 方法
		}
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_DELIVER_MESSAGE,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		DeliverMessage: &pb.TraceEvent_DeliverMessage{
			MessageID:    []byte(t.idGen.ID(msg)),
			Topic:        msg.Topic,
			ReceivedFrom: []byte(msg.ReceivedFrom),
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// AddPeer 方法记录添加对等节点的事件。
// 参数:
//   - p: 对等节点 ID
//   - proto: 协议 ID
func (t *pubsubTracer) AddPeer(p peer.ID, proto protocol.ID) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.AddPeer(p, proto) // 调用所有低级追踪器的 AddPeer 方法
	}

	if t.tracer == nil {
		return
	}

	protoStr := string(proto)
	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_ADD_PEER,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		AddPeer: &pb.TraceEvent_AddPeer{
			PeerID: []byte(p),
			Proto:  protoStr,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// RemovePeer 方法记录移除对等节点的事件。
// 参数:
//   - p: 对等节点 ID
func (t *pubsubTracer) RemovePeer(p peer.ID) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.RemovePeer(p) // 调用所有低级追踪器的 RemovePeer 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_REMOVE_PEER,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		RemovePeer: &pb.TraceEvent_RemovePeer{
			PeerID: []byte(p),
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// RecvRPC 方法记录接收 RPC 的事件。
// 参数:
//   - rpc: 接收的 RPC
func (t *pubsubTracer) RecvRPC(rpc *RPC) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.RecvRPC(rpc) // 调用所有低级追踪器的 RecvRPC 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_RECV_RPC,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		RecvRPC: &pb.TraceEvent_RecvRPC{
			ReceivedFrom: []byte(rpc.from),
			Meta:         t.traceRPCMeta(rpc),
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// SendRPC 方法记录发送 RPC 的事件。
// 参数:
//   - rpc: 发送的 RPC
//   - p: 目标对等节点 ID
func (t *pubsubTracer) SendRPC(rpc *RPC, p peer.ID) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.SendRPC(rpc, p) // 调用所有低级追踪器的 SendRPC 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_SEND_RPC,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		SendRPC: &pb.TraceEvent_SendRPC{
			SendTo: []byte(p),
			Meta:   t.traceRPCMeta(rpc),
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// DropRPC 方法记录丢弃 RPC 的事件。
// 参数:
//   - rpc: 丢弃的 RPC
//   - p: 目标对等节点 ID
func (t *pubsubTracer) DropRPC(rpc *RPC, p peer.ID) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.DropRPC(rpc, p) // 调用所有低级追踪器的 DropRPC 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_DROP_RPC,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		DropRPC: &pb.TraceEvent_DropRPC{
			SendTo: []byte(p),
			Meta:   t.traceRPCMeta(rpc),
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// UndeliverableMessage 方法记录不可传递消息的事件。
// 参数:
//   - msg: 不可传递的消息
func (t *pubsubTracer) UndeliverableMessage(msg *Message) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.UndeliverableMessage(msg) // 调用所有低级追踪器的 UndeliverableMessage 方法
	}
}

// traceRPCMeta 方法生成 RPC 的元数据。
// 参数:
//   - rpc: RPC 消息
//
// 返回值：
//   - *pb.TraceEvent_RPCMeta: 生成的 RPC 元数据
func (t *pubsubTracer) traceRPCMeta(rpc *RPC) *pb.TraceEvent_RPCMeta {
	rpcMeta := new(pb.TraceEvent_RPCMeta)

	// 处理发布的消息
	var msgs []*pb.TraceEvent_MessageMeta
	for _, m := range rpc.Publish {
		msgs = append(msgs, &pb.TraceEvent_MessageMeta{
			MessageID: []byte(t.idGen.RawID(m)),
			Topic:     m.Topic,
		})
	}
	rpcMeta.Messages = msgs

	// 处理订阅信息
	var subs []*pb.TraceEvent_SubMeta
	for _, sub := range rpc.Subscriptions {
		subs = append(subs, &pb.TraceEvent_SubMeta{
			Subscribe: sub.Subscribe,
			Topic:     sub.Topicid,
		})
	}
	rpcMeta.Subscription = subs

	if rpc.Control != nil {
		// 处理 IHAVE 控制消息
		var ihave []*pb.TraceEvent_ControlIHaveMeta
		for _, ctl := range rpc.Control.Ihave {
			var mids [][]byte
			for _, mid := range ctl.MessageIDs {
				mids = append(mids, []byte(mid))
			}
			ihave = append(ihave, &pb.TraceEvent_ControlIHaveMeta{
				Topic:      ctl.TopicID,
				MessageIDs: mids,
			})
		}

		// 处理 IWANT 控制消息
		var iwant []*pb.TraceEvent_ControlIWantMeta
		for _, ctl := range rpc.Control.Iwant {
			var mids [][]byte
			for _, mid := range ctl.MessageIDs {
				mids = append(mids, []byte(mid))
			}
			iwant = append(iwant, &pb.TraceEvent_ControlIWantMeta{
				MessageIDs: mids,
			})
		}

		// 处理 GRAFT 控制消息
		var graft []*pb.TraceEvent_ControlGraftMeta
		for _, ctl := range rpc.Control.Graft {
			graft = append(graft, &pb.TraceEvent_ControlGraftMeta{
				Topic: ctl.TopicID,
			})
		}

		// 处理 PRUNE 控制消息
		var prune []*pb.TraceEvent_ControlPruneMeta
		for _, ctl := range rpc.Control.Prune {
			peers := make([][]byte, 0, len(ctl.Peers))
			for _, pi := range ctl.Peers {
				peers = append(peers, pi.PeerID)
			}
			prune = append(prune, &pb.TraceEvent_ControlPruneMeta{
				Topic: ctl.TopicID,
				Peers: peers,
			})
		}

		rpcMeta.Control = &pb.TraceEvent_ControlMeta{
			Ihave: ihave,
			Iwant: iwant,
			Graft: graft,
			Prune: prune,
		}
	}

	return rpcMeta
}

// Join 方法记录加入主题的事件。
// 参数:
//   - topic: 加入的主题
func (t *pubsubTracer) Join(topic string) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.Join(topic) // 调用所有低级追踪器的 Join 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_JOIN,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		Join: &pb.TraceEvent_Join{
			Topic: topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// Leave 方法记录离开主题的事件。
// 参数:
//   - topic: 离开的主题
func (t *pubsubTracer) Leave(topic string) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.Leave(topic) // 调用所有低级追踪器的 Leave 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_LEAVE,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		Leave: &pb.TraceEvent_Leave{
			Topic: topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// Graft 方法记录网格中添加对等节点的事件。
// 参数:
//   - p: 对等节点 ID
//   - topic: 主题
func (t *pubsubTracer) Graft(p peer.ID, topic string) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.Graft(p, topic) // 调用所有低级追踪器的 Graft 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_GRAFT,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		Graft: &pb.TraceEvent_Graft{
			PeerID: []byte(p),
			Topic:  topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// Prune 方法记录网格中移除对等节点的事件。
// 参数:
//   - p: 对等节点 ID
//   - topic: 主题
func (t *pubsubTracer) Prune(p peer.ID, topic string) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.Prune(p, topic) // 调用所有低级追踪器的 Prune 方法
	}

	if t.tracer == nil {
		return
	}

	now := time.Now().UnixNano() // 获取当前时间戳
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_PRUNE,
		PeerID:    []byte(t.pid),
		Timestamp: now,
		Prune: &pb.TraceEvent_Prune{
			PeerID: []byte(p),
			Topic:  topic,
		},
	}

	t.tracer.Trace(evt) // 记录事件
}

// ThrottlePeer 方法记录限制对等节点的事件。
// 参数:
//   - p: 被限制的对等节点 ID
func (t *pubsubTracer) ThrottlePeer(p peer.ID) {
	if t == nil {
		return
	}

	for _, tr := range t.raw {
		tr.ThrottlePeer(p) // 调用所有低级追踪器的 ThrottlePeer 方法
	}
}
