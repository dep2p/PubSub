// 作用：实现对GossipSub消息的追踪。
// 功能：记录和跟踪GossipSub协议中的消息流，帮助分析和调试消息传递路径。

package pubsub

import (
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// gossipTracer 是一个内部追踪器，跟踪 IWANT 请求，以便惩罚在 IHAVE 广告后不跟进 IWANT 请求的对等节点。
// 承诺的跟踪是概率性的，以避免使用过多的内存。
type gossipTracer struct {
	sync.Mutex

	idGen *msgIDGenerator // 消息ID生成器

	followUpTime time.Duration // 跟进时间

	// 根据消息ID跟踪的承诺；对于跟踪的每条消息，我们跟踪每个对等节点的承诺到期时间。
	promises map[string]map[peer.ID]time.Time
	// 每个对等节点的承诺；对于每个对等节点，我们跟踪承诺的消息ID。
	// 这个索引允许我们在对等节点被限制时快速取消承诺。
	peerPromises map[peer.ID]map[string]struct{}
}

// newGossipTracer 创建一个新的 gossipTracer 实例
// 返回值:
//   - *gossipTracer: 新创建的 gossipTracer 实例
func newGossipTracer() *gossipTracer {
	return &gossipTracer{
		idGen:        newMsgIdGenerator(),                    // 初始化消息ID生成器
		promises:     make(map[string]map[peer.ID]time.Time), // 初始化 promises 映射
		peerPromises: make(map[peer.ID]map[string]struct{}),  // 初始化 peerPromises 映射
	}
}

// Start 方法启动 gossipTracer
// 参数:
//   - gs: GossipSubRouter 实例
func (gt *gossipTracer) Start(gs *GossipSubRouter) {
	if gt == nil {
		return
	}

	gt.idGen = gs.p.idGen                         // 设置消息ID生成器
	gt.followUpTime = gs.params.IWantFollowupTime // 设置跟进时间
}

// AddPromise 方法用于跟踪给定对等节点对指定消息ID列表的传递承诺。
// 参数:
//   - p: 对等节点ID
//   - msgIDs: 消息ID列表
func (gt *gossipTracer) AddPromise(p peer.ID, msgIDs []string) {
	// 如果跟踪器为空，则直接返回
	if gt == nil {
		return
	}

	// 从消息ID列表中随机选择一个消息ID
	idx := rand.Intn(len(msgIDs))
	mid := msgIDs[idx]

	gt.Lock() // 加锁以确保并发安全
	defer gt.Unlock()

	// 获取该消息ID的承诺映射
	promises, ok := gt.promises[mid]
	if !ok {
		// 如果映射不存在，则创建一个新的空映射
		promises = make(map[peer.ID]time.Time)
		gt.promises[mid] = promises
	}

	// 检查是否已经存在对于该对等节点的承诺
	_, ok = promises[p]
	if !ok {
		// 如果不存在，则添加该对等节点和承诺到期时间
		promises[p] = time.Now().Add(gt.followUpTime)

		// 获取给定对等节点的消息ID集合映射
		peerPromises, ok := gt.peerPromises[p]
		if !ok {
			// 如果不存在，则创建一个新的空集合映射
			peerPromises = make(map[string]struct{})
			gt.peerPromises[p] = peerPromises
		}

		// 将该消息ID添加到对等节点的消息ID集合中
		peerPromises[mid] = struct{}{}
	}
}

// GetBrokenPromises 返回未跟进 IWANT 请求的每个对等节点的违约承诺数量
// 返回值:
//   - map[peer.ID]int: 每个对等节点的违约承诺数量
func (gt *gossipTracer) GetBrokenPromises() map[peer.ID]int {
	// 如果跟踪器为空，则返回 nil
	if gt == nil {
		return nil
	}

	gt.Lock() // 加锁以确保并发安全
	defer gt.Unlock()

	var res map[peer.ID]int // 存储违约承诺的映射
	now := time.Now()       // 获取当前时间

	// 遍历所有的承诺映射
	for mid, promises := range gt.promises {
		for p, expire := range promises {
			// 如果承诺已过期
			if expire.Before(now) {
				if res == nil {
					res = make(map[peer.ID]int)
				}
				res[p]++ // 增加对该对等节点的违约承诺计数

				delete(promises, p) // 删除过期的承诺

				peerPromises := gt.peerPromises[p] // 获取对等节点的消息ID集合映射
				delete(peerPromises, mid)          // 删除对等节点的该消息ID
				if len(peerPromises) == 0 {
					delete(gt.peerPromises, p) // 如果集合为空，则删除对等节点的映射
				}
			}
		}

		if len(promises) == 0 {
			delete(gt.promises, mid) // 如果消息ID的承诺映射为空，则删除该消息ID的映射
		}
	}

	return res // 返回违约承诺的映射
}

// fulfillPromise 方法履行消息的承诺
// 参数:
//   - msg: 消息
func (gt *gossipTracer) fulfillPromise(msg *Message) {
	mid := gt.idGen.ID(msg) // 获取消息的唯一标识符

	gt.Lock() // 加锁以确保并发安全
	defer gt.Unlock()

	promises, ok := gt.promises[mid] // 查找该消息的承诺映射
	if !ok {
		return // 如果没有找到承诺，则直接返回
	}
	delete(gt.promises, mid) // 从承诺映射中删除该消息的记录

	// 删除所有对等节点对该消息的承诺，因为它已经被履行
	for p := range promises {
		peerPromises, ok := gt.peerPromises[p] // 获取对等节点的消息ID集合映射
		if ok {
			delete(peerPromises, mid) // 从集合中删除该消息ID
			if len(peerPromises) == 0 {
				delete(gt.peerPromises, p) // 如果集合为空，则删除对等节点的映射
			}
		}
	}
}

// DeliverMessage 方法处理消息传递
// 参数:
//   - msg: 消息
func (gt *gossipTracer) DeliverMessage(msg *Message) {
	gt.fulfillPromise(msg) // 履行消息的承诺
}

// RejectMessage 方法处理消息拒绝
// 参数:
//   - msg: 消息
//   - reason: 拒绝原因
func (gt *gossipTracer) RejectMessage(msg *Message, reason string) {
	switch reason {
	case RejectMissingSignature: // 拒绝消息的原因常量
		return
	case RejectInvalidSignature: // 拒绝消息的原因常量
		return
	}

	gt.fulfillPromise(msg) // 履行消息的承诺
}

// ValidateMessage 方法处理消息验证
// 参数:
//   - msg: 消息
func (gt *gossipTracer) ValidateMessage(msg *Message) {
	gt.fulfillPromise(msg) // 在消息开始验证时履行承诺
}

func (gt *gossipTracer) AddPeer(p peer.ID, proto protocol.ID) {}
func (gt *gossipTracer) RemovePeer(p peer.ID)                 {}
func (gt *gossipTracer) Join(topic string)                    {}
func (gt *gossipTracer) Leave(topic string)                   {}
func (gt *gossipTracer) Graft(p peer.ID, topic string)        {}
func (gt *gossipTracer) Prune(p peer.ID, topic string)        {}
func (gt *gossipTracer) DuplicateMessage(msg *Message)        {}
func (gt *gossipTracer) RecvRPC(rpc *RPC)                     {}
func (gt *gossipTracer) SendRPC(rpc *RPC, p peer.ID)          {}
func (gt *gossipTracer) DropRPC(rpc *RPC, p peer.ID)          {}
func (gt *gossipTracer) UndeliverableMessage(msg *Message)    {}

// ThrottlePeer 方法限制对等节点
// 参数:
//   - p: 对等节点ID
func (gt *gossipTracer) ThrottlePeer(p peer.ID) {
	gt.Lock() // 加锁以确保并发安全
	defer gt.Unlock()

	peerPromises, ok := gt.peerPromises[p] // 获取对等节点的消息ID集合映射
	if !ok {
		return // 如果对等节点没有承诺，则直接返回
	}

	// 删除对等节点对所有消息的承诺
	for mid := range peerPromises {
		promises := gt.promises[mid] // 获取消息ID对应的承诺映射
		delete(promises, p)          // 删除对等节点的承诺
		if len(promises) == 0 {
			delete(gt.promises, mid) // 如果该消息没有其他承诺，删除对应的消息ID映射
		}
	}

	delete(gt.peerPromises, p) // 最后删除对等节点的消息ID集合映射
}
