// 作用：对等节点通知管理。
// 功能：处理对等节点的连接和断开通知，维护对等节点的状态。

package pubsub

import (
	"context"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/sirupsen/logrus"
)

// watchForNewPeers 监听新的 peers 加入
// 参数:
//   - ctx: context 上下文，用于控制 goroutine 的生命周期
func (ps *PubSub) watchForNewPeers(ctx context.Context) {
	// 订阅 peer 识别完成和协议更新事件
	sub, err := ps.host.EventBus().Subscribe([]interface{}{
		&event.EvtPeerIdentificationCompleted{},
		&event.EvtPeerProtocolsUpdated{},
	})
	if err != nil {
		// 订阅失败时，记录错误日志并返回
		logrus.Errorf("订阅 peer 识别事件失败: %v", err)
		return
	}
	defer sub.Close() // 确保在函数结束时关闭订阅

	// 锁定 newPeersPrioLk 进行读操作，防止并发修改
	ps.newPeersPrioLk.RLock()
	ps.newPeersMx.Lock()
	// 遍历当前已连接的 peers 并添加到 newPeersPend 中
	for _, pid := range ps.host.Network().Peers() {
		if ps.host.Network().Connectedness(pid) != network.Connected {
			continue // 跳过未连接的 peers
		}
		ps.newPeersPend[pid] = struct{}{} // 将已连接的 peer 添加到 newPeersPend 中
	}
	ps.newPeersMx.Unlock()
	ps.newPeersPrioLk.RUnlock()

	// 向 newPeers 通道发送一个空结构体，以通知有新的 peers 加入
	select {
	case ps.newPeers <- struct{}{}:
	default:
	}

	// 根据是否有 protoMatchFunc 来决定如何检查支持的协议
	var supportsProtocol func(protocol.ID) bool
	if ps.protoMatchFunc != nil {
		var supportedProtocols []func(protocol.ID) bool
		// 遍历每个协议，应用 protoMatchFunc
		for _, proto := range ps.rt.Protocols() {
			supportedProtocols = append(supportedProtocols, ps.protoMatchFunc(proto))
		}
		// 定义 supportsProtocol 函数，检查协议是否被支持
		supportsProtocol = func(proto protocol.ID) bool {
			for _, fn := range supportedProtocols {
				if fn(proto) {
					return true
				}
			}
			return false
		}
	} else {
		// 如果没有 protoMatchFunc，使用默认的支持协议集合
		supportedProtocols := make(map[protocol.ID]struct{})
		// 遍历每个协议，将其添加到 supportedProtocols 集合中
		for _, proto := range ps.rt.Protocols() {
			supportedProtocols[proto] = struct{}{}
		}
		// 定义 supportsProtocol 函数，检查协议是否在 supportedProtocols 集合中
		supportsProtocol = func(proto protocol.ID) bool {
			_, ok := supportedProtocols[proto]
			return ok
		}
	}

	// 持续监听新的 peer 事件
	for ctx.Err() == nil {
		var ev any // 定义事件变量 ev，用于存储接收到的事件
		select {
		case <-ctx.Done(): // 如果上下文被取消，退出循环
			return
		case ev = <-sub.Out(): // 从订阅中接收事件
		}

		var protos []protocol.ID // 定义协议列表，用于存储事件中的协议
		var peer peer.ID         // 定义 peer ID，用于存储事件中的 peer
		// 根据事件类型获取 peer 和协议列表
		switch ev := ev.(type) {
		case event.EvtPeerIdentificationCompleted: // 如果事件是 EvtPeerIdentificationCompleted
			peer = ev.Peer        // 获取 peer ID
			protos = ev.Protocols // 获取协议列表
		case event.EvtPeerProtocolsUpdated: // 如果事件是 EvtPeerProtocolsUpdated
			peer = ev.Peer    // 获取 peer ID
			protos = ev.Added // 获取新增的协议列表
		default: // 如果事件类型不匹配
			continue // 跳过该事件
		}

		// 检查新的协议是否被支持
		for _, p := range protos { // 遍历协议列表中的每个协议
			if supportsProtocol(p) { // 如果协议被支持
				ps.notifyNewPeer(peer) // 通知有新的 peer 加入
				break                  // 退出循环
			}
		}
	}
}

// notifyNewPeer 通知有新的 peer 加入
// 参数:
//   - peer: peer.ID 新加入的 peer
func (ps *PubSub) notifyNewPeer(peer peer.ID) {
	ps.newPeersPrioLk.RLock() // 锁定 newPeersPrioLk 进行读操作，防止并发修改
	ps.newPeersMx.Lock()
	ps.newPeersPend[peer] = struct{}{}
	ps.newPeersMx.Unlock()
	ps.newPeersPrioLk.RUnlock()

	// 向 newPeers 通道发送一个空结构体，以通知有新的 peers 加入
	select {
	case ps.newPeers <- struct{}{}:
	default:
	}
}
