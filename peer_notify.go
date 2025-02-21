// 作用：对等节点通知管理。
// 功能：处理对等节点的连接和断开通知，维护对等节点的状态。

package pubsub

import (
	"context"
	"fmt"

	"github.com/dep2p/go-dep2p/core/event"
	"github.com/dep2p/go-dep2p/core/network"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/core/protocol"
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
		logger.Errorf("订阅 peer 识别事件失败: %v", err)
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

// NotifyNewPeer 通知系统有新的对等节点加入
// 参数:
//   - peer: 新加入节点的ID
//
// 返回值:
//   - error: 如果节点不满足要求则返回错误
func (ps *PubSub) NotifyNewPeer(peer peer.ID) error {
	// 1. 检查 PubSub 是否已初始化
	if ps == nil {
		logger.Error("PubSub 未初始化")
		return fmt.Errorf("PubSub 未初始化")
	}

	// 2. 检查节点 ID 是否有效
	if peer == "" {
		logger.Error("无效的节点 ID")
		return fmt.Errorf("无效的节点 ID")
	}

	// 3. 检查连接状态
	if ps.host.Network().Connectedness(peer) != network.Connected {
		logger.Errorf("节点 %s 未连接", peer)
		return fmt.Errorf("节点 %s 未连接", peer.String())
	}

	// 4. 检查协议支持
	protos, err := ps.host.Peerstore().GetProtocols(peer)
	if err != nil {
		logger.Errorf("获取节点 %s 的协议失败: %v", peer, err)
		return fmt.Errorf("获取节点 %s 的协议失败", peer.String())
	}

	if len(protos) == 0 {
		logger.Errorf("节点 %s 没有支持的协议", peer)
		return fmt.Errorf("节点 %s 没有支持的协议", peer.String())
	}

	// 定义协议匹配函数
	var supportsProtocol func(protocol.ID) bool
	if ps.protoMatchFunc != nil {
		// 如果存在自定义的协议匹配函数，使用它来构建支持的协议列表
		var supportedProtocols []func(protocol.ID) bool
		for _, proto := range ps.rt.Protocols() {
			supportedProtocols = append(supportedProtocols, ps.protoMatchFunc(proto))
		}

		// 定义协议匹配检查函数
		supportsProtocol = func(proto protocol.ID) bool {
			for _, fn := range supportedProtocols {
				if fn(proto) {
					return true
				}
			}
			return false
		}
	} else {
		// 如果没有自定义匹配函数，使用简单的协议ID匹配
		supportedProtocols := make(map[protocol.ID]struct{})
		for _, proto := range ps.rt.Protocols() {
			supportedProtocols[proto] = struct{}{}
		}

		supportsProtocol = func(proto protocol.ID) bool {
			_, ok := supportedProtocols[proto]
			return ok
		}
	}

	// 检查节点是否支持任一所需协议
	var supported bool
	for _, p := range protos {
		if supportsProtocol(p) {
			supported = true
			break
		}
	}

	if !supported {
		logger.Errorf("节点 %s 不支持任何所需协议", peer)
		return fmt.Errorf("节点 %s 不支持任何所需协议", peer.String())
	}

	// 5. 使用读写锁保护并发访问
	ps.newPeersPrioLk.RLock()
	defer ps.newPeersPrioLk.RUnlock()

	ps.newPeersMx.Lock()
	defer ps.newPeersMx.Unlock()

	// 6. 检查节点是否已在待处理列表中
	if _, ok := ps.newPeersPend[peer]; ok {
		logger.Warnf("节点 %s 已在待处理列表中", peer)
		return fmt.Errorf("节点 %s 已在待处理列表中", peer.String())
	}

	// 7. 添加到待处理列表
	ps.newPeersPend[peer] = struct{}{}
	logger.Debugf("节点 %s 已添加到待处理列表", peer.String())

	// 8. 通知处理循环
	select {
	case ps.newPeers <- struct{}{}:
		// logger.Infof("已通知处理循环新节点 %s 的加入", peer.String())
	default:
		// logger.Warnf("通知通道已满，节点 %s 将在下一轮处理", peer.String())
	}

	return nil
}
