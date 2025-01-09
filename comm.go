// 作用：处理pubsub消息的通信部分。
// 功能：定义消息传输的接口和实现，处理消息的发送和接收。

package pubsub

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	logging "github.com/dep2p/log"
	pb "github.com/dep2p/pubsub/pb"
	"github.com/gogo/protobuf/proto"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"

	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/libp2p/go-msgio"
)

var logger = logging.Logger("pubsub")

// getHelloPacket 方法获取包含所有订阅信息的初始RPC包，发送给新连接的节点
// 返回值:
//   - *RPC: 包含当前所有订阅信息的RPC包
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC

	subscriptions := make(map[string]bool) // 订阅集合，存储所有的订阅主题

	// 收集当前节点的所有订阅主题
	for t := range p.mySubs {
		subscriptions[t] = true
	}

	// 收集当前节点的所有中继主题
	for t := range p.myRelays {
		subscriptions[t] = true
	}

	// 将收集到的订阅主题添加到RPC包中
	for t := range subscriptions {
		as := &pb.RPC_SubOpts{
			Topicid:   *proto.String(t),  // 设置主题ID
			Subscribe: *proto.Bool(true), // 设置订阅标志为true
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as) // 将订阅选项添加到RPC的Subscriptions列表中
	}
	return &rpc // 返回构建好的RPC包
}

// handleNewStream 方法处理新建的流连接
// 参数:
//   - s: 新建的网络流
func (p *PubSub) handleNewStream(s network.Stream) {
	peer := s.Conn().RemotePeer() // 获取远端节点的ID

	// 处理重复的入站流
	p.inboundStreamsMx.Lock()            // 加锁保护对入站流映射的访问
	other, dup := p.inboundStreams[peer] // 检查是否已有来自该节点的入站流
	if dup {
		logger.Debugf("重复的入站流来自 %s; 重置旧的重复流", peer)
		other.Reset() // 如果有重复的入站流，重置旧的重复流
	}
	p.inboundStreams[peer] = s  // 将新建的流添加到入站流映射中
	p.inboundStreamsMx.Unlock() // 解锁

	defer func() { // 使用defer语句确保在方法返回前执行清理操作
		p.inboundStreamsMx.Lock() // 加锁保护对入站流映射的访问
		if p.inboundStreams[peer] == s {
			delete(p.inboundStreams, peer) // 如果流没有被其他流替换，删除断开的流记录
		}
		p.inboundStreamsMx.Unlock() // 解锁
	}()

	r := msgio.NewVarintReaderSize(s, p.maxMessageSize) // 创建带有变长整数读取器的消息读取器，读取消息时自动处理消息大小
	for {
		msgbytes, err := r.ReadMsg() // 从流中读取消息
		if err != nil {
			r.ReleaseMsg(msgbytes) // 释放消息缓冲区
			if err != io.EOF {     // 如果不是正常的流结束错误
				s.Reset()                                                       // 重置流
				logger.Debugf("从 %s 读取 RPC 失败: %s", s.Conn().RemotePeer(), err) // 记录读取错误
			} else {
				// 友好关闭连接
				s.Close() // 关闭流
			}
			return // 退出方法
		}
		if len(msgbytes) == 0 { // 如果消息长度为0，继续读取下一条消息
			continue
		}

		rpc := new(RPC)               // 创建一个新的RPC消息
		err = rpc.Unmarshal(msgbytes) // 解码消息字节到RPC对象
		r.ReleaseMsg(msgbytes)        // 释放消息缓冲区
		if err != nil {
			s.Reset()                                                     // 重置流
			logger.Warnf("从 %s 读取无效 RPC: %s", s.Conn().RemotePeer(), err) // 记录无效RPC错误
			return                                                        // 退出方法
		}

		rpc.from = peer // 设置消息的来源节点ID
		select {
		case p.incoming <- rpc: // 将RPC消息发送到incoming通道
		case <-p.ctx.Done(): // 如果上下文完成，意味着PubSub停止工作
			// 关闭流，因为对方不再读取
			s.Reset() // 重置流
			return    // 退出方法
		}
	}
}

// notifyPeerDead 方法通知节点死亡事件
// 参数:
//   - pid: 节点ID
func (p *PubSub) notifyPeerDead(pid peer.ID) {
	// 读取优先级锁以确保peerDeadPend可以安全访问
	p.peerDeadPrioLk.RLock()
	// 锁定peerDeadMx以便安全地修改peerDeadPend映射
	p.peerDeadMx.Lock()
	// 将节点ID添加到peerDeadPend映射中，标记节点为死亡
	p.peerDeadPend[pid] = struct{}{}
	// 解除对peerDeadMx的锁定
	p.peerDeadMx.Unlock()
	// 解除对peerDeadPrioLk的读取锁定
	p.peerDeadPrioLk.RUnlock()

	// 尝试向peerDead通道发送一个空的结构体通知
	select {
	case p.peerDead <- struct{}{}:
	default:
		// 如果peerDead通道已满，则不进行任何操作
	}
}

// handleNewPeer 方法处理新节点的连接
// 参数:
//   - ctx: 上下文
//   - pid: 新节点ID
//   - outgoing: 发往新节点的RPC消息通道
func (p *PubSub) handleNewPeer(ctx context.Context, pid peer.ID, outgoing <-chan *RPC) {
	// 尝试建立到新节点的流连接
	s, err := p.host.NewStream(p.ctx, pid, p.rt.Protocols()...)
	if err != nil {
		logger.Debugf("打开新流到对等节点失败: %s", err)

		// 如果创建流连接失败，发送新节点错误到newPeerError通道
		select {
		case p.newPeerError <- pid:
		case <-ctx.Done():
			// 如果上下文已完成，退出
		}

		return
	}

	// 启动协程处理发送消息到新节点
	go handleSendingMessages(ctx, s, outgoing)
	// 启动协程处理节点死亡事件
	go p.handlePeerDead(s)

	// 将新建立的流发送到newPeerStream通道
	select {
	case p.newPeerStream <- s:
	case <-ctx.Done():
		// 如果上下文已完成，退出
	}
}

// handleNewPeerWithBackoff 方法处理带有退避的节点连接
// 参数:
//   - ctx: 上下文
//   - pid: 节点ID
//   - backoff: 退避时间
//   - outgoing: 发往节点的RPC消息通道
func (p *PubSub) handleNewPeerWithBackoff(ctx context.Context, pid peer.ID, backoff time.Duration, outgoing <-chan *RPC) {
	select {
	case <-time.After(backoff): // 等待退避时间
		p.handleNewPeer(ctx, pid, outgoing)
	case <-ctx.Done():
		return
	}
}

// handlePeerDead 方法处理节点死亡事件
// 参数:
//   - s: 网络流
func (p *PubSub) handlePeerDead(s network.Stream) {
	pid := s.Conn().RemotePeer() // 获取远端节点ID

	_, err := s.Read([]byte{0})
	if err == nil {
		logger.Debugf("从 %s 收到意外消息", pid)
	}

	s.Reset()             // 重置流
	p.notifyPeerDead(pid) // 通知节点死亡
}

// handleSendingMessages 方法处理发送消息到节点
// 参数:
//   - ctx: 上下文
//   - s: 网络流
//   - outgoing: 发往节点的RPC消息通道
func handleSendingMessages(ctx context.Context, s network.Stream, outgoing <-chan *RPC) {
	// 定义内部函数 writeRpc 用于写入RPC消息
	writeRpc := func(rpc *RPC) error {
		size := uint64(rpc.Size()) // 获取RPC消息的大小

		buf := pool.Get(varint.UvarintSize(size) + int(size)) // 从池中获取缓冲区
		defer pool.Put(buf)                                   // 使用完毕后将缓冲区放回池中

		n := binary.PutUvarint(buf, size) // 将消息大小编码为变长整数并写入缓冲区
		_, err := rpc.MarshalTo(buf[n:])  // 将RPC消息序列化到缓冲区
		if err != nil {
			return err // 如果序列化出错，返回错误
		}

		_, err = s.Write(buf) // 将缓冲区中的数据写入网络流
		return err            // 返回写入操作的错误（如果有）
	}

	defer s.Close() // 函数结束时关闭流
	for {
		select {
		case rpc, ok := <-outgoing: // 从 outgoing 通道接收RPC消息
			if !ok { // 如果通道已关闭
				return
			}

			err := writeRpc(rpc) // 调用 writeRpc 写入RPC消息
			if err != nil {
				s.Reset()                                                    // 如果写入失败，重置流
				logger.Debugf("写入消息到 %s 失败: %s", s.Conn().RemotePeer(), err) // 记录写入错误
				return
			}
		case <-ctx.Done(): // 如果上下文完成
			return
		}
	}
}

// rpcWithSubs 方法创建带有订阅选项的RPC消息
// 参数:
//   - subs: 订阅选项
//
// 返回值:
//   - *RPC: 带有订阅选项的RPC消息
func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{ // 返回一个RPC指针
		RPC: pb.RPC{ // 初始化RPC结构体
			Subscriptions: subs, // 将传入的订阅选项赋值给Subscriptions字段
		},
	}
}

// rpcWithMessages 方法创建带有消息的RPC消息
// 参数:
//   - msgs: 消息
//
// 返回值:
//   - *RPC: 带有消息的RPC消息
func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{ // 返回一个RPC指针
		RPC: pb.RPC{ // 初始化RPC结构体
			Publish: msgs, // 将传入的消息赋值给Publish字段
		},
	}
}

// rpcWithControl 方法创建带有控制信息的RPC消息
// 参数:
//   - msgs: 消息
//   - ihave: Ihave控制消息
//   - iwant: Iwant控制消息
//   - graft: Graft控制消息
//   - prune: Prune控制消息
//
// 返回值:
//   - *RPC: 带有控制信息的RPC消息
func rpcWithControl(
	msgs []*pb.Message, // 发布的消息列表
	ihave []*pb.ControlIHave, // Ihave控制消息列表
	iwant []*pb.ControlIWant, // Iwant控制消息列表
	graft []*pb.ControlGraft, // Graft控制消息列表
	prune []*pb.ControlPrune, // Prune控制消息列表
) *RPC {
	return &RPC{ // 返回一个RPC指针
		RPC: pb.RPC{ // 初始化RPC结构体
			Publish: msgs, // 将传入的消息赋值给Publish字段
			Control: &pb.ControlMessage{ // 初始化Control字段为ControlMessage指针
				Ihave: ihave, // 将传入的Ihave控制消息列表赋值给Ihave字段
				Iwant: iwant, // 将传入的Iwant控制消息列表赋值给Iwant字段
				Graft: graft, // 将传入的Graft控制消息列表赋值给Graft字段
				Prune: prune, // 将传入的Prune控制消息列表赋值给Prune字段
			},
		},
	}
}

// copyRPC 方法复制一个RPC消息
// 参数:
//   - rpc: 要复制的RPC消息
//
// 返回值:
//   - *RPC: 复制后的RPC消息
func copyRPC(rpc *RPC) *RPC {
	res := new(RPC)         // 创建一个新的RPC对象
	*res = *rpc             // 将传入的RPC对象的值赋值给新创建的RPC对象
	if rpc.Control != nil { // 如果Control字段不为空
		res.Control = new(pb.ControlMessage) // 为新对象的Control字段分配内存
		*res.Control = *rpc.Control          // 复制Control字段的值
	}
	return res // 返回复制后的RPC对象
}
