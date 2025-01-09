// 作用：管理主题。
// 功能：定义和管理pubsub系统中的主题，包括主题的订阅和发布。

package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/network"
	"github.com/google/uuid"

	"github.com/dep2p/libp2p/core/peer"
)

// ErrTopicClosed 表示如果在主题关闭后使用 Topic，将返回此错误。
var ErrTopicClosed = errors.New("主题已关闭，请打开一个新的主题")

// ErrNilSignKey 表示如果提供了一个空的私钥，将返回此错误。
var ErrNilSignKey = errors.New("空的私钥")

// ErrEmptyPeerID 表示如果提供了一个空的对等节点 ID，将返回此错误。
var ErrEmptyPeerID = errors.New("空的对等节点 ID")

// Topic 表示 pubsub 主题的句柄。
type Topic struct {
	p     *PubSub // PubSub 实例
	topic string  // 主题名称

	evtHandlerMux sync.RWMutex                    // 事件处理程序的读写锁
	evtHandlers   map[*TopicEventHandler]struct{} // 事件处理程序的集合

	mux    sync.RWMutex // 主题的读写锁
	closed bool         // 主题是否已关闭
}

// String 返回与 t 关联的主题。
// 返回值:
// - string: 主题名称
func (t *Topic) String() string {
	return t.topic // 返回主题名称
}

// SetScoreParams 设置主题的评分参数，如果 pubsub 路由器支持对等评分。
// 参数:
// - p: *TopicScoreParams 评分参数
// 返回值:
// - error: 错误信息，如果有的话
func (t *Topic) SetScoreParams(p *TopicScoreParams) error {
	err := p.validate() // 验证评分参数
	if err != nil {
		logger.Warnf("主题评分参数无效: %s", err)      // 如果评分参数无效，返回错误
		return fmt.Errorf("主题评分参数无效: %w", err) // 如果评分参数无效，返回错误
	}

	t.mux.Lock()         // 加锁以防止并发修改
	defer t.mux.Unlock() // 在函数返回前解锁

	if t.closed {
		return ErrTopicClosed // 如果主题已关闭，返回错误
	}

	result := make(chan error, 1) // 创建一个结果通道，用于接收更新操作的结果
	update := func() {            // 定义一个更新函数
		gs, ok := t.p.rt.(*GossipSubRouter) // 检查路由器是否为GossipSubRouter
		if !ok {
			logger.Warnf("pubsub 路由器不是 gossipsub")         // 如果不是GossipSubRouter，发送错误
			result <- fmt.Errorf("pubsub 路由器不是 gossipsub") // 如果不是GossipSubRouter，发送错误
			return
		}

		if gs.score == nil {
			logger.Warnf("对等评分在路由器中未启用")         // 如果未启用对等评分，发送错误
			result <- fmt.Errorf("对等评分在路由器中未启用") // 如果未启用对等评分，发送错误
			return
		}

		err := gs.score.SetTopicScoreParams(t.topic, p) // 设置主题评分参数
		result <- err                                   // 发送操作结果
	}

	select {
	case t.p.eval <- update: // 提交更新操作到评估通道
		err = <-result // 等待并接收结果
		return err     // 返回结果
	case <-t.p.ctx.Done(): // 如果上下文已关闭，返回上下文错误
		return t.p.ctx.Err()
	}
}

// EventHandler 创建特定主题事件的句柄。
// 参数:
// - opts: ...TopicEventHandlerOpt 事件处理程序选项
// 返回值:
// - *TopicEventHandler: 创建的事件处理程序
// - error: 错误信息，如果有的话
func (t *Topic) EventHandler(opts ...TopicEventHandlerOpt) (*TopicEventHandler, error) {
	t.mux.RLock()         // 加读锁，确保并发安全
	defer t.mux.RUnlock() // 在函数返回前解锁
	if t.closed {
		logger.Warnf("主题已关闭")      // 如果主题已关闭，返回错误
		return nil, ErrTopicClosed // 如果主题已关闭，返回错误
	}

	h := &TopicEventHandler{
		topic:    t,                           // 设置事件处理程序的主题
		err:      nil,                         // 初始化错误为空
		evtLog:   make(map[peer.ID]EventType), // 初始化事件日志
		evtLogCh: make(chan struct{}, 1),      // 创建事件日志通道，带缓冲
	}

	for _, opt := range opts { // 遍历所有事件处理程序选项并应用
		err := opt(h) // 应用事件处理程序选项
		if err != nil {
			logger.Warnf("应用事件处理程序选项失败: %s", err) // 如果应用选项出错，返回错误
			return nil, err                       // 如果应用选项出错，返回错误
		}
	}

	done := make(chan struct{}, 1) // 创建一个完成通道

	select {
	case t.p.eval <- func() { // 提交事件处理程序的初始化操作
		tmap := t.p.topics[t.topic] // 获取订阅当前主题的对等节点集合
		for p := range tmap {       // 遍历所有对等节点
			h.evtLog[p] = PeerJoin // 将对等节点加入事件日志
		}

		t.evtHandlerMux.Lock()        // 锁定事件处理程序互斥锁
		t.evtHandlers[h] = struct{}{} // 将新事件处理程序添加到事件处理程序集合中
		t.evtHandlerMux.Unlock()      // 解锁事件处理程序互斥锁
		done <- struct{}{}            // 发送完成信号
	}:
	case <-t.p.ctx.Done(): // 如果上下文已关闭，返回上下文错误
		logger.Warnf("上下文已关闭") // 如果上下文已关闭，返回上下文错误
		return nil, t.p.ctx.Err()
	}

	<-done // 等待完成信号

	return h, nil // 返回创建的事件处理程序
}

// sendNotification 向事件处理程序发送通知。
// 参数:
// - evt: PeerEvent 需要通知的事件
func (t *Topic) sendNotification(evt PeerEvent) {
	t.evtHandlerMux.RLock()         // 加读锁，确保并发安全
	defer t.evtHandlerMux.RUnlock() // 在函数返回前解锁

	for h := range t.evtHandlers { // 遍历所有事件处理程序
		h.sendNotification(evt) // 向每个事件处理程序发送通知
	}
}

// Subscribe 返回该主题的新订阅。
// 注意订阅并不是瞬时操作。在 pubsub 主循环处理并传播给我们的对等节点之前，可能需要一些时间。
// 参数:
// - opts: ...SubOpt 订阅选项
// 返回值:
// - *Subscription: 创建的订阅
// - error: 错误信息，如果有的话
func (t *Topic) Subscribe(opts ...SubOpt) (*Subscription, error) {
	t.mux.RLock()         // 加读锁，确保并发安全
	defer t.mux.RUnlock() // 在函数返回前解锁
	if t.closed {
		logger.Warnf("主题已关闭")      // 如果主题已关闭，返回错误
		return nil, ErrTopicClosed // 如果主题已关闭，返回错误
	}

	sub := &Subscription{
		topic: t.topic, // 设置订阅的主题
		ctx:   t.p.ctx, // 设置订阅的上下文
	}

	for _, opt := range opts { // 遍历所有订阅选项并应用
		err := opt(sub) // 应用订阅选项
		if err != nil {
			logger.Warnf("应用订阅选项失败: %s", err) // 如果应用选项出错，返回错误
			return nil, err                   // 如果应用选项出错，返回错误
		}
	}

	if sub.ch == nil { // 如果订阅通道为空
		// 应用默认大小
		sub.ch = make(chan *Message, 32) // 创建一个带缓冲的订阅通道，默认大小为32
	}

	out := make(chan *Subscription, 1) // 创建一个输出通道，用于接收创建的订阅

	t.p.disc.Discover(sub.topic) // 通过发现机制发现订阅的主题

	select {
	case t.p.addSub <- &addSubReq{
		sub:  sub, // 设置要添加的订阅
		resp: out, // 设置响应通道
	}:
	case <-t.p.ctx.Done(): // 如果上下文已关闭，返回上下文错误
		logger.Warnf("上下文已关闭") // 如果上下文已关闭，返回上下文错误
		return nil, t.p.ctx.Err()
	}

	return <-out, nil // 返回创建的订阅
}

// Relay 启用主题的消息中继并返回引用取消函数。
// 随后的调用增加引用计数器。
// 要完全禁用中继，必须取消所有引用。
// 返回值:
// - RelayCancelFunc: 取消中继的函数
// - error: 错误信息，如果有的话
func (t *Topic) Relay() (RelayCancelFunc, error) {
	t.mux.RLock()         // 加读锁，确保并发安全
	defer t.mux.RUnlock() // 在函数返回前解锁
	if t.closed {
		logger.Warnf("主题已关闭")      // 如果主题已关闭，返回错误
		return nil, ErrTopicClosed // 如果主题已关闭，返回错误
	}

	out := make(chan RelayCancelFunc, 1) // 创建一个输出通道，用于接收取消中继函数

	t.p.disc.Discover(t.topic) // 通过发现机制发现主题

	select {
	case t.p.addRelay <- &addRelayReq{
		topic: t.topic, // 设置要添加中继的主题
		resp:  out,     // 设置响应通道
	}:
	case <-t.p.ctx.Done(): // 如果上下文已关闭，返回上下文错误
		logger.Warnf("上下文已关闭") // 如果上下文已关闭，返回上下文错误
		return nil, t.p.ctx.Err()
	}

	return <-out, nil // 返回取消中继函数
}

// RouterReady 是一个函数，用于决定路由器是否准备好发布。
type RouterReady func(rt PubSubRouter, topic string) (bool, error)

// ProvideKey 是一个函数，在发布新消息时提供私钥及其关联的对等节点 ID。
type ProvideKey func() (crypto.PrivKey, peer.ID)

// PublishOptions 表示发布选项。
type PublishOptions struct {
	ready     RouterReady        // 准备好路由的回调函数
	customKey ProvideKey         // 自定义密钥的提供函数
	local     bool               // 是否为本地发布
	targetMap []peer.ID          // 目标节点列表
	metadata  MessageMetadataOpt // 消息元信息
}

// MessageMetadataOpt 表示消息元信息的选项。
type MessageMetadataOpt struct {
	messageID string                         // 消息ID
	msgType   pb.MessageMetadata_MessageType // 消息类型（请求或响应）
}

// PubOpt 定义发布选项的类型。
type PubOpt func(pub *PublishOptions) error

// PublishWithReply 发送消息并等待响应
//
// 使用示例:
//
//	// 不指定目标节点
//	reply, err := topic.PublishWithReply(ctx, data)
//
//	// 指定一个目标节点
//	reply, err := topic.PublishWithReply(ctx, data, peerID1)
//
//	// 指定多个目标节点
//	reply, err := topic.PublishWithReply(ctx, data, peerID1, peerID2, peerID3)
//
// 参数:
// - ctx: context.Context 表示上下文，用于控制流程
// - data: []byte 表示要发送的消息内容
// - targetNodes: ...peer.ID 表示需要将消息发送到的目标节点列表（可选）
// 返回值:
// - []byte: 接收到的回复消息
// - error: 如果出现错误，返回错误信息
// func (t *Topic) PublishWithReply(ctx context.Context, data []byte, targetNodes ...peer.ID) ([]byte, error) {
// 	msgID := uuid.New().String()      // 生成一个唯一的消息 ID，用于匹配回复
// 	replyChan := make(chan []byte, 1) // 创建一个带缓冲的通道，用于接收回复

// 	t.mux.Lock()                   // 加锁以保护共享数据结构的访问
// 	t.p.replies[msgID] = replyChan // 将消息 ID 和通道关联存储到 replies map 中
// 	t.mux.Unlock()                 // 解锁

// 	var err error
// 	for i := 0; i < t.p.retry; i++ { // 循环执行重试机制
// 		pubOpts := []PubOpt{
// 			WithReadiness(MinTopicSize(numHosts)),                  // 设置对等节点准备
// 			WithMessageMetadata(msgID, pb.MessageMetadata_REQUEST), // 设置消息元数据
// 		}

// 		// 只有在提供了目标节点时才添加 WithTargetMap 选项
// 		if len(targetNodes) > 0 {
// 			pubOpts = append(pubOpts, WithTargetMap(targetNodes))
// 		}

// 		err = t.Publish(ctx, data, pubOpts...)
// 		if err == nil { // 如果发布成功，退出循环
// 			break
// 		}
// 		logger.Printf("第 %d 次发布尝试失败: %v", i+1, err)
// 		time.Sleep(500 * time.Millisecond) // 在重试之间添加短暂的延迟
// 	}

// 	if err != nil { // 如果经过多次重试仍然失败
// 		return nil, fmt.Errorf("多次重试后发布消息失败: %d 次重试后失败: %v", t.p.retry, err)
// 	}

// 	select {
// 	case reply := <-replyChan: // 如果收到回复
// 		return reply, nil // 返回收到的回复
// 	case <-time.After(t.p.timeout): // 如果等待超时
// 		return nil, errors.New("等待回复超时")
// 	}
// }

// PublishWithReply 发送消息并等待响应
//
// 使用示例:
//
//	// 不指定目标节点
//	reply, err := topic.PublishWithReply(ctx, data)
//
//	// 指定一个目标节点
//	reply, err := topic.PublishWithReply(ctx, data, peerID1)
//
//	// 指定多个目标节点
//	reply, err := topic.PublishWithReply(ctx, data, peerID1, peerID2, peerID3)
//
// 参数:
// - ctx: context.Context 表示上下文，用于控制流程
// - data: []byte 表示要发送的消息内容
// - targetNodes: ...peer.ID 表示需要将消息发送到的目标节点列表（可选）
// 返回值:
// - []byte: 接收到的回复消息
// - error: 如果出现错误，返回错误信息
func (t *Topic) PublishWithReply(ctx context.Context, data []byte, targetNodes ...peer.ID) ([]byte, error) {
	// 基础配置
	const (
		retryCount  = 3                // 重试次数
		retryDelay  = 2 * time.Second  // 重试延迟
		baseTimeout = 5 * time.Second  // 基础超时时间
		maxTimeout  = 60 * time.Second // 最大超时时间
	)

	logger.Infof("开始发布消息并等待回复，目标节点数量: %d", len(targetNodes))

	// 上下文检查
	if ctx == nil {
		ctx = context.Background()
		logger.Info("使用默认上下文")
	}

	// 创建带超时的上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, maxTimeout)
	defer cancel()

	// 生成消息ID和回复通道
	msgID := uuid.New().String()
	replyChan := make(chan []byte, 1)
	logger.Infof("生成消息ID: %s", msgID)

	// 注册回复通道并确保清理
	t.mux.Lock()
	if t.closed {
		t.mux.Unlock()
		logger.Error("主题已关闭")
		return nil, ErrTopicClosed
	}
	t.p.replies[msgID] = replyChan
	t.mux.Unlock()

	defer func() {
		t.mux.Lock()
		delete(t.p.replies, msgID)
		t.mux.Unlock()
		logger.Infof("清理消息ID: %s 的回复通道", msgID)
	}()

	// 检查并过滤目标节点
	var validTargets []peer.ID
	if len(targetNodes) > 0 {
		logger.Info("开始验证目标节点")
		for _, target := range targetNodes {
			// 检查节点连接状态
			switch t.p.host.Network().Connectedness(target) {
			case network.Connected:
				validTargets = append(validTargets, target)
				logger.Infof("节点 %s 已连接", target)
			default:
				logger.Infof("尝试连接节点 %s", target)
				// 尝试连接
				if err := t.p.host.Connect(timeoutCtx, peer.AddrInfo{ID: target}); err == nil {
					validTargets = append(validTargets, target)
					logger.Infof("成功连接节点 %s", target)
				} else {
					logger.Warnf("连接节点 %s 失败: %v", target, err)
				}
			}
		}
		// 如果没有有效的目标节点
		if len(validTargets) == 0 {
			logger.Error("没有可用的目标节点")
			return nil, fmt.Errorf("没有可用的目标节点")
		}
		logger.Infof("有效目标节点数量: %d", len(validTargets))
	}

	// 发布消息（带重试）
	publishWithRetry := func() error {
		for i := 0; i < retryCount; i++ {
			select {
			case <-timeoutCtx.Done():
				logger.Error("发布超时")
				return timeoutCtx.Err()
			default:
				logger.Infof("开始第 %d/%d 次发布尝试", i+1, retryCount)
				// 构建发布选项
				pubOpts := []PubOpt{
					WithMessageMetadata(msgID, pb.MessageMetadata_REQUEST),
				}

				// 添加目标节点（如果有）
				if len(validTargets) > 0 {
					pubOpts = append(pubOpts, WithTargetMap(validTargets))
				}

				// 发布消息
				if err := t.Publish(timeoutCtx, data, pubOpts...); err == nil {
					logger.Infof("第 %d 次发布成功", i+1)
					return nil
				} else {
					logger.Warnf("发布尝试 %d/%d 失败: %v", i+1, retryCount, err)
					// 最后一次重试失败直接返回错误
					if i == retryCount-1 {
						return fmt.Errorf("发布失败，已重试 %d 次: %v", retryCount, err)
					}
				}

				// 重试延迟
				timer := time.NewTimer(retryDelay)
				select {
				case <-timeoutCtx.Done():
					timer.Stop()
					logger.Error("重试等待时超时")
					return timeoutCtx.Err()
				case <-timer.C:
					logger.Infof("准备第 %d 次重试", i+2)
					continue
				}
			}
		}
		return nil
	}

	// 执行发布
	if err := publishWithRetry(); err != nil {
		logger.Errorf("发布最终失败: %v", err)
		return nil, err
	}

	// 等待响应
	logger.Info("等待响应...")
	select {
	case reply := <-replyChan:
		if len(reply) == 0 {
			logger.Error("收到空响应")
			return nil, fmt.Errorf("收到空响应")
		}
		logger.Infof("成功收到响应，长度: %d bytes", len(reply))
		return reply, nil
	case <-timeoutCtx.Done():
		logger.Errorf("等待响应超时（%v）", maxTimeout)
		return nil, fmt.Errorf("等待响应超时（%v）", maxTimeout)
	}
}

// Publish 发布数据到主题。
// 注意，如果是响应消息，需要为 opts ...PubOpt 设置消息元数据 WithMessageMetadata(msgID, pb.MessageMetadata_RESPONSE)
// 参数:
// - ctx: 上下文，用于控制发布操作
// - data: 要发布的数据
// - opts: 发布选项
// 返回值:
// - error: 错误信息，如果有的话
func (t *Topic) Publish(ctx context.Context, data []byte, opts ...PubOpt) error {
	t.mux.RLock()         // 加读锁，确保并发安全
	defer t.mux.RUnlock() // 在函数返回前解锁
	if t.closed {
		return ErrTopicClosed // 如果主题已关闭，返回错误
	}

	// 确保 data 不为空
	// TODO:暂时注释，后面再优化
	// if len(data) == 0 {
	// 	return fmt.Errorf("消息数据不能为空")
	// }

	pid := t.p.signID  // 获取发布者的对等节点 ID
	key := t.p.signKey // 获取发布者的签名密钥

	pub := &PublishOptions{}   // 初始化发布选项
	for _, opt := range opts { // 遍历所有发布选项并应用
		err := opt(pub) // 应用发布选项
		if err != nil {
			logger.Warnf("应用发布选项失败: %s", err) // 如果应用选项出错，返回错误
			return err                        // 如果应用选项出错，返回错误
		}
	}

	// 创建消息对象，初始时 Metadata 赋值为 nil
	m := &pb.Message{
		Data:     data,    // 消息数据
		Topic:    t.topic, // 主题名称
		From:     nil,     // 发送者
		Seqno:    nil,     // 序列号
		Targets:  nil,     // 目标节点初始为空
		Metadata: nil,     // Metadata 初始为 nil
	}

	if pub.metadata.messageID != "" {
		// 如果消息是请求或响应类型，则必须设置 messageID
		if pub.metadata.msgType == pb.MessageMetadata_REQUEST || pub.metadata.msgType == pb.MessageMetadata_RESPONSE {
			if pub.metadata.messageID == "" {
				logger.Warnf("消息类型为请求或响应时，必须设置 messageID")
				return fmt.Errorf("消息类型为请求或响应时，必须设置 messageID")
			}
			m.Metadata = &pb.MessageMetadata{
				MessageID: pub.metadata.messageID,
				Type:      pb.MessageMetadata_MessageType(pub.metadata.msgType),
			}
		}
	}

	if pid != "" { // 如果存在对等节点 ID
		m.From = []byte(pid)      // 设置发送者的对等节点 ID
		m.Seqno = t.p.nextSeqno() // 获取并设置消息序列号
	}
	if key != nil { // 如果存在签名密钥
		m.From = []byte(pid)            // 再次设置发送者的对等节点 ID（确保存在）
		err := signMessage(pid, key, m) // 签署消息
		if err != nil {
			logger.Warnf("签署消息失败: %s", err) // 如果签署消息出错，返回错误
			return err                      // 如果签署消息出错，返回错误
		}
	}

	// 如果设置了 ready 回调函数，则处理准备操作
	if pub.ready != nil {
		if t.p.disc.discovery != nil { // 如果启用了发现机制
			t.p.disc.Bootstrap(ctx, t.topic, pub.ready) // 引导发现机制
		} else {
			var ticker *time.Ticker
		readyLoop:
			for { // 循环检查路由器是否准备好发布消息
				res := make(chan bool, 1) // 创建结果通道
				select {
				case t.p.eval <- func() { // 发送评估函数到评估通道
					done, _ := pub.ready(t.p.rt, t.topic)
					res <- done // 将评估结果发送到结果通道
				}:
					if <-res { // 如果准备好了
						break readyLoop // 跳出循环
					}
				case <-t.p.ctx.Done(): // 如果全局上下文完成
					return t.p.ctx.Err() // 返回全局上下文错误
				case <-ctx.Done(): // 如果操作上下文完成
					return ctx.Err() // 返回操作上下文错误
				}
				if ticker == nil { // 如果计时器尚未初始化
					ticker = time.NewTicker(200 * time.Millisecond) // 每200毫秒检查一次
					defer ticker.Stop()                             // 确保在返回前停止计时器
				}

				select {
				case <-ticker.C: // 每200毫秒触发一次检查
				case <-ctx.Done(): // 如果操作上下文完成
					return fmt.Errorf("路由器未准备好: %w", ctx.Err()) // 返回上下文错误
				}
			}
		}
	}

	// 如果 targetMap 不为空，则处理目标节点
	if len(pub.targetMap) > 0 {
		targets := convertMapToTargets(pub.targetMap) // 将 []peer.ID 转换为 []*pb.Target
		m.Targets = targets                           // 设置目标节点列表
	}

	// 推送本地消息到验证模块
	return t.p.val.PushLocal(
		&Message{
			m,             // 消息内容
			"",            // 主题，当前为空
			t.p.host.ID(), // 发送者的对等节点 ID
			nil,           // 序列号，当前为空
			pub.local,     // 是否为本地发布
		})
}

// convertMapToTargets 将 []peer.ID 转换为 []*pb.Target
// 参数:
// - targetMap: 目标节点列表，包含需要发送消息的目标节点
// 返回值:
// - []*pb.Target: 转换后的目标节点列表
func convertMapToTargets(targetMap []peer.ID) []*pb.Target {
	var targets []*pb.Target
	for _, pid := range targetMap {
		targets = append(targets, &pb.Target{
			PeerId:   []byte(pid), // 将 peer.ID 转换为字节数组
			Received: false,       // 初始状态下，所有目标节点都未接收到消息
		})
	}
	return targets
}

// WithReadiness 返回一个发布选项，仅在路由器准备好时发布。
// 此选项仅在 PubSub 也使用 WithDiscovery 时有用。
// 参数:
// - ready: RouterReady 类型的回调函数，当路由器准备好时被调用。
// 返回值:
// - PubOpt: 返回一个发布选项函数，用于设置 PublishOptions 中的 ready 字段。
func WithReadiness(ready RouterReady) PubOpt {
	return func(pub *PublishOptions) error {
		pub.ready = ready // 设置发布选项中的 ready 回调函数
		return nil        // 返回 nil 表示没有错误
	}
}

// WithLocalPublication 返回一个发布选项，仅通知进程内的订阅者。
// 它阻止消息发布到网状对等节点。
// 参数:
// - local: bool 类型的标志，指示是否仅在本地发布消息。
// 返回值:
// - PubOpt: 返回一个发布选项函数，用于设置 PublishOptions 中的 local 字段。
func WithLocalPublication(local bool) PubOpt {
	return func(pub *PublishOptions) error {
		pub.local = local // 设置发布选项中的 local 标志
		return nil        // 返回 nil 表示没有错误
	}
}

// WithSecretKeyAndPeerId 返回一个发布选项，用于提供自定义私钥及其对应的对等节点 ID。
// 这个选项在我们希望从网络中的"虚拟"不可连接的对等节点发送消息时非常有用。
// 参数:
// - key: crypto.PrivKey 类型，自定义私钥。
// - pid: peer.ID 类型，对应的对等节点 ID。
// 返回值:
// - PubOpt: 返回一个发布选项函数，用于设置 PublishOptions 中的 customKey 字段。
func WithSecretKeyAndPeerId(key crypto.PrivKey, pid peer.ID) PubOpt {
	return func(pub *PublishOptions) error {
		pub.customKey = func() (crypto.PrivKey, peer.ID) {
			return key, pid // 返回自定义的私钥和对等节点 ID
		}

		return nil // 返回 nil 表示没有错误
	}
}

// WithTargetMap 设置目标节点列表。
// 参数:
// - targets: 目标节点的列表，类型为 []peer.ID。
// 返回值:
// - PubOpt: 返回一个发布选项函数，用于设置 PublishOptions 中的 targetMap 字段。
func WithTargetMap(targets []peer.ID) PubOpt {
	return func(pub *PublishOptions) error {
		pub.targetMap = targets // 设置目标节点列表
		return nil              // 返回 nil 表示没有错误
	}
}

// WithMessageMetadata 设置消息的元信息。
// 参数:
// - messageID: string 类型，表示消息ID。
// - msgType: MessageType 类型，表示消息的类型（请求或响应）。
// 返回值:
// - PubOpt: 返回一个发布选项函数，用于设置 PublishOptions 中的消息元信息。
func WithMessageMetadata(messageID string, msgType pb.MessageMetadata_MessageType) PubOpt {
	return func(pub *PublishOptions) error {
		pub.metadata = MessageMetadataOpt{
			messageID: messageID,
			msgType:   msgType,
		} // 设置消息元信息
		return nil // 返回 nil 表示没有错误
	}
}

// Close 关闭主题。返回错误，除非没有活动的事件处理程序或订阅。
// 如果主题已经关闭，则不会返回错误。
// 返回值:
// - error: 错误信息，如果有的话。
func (t *Topic) Close() error {
	t.mux.Lock()         // 加写锁，确保并发安全
	defer t.mux.Unlock() // 在函数返回前解锁
	if t.closed {
		return nil // 如果主题已关闭，返回 nil
	}

	req := &rmTopicReq{t, make(chan error, 1)} // 创建一个移除主题请求对象，带有一个缓冲通道

	select {
	case t.p.rmTopic <- req: // 发送移除主题请求
	case <-t.p.ctx.Done(): // 如果上下文已完成
		return t.p.ctx.Err() // 返回上下文错误
	}

	err := <-req.resp // 从响应通道接收错误信息

	if err == nil {
		t.closed = true // 如果没有错误，标记主题为已关闭
	}

	return err // 返回错误信息
}

// ListPeers 返回我们在给定主题中连接的对等节点列表。
// 返回值:
// - []peer.ID: 对等节点列表。
func (t *Topic) ListPeers() []peer.ID {
	t.mux.RLock()         // 加读锁，确保并发安全
	defer t.mux.RUnlock() // 在函数返回前解锁
	if t.closed {
		return []peer.ID{} // 如果主题已关闭，返回空列表
	}

	return t.p.ListPeers(t.topic) // 返回主题中连接的对等节点列表
}

// EventType 表示事件类型。
type EventType int

const (
	PeerJoin  EventType = iota // 对等节点加入事件
	PeerLeave                  // 对等节点离开事件
)

// TopicEventHandler 用于管理特定主题事件。无需订阅即可接收事件。
type TopicEventHandler struct {
	topic *Topic // 关联的主题
	err   error  // 事件处理程序的错误

	evtLogMx sync.Mutex            // 事件日志的互斥锁
	evtLog   map[peer.ID]EventType // 事件日志
	evtLogCh chan struct{}         // 事件日志的信号通道
}

// TopicEventHandlerOpt 定义了一个用于设置 TopicEventHandler 选项的函数类型。
type TopicEventHandlerOpt func(t *TopicEventHandler) error

// PeerEvent 表示对等节点事件。
type PeerEvent struct {
	Type EventType // 事件类型
	Peer peer.ID   // 对等节点 ID
}

// Cancel 关闭主题事件处理程序。
func (t *TopicEventHandler) Cancel() {
	topic := t.topic                                      // 获取当前事件处理程序的主题
	t.err = fmt.Errorf("主题事件处理程序已通过 handler.Cancel() 取消") // 设置错误信息，表示事件处理程序已被取消

	topic.evtHandlerMux.Lock()     // 锁定主题的事件处理程序互斥锁
	delete(topic.evtHandlers, t)   // 从主题的事件处理程序映射中删除当前处理程序
	t.topic.evtHandlerMux.Unlock() // 解锁主题的事件处理程序互斥锁
}

// sendNotification 发送事件通知。
// 参数:
// - evt: PeerEvent 类型，表示需要通知的事件。
func (t *TopicEventHandler) sendNotification(evt PeerEvent) {
	t.evtLogMx.Lock()    // 锁定事件日志互斥锁
	t.addToEventLog(evt) // 将事件添加到事件日志中
	t.evtLogMx.Unlock()  // 解锁事件日志互斥锁
}

// addToEventLog 假定已经对事件日志加锁。
// 参数:
// - evt: PeerEvent 类型，表示需要添加到日志的事件。
func (t *TopicEventHandler) addToEventLog(evt PeerEvent) {
	e, ok := t.evtLog[evt.Peer] // 检查事件日志中是否已有该对等节点的事件
	if !ok {                    // 如果没有该对等节点的事件
		t.evtLog[evt.Peer] = evt.Type // 将事件类型添加到事件日志中
		// 发送信号，表示事件已添加到日志中
		select {
		case t.evtLogCh <- struct{}{}: // 如果信号通道未满，发送信号
		default: // 如果信号通道已满，不做任何操作
		}
	} else if e != evt.Type { // 如果已有事件且事件类型不同
		delete(t.evtLog, evt.Peer) // 从事件日志中删除该对等节点的事件
	}
}

// pullFromEventLog 假定已经对事件日志加锁。
// 返回值:
// - PeerEvent: 拉取的事件。
// - bool: 是否成功拉取事件。
func (t *TopicEventHandler) pullFromEventLog() (PeerEvent, bool) {
	for k, v := range t.evtLog { // 遍历事件日志
		evt := PeerEvent{Peer: k, Type: v} // 创建事件对象
		delete(t.evtLog, k)                // 从事件日志中删除该事件
		return evt, true                   // 返回事件和成功标志
	}
	return PeerEvent{}, false // 如果没有事件，返回空事件和失败标志
}

// NextPeerEvent 返回有关订阅对等节点的下一个事件。
// 保证：给定对等节点的 Peer Join 和 Peer Leave 事件将按顺序触发。
// 参数:
// - ctx: 上下文，用于控制操作。
// 返回值:
// - PeerEvent: 下一个对等节点事件。
// - error: 错误信息，如果有的话。
func (t *TopicEventHandler) NextPeerEvent(ctx context.Context) (PeerEvent, error) {
	for {
		t.evtLogMx.Lock()               // 锁定事件日志互斥锁
		evt, ok := t.pullFromEventLog() // 尝试从事件日志中拉取事件
		if ok {                         // 如果成功拉取到事件
			// 确保如果事件日志中有事件，事件日志信号可用
			if len(t.evtLog) > 0 {
				select {
				case t.evtLogCh <- struct{}{}: // 如果信号通道未满，发送信号
				default: // 如果信号通道已满，不做任何操作
				}
			}
			t.evtLogMx.Unlock() // 解锁事件日志互斥锁
			return evt, nil     // 返回事件和nil错误
		}
		t.evtLogMx.Unlock() // 如果没有事件，解锁事件日志互斥锁

		select {
		case <-t.evtLogCh: // 等待事件日志信号
			continue // 收到信号后继续循环
		case <-ctx.Done(): // 如果上下文完成
			return PeerEvent{}, ctx.Err() // 返回空事件和上下文错误
		}
	}
}
