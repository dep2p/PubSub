package pubsub

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/go-dep2p/core/peer"
)

// 获取多个PubSub实例的Topic对象
// 参数:
//   - psubs: PubSub实例切片
//   - topicID: 主题ID
//   - opts: Topic选项
//
// 返回:
//   - []*Topic: Topic对象切片
func getTopics(psubs []*PubSub, topicID string, opts ...TopicOpt) []*Topic {
	// 创建一个与psubs长度相同的Topic切片
	topics := make([]*Topic, len(psubs))

	// 遍历每个PubSub实例
	for i, ps := range psubs {
		// 加入主题
		t, err := ps.Join(topicID, opts...)
		if err != nil {
			panic(err)
		}
		// 将Topic对象存入切片
		topics[i] = t
	}

	// 返回Topic切片
	return topics
}

// 获取多个Topic的事件处理器
// 参数:
//   - topics: Topic对象切片
//   - opts: 事件处理器选项
//
// 返回:
//   - []*TopicEventHandler: 事件处理器切片
func getTopicEvts(topics []*Topic, opts ...TopicEventHandlerOpt) []*TopicEventHandler {
	// 创建一个与topics长度相同的事件处理器切片
	handlers := make([]*TopicEventHandler, len(topics))

	// 遍历每个Topic
	for i, t := range topics {
		// 获取事件处理器
		h, err := t.EventHandler(opts...)
		if err != nil {
			panic(err)
		}
		// 将事件处理器存入切片
		handlers[i] = h
	}

	// 返回事件处理器切片
	return handlers
}

// 测试带有打开的订阅时关闭Topic
func TestTopicCloseWithOpenSubscription(t *testing.T) {
	var sub *Subscription
	var err error
	testTopicCloseWithOpenResource(t,
		// 打开资源的函数:订阅Topic
		func(topic *Topic) {
			sub, err = topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
		},
		// 关闭资源的函数:取消订阅
		func() {
			sub.Cancel()
		},
	)
}

// 测试带有打开的事件处理器时关闭Topic
func TestTopicCloseWithOpenEventHandler(t *testing.T) {
	var evts *TopicEventHandler
	var err error
	testTopicCloseWithOpenResource(t,
		// 打开资源的函数:获取事件处理器
		func(topic *Topic) {
			evts, err = topic.EventHandler()
			if err != nil {
				t.Fatal(err)
			}
		},
		// 关闭资源的函数:取消事件处理器
		func() {
			evts.Cancel()
		},
	)
}

// 测试带有打开的中继时关闭Topic
func TestTopicCloseWithOpenRelay(t *testing.T) {
	var relayCancel RelayCancelFunc
	var err error
	testTopicCloseWithOpenResource(t,
		// 打开资源的函数:启动中继
		func(topic *Topic) {
			relayCancel, err = topic.Relay()
			if err != nil {
				t.Fatal(err)
			}
		},
		// 关闭资源的函数:取消中继
		func() {
			relayCancel()
		},
	)
}

// 测试带有打开资源时关闭Topic的通用函数
// 参数:
//   - t: 测试对象
//   - openResource: 打开资源的函数
//   - closeResource: 关闭资源的函数
func testTopicCloseWithOpenResource(t *testing.T, openResource func(topic *Topic), closeResource func()) {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置主机数量为1
	const numHosts = 1
	topicID := "foobar"
	// 获取默认主机
	hosts := getDefaultHosts(t, numHosts)
	// 获取PubSub实例
	ps := getPubsub(ctx, hosts[0])

	// 尝试创建并关闭Topic
	topic, err := ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 关闭Topic应该成功
	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}

	// 尝试创建Topic并在有打开的资源时关闭它
	topic, err = ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 打开资源
	openResource(topic)

	// 关闭Topic应该失败
	if err := topic.Close(); err == nil {
		t.Fatal("关闭带有打开资源的主题时应该出现错误")
	}

	// 检查关闭资源后Topic是否能正常关闭
	closeResource()
	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 关闭Topic应该成功
	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}
}

// 测试Topic重用
func TestTopicReuse(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置主机数量为2
	const numHosts = 2
	topicID := "foobar"
	// 获取默认主机
	hosts := getDefaultHosts(t, numHosts)

	// 创建发送者和接收者的PubSub实例
	sender := getPubsub(ctx, hosts[0], WithDiscovery(&dummyDiscovery{}))
	receiver := getPubsub(ctx, hosts[1])

	// 连接所有主机
	connectAll(t, hosts)

	// 发送者创建Topic
	sendTopic, err := sender.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 接收者创建并订阅Topic
	receiveTopic, err := receiver.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 创建订阅
	sub, err := receiveTopic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 发送第一条消息
	firstMsg := []byte("1")
	if err := sendTopic.Publish(ctx, firstMsg, WithReadiness(MinTopicSize(1))); err != nil {
		t.Fatal(err)
	}

	// 接收消息
	msg, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 验证消息内容
	if !bytes.Equal(msg.GetData(), firstMsg) {
		t.Fatal("收到错误的消息")
	}

	// 关闭发送者的Topic
	if err := sendTopic.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新创建相同的Topic
	newSendTopic, err := sender.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 尝试使用原始Topic发送数据
	illegalSend := []byte("illegal")
	if err := sendTopic.Publish(ctx, illegalSend); err != ErrTopicClosed {
		t.Fatal(err)
	}

	// 设置超时上下文
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	// 尝试接收消息
	msg, err = sub.Next(timeoutCtx)
	if err != context.DeadlineExceeded {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(msg.GetData(), illegalSend) {
			t.Fatal("从非法主题收到错误的消息")
		}
		t.Fatal("收到非法主题发送的消息")
	}
	timeoutCancel()

	// 尝试使用原始Topic取消新Topic
	if err := sendTopic.Close(); err != nil {
		t.Fatal(err)
	}

	// 发送第二条消息
	secondMsg := []byte("2")
	if err := newSendTopic.Publish(ctx, secondMsg); err != nil {
		t.Fatal(err)
	}

	// 设置超时上下文
	timeoutCtx, timeoutCancel = context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	// 接收消息
	msg, err = sub.Next(timeoutCtx)
	if err != nil {
		t.Fatal(err)
	}
	// 验证消息内容
	if !bytes.Equal(msg.GetData(), secondMsg) {
		t.Fatal("收到错误的消息")
	}
}

// 测试Topic事件处理器取消
func TestTopicEventHandlerCancel(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置主机数量为5
	const numHosts = 5
	topicID := "foobar"
	// 获取默认主机
	hosts := getDefaultHosts(t, numHosts)
	// 获取PubSub实例
	ps := getPubsub(ctx, hosts[0])

	// 尝试创建Topic
	topic, err := ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 获取事件处理器
	evts, err := topic.EventHandler()
	if err != nil {
		t.Fatal(err)
	}
	// 取消事件处理器
	evts.Cancel()
	// 设置超时上下文
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	connectAll(t, hosts)
	_, err = evts.NextPeerEvent(timeoutCtx)
	if err != context.DeadlineExceeded {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("取消事件处理器后收到事件")
	}
}

// 测试订阅加入通知
func TestSubscriptionJoinNotification(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 设置后期订阅者数量为10
	const numLateSubscribers = 10
	// 设置总主机数量为20
	const numHosts = 20
	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)
	// 为所有主机创建主题
	topics := getTopics(getPubsubs(ctx, hosts), "foobar")
	// 获取所有主题的事件处理器
	evts := getTopicEvts(topics)

	// 创建订阅切片
	subs := make([]*Subscription, numHosts)
	// 创建主题对等节点映射切片
	topicPeersFound := make([]map[peer.ID]struct{}, numHosts)

	// 让部分对等节点先订阅
	// 这样可以测试是否能从现有对等节点获取订阅通知
	for i, topic := range topics[numLateSubscribers:] {
		// 订阅主题
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		// 保存订阅
		subs[i] = subch
	}

	// 连接所有主机
	connectAll(t, hosts)

	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 让剩余的对等节点订阅
	for i, topic := range topics[:numLateSubscribers] {
		// 订阅主题
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		// 保存订阅
		subs[i+numLateSubscribers] = subch
	}

	// 创建等待组
	wg := sync.WaitGroup{}
	// 为每个主机创建一个goroutine监听对等节点事件
	for i := 0; i < numHosts; i++ {
		// 创建对等节点映射
		peersFound := make(map[peer.ID]struct{})
		// 保存映射
		topicPeersFound[i] = peersFound
		// 获取事件处理器
		evt := evts[i]
		// 增加等待组计数
		wg.Add(1)
		// 启动goroutine监听事件
		go func(peersFound map[peer.ID]struct{}) {
			// 完成时减少等待组计数
			defer wg.Done()
			// 持续监听直到找到所有对等节点
			for len(peersFound) < numHosts-1 {
				// 获取下一个对等节点事件
				event, err := evt.NextPeerEvent(ctx)
				if err != nil {
					panic(err)
				}
				// 如果是加入事件则记录对等节点
				if event.Type == PeerJoin {
					peersFound[event.Peer] = struct{}{}
				}
			}
		}(peersFound)
	}

	// 等待所有goroutine完成
	wg.Wait()
	// 验证每个主机是否都找到了正确数量的对等节点
	for _, peersFound := range topicPeersFound {
		if len(peersFound) != numHosts-1 {
			t.Fatal("找到的对等节点数量不正确")
		}
	}
}

// 测试订阅离开通知
func TestSubscriptionLeaveNotification(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 设置主机数量为20
	const numHosts = 20
	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)
	// 获取PubSub实例列表
	psubs := getPubsubs(ctx, hosts)
	// 为所有主机创建主题
	topics := getTopics(psubs, "foobar")
	// 获取所有主题的事件处理器
	evts := getTopicEvts(topics)

	// 创建订阅切片
	subs := make([]*Subscription, numHosts)
	// 创建主题对等节点映射切片
	topicPeersFound := make([]map[peer.ID]struct{}, numHosts)

	// 让所有对等节点订阅并等待直到都被发现
	for i, topic := range topics {
		// 订阅主题
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		// 保存订阅
		subs[i] = subch
	}

	// 连接所有主机
	connectAll(t, hosts)

	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 创建等待组
	wg := sync.WaitGroup{}
	// 为每个主机创建一个goroutine监听对等节点事件
	for i := 0; i < numHosts; i++ {
		// 创建对等节点映射
		peersFound := make(map[peer.ID]struct{})
		// 保存映射
		topicPeersFound[i] = peersFound
		// 获取事件处理器
		evt := evts[i]
		// 增加等待组计数
		wg.Add(1)
		// 启动goroutine监听事件
		go func(peersFound map[peer.ID]struct{}) {
			// 完成时减少等待组计数
			defer wg.Done()
			// 持续监听直到找到所有对等节点
			for len(peersFound) < numHosts-1 {
				// 获取下一个对等节点事件
				event, err := evt.NextPeerEvent(ctx)
				if err != nil {
					panic(err)
				}
				// 如果是加入事件则记录对等节点
				if event.Type == PeerJoin {
					peersFound[event.Peer] = struct{}{}
				}
			}
		}(peersFound)
	}

	// 等待所有goroutine完成
	wg.Wait()
	// 验证每个主机是否都找到了正确数量的对等节点
	for _, peersFound := range topicPeersFound {
		if len(peersFound) != numHosts-1 {
			t.Fatal("找到的对等节点数量不正确")
		}
	}

	// 测试移除对等节点并验证它们是否触发事件
	// 取消第2个订阅
	subs[1].Cancel()
	// 关闭第3个主机
	_ = hosts[2].Close()
	// 将第4个主机加入黑名单
	psubs[0].BlacklistPeer(hosts[3].ID())

	// 创建离开对等节点映射
	leavingPeers := make(map[peer.ID]struct{})
	// 等待直到有3个对等节点离开
	for len(leavingPeers) < 3 {
		// 获取下一个对等节点事件
		event, err := evts[0].NextPeerEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// 如果是离开事件则记录对等节点
		if event.Type == PeerLeave {
			leavingPeers[event.Peer] = struct{}{}
		}
	}

	// 验证取消订阅是否触发离开事件
	if _, ok := leavingPeers[hosts[1].ID()]; !ok {
		t.Fatal("取消订阅未触发离开事件")
	}
	// 验证关闭主机是否触发离开事件
	if _, ok := leavingPeers[hosts[2].ID()]; !ok {
		t.Fatal("关闭主机未触发离开事件")
	}
	// 验证加入黑名单是否触发离开事件
	if _, ok := leavingPeers[hosts[3].ID()]; !ok {
		t.Fatal("加入黑名单未触发离开事件")
	}
}

// 测试大量订阅通知
func TestSubscriptionManyNotifications(t *testing.T) {
	// 跳过这个不稳定的测试
	t.Skip("flaky test disabled")

	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 设置主题名称
	const topic = "foobar"

	// 设置主机数量为33
	const numHosts = 33
	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)
	// 为所有主机创建主题
	topics := getTopics(getPubsubs(ctx, hosts), topic)
	// 获取所有主题的事件处理器
	evts := getTopicEvts(topics)

	// 创建订阅切片
	subs := make([]*Subscription, numHosts)
	// 创建主题对等节点映射切片
	topicPeersFound := make([]map[peer.ID]struct{}, numHosts)

	// 除了一个对等节点外,让所有对等节点订阅并等待直到都被发现
	for i := 1; i < numHosts; i++ {
		// 订阅主题
		subch, err := topics[i].Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		// 保存订阅
		subs[i] = subch
	}

	// 连接所有主机
	connectAll(t, hosts)

	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 创建等待组
	wg := sync.WaitGroup{}
	// 为每个已订阅的主机创建一个goroutine监听对等节点事件
	for i := 1; i < numHosts; i++ {
		// 创建对等节点映射
		peersFound := make(map[peer.ID]struct{})
		// 保存映射
		topicPeersFound[i] = peersFound
		// 获取事件处理器
		evt := evts[i]
		// 增加等待组计数
		wg.Add(1)
		// 启动goroutine监听事件
		go func(peersFound map[peer.ID]struct{}) {
			// 完成时减少等待组计数
			defer wg.Done()
			// 持续监听直到找到所有对等节点
			for len(peersFound) < numHosts-2 {
				// 获取下一个对等节点事件
				event, err := evt.NextPeerEvent(ctx)
				if err != nil {
					panic(err)
				}
				// 如果是加入事件则记录对等节点
				if event.Type == PeerJoin {
					peersFound[event.Peer] = struct{}{}
				}
			}
		}(peersFound)
	}

	// 等待所有goroutine完成
	wg.Wait()
	// 验证每个已订阅的主机是否都找到了正确数量的对等节点
	for _, peersFound := range topicPeersFound[1:] {
		if len(peersFound) != numHosts-2 {
			t.Fatalf("找到 %d 个对等节点, 预期 %d 个", len(peersFound), numHosts-2)
		}
	}

	// 等待剩余的对等节点找到其他对等节点
	remPeerTopic, remPeerEvts := topics[0], evts[0]
	for len(remPeerTopic.ListPeers()) < numHosts-1 {
		time.Sleep(time.Millisecond * 100)
	}

	// 订阅剩余的对等节点并检查所有事件是否都到达
	sub, err := remPeerTopic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 保存订阅
	subs[0] = sub

	// 读取所有排队的事件
	peerState := readAllQueuedEvents(ctx, t, remPeerEvts)

	// 验证找到的对等节点数量是否正确
	if len(peerState) != numHosts-1 {
		t.Fatal("找到的对等节点数量不正确")
	}

	// 验证所有事件是否都是加入事件
	for _, e := range peerState {
		if e != PeerJoin {
			t.Fatal("非加入事件发生")
		}
	}

	// 除了一个对等节点外,取消所有对等节点的订阅并检查所有事件是否都到达
	for i := 1; i < numHosts; i++ {
		subs[i].Cancel()
	}

	// 等待剩余的对等节点与其他对等节点断开连接
	for len(topics[0].ListPeers()) != 0 {
		time.Sleep(time.Millisecond * 100)
	}

	// 读取所有排队的事件
	peerState = readAllQueuedEvents(ctx, t, remPeerEvts)

	// 验证找到的对等节点数量是否正确
	if len(peerState) != numHosts-1 {
		t.Fatal("找到的对等节点数量不正确")
	}

	// 验证所有事件是否都是离开事件
	for _, e := range peerState {
		if e != PeerLeave {
			t.Fatal("非离开事件发生")
		}
	}
}

// 测试订阅通知的订阅和取消订阅
func TestSubscriptionNotificationSubUnSub(t *testing.T) {
	// 重新订阅和取消订阅对等节点并检查状态的一致性
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 设置主题名称
	const topic = "foobar"

	// 设置主机数量为35
	const numHosts = 35
	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)
	// 为所有主机创建主题
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	// 将所有主机连接到第一个主机
	for i := 1; i < numHosts; i++ {
		connect(t, hosts[0], hosts[i])
	}
	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 测试先订阅后取消订阅
	notifSubThenUnSub(ctx, t, topics)
}

// 测试主题中继
func TestTopicRelay(t *testing.T) {
	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// 延迟调用取消函数
	defer cancel()

	// 设置主题名称
	const topic = "foobar"
	// 设置主机数量为5
	const numHosts = 5

	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)
	// 为所有主机创建主题
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	// 创建如下网络拓扑:
	// [0.Rel] - [1.Rel] - [2.Sub]
	//             |
	//           [3.Rel] - [4.Sub]

	// 连接主机形成网络拓扑
	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[1], hosts[3])
	connect(t, hosts[3], hosts[4])

	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 创建订阅切片
	var subs []*Subscription

	// 为每个主题设置订阅或中继
	for i, topic := range topics {
		if i == 2 || i == 4 {
			// 为2号和4号主机创建订阅
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}

			subs = append(subs, sub)
		} else {
			// 为其他主机创建中继
			_, err := topic.Relay()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// 等待100毫秒
	time.Sleep(time.Millisecond * 100)

	// 发送100条消息并验证
	for i := 0; i < 100; i++ {
		// 创建消息
		msg := []byte("message")

		// 随机选择一个主题作为发送者
		owner := rand.Intn(len(topics))

		// 发布消息
		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

		// 验证所有订阅者是否都收到了消息
		for _, sub := range subs {
			received, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(msg, received.Data) {
				t.Fatal("收到的消息与预期不符")
			}
		}
	}
}

// 测试主题中继重用功能
func TestTopicRelayReuse(t *testing.T) {
	// 创建带取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 定义主题名称
	const topic = "foobar"
	// 定义主机数量
	const numHosts = 1

	// 获取默认主机
	hosts := getDefaultHosts(t, numHosts)
	// 获取发布订阅系统
	pubsubs := getPubsubs(ctx, hosts)
	// 获取主题列表
	topics := getTopics(pubsubs, topic)

	// 创建第一个中继并获取取消函数
	relay1Cancel, err := topics[0].Relay()
	if err != nil {
		t.Fatal(err)
	}

	// 创建第二个中继并获取取消函数
	relay2Cancel, err := topics[0].Relay()
	if err != nil {
		t.Fatal(err)
	}

	// 创建第三个中继并获取取消函数
	relay3Cancel, err := topics[0].Relay()
	if err != nil {
		t.Fatal(err)
	}

	// 等待100毫秒让中继建立
	time.Sleep(time.Millisecond * 100)

	// 创建用于接收结果的通道
	res := make(chan bool, 1)
	// 检查中继数量是否为3
	pubsubs[0].eval <- func() {
		res <- pubsubs[0].myRelays[topic] == 3
	}

	// 获取检查结果
	isCorrectNumber := <-res
	if !isCorrectNumber {
		t.Fatal("中继数量不正确")
	}

	// 多次调用第一个取消函数，但只有第一次调用生效
	relay1Cancel()
	relay1Cancel()
	relay1Cancel()

	// 检查中继数量是否为2
	pubsubs[0].eval <- func() {
		res <- pubsubs[0].myRelays[topic] == 2
	}

	// 获取检查结果
	isCorrectNumber = <-res
	if !isCorrectNumber {
		t.Fatal("中继数量不正确")
	}

	// 取消剩余的中继
	relay2Cancel()
	relay3Cancel()

	// 等待100毫秒让取消操作生效
	time.Sleep(time.Millisecond * 100)

	// 检查中继数量是否为0
	pubsubs[0].eval <- func() {
		res <- pubsubs[0].myRelays[topic] == 0
	}

	// 获取检查结果
	isCorrectNumber = <-res
	if !isCorrectNumber {
		t.Fatal("中继数量不正确")
	}
}

// 测试在已关闭的主题上创建中继
func TestTopicRelayOnClosedTopic(t *testing.T) {
	// 创建带取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 定义主题名称
	const topic = "foobar"
	// 定义主机数量
	const numHosts = 1

	// 获取默认主机
	hosts := getDefaultHosts(t, numHosts)
	// 获取主题列表
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	// 关闭主题
	err := topics[0].Close()
	if err != nil {
		t.Fatal(err)
	}

	// 尝试在已关闭的主题上创建中继，应该返回错误
	_, err = topics[0].Relay()
	if err == nil {
		t.Fatalf("应该返回错误")
	}
}

// 测试产生panic的情况
func TestProducePanic(t *testing.T) {
	// 创建带取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟调用取消函数
	defer cancel()

	// 定义主机数量和主题ID
	const numHosts = 5
	topicID := "foobar"
	// 获取默认主机
	hosts := getDefaultHosts(t, numHosts)
	// 获取发布订阅系统
	ps := getPubsub(ctx, hosts[0])

	// 创建主题
	topic, err := ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 创建第一个订阅（将被取消）
	s, err := topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	// 创建第二个订阅（保持活跃）
	s2, err := topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	_ = s2

	// 多次取消第一个订阅
	s.Cancel()
	time.Sleep(time.Second)
	s.Cancel()
	time.Sleep(time.Second)
}

// 测试订阅然后取消订阅的功能
func notifSubThenUnSub(ctx context.Context, t *testing.T, topics []*Topic) {
	// 获取主要主题
	primaryTopic := topics[0]
	// 创建订阅数组
	msgs := make([]*Subscription, len(topics))
	// 计算检查大小
	checkSize := len(topics) - 1

	// 为所有节点订阅主题
	var err error
	for i, topic := range topics {
		msgs[i], err = topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}

	// 等待主要节点与其他节点建立连接
	for len(primaryTopic.ListPeers()) < checkSize {
		time.Sleep(time.Millisecond * 100)
	}

	// 取消除主要节点外的所有订阅
	for i := 1; i < checkSize+1; i++ {
		msgs[i].Cancel()
	}

	// 等待取消订阅消息到达主要节点
	for len(primaryTopic.ListPeers()) > 0 {
		time.Sleep(time.Millisecond * 100)
	}

	// 获取主要主题的事件处理器
	primaryEvts, err := primaryTopic.EventHandler()
	if err != nil {
		t.Fatal(err)
	}
	// 读取所有队列中的事件
	peerState := readAllQueuedEvents(ctx, t, primaryEvts)

	// 验证没有剩余事件
	if len(peerState) != 0 {
		for p, s := range peerState {
			fmt.Println(p, s)
		}
		t.Fatalf("收到错误的事件。%d个额外事件", len(peerState))
	}
}

// 读取所有队列中的事件
func readAllQueuedEvents(ctx context.Context, t *testing.T, evt *TopicEventHandler) map[peer.ID]EventType {
	// 创建节点状态映射
	peerState := make(map[peer.ID]EventType)
	for {
		// 创建带超时的上下文
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		// 获取下一个节点事件
		event, err := evt.NextPeerEvent(ctx)
		cancel()

		// 处理超时错误
		if err == context.DeadlineExceeded {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		// 更新节点状态
		e, ok := peerState[event.Peer]
		if !ok {
			peerState[event.Peer] = event.Type
		} else if e != event.Type {
			delete(peerState, event.Peer)
		}
	}
	return peerState
}

func TestPublishWithReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建多个主机
	const numHosts = 3
	hosts := getDefaultHosts(t, numHosts)
	pubsubs := getPubsubs(ctx, hosts)
	defer func() {
		for _, h := range hosts {
			h.Close()
		}
	}()

	// 连接所有主机
	connectAll(t, hosts)

	// 创建主题
	topicName := "test-publish-with-reply"
	topics := getTopics(pubsubs, topicName)

	// 为每个主题创建订阅
	var subs []*Subscription
	for _, topic := range topics {
		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待主题传播
	time.Sleep(time.Second)

	// 测试不指定目标节点的情况
	t.Run("Broadcast", func(t *testing.T) {
		testPublishWithReply(t, ctx, topics, subs, nil)
	})

	// 测试指定单个目标节点的情况
	// t.Run("SingleTarget", func(t *testing.T) {
	// 	testPublishWithReply(t, ctx, topics, subs, []peer.ID{hosts[1].ID()})
	// })

	// 测试指定多个目标节点的情况
	// t.Run("MultipleTargets", func(t *testing.T) {
	// 	testPublishWithReply(t, ctx, topics, subs, []peer.ID{hosts[1].ID(), hosts[2].ID()})
	// })
}

func testPublishWithReply(t *testing.T, ctx context.Context, topics []*Topic, subs []*Subscription, targetNodes []peer.ID) {
	// 创建一个新的上下文，用于控制 goroutine
	testCtx, testCancel := context.WithCancel(ctx)
	defer testCancel()

	// 发送方发送消息
	sender := topics[0]
	message := []byte("Hello, world!")

	// 打印发送的消息
	fmt.Printf("发送消息: %s\n", message)

	// 启动接收和回复的 goroutine
	var wg sync.WaitGroup
	for i := 1; i < len(topics); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, err := subs[i].Next(testCtx)
			if err != nil {
				t.Errorf("节点 %d 接收消息失败: %v", i, err)
				return
			}

			// 打印接收到的消息
			fmt.Printf("节点 %d 接收到消息: %s\n", i, msg.Data)

			if !bytes.Equal(msg.Data, message) {
				t.Errorf("节点 %d 收到的消息不符合预期. 预期: %s, 实际: %s", i, message, msg.Data)
				return
			}

			// 检查消息类型
			if msg.GetMetadata().GetType() != pb.MessageMetadata_REQUEST {
				t.Errorf("节点 %d 收到的消息类型不正确. 预期: REQUEST, 实际: %v", i, msg.GetMetadata().GetType())
				return
			}

			// 发送回复
			replyMsg := append([]byte("Reply: "), msg.Data...)
			err = topics[i].Publish(testCtx, replyMsg, WithMessageMetadata(msg.GetMetadata().GetMessageID(), pb.MessageMetadata_RESPONSE))
			if err != nil {
				t.Errorf("节点 %d 发送回复失败: %v", i, err)
			} else {
				// 打印发送的回复
				fmt.Printf("节点 %d 发送回复: %s\n", i, replyMsg)
			}
		}(i)
	}

	// 发送消息并等待回复
	var reply []byte
	var err error

	if len(targetNodes) == 0 {
		reply, err = sender.PublishWithReply(ctx, message)
	} else {
		reply, err = sender.PublishWithReply(ctx, message, targetNodes...)
	}

	if err != nil {
		t.Fatalf("发送消息失败: %v", err)
	}

	// 等待所有接收和回复的 goroutine 完成
	wg.Wait()

	// 打印接收到的回复
	fmt.Printf("接收到的回复: %s\n", reply)

	// 检查回复
	expectedReply := []byte("Reply: Hello, world!")
	if !bytes.Equal(reply, expectedReply) {
		t.Fatalf("收到的回复不符合预期. 预期: %s, 实际: %s", expectedReply, reply)
	}
}

// 测试在没有发现机制的情况下的最小主题大小
func TestMinTopicSizeNoDiscovery(t *testing.T) {
	// 创建一个10秒超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// 延迟取消上下文
	defer cancel()

	// 定义主机数量为3
	const numHosts = 3
	// 定义主题ID
	topicID := "foobar"
	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)

	// 为发送者创建PubSub实例
	sender := getPubsub(ctx, hosts[0])
	// 为接收者1创建PubSub实例
	receiver1 := getPubsub(ctx, hosts[1])
	// 为接收者2创建PubSub实例
	receiver2 := getPubsub(ctx, hosts[2])

	// 连接所有主机
	connectAll(t, hosts)

	// 发送者创建主题
	sendTopic, err := sender.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 接收者1创建并订阅主题
	receiveTopic1, err := receiver1.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 接收者1订阅主题
	sub1, err := receiveTopic1.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 创建第一条消息
	oneMsg := []byte("minimum one")
	// 发布消息,要求至少有1个订阅者
	if err := sendTopic.Publish(ctx, oneMsg, WithReadiness(MinTopicSize(1))); err != nil {
		t.Fatal(err)
	}

	// 接收并验证消息
	if msg, err := sub1.Next(ctx); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(msg.GetData(), oneMsg) {
		t.Fatal("收到的消息与预期不符")
	}

	// 创建第二条消息
	twoMsg := []byte("minimum two")

	// 尝试发布消息,要求至少有2个订阅者(应该失败)
	{
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := sendTopic.Publish(ctx, twoMsg, WithReadiness(MinTopicSize(2))); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}

	// 接收者2创建并订阅主题
	receiveTopic2, err := receiver2.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// 接收者2订阅主题
	sub2, err := receiveTopic2.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 再次尝试发布消息,现在有2个订阅者(应该成功)
	{
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := sendTopic.Publish(ctx, twoMsg, WithReadiness(MinTopicSize(2))); err != nil {
			t.Fatal(err)
		}
	}

	// 接收并验证消息
	if msg, err := sub2.Next(ctx); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(msg.GetData(), twoMsg) {
		t.Fatal("收到的消息与预期不符")
	}
}

// 测试使用自定义主题消息ID函数
func TestWithTopicMsgIdFunction(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟取消上下文
	defer cancel()

	// 定义两个主题名称和主机数量
	const topicA, topicB = "foobarA", "foobarB"
	const numHosts = 2

	// 获取默认主机列表
	hosts := getDefaultHosts(t, numHosts)
	// 创建PubSub实例,使用SHA256作为全局消息ID生成函数
	pubsubs := getPubsubs(ctx, hosts, WithMessageIdFn(func(pmsg *pb.Message) string {
		hash := sha256.Sum256(pmsg.Data)
		return string(hash[:])
	}))
	// 连接所有主机
	connectAll(t, hosts)

	// 创建主题A,使用全局消息ID函数
	topicsA := getTopics(pubsubs, topicA)
	// 创建主题B,使用自定义SHA1消息ID函数
	topicsB := getTopics(pubsubs, topicB, WithTopicMessageIdFn(func(pmsg *pb.Message) string {
		hash := sha1.Sum(pmsg.Data)
		return string(hash[:])
	}))

	// 创建测试消息
	payload := []byte("pubsub rocks")

	// 订阅主题A
	subA, err := topicsA[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 发布消息到主题A
	err = topicsA[1].Publish(ctx, payload, WithReadiness(MinTopicSize(1)))
	if err != nil {
		t.Fatal(err)
	}

	// 接收主题A的消息
	msgA, err := subA.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 订阅主题B
	subB, err := topicsB[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 发布消息到主题B
	err = topicsB[1].Publish(ctx, payload, WithReadiness(MinTopicSize(1)))
	if err != nil {
		t.Fatal(err)
	}

	// 接收主题B的消息
	msgB, err := subB.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 验证两个消息的ID不相同
	if msgA.ID == msgB.ID {
		t.Fatal("消息ID相同")
	}
}

// 测试本地发布功能
func TestWithLocalPublication(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 延迟取消上下文
	defer cancel()

	// 定义主题名称
	const topic = "test"

	// 获取默认主机列表
	hosts := getDefaultHosts(t, 2)
	// 创建PubSub实例
	pubsubs := getPubsubs(ctx, hosts)
	// 创建主题
	topics := getTopics(pubsubs, topic)
	// 连接所有主机
	connectAll(t, hosts)

	// 创建测试消息
	payload := []byte("pubsub smashes")

	// 创建本地订阅
	local, err := topics[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 创建远程订阅
	remote, err := topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 发布本地消息
	err = topics[0].Publish(ctx, payload, WithLocalPublication(true))
	if err != nil {
		t.Fatal(err)
	}

	// 创建100毫秒超时的上下文
	remoteCtx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()

	// 验证远程订阅者没有收到消息
	msg, err := remote.Next(remoteCtx)
	if msg != nil || err == nil {
		t.Fatal("收到了意外的消息")
	}

	// 验证本地订阅者收到了正确的消息
	msg, err = local.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !msg.Local || !bytes.Equal(msg.Data, payload) {
		t.Fatal("消息错误")
	}
}
