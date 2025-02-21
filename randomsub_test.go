package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dep2p/go-dep2p/core/host"
)

// getRandomsub 创建并返回一个带有随机订阅的 PubSub 实例。
// ctx: 上下文
// h: 主机
// size: 订阅大小
// opts: 选项
func getRandomsub(ctx context.Context, h host.Host, size int, opts ...Option) *PubSub {
	ps, err := NewRandomSub(ctx, h, size, opts...)
	if err != nil {
		panic(err)
	}
	return ps
}

// getRandomsubs 创建并返回多个带有随机订阅的 PubSub 实例。
// ctx: 上下文
// hs: 主机列表
// size: 订阅大小
// opts: 选项
func getRandomsubs(ctx context.Context, hs []host.Host, size int, opts ...Option) []*PubSub {
	var psubs []*PubSub
	for _, h := range hs {
		psubs = append(psubs, getRandomsub(ctx, h, size, opts...))
	}
	return psubs
}

// tryReceive 尝试从订阅中接收消息。
// sub: 订阅
func tryReceive(sub *Subscription) *Message {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	m, err := sub.Next(ctx)
	if err != nil {
		return nil
	} else {
		// 使用正确的日志包打印消息
		// logger.Infof("节点 %s 收到消息: %s", sub.topic, string(m.Data))
		return m
	}
}

// TestRandomsubSmall 测试随机订阅的小规模场景。
// 创建 10 个主机和 PubSub 实例，订阅主题并发送消息，验证消息接收情况。
func TestRandomsubSmall(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 10 个默认主机
	hosts := getDefaultHosts(t, 10)
	// 创建 10 个带有随机订阅的 PubSub 实例
	psubs := getRandomsubs(ctx, hosts, 10)

	// 连接所有主机
	connectAll(t, hosts)

	// 先获取所有节点的 Topic 对象
	var topics []*Topic
	for _, ps := range psubs {
		topic, err := ps.Join("test") // 获取 Topic 对象用于发布
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, topic)
	}

	// 订阅主题
	var subs []*Subscription
	for _, topic := range topics {
		sub, err := topic.Subscribe() // 使用 Topic 对象订阅
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待订阅建立
	time.Sleep(2 * time.Second)

	// 发布消息
	count := 0
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		// logger.Infof("节点 %s 发布消息: %s", topics[i].String(), string(msg))

		if err := topics[i].Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs {
			if m := tryReceive(sub); m != nil {
				count++
				// logger.Infof("节点 %d 成功接收消息，当前接收计数: %d", j, count)
			}
		}
	}

	// 等待消息传播
	time.Sleep(time.Second)

	// 检查接收到的消息数量是否符合预期
	if count < 7*len(hosts) {
		t.Fatalf("received too few messages; expected at least %d but got %d", 7*len(hosts), count)
	}
}

// TestRandomsubBig 测试随机订阅的大规模场景。
// 创建 50 个主机和 PubSub 实例，订阅主题并发送消息，验证消息接收情况。
func TestRandomsubBig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 50)
	psubs := getRandomsubs(ctx, hosts, 50)

	connectSome(t, hosts, 12)

	// 先获取所有节点的 Topic 对象
	var topics []*Topic
	for _, ps := range psubs {
		topic, err := ps.Join("test") // 获取 Topic 对象用于发布
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, topic)
	}

	// 订阅主题
	var subs []*Subscription
	for _, topic := range topics {
		sub, err := topic.Subscribe() // 使用 Topic 对象订阅
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待订阅建立
	time.Sleep(2 * time.Second)

	// 发布消息
	count := 0
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		// logger.Infof("节点 %s 发布消息: %s", topics[i].String(), string(msg))

		if err := topics[i].Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs {
			if m := tryReceive(sub); m != nil {
				count++
				// logger.Infof("节点 %d 成功接收消息，当前接收计数: %d", j, count)
			}
		}
	}

	// 等待消息传播
	time.Sleep(time.Second)

	// 检查接收到的消息数量是否符合预期
	if count < 7*len(hosts) {
		t.Fatalf("received too few messages; expected at least %d but got %d", 7*len(hosts), count)
	}
}

// TestRandomsubMixed 测试混合订阅的场景。
// 创建 40 个主机，其中前 10 个使用默认 PubSub，其余使用随机订阅，验证消息接收情况。
func TestRandomsubMixed(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 40 个默认主机
	hosts := getDefaultHosts(t, 40)
	// 创建前 10 个使用默认 PubSub，其余使用随机订阅的 PubSub 实例
	fsubs := getPubsubs(ctx, hosts[:10])
	rsubs := getRandomsubs(ctx, hosts[10:], 30)
	psubs := append(fsubs, rsubs...)

	// 连接部分主机
	connectSome(t, hosts, 12)

	// 订阅 "test" 主题
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待订阅建立
	time.Sleep(2 * time.Second)

	// 发布 10 条消息并统计接收情况
	count := 0
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))

		topic, err := psubs[i].Join("test")
		if err != nil {
			t.Fatal(err)
		}

		err = topic.Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs {
			if tryReceive(sub) != nil {
				count++
			}
		}
	}

	// 等待消息传播
	time.Sleep(time.Second)

	// 检查接收到的消息数量是否符合预期
	if count < 7*len(hosts) {
		t.Fatalf("received too few messages; expected at least %d but got %d", 7*len(hosts), count)
	}
}

// TestRandomsubEnoughPeers 测试随机订阅的足够的 peer 场景。
// 创建 40 个主机，其中前 10 个使用默认 PubSub，其余使用随机订阅，验证足够的 peer 数量。
func TestRandomsubEnoughPeers(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 40 个默认主机
	hosts := getDefaultHosts(t, 40)
	// 创建前 10 个使用默认 PubSub，其余使用随机订阅的 PubSub 实例
	fsubs := getPubsubs(ctx, hosts[:10])
	rsubs := getRandomsubs(ctx, hosts[10:], 30)
	psubs := append(fsubs, rsubs...)

	// 连接部分主机
	connectSome(t, hosts, 12)

	// 订阅 "test" 主题
	for _, ps := range psubs {
		_, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
	}

	// 等待订阅建立
	time.Sleep(2 * time.Second)

	// 验证是否有足够的 peers
	res := make(chan bool, 1)
	rsubs[0].eval <- func() {
		rs := rsubs[0].rt.(*RandomSubRouter)
		res <- rs.EnoughPeers("test", 0)
	}

	enough := <-res
	if !enough {
		t.Fatal("expected enough peers")
	}

	rsubs[0].eval <- func() {
		rs := rsubs[0].rt.(*RandomSubRouter)
		res <- rs.EnoughPeers("test", 100)
	}

	enough = <-res
	if !enough {
		t.Fatal("expected enough peers")
	}
}
