package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dep2p/pubsub/logger"
	pb "github.com/dep2p/pubsub/pb"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"

	"github.com/libp2p/go-msgio/protoio"
)

// getGossipsub 创建一个新的 GossipSub 实例并返回。
//
// 参数：
//   - ctx: context.Context 表示上下文，用于管理请求的生命周期。
//   - h: host.Host 表示网络主机，用于创建 GossipSub 实例。
//   - opts: Option 可选参数，用于配置 GossipSub 实例。
//
// 返回值：
//   - *PubSub: 创建的 GossipSub 实例指针。如果创建失败，则会触发 panic。
func getGossipsub(ctx context.Context, h host.Host, opts ...Option) *PubSub {
	// 调用 NewGossipSub 创建一个新的 GossipSub 实例
	ps, err := NewGossipSub(ctx, h, opts...)
	if err != nil {
		// 如果创建过程中发生错误，则触发 panic
		panic(err)
	}
	// 返回创建的 GossipSub 实例指针
	return ps
}

// getGossipsubs 创建一组 GossipSub 实例并返回。
//
// 参数：
//   - ctx: context.Context 表示上下文，用于管理请求的生命周期。
//   - hs: []host.Host 表示网络主机的切片，用于创建多个 GossipSub 实例。
//   - opts: Option 可选参数，用于配置 GossipSub 实例。
//
// 返回值：
//   - []*PubSub: 创建的 GossipSub 实例指针切片。
func getGossipsubs(ctx context.Context, hs []host.Host, opts ...Option) []*PubSub {
	// 创建一个空的 GossipSub 实例切片
	var psubs []*PubSub
	// 遍历所有主机
	for _, h := range hs {
		// 为每个主机创建一个 GossipSub 实例并添加到切片中
		psubs = append(psubs, getGossipsub(ctx, h, opts...))
	}
	// 返回创建的 GossipSub 实例切片
	return psubs
}

// TestSparseGossipsub 测试稀疏的 GossipSub 网络连接和消息传递。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  3. 通过 sparseConnect 函数建立主机之间的连接。
//  4. 等待一段时间以便建立 GossipSub 网络的拓扑。
//  5. 循环 100 次，生成测试消息并发布到 "foobar" 主题。
//  6. 验证每个订阅者是否能接收到正确的消息。
func TestSparseGossipsub(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	// 确保测试结束时取消上下文
	defer cancel()
	// 获取默认的 20 个主机
	hosts := getDefaultHosts(t, 20)

	// 为所有主机创建 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	var topics []*Topic
	// 创建一个存储所有订阅的切片
	var msgs []*Subscription
	// 遍历所有 GossipSub 实例
	for _, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err)
		}
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, topic)

		// 将订阅添加到切片中
		msgs = append(msgs, subch)
	}

	// 通过 sparseConnect 函数建立主机之间的连接
	sparseConnect(t, hosts)

	// 等待心跳信号建立网络拓扑
	time.Sleep(time.Second * 2)

	// 循环 100 次进行测试
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个 GossipSub 实例作为消息发布者
		owner := rand.Intn(len(psubs))

		// 发布消息到主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			// 从订阅中获取下一条消息
			got, err := sub.Next(ctx)
			if err != nil {
				// 如果获取消息失败，则报告错误
				t.Fatal(sub.err)
			}
			logger.Printf("========== %s", got.Data)
			// 验证接收到的消息是否与发布的消息一致
			if !bytes.Equal(msg, got.Data) {
				// 如果消息不一致，则报告错误
				t.Fatal("got wrong message!")
			}
		}
	}
}

// TestDenseGossipsub 测试密集型 GossipSub 网络连接和消息传递。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  3. 通过 denseConnect 函数建立主机之间的连接。
//  4. 等待一段时间以便建立 GossipSub 网络的拓扑。
//  5. 循环 100 次，生成测试消息并发布到 "foobar" 主题。
//  6. 验证每个订阅者是否能接收到正确的消息。
func TestDenseGossipsub(t *testing.T) {
	// 创建一个可以取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 在函数结束时自动取消上下文

	// 创建 20 个默认的主机
	hosts := getDefaultHosts(t, 20)

	// 为这些主机创建 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	var topics []*Topic      // 存储每个主题的指针
	var msgs []*Subscription // 存储每个订阅的通道

	// 为每个 GossipSub 实例加入主题 "foobar" 并订阅
	for _, ps := range psubs {
		// 加入主题 "foobar"
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果加入失败，则终止测试
		}

		// 订阅该主题
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，则终止测试
		}

		// 将主题和订阅添加到列表中
		topics = append(topics, topic)
		msgs = append(msgs, subch)
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳以建立 GossipSub 网络的拓扑
	time.Sleep(time.Second * 2)

	// 循环 100 次，生成测试消息并发布到 "foobar" 主题
	for i := 0; i < 100; i++ {
		// 生成一条消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个 GossipSub 实例发布消息
		owner := rand.Intn(len(topics))

		// 发布消息到主题 "foobar"
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，则终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			// 从订阅通道中接收消息
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收消息失败，则终止测试
			}
			// 比较接收到的消息和发送的消息
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果消息不匹配，则终止测试
			}
		}
	}
}

// TestGossipsubFanout 测试 GossipSub 的广播功能，确保消息可以被所有订阅者接收到。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例。
//  3. 订阅所有除第一个外的 GossipSub 实例的 "foobar" 主题。
//  4. 通过 denseConnect 函数建立主机之间的连接。
//  5. 等待一段时间以便建立 GossipSub 网络的拓扑。
//  6. 循环 100 次，生成测试消息并通过 topic.Publish 发布到 "foobar" 主题，验证所有订阅者是否能接收到正确的消息。
//  7. 订阅第一个 GossipSub 实例的 "foobar" 主题。
//  8. 等待一段时间以便新的订阅生效。
//  9. 循环 100 次，生成测试消息并通过 topic.Publish 发布到 "foobar" 主题，验证所有订阅者（包括第一个 GossipSub 实例）是否能接收到正确的消息。
func TestGossipsubFanout(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 创建对应的 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 创建主题“foobar”，并订阅所有除第一个外的 GossipSub 实例
	topic, err := psubs[0].Join("foobar")
	if err != nil {
		t.Fatal(err)
	}

	// 存储所有订阅的通道
	var msgs []*Subscription

	// 为所有除第一个外的 GossipSub 实例订阅 "foobar" 主题
	for _, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 第一次消息广播：在订阅者中传播消息，确保消息正确传递
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topic.Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}

	// 订阅第一个 GossipSub 实例的 "foobar" 主题
	subch, err := psubs[0].Subscribe("foobar")
	if err != nil {
		t.Fatal(err) // 如果订阅失败，终止测试
	}
	msgs = append(msgs, subch) // 将新订阅的通道添加到列表中

	// 等待一个心跳周期，以便新的订阅生效
	time.Sleep(time.Second * 1)

	// 第二次消息广播：包括新订阅者在内，验证所有订阅者是否正确接收消息
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topic.Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者（包括第一个 GossipSub 实例）是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubFanoutExpiry 测试 GossipSub 的广播功能，确保在 Fanout TTL 过期后，fanout 会被正确清除。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 设置 GossipSub 的 Fanout TTL 为 1 秒。
//  2. 创建测试上下文，并获取默认的 10 个主机。
//  3. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  4. 通过 denseConnect 函数建立主机之间的连接。
//  5. 循环 5 次，生成测试消息并通过 topic.Publish 发布到 "foobar" 主题，验证所有订阅者是否能接收到正确的消息。
//  6. 检查 fanout 是否存在。
//  7. 等待 TTL 过期后，检查 fanout 是否已被清除。
//  8. 等待事件循环完成。
func TestGossipsubFanoutExpiry(t *testing.T) {
	// 设置 GossipSub 的 Fanout TTL 为 1 秒
	GossipSubFanoutTTL = 1 * time.Second
	defer func() {
		GossipSubFanoutTTL = 60 * time.Second
	}()

	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 10 个默认的主机实例
	hosts := getDefaultHosts(t, 10)

	// 创建对应的 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 创建主题“foobar”，并订阅所有除第一个外的 GossipSub 实例
	topic, err := psubs[0].Join("foobar")
	if err != nil {
		t.Fatal(err)
	}

	// 存储所有订阅的通道
	var msgs []*Subscription

	// 为所有除第一个外的 GossipSub 实例订阅 "foobar" 主题
	for _, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 在 Fanout TTL 内广播 5 次消息，并验证订阅者是否接收到正确的消息
	for i := 0; i < 5; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topic.Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}

	// 在 eval 函数中检查 fanout 是否存在
	psubs[0].eval <- func() {
		if len(psubs[0].rt.(*GossipSubRouter).fanout) == 0 {
			t.Fatal("owner has no fanout")
		}
	}

	// 等待 TTL 过期，以检查 fanout 是否已被清除
	time.Sleep(time.Second * 2)

	// 在 eval 函数中检查 fanout 是否已被清除
	psubs[0].eval <- func() {
		if len(psubs[0].rt.(*GossipSubRouter).fanout) > 0 {
			t.Fatal("fanout hasn't expired")
		}
	}

	// 等待事件循环完成
	time.Sleep(10 * time.Millisecond)
}

// TestGossipsubGossip 测试 GossipSub 的网络广播功能，确保消息可以通过 Gossip 协议在网络中正确传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  3. 通过 denseConnect 函数建立主机之间的连接。
//  4. 等待一段时间以便建立 GossipSub 网络的拓扑。
//  5. 循环 100 次，随机选择一个主机发布消息到 "foobar" 主题，验证所有订阅者是否能接收到正确的消息。
//  6. 在每次消息发布后，等待一小段时间以模拟 Gossip 协议的消息传播。
//  7. 最后等待一段时间，以确保 Gossip 消息的刷新完成。
func TestGossipsubGossip(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 创建对应的 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 存储所有订阅的通道
	var msgs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 为每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 在 Gossip 网络中随机选择主机进行消息发布和验证
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个主机作为消息发布者
		owner := rand.Intn(len(psubs))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}

		// 等待一小段时间，以便消息在 Gossip 网络中传播
		time.Sleep(time.Millisecond * 100)
	}

	// 最后等待一段时间，以确保 Gossip 消息的刷新完成
	time.Sleep(time.Second * 2)
}

// TestGossipsubGossipPiggyback 测试 GossipSub 的网络广播功能，通过 Piggyback 的方式，确保消息可以在不同主题之间正确传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并分别订阅 "foobar" 和 "bazcrux" 两个主题。
//  3. 通过 denseConnect 函数建立主机之间的连接。
//  4. 等待一段时间以便建立 GossipSub 网络的拓扑。
//  5. 循环 100 次，随机选择一个主机发布消息到 "foobar" 和 "bazcrux" 主题，验证所有订阅者是否能接收到正确的消息。
//  6. 在每次消息发布后，等待一小段时间以模拟 Gossip 协议的消息传播。
//  7. 最后等待一段时间，以确保 Gossip 消息的刷新完成。
func TestGossipsubGossipPiggyback(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 创建对应的 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 存储所有订阅的通道
	var msgs []*Subscription
	var xmsgs []*Subscription
	foobarTopics := make([]*Topic, len(psubs))
	bazcruxTopics := make([]*Topic, len(psubs))

	// 为每个 GossipSub 实例加入 "foobar" 和 "bazcrux" 主题并订阅
	for i, ps := range psubs {
		// 加入 "foobar" 主题
		foobarTopic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		foobarTopics[i] = foobarTopic

		subch, err := foobarTopic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中

		// 加入 "bazcrux" 主题
		bazcruxTopic, err := ps.Join("bazcrux")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		bazcruxTopics[i] = bazcruxTopic

		subch, err = bazcruxTopic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		xmsgs = append(xmsgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 在 Gossip 网络中随机选择主机进行消息发布和验证
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个主机作为消息发布者
		owner := rand.Intn(len(psubs))

		// 通过 topic.Publish 发布消息到 "foobar" 和 "bazcrux" 主题
		if err := foobarTopics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}
		if err := bazcruxTopics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有 "foobar" 订阅者是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}

		// 验证所有 "bazcrux" 订阅者是否接收到正确的消息
		for _, sub := range xmsgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}

		// 等待一小段时间，以便消息在 Gossip 网络中传播
		time.Sleep(time.Millisecond * 100)
	}

	// 最后等待一段时间，以确保 Gossip 消息的刷新完成
	time.Sleep(time.Second * 2)
}

// TODO: 测试报错
//
// TestGossipsubGossipPropagation 测试 GossipSub 的消息传播功能，确保消息可以在不同连接组之间正确传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 将这些主机分为两个组，分别进行密集连接。
//  3. 为第一个组中的 GossipSub 实例订阅 "foobar" 主题，并进行消息发布和验证。
//  4. 等待一小段时间后，为第二个组中的 GossipSub 实例订阅 "foobar" 主题，并验证其是否能够正确接收到来自第一个组的消息。
//  5. 验证消息在网络中正确传播。
func TestGossipsubGossipPropagation(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)
	psubs := getGossipsubs(ctx, hosts)

	// 将主机分为两组
	hosts1 := hosts[:GossipSubD+1]
	hosts2 := append(hosts[GossipSubD+1:], hosts[0])

	// 对两个组分别进行密集连接
	denseConnect(t, hosts1)
	denseConnect(t, hosts2)

	// 存储第一个组的订阅通道
	var msgs1 []*Subscription
	topics1 := make([]*Topic, len(hosts1))

	// 为第一个组中的每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs[1 : GossipSubD+1] {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics1[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs1 = append(msgs1, subch) // 将订阅通道添加到列表中
	}

	// 等待 GossipSub 网络的拓扑建立
	time.Sleep(time.Second * 1)

	// 在第一个组中进行消息发布和验证
	for i := 0; i < 10; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 选择第一个主机作为消息发布者
		owner := 0

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics1[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证第一个组的所有订阅者是否接收到正确的消息
		for _, sub := range msgs1 {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}

	// 等待一小段时间以便消息传播
	time.Sleep(time.Millisecond * 100)

	// 存储第二个组的订阅通道
	var msgs2 []*Subscription
	topics2 := make([]*Topic, len(hosts2))

	// 为第二个组中的每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs[GossipSubD+1:] {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics2[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs2 = append(msgs2, subch) // 将订阅通道添加到列表中
	}

	// 收集第二个组收到的消息
	var collect [][]byte
	for i := 0; i < 10; i++ {
		for _, sub := range msgs2 {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			collect = append(collect, got.Data)
		}
	}

	// 验证消息在网络中的正确传播
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))
		gotit := false
		for j := 0; j < len(collect); j++ {
			if bytes.Equal(msg, collect[j]) {
				gotit = true
				break
			}
		}
		if !gotit {
			t.Fatalf("Didn't get message %s", string(msg)) // 如果未收到消息，终止测试
		}
	}
}

// TestGossipsubPrune 测试 GossipSub 的 PRUNE 操作，即当部分节点从消息传播网络中移除时，消息依然能够正常传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  3. 通过 denseConnect 函数建立主机之间的连接。
//  4. 等待一段时间以便 GossipSub 网络的拓扑建立。
//  5. 取消部分订阅，模拟节点从网络中移除（PRUNE 操作）。
//  6. 验证剩余的订阅者是否能够正常接收到消息。
func TestGossipsubPrune(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)
	psubs := getGossipsubs(ctx, hosts)

	// 存储所有订阅的通道
	var msgs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 为每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 取消部分订阅，模拟 PRUNE 操作
	for _, sub := range msgs[:5] {
		sub.Cancel()
	}

	// 等待 PRUNE 操作生效
	time.Sleep(time.Millisecond * 100)

	// 验证消息在剩余订阅者中的传播
	for i := 0; i < 10; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个主机作为消息发布者
		owner := rand.Intn(len(psubs))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证剩余订阅者是否接收到正确的消息
		for _, sub := range msgs[5:] {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubPruneBackoffTime 测试 GossipSub 的 PRUNE 操作后的回退时间，确保 PRUNE 操作后节点会有适当的回退时间。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 10 个主机。
//  2. 设置 GossipSub 参数，包括心跳初始延迟和心跳间隔。
//  3. 设置 PeerScore，模拟特定主机的评分变化。
//  4. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  5. 通过 connectAll 函数建立主机之间的连接。
//  6. 等待一段时间以便 GossipSub 网络的拓扑建立。
//  7. 修改特定主机的评分，触发 PRUNE 操作，并验证回退时间是否正确。
//  8. 验证消息在剩余订阅者中的传播。
func TestGossipsubPruneBackoffTime(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 10 个默认的主机实例
	hosts := getDefaultHosts(t, 10)

	// 设置特定主机的初始评分
	currentScoreForHost0 := int32(0)

	// 配置 GossipSub 参数
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Millisecond * 10
	params.HeartbeatInterval = time.Millisecond * 100

	// 创建 GossipSub 实例并配置 PeerScore
	psubs := getGossipsubs(ctx, hosts, WithGossipSubParams(params), WithPeerScore(
		&PeerScoreParams{
			AppSpecificScore: func(p peer.ID) float64 {
				if p == hosts[0].ID() {
					return float64(atomic.LoadInt32(&currentScoreForHost0))
				}
				return 0
			},
			AppSpecificWeight: 1,
			DecayInterval:     time.Second,
			DecayToZero:       0.01,
		},
		&PeerScoreThresholds{
			GossipThreshold:   -1,
			PublishThreshold:  -1,
			GraylistThreshold: -1,
		}))

	// 存储所有订阅的通道
	var msgs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 为每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的连接
	connectAll(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second)

	pruneTime := time.Now()
	// 修改特定主机的评分，使其被其他主机 PRUNE
	atomic.StoreInt32(&currentScoreForHost0, -1000)

	// 等待心跳运行并触发 PRUNE 操作
	time.Sleep(time.Second)

	// 检查回退时间
	wg := sync.WaitGroup{}
	var missingBackoffs uint32 = 0
	for i := 1; i < len(psubs); i++ {
		wg.Add(1)
		var idx = i
		psubs[idx].rt.(*GossipSubRouter).p.eval <- func() {
			defer wg.Done()
			backoff, ok := psubs[idx].rt.(*GossipSubRouter).backoff["foobar"][hosts[0].ID()]
			if !ok {
				atomic.AddUint32(&missingBackoffs, 1)
			}
			if ok && backoff.Sub(pruneTime)-params.PruneBackoff > time.Second {
				t.Errorf("backoff time should be equal to prune backoff (with some slack), was %v", backoff.Sub(pruneTime)-params.PruneBackoff)
			}
		}
	}
	wg.Wait()

	// 检查回退时间是否正确设置
	if missingBackoffs >= 5 {
		t.Errorf("missing too many backoffs: %v", missingBackoffs)
	}

	// 验证消息在剩余订阅者中的传播
	for i := 0; i < 10; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 不从主机 0 发布消息，因为它应该已经被 PRUNE
		owner := rand.Intn(len(psubs)-1) + 1

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证剩余订阅者是否接收到正确的消息
		for _, sub := range msgs[1:] {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubGraft 测试 GossipSub 的 GRAFT 操作，确保节点可以通过 GRAFT 操作重新加入消息传播网络。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  3. 通过 sparseConnect 函数建立稀疏连接的网络拓扑。
//  4. 等待一段时间以便 GossipSub 网络的拓扑建立。
//  5. 验证消息可以在重新 GRAFT 的节点间正确传播。
func TestGossipsubGraft(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)
	psubs := getGossipsubs(ctx, hosts)

	// 建立主机之间的稀疏连接
	sparseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 1)

	// 存储所有订阅的通道
	var msgs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 为每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中

		// 等待公告传播
		time.Sleep(time.Millisecond * 100)
	}

	// 等待心跳周期，以便 GossipSub 网络拓扑稳定
	time.Sleep(time.Second * 1)

	// 在 Gossip 网络中随机选择主机进行消息发布和验证
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个主机作为消息发布者
		owner := rand.Intn(len(psubs))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubRemovePeer 测试 GossipSub 的 RemovePeer 操作，确保在移除部分节点后，消息依然能够在剩余节点中传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "foobar" 主题。
//  3. 通过 denseConnect 函数建立主机之间的密集连接。
//  4. 等待一段时间以便 GossipSub 网络的拓扑建立。
//  5. 关闭部分主机，模拟节点移除操作（RemovePeer）。
//  6. 验证剩余的订阅者是否能够正常接收到消息。
func TestGossipsubRemovePeer(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)
	psubs := getGossipsubs(ctx, hosts)

	// 存储所有订阅的通道
	var msgs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 为每个 GossipSub 实例加入 "foobar" 主题并订阅
	for i, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 关闭部分主机，模拟节点移除操作（RemovePeer）
	for _, host := range hosts[:5] {
		host.Close()
	}

	// 等待心跳周期，让移除操作生效
	time.Sleep(time.Second * 1)

	// 验证消息在剩余订阅者中的传播
	for i := 0; i < 10; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个未被移除的主机作为消息发布者
		owner := 5 + rand.Intn(len(psubs)-5)

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证剩余订阅者是否接收到正确的消息
		for _, sub := range msgs[5:] {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubGraftPruneRetry 测试 GossipSub 的 GRAFT 和 PRUNE 操作，
// 确保节点可以在多主题场景下正确处理 GRAFT 和 PRUNE 操作，并在需要时重试。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 10 个主机。
//  2. 为每个主机创建 GossipSub 实例，并为 35 个不同的主题订阅。
//  3. 通过 denseConnect 函数建立主机之间的密集连接。
//  4. 等待一段时间以便 GossipSub 网络的拓扑建立。
//  5. 对每个主题进行消息发布，验证所有订阅者是否能接收到正确的消息。
func TestGossipsubGraftPruneRetry(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 10 个默认的主机实例
	hosts := getDefaultHosts(t, 10)
	psubs := getGossipsubs(ctx, hosts)

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 存储主题和订阅通道
	var topics []string
	var msgs [][]*Subscription

	// 为 35 个不同的主题订阅
	for i := 0; i < 35; i++ {
		topicName := fmt.Sprintf("topic%d", i)
		topics = append(topics, topicName)

		var subs []*Subscription
		for _, ps := range psubs {
			// 为每个 GossipSub 实例加入相应的主题
			topic, err := ps.Join(topicName)
			if err != nil {
				t.Fatal(err) // 如果主题加入失败，终止测试
			}

			// 订阅主题
			subch, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err) // 如果订阅失败，终止测试
			}
			subs = append(subs, subch) // 将订阅通道添加到列表中
		}
		msgs = append(msgs, subs) // 将每个主题的订阅列表存储起来
	}

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 5)

	// 对每个主题进行消息发布和验证
	for i, topicName := range topics {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个主机作为消息发布者
		owner := rand.Intn(len(psubs))

		// 通过 topic.Publish 发布消息到相应的主题
		topic, err := psubs[owner].Join(topicName)
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		if err := topic.Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs[i] {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubControlPiggyback 测试在控制消息的情况下，GossipSub 是否能够在消息泛洪期间正确处理 Piggyback（携带）控制消息。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 10 个主机。
//  2. 为每个主机创建 GossipSub 实例，并订阅 "flood" 主题。
//  3. 通过 denseConnect 函数建立主机之间的密集连接。
//  4. 开启一个后台任务，生成大量 "flood" 消息，模拟消息泛洪以超载队列。
//  5. 同时订阅多个新主题，测试在消息泛洪的情况下是否会丢失控制消息，并且是否通过 Piggyback 机制进行补偿。
//  6. 验证所有订阅者是否能接收到正确的消息。
func TestGossipsubControlPiggyback(t *testing.T) {
	// 跳过此测试，因为在 Travis CI 上经常失败
	t.Skip("travis regularly fails on this test")

	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 10 个默认的主机实例
	hosts := getDefaultHosts(t, 10)
	psubs := getGossipsubs(ctx, hosts)

	// 建立主机之间的密集连接
	denseConnect(t, hosts)

	// 订阅 "flood" 主题并启动 goroutine 消费消息
	for _, ps := range psubs {
		subch, err := ps.Subscribe("flood")
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		go func(sub *Subscription) {
			for {
				_, err := sub.Next(ctx)
				if err != nil {
					break // 如果接收失败，终止循环
				}
			}
		}(subch)
	}

	// 等待一段时间，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 1)

	// 创建一个后台泛洪任务，发送大量消息以超载队列
	done := make(chan struct{})
	go func() {
		owner := rand.Intn(len(psubs))
		topic, err := psubs[owner].Join("flood")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		for i := 0; i < 10000; i++ {
			msg := []byte("background flooooood")
			if err := topic.Publish(ctx, msg); err != nil {
				t.Fatal(err) // 如果发布失败，终止测试
			}
		}
		done <- struct{}{}
	}()

	// 等待一小段时间，让泛洪任务开始工作
	time.Sleep(time.Millisecond * 20)

	// 同时订阅多个新主题，测试 Piggyback 控制消息的处理
	var topics []string
	var msgs [][]*Subscription
	for i := 0; i < 5; i++ {
		topicName := fmt.Sprintf("topic%d", i)
		topics = append(topics, topicName)

		var subs []*Subscription
		for _, ps := range psubs {
			topic, err := ps.Join(topicName)
			if err != nil {
				t.Fatal(err) // 如果主题加入失败，终止测试
			}
			subch, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err) // 如果订阅失败，终止测试
			}
			subs = append(subs, subch) // 将订阅通道添加到列表中
		}
		msgs = append(msgs, subs) // 将每个主题的订阅列表存储起来
	}

	// 等待泛洪任务结束
	<-done

	// 验证新订阅的主题是否正常工作
	for i, topicName := range topics {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		topic, err := psubs[owner].Join(topicName)
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		if err := topic.Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs[i] {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestMixedGossipsub 测试混合使用 GossipSub 和普通 PubSub 节点的网络消息传播功能，确保消息在混合网络中正确传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 30 个主机。
//  2. 为前 20 个主机创建 GossipSub 实例，为后 10 个主机创建普通 PubSub 实例。
//  3. 所有节点订阅 "foobar" 主题。
//  4. 通过 sparseConnect 函数建立主机之间的稀疏连接。
//  5. 等待一段时间以便 GossipSub 网络拓扑建立。
//  6. 循环 100 次，随机选择一个主机发布消息到 "foobar" 主题，验证所有订阅者是否能接收到正确的消息。
func TestMixedGossipsub(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 30 个默认的主机实例
	hosts := getDefaultHosts(t, 30)

	// 创建前 20 个主机的 GossipSub 实例，后 10 个主机的普通 PubSub 实例
	gsubs := getGossipsubs(ctx, hosts[:20])
	fsubs := getPubsubs(ctx, hosts[20:])
	psubs := append(gsubs, fsubs...)

	// 存储所有订阅的通道
	var msgs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 所有节点订阅 "foobar" 主题
	for i, ps := range psubs {
		topic, err := ps.Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		msgs = append(msgs, subch) // 将订阅通道添加到列表中
	}

	// 建立主机之间的稀疏连接
	sparseConnect(t, hosts)

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 在混合网络中随机选择主机进行消息发布和验证
	for i := 0; i < 100; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		// 随机选择一个主机作为消息发布者
		owner := rand.Intn(len(psubs))

		// 通过 topic.Publish 发布消息到 "foobar" 主题
		if err := topics[owner].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		// 验证所有订阅者是否接收到正确的消息
		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err) // 如果接收失败，终止测试
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!") // 如果接收消息与发送消息不一致，终止测试
			}
		}
	}
}

// TestGossipsubMultihops 测试 GossipSub 的多跳传播功能，确保消息能够通过多个节点的链路传播到终点。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 6 个主机。
//  2. 为每个主机创建 GossipSub 实例。
//  3. 按顺序连接主机，使它们形成一个链路。
//  4. 订阅从第二个到第六个主机的 "foobar" 主题。
//  5. 等待一段时间，以便 GossipSub 网络拓扑建立。
//  6. 从第一个主机发布消息，并验证消息能否传递到链路末端的主机。
func TestGossipsubMultihops(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 6 个默认的主机实例
	hosts := getDefaultHosts(t, 6)

	// 为每个主机创建 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 按顺序连接主机，形成一个链路
	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[2], hosts[3])
	connect(t, hosts[3], hosts[4])
	connect(t, hosts[4], hosts[5])

	// 存储订阅通道
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 从第二个到第六个主机订阅 "foobar" 主题
	for i := 1; i < 6; i++ {
		topic, err := psubs[i].Join("foobar")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		ch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, ch) // 将订阅通道添加到列表中
	}

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 生成测试消息
	msg := []byte("i like cats")

	// 从第一个主机发布消息
	err := topics[0].Publish(ctx, msg)
	if err != nil {
		t.Fatal(err) // 如果发布失败，终止测试
	}

	// 验证链路末端的主机是否接收到消息
	select {
	case out := <-subs[4].ch:
		if !bytes.Equal(out.GetData(), msg) {
			t.Fatal("got wrong data") // 如果接收消息与发送消息不一致，终止测试
		}
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for message") // 如果超时未收到消息，终止测试
	}
}

// TestGossipsubTreeTopology 测试 GossipSub 在树形拓扑结构中的消息传播功能，确保消息能够沿着树形结构正确传播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 10 个主机。
//  2. 为每个主机创建 GossipSub 实例。
//  3. 将主机按树形拓扑结构连接。
//  4. 订阅每个 GossipSub 实例的 "fizzbuzz" 主题。
//  5. 等待一段时间，以便 GossipSub 网络拓扑建立。
//  6. 验证树形拓扑中各节点的对等连接情况。
//  7. 检查消息能否从特定节点正确路由到目标节点。
func TestGossipsubTreeTopology(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 10 个默认的主机实例
	hosts := getDefaultHosts(t, 10)

	// 为每个主机创建 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 按照树形拓扑结构连接主机
	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[1], hosts[4])
	connect(t, hosts[2], hosts[3])
	connect(t, hosts[0], hosts[5])
	connect(t, hosts[5], hosts[6])
	connect(t, hosts[5], hosts[8])
	connect(t, hosts[6], hosts[7])
	connect(t, hosts[8], hosts[9])

	/*
		拓扑结构示意图:
		[0] -> [1] -> [2] -> [3]
		 |      L->[4]
		 v
		[5] -> [6] -> [7]
		 |
		 v
		[8] -> [9]
	*/

	// 存储所有订阅的通道
	var chs []*Subscription
	topics := make([]*Topic, len(psubs))

	// 订阅每个 GossipSub 实例的 "fizzbuzz" 主题
	for i, ps := range psubs {
		topic, err := ps.Join("fizzbuzz")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		ch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		chs = append(chs, ch) // 将订阅通道添加到列表中
	}

	// 等待心跳周期，以便 GossipSub 网络拓扑建立
	time.Sleep(time.Second * 2)

	// 验证每个节点的对等连接情况
	assertPeerLists(t, hosts, psubs[0], 1, 5)
	assertPeerLists(t, hosts, psubs[1], 0, 2, 4)
	assertPeerLists(t, hosts, psubs[2], 1, 3)

	// 检查消息能否从节点 9 和 3 通过 GossipSub 网络正确路由到其他节点
	checkMessageRouting(t, "fizzbuzz", []*PubSub{psubs[9], psubs[3]}, chs)
}

// TestGossipsubStarTopology 测试 GossipSub 在星形拓扑结构中的网络构建和消息传播能力。
// 通过 prune 机制和 peer exchange 来构建网络的 mesh。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 保存并修改 GossipSub 参数以适应测试。
//  2. 创建测试上下文，并获取默认的 20 个主机。
//  3. 为每个主机创建 GossipSub 实例，并启用 Peer Exchange 和 Flood Publish。
//  4. 配置星形拓扑中心节点（第一个节点）的连接参数，使其具有极低的连接度。
//  5. 将所有对等节点的地址添加到 peerstore 中。
//  6. 构建星形拓扑结构，将所有其他节点连接到中心节点。
//  7. 订阅每个 GossipSub 实例的 "test" 主题。
//  8. 等待一段时间以便 GossipSub 网络 mesh 构建完成。
//  9. 验证每个节点是否有超过 1 个连接，确保 mesh 成功构建。
//
// 10. 从每个节点发送消息，验证消息是否在所有订阅者中成功传播。
func TestGossipsubStarTopology(t *testing.T) {
	// 保存并修改 GossipSub 的全局参数
	originalGossipSubD := GossipSubD
	GossipSubD = 4
	originalGossipSubDhi := GossipSubDhi
	GossipSubDhi = GossipSubD + 1
	originalGossipSubDlo := GossipSubDlo
	GossipSubDlo = GossipSubD - 1
	originalGossipSubDscore := GossipSubDscore
	GossipSubDscore = GossipSubDlo

	// 在函数结束后恢复原始参数
	defer func() {
		GossipSubD = originalGossipSubD
		GossipSubDhi = originalGossipSubDhi
		GossipSubDlo = originalGossipSubDlo
		GossipSubDscore = originalGossipSubDscore
	}()

	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 为每个主机创建 GossipSub 实例，并启用 Peer Exchange 和 Flood Publish
	psubs := getGossipsubs(ctx, hosts, WithPeerExchange(true), WithFloodPublish(true))

	// 配置星形拓扑中心节点（第一个节点）的连接参数，使其具有极低的连接度
	psubs[0].eval <- func() {
		gs := psubs[0].rt.(*GossipSubRouter)
		gs.params.D = 0
		gs.params.Dlo = 0
		gs.params.Dhi = 0
		gs.params.Dscore = 0
	}

	// 将所有对等节点的地址添加到 peerstore 中
	for i := range hosts {
		for j := range hosts {
			if i == j {
				continue
			}
			hosts[i].Peerstore().AddAddrs(hosts[j].ID(), hosts[j].Addrs(), peerstore.PermanentAddrTTL)
		}
	}

	// 构建星形拓扑，将所有其他节点连接到中心节点
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	// 等待一段时间以便网络连接稳定
	time.Sleep(time.Second)

	// 订阅每个 GossipSub 实例的 "test" 主题
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待 10 秒以便 GossipSub 网络的 mesh 构建完成
	time.Sleep(10 * time.Second)

	// 验证每个节点是否有超过 1 个连接，确保 mesh 成功构建
	for i, h := range hosts {
		if len(h.Network().Conns()) == 1 {
			t.Errorf("peer %d has only a single connection", i)
		}
	}

	// 从每个节点发送消息，并验证所有订阅者是否接收到消息
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))

		if err := topics[i].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		for _, sub := range subs {
			assertReceive(t, sub, msg) // 验证消息接收
		}
	}
}

// TestGossipsubStarTopologyWithSignedPeerRecords 测试 GossipSub 在具有签名的 Peer 记录情况下的星形拓扑结构中的网络构建和消息传播能力。
// 通过 prune 机制和 peer exchange 构建网络的 mesh，并使用签名的 Peer 记录交换地址。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 保存并修改 GossipSub 参数以适应测试。
//  2. 创建测试上下文，并获取默认的 20 个主机。
//  3. 为每个主机创建 GossipSub 实例，并启用 Peer Exchange 和 Flood Publish。
//  4. 配置星形拓扑中心节点（第一个节点）的连接参数，使其具有极低的连接度。
//  5. 手动为每个主机创建签名的 Peer 记录，并将其添加到中心节点的 peerstore 中。
//  6. 构建星形拓扑结构，将所有其他节点连接到中心节点。
//  7. 订阅每个 GossipSub 实例的 "test" 主题。
//  8. 等待一段时间以便 GossipSub 网络 mesh 构建完成。
//  9. 验证每个节点是否有超过 1 个连接，确保 mesh 成功构建。
//
// 10. 从每个节点发送消息，验证消息是否在所有订阅者中成功传播。
func TestGossipsubStarTopologyWithSignedPeerRecords(t *testing.T) {
	// 保存并修改 GossipSub 的全局参数
	originalGossipSubD := GossipSubD
	GossipSubD = 4
	originalGossipSubDhi := GossipSubDhi
	GossipSubDhi = GossipSubD + 1
	originalGossipSubDlo := GossipSubDlo
	GossipSubDlo = GossipSubD - 1
	originalGossipSubDscore := GossipSubDscore
	GossipSubDscore = GossipSubDlo

	// 在函数结束后恢复原始参数
	defer func() {
		GossipSubD = originalGossipSubD
		GossipSubDhi = originalGossipSubDhi
		GossipSubDlo = originalGossipSubDlo
		GossipSubDscore = originalGossipSubDscore
	}()

	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 为每个主机创建 GossipSub 实例，并启用 Peer Exchange 和 Flood Publish
	psubs := getGossipsubs(ctx, hosts, WithPeerExchange(true), WithFloodPublish(true))

	// 配置星形拓扑中心节点（第一个节点）的连接参数，使其具有极低的连接度
	psubs[0].eval <- func() {
		gs := psubs[0].rt.(*GossipSubRouter)
		gs.params.D = 0
		gs.params.Dlo = 0
		gs.params.Dhi = 0
		gs.params.Dscore = 0
	}

	// 手动为每个主机创建签名的 Peer 记录，并将其添加到中心节点的 peerstore 中
	for i := range hosts[1:] {
		privKey := hosts[i].Peerstore().PrivKey(hosts[i].ID())
		if privKey == nil {
			t.Fatalf("unable to get private key for host %s", hosts[i].ID())
		}
		ai := host.InfoFromHost(hosts[i])
		rec := peer.PeerRecordFromAddrInfo(*ai)
		signedRec, err := record.Seal(rec, privKey)
		if err != nil {
			t.Fatalf("error creating signed peer record: %s", err)
		}

		cab, ok := peerstore.GetCertifiedAddrBook(hosts[0].Peerstore())
		if !ok {
			t.Fatal("peerstore does not implement CertifiedAddrBook")
		}
		_, err = cab.ConsumePeerRecord(signedRec, peerstore.PermanentAddrTTL)
		if err != nil {
			t.Fatalf("error adding signed peer record: %s", err)
		}
	}

	// 构建星形拓扑结构，将所有其他节点连接到中心节点
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	// 等待一段时间以便网络连接稳定
	time.Sleep(time.Second)

	// 订阅每个 GossipSub 实例的 "test" 主题
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待 10 秒以便 GossipSub 网络的 mesh 构建完成
	time.Sleep(10 * time.Second)

	// 验证每个节点是否有超过 1 个连接，确保 mesh 成功构建
	for i, h := range hosts {
		if len(h.Network().Conns()) == 1 {
			t.Errorf("peer %d has only a single connection", i)
		}
	}

	// 从每个节点发送消息，并验证所有订阅者是否接收到消息
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))

		if err := topics[i].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		for _, sub := range subs {
			assertReceive(t, sub, msg) // 验证消息接收
		}
	}
}

// TestGossipsubDirectPeers 测试 GossipSub 的 Direct Peers 功能，确保直接对等节点能够正确连接并在断开后重新连接。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 3 个主机。
//  2. 创建 GossipSub 实例，配置直接对等连接。
//  3. 建立主机之间的初始连接，验证直接对等节点之间的连接是否成功。
//  4. 订阅每个 GossipSub 实例的 "test" 主题。
//  5. 发布测试消息，验证所有订阅者是否接收到正确的消息。
//  6. 断开直接对等节点之间的连接，等待一段时间后验证它们是否成功重新连接。
//  7. 再次发布测试消息，验证消息的传播和订阅者的接收情况。
func TestGossipsubDirectPeers(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 3 个默认的主机实例
	h := getDefaultHosts(t, 3)

	// 创建 GossipSub 实例并配置直接对等连接
	psubs := []*PubSub{
		getGossipsub(ctx, h[0], WithDirectConnectTicks(2)),                                                                         // 配置第一个主机，使用 DirectConnectTicks 选项
		getGossipsub(ctx, h[1], WithDirectPeers([]peer.AddrInfo{{ID: h[2].ID(), Addrs: h[2].Addrs()}}), WithDirectConnectTicks(2)), // 第二个主机连接到第三个主机
		getGossipsub(ctx, h[2], WithDirectPeers([]peer.AddrInfo{{ID: h[1].ID(), Addrs: h[1].Addrs()}}), WithDirectConnectTicks(2)), // 第三个主机连接到第二个主机
	}

	// 建立主机之间的初始连接
	connect(t, h[0], h[1])
	connect(t, h[0], h[2])

	// 等待一段时间以便直接对等节点之间建立连接
	time.Sleep(2 * time.Second)

	// 验证直接对等节点之间的连接是否成功
	if len(h[1].Network().ConnsToPeer(h[2].ID())) == 0 {
		t.Fatal("expected a connection between direct peers")
	}

	// 订阅每个 GossipSub 实例的 "test" 主题
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待一段时间以便 GossipSub 网络的 mesh 构建完成
	time.Sleep(time.Second)

	// 发布测试消息，验证所有订阅者是否接收到正确的消息
	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		if err := topics[i].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		for _, sub := range subs {
			assertReceive(t, sub, msg) // 验证消息接收
		}
	}

	// 断开直接对等节点之间的连接，以测试重新连接功能
	for _, c := range h[1].Network().ConnsToPeer(h[2].ID()) {
		c.Close() // 关闭连接
	}

	// 等待 5 秒以便直接对等节点重新连接
	time.Sleep(5 * time.Second)

	// 验证重新连接是否成功
	if len(h[1].Network().ConnsToPeer(h[2].ID())) == 0 {
		t.Fatal("expected a connection between direct peers")
	}

	// 再次发布测试消息，验证消息的传播和订阅者的接收情况
	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		if err := topics[i].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		for _, sub := range subs {
			assertReceive(t, sub, msg) // 验证消息接收
		}
	}
}

// TestGossipSubPeerFilter 测试 GossipSub 的 PeerFilter 功能，确保只有满足过滤条件的对等节点能够接收到消息。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 3 个主机。
//  2. 创建 GossipSub 实例，并配置 PeerFilter 过滤条件。
//  3. 建立主机之间的连接，确保过滤后的对等节点能够正常通信。
//  4. 订阅每个 GossipSub 实例的 "test" 主题。
//  5. 发布测试消息，并验证只有通过 PeerFilter 的对等节点接收到消息。
//  6. 验证未通过 PeerFilter 的对等节点不会接收到消息。
func TestGossipSubPeerFilter(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 3 个默认的主机实例
	h := getDefaultHosts(t, 3)

	// 创建 GossipSub 实例，并配置 PeerFilter 过滤条件
	psubs := []*PubSub{
		// 第一个主机只允许来自第二个主机的消息
		getGossipsub(ctx, h[0], WithPeerFilter(func(pid peer.ID, topic string) bool {
			return pid == h[1].ID()
		})),
		// 第二个主机只允许来自第一个主机的消息
		getGossipsub(ctx, h[1], WithPeerFilter(func(pid peer.ID, topic string) bool {
			return pid == h[0].ID()
		})),
		// 第三个主机没有配置过滤器，可以与任何对等节点通信
		getGossipsub(ctx, h[2]),
	}

	// 建立主机之间的连接
	connect(t, h[0], h[1])
	connect(t, h[0], h[2])

	// 订阅每个 GossipSub 实例的 "test" 主题
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待一段时间以便 GossipSub 网络的 mesh 构建完成
	time.Sleep(time.Second)

	// 发布测试消息，并验证消息传播
	msg := []byte("message")

	// 第一个主机发布消息，第二个主机应该接收到，第三个主机不应接收到
	if err := topics[0].Publish(ctx, msg); err != nil {
		t.Fatal(err) // 如果发布失败，终止测试
	}
	assertReceive(t, subs[1], msg)               // 验证第二个主机接收到消息
	assertNeverReceives(t, subs[2], time.Second) // 验证第三个主机未接收到消息

	// 第二个主机发布消息，第一个主机应该接收到，第三个主机不应接收到
	if err := topics[1].Publish(ctx, msg); err != nil {
		t.Fatal(err) // 如果发布失败，终止测试
	}
	assertReceive(t, subs[0], msg)               // 验证第一个主机接收到消息
	assertNeverReceives(t, subs[2], time.Second) // 验证第三个主机未接收到消息
}

// TestGossipsubDirectPeersFanout 测试 GossipSub 中 direct peers 的 fanout 行为。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 3 个主机。
//  2. 创建 GossipSub 实例，并配置 direct peers。
//  3. 建立主机之间的连接，确保 direct peers 能正常通信。
//  4. 订阅前两个 GossipSub 实例的 "test" 主题，第三个实例不订阅。
//  5. 让第三个实例发布消息，以构建 fanout。
//  6. 验证第一个主机在第三个主机的 fanout 中，但第二个主机不在其中。
//  7. 订阅第三个 GossipSub 实例的 "test" 主题，并验证第一个主机在 mesh 中，但第二个主机不在其中。
func TestGossipsubDirectPeersFanout(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 3 个默认的主机实例
	h := getDefaultHosts(t, 3)

	// 创建 GossipSub 实例，并配置 direct peers
	psubs := []*PubSub{
		getGossipsub(ctx, h[0]),
		getGossipsub(ctx, h[1], WithDirectPeers([]peer.AddrInfo{{ID: h[2].ID(), Addrs: h[2].Addrs()}})),
		getGossipsub(ctx, h[2], WithDirectPeers([]peer.AddrInfo{{ID: h[1].ID(), Addrs: h[1].Addrs()}})),
	}

	// 建立主机之间的连接
	connect(t, h[0], h[1])
	connect(t, h[0], h[2])

	// 订阅前两个 GossipSub 实例的 "test" 主题，第三个实例不订阅
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs[:2] {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待 GossipSub 网络的 mesh 构建完成
	time.Sleep(time.Second)

	// 第三个实例发布一些消息，以构建 fanout
	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		if err := topics[2].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}

	// 验证第一个主机在第三个主机的 fanout 中，但第二个主机不在其中
	result := make(chan bool, 2)
	psubs[2].eval <- func() {
		rt := psubs[2].rt.(*GossipSubRouter)
		fanout := rt.fanout["test"]
		_, ok := fanout[h[0].ID()]
		result <- ok
		_, ok = fanout[h[1].ID()]
		result <- ok
	}

	inFanout := <-result
	if !inFanout {
		t.Fatal("expected peer 0 to be in fanout")
	}

	inFanout = <-result
	if inFanout {
		t.Fatal("expected peer 1 to not be in fanout")
	}

	// 现在第三个实例也订阅 "test" 主题，并验证第一个主机在 mesh 中，但第二个主机不在其中
	_, err := psubs[2].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	psubs[2].eval <- func() {
		rt := psubs[2].rt.(*GossipSubRouter)
		mesh := rt.mesh["test"]
		_, ok := mesh[h[0].ID()]
		result <- ok
		_, ok = mesh[h[1].ID()]
		result <- ok
	}

	inMesh := <-result
	if !inMesh {
		t.Fatal("expected peer 0 to be in mesh")
	}

	inMesh = <-result
	if inMesh {
		t.Fatal("expected peer 1 to not be in mesh")
	}
}

// TestGossipsubFloodPublish 测试在 star 拓扑下通过 FloodPublish 进行消息广播。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 配置 GossipSub 实例，启用 FloodPublish。
//  3. 建立 star 拓扑结构，将一个中心节点与其他所有节点连接。
//  4. 订阅所有 GossipSub 实例的 "test" 主题。
//  5. 从中心节点发送消息，并验证所有订阅者是否接收到正确的消息。
func TestGossipsubFloodPublish(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 创建 GossipSub 实例，启用 FloodPublish
	psubs := getGossipsubs(ctx, hosts, WithFloodPublish(true))

	// 建立 star 拓扑结构，将中心节点（hosts[0]）与其他节点连接
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	// 为所有 GossipSub 实例订阅 "test" 主题
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待 GossipSub 网络的 mesh 构建完成
	time.Sleep(time.Second)

	// 从中心节点发送消息，并验证所有订阅者是否接收到正确的消息
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		if err := topics[0].Publish(ctx, msg); err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}
}

// TestGossipsubEnoughPeers 测试在 GossipSub 网络中是否有足够的 peers 来构建 mesh。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 为所有主机创建 GossipSub 实例，并订阅 "test" 主题。
//  3. 在没有连接的情况下，验证 `EnoughPeers` 方法返回 false。
//  4. 通过密集连接建立 mesh，然后验证 `EnoughPeers` 方法返回 true。
func TestGossipsubEnoughPeers(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 为所有主机创建 GossipSub 实例，并订阅 "test" 主题
	psubs := getGossipsubs(ctx, hosts)
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		if _, err := topic.Subscribe(); err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
	}

	// 在没有连接的情况下，验证 `EnoughPeers` 方法返回 false
	res := make(chan bool, 1)
	psubs[0].eval <- func() {
		res <- psubs[0].rt.EnoughPeers("test", 0)
	}
	enough := <-res
	if enough {
		t.Fatal("should not have enough peers")
	}

	// 通过密集连接建立 mesh
	denseConnect(t, hosts)

	// 等待 GossipSub 网络的 mesh 构建完成
	time.Sleep(3 * time.Second)

	// 再次验证 `EnoughPeers` 方法返回 true
	psubs[0].eval <- func() {
		res <- psubs[0].rt.EnoughPeers("test", 0)
	}
	enough = <-res
	if !enough {
		t.Fatal("should have enough peers")
	}
}

// TestGossipsubCustomParams 测试自定义 GossipSub 参数的正确性。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 1 个主机。
//  2. 配置自定义的 GossipSub 参数并验证参数的正确设置。
func TestGossipsubCustomParams(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 自定义 GossipSub 参数
	params := DefaultGossipSubParams()
	wantedFollowTime := 1 * time.Second
	params.IWantFollowupTime = wantedFollowTime
	customGossipFactor := 0.12
	params.GossipFactor = customGossipFactor
	wantedMaxPendingConns := 23
	params.MaxPendingConnections = wantedMaxPendingConns

	// 获取 1 个默认的主机实例并使用自定义参数创建 GossipSub 实例
	hosts := getDefaultHosts(t, 1)
	psubs := getGossipsubs(ctx, hosts, WithGossipSubParams(params))

	if len(psubs) != 1 {
		t.Fatalf("incorrect number of pusbub objects received: wanted %d but got %d", 1, len(psubs))
	}

	// 验证 GossipSub 路由器的参数设置
	rt, ok := psubs[0].rt.(*GossipSubRouter)
	if !ok {
		t.Fatal("Did not get gossip sub router from pub sub object")
	}

	if rt.params.IWantFollowupTime != wantedFollowTime {
		t.Errorf("Wanted %d of param GossipSubIWantFollowupTime but got %d", wantedFollowTime, rt.params.IWantFollowupTime)
	}
	if rt.params.GossipFactor != customGossipFactor {
		t.Errorf("Wanted %f of param GossipSubGossipFactor but got %f", customGossipFactor, rt.params.GossipFactor)
	}
	if rt.params.MaxPendingConnections != wantedMaxPendingConns {
		t.Errorf("Wanted %d of param GossipSubMaxPendingConnections but got %d", wantedMaxPendingConns, rt.params.MaxPendingConnections)
	}
}

// TestGossipsubNegativeScore 测试对负分 peer 的处理。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 20 个主机。
//  2. 配置 GossipSub 实例，设置对第一个主机使用负分数。
//  3. 连接所有主机并构建 mesh。
//  4. 订阅所有主机的 "test" 主题，并发送消息。
//  5. 验证负分数主机是否被隔离。
func TestGossipsubNegativeScore(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 20 个默认的主机实例
	hosts := getDefaultHosts(t, 20)

	// 配置 GossipSub 实例，设置对第一个主机使用负分数
	psubs := getGossipsubs(ctx, hosts, WithPeerScore(
		&PeerScoreParams{
			AppSpecificScore: func(p peer.ID) float64 {
				if p == hosts[0].ID() {
					return -1000
				}
				return 0
			},
			AppSpecificWeight: 1,
			DecayInterval:     time.Second,
			DecayToZero:       0.01,
		},
		&PeerScoreThresholds{
			GossipThreshold:   -10,
			PublishThreshold:  -100,
			GraylistThreshold: -10000,
		}))

	// 连接所有主机并构建 mesh
	denseConnect(t, hosts)

	// 为所有 GossipSub 实例订阅 "test" 主题
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err) // 如果主题加入失败，终止测试
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err) // 如果订阅失败，终止测试
		}
		subs = append(subs, sub) // 将订阅通道添加到列表中
	}

	// 等待 mesh 建立
	time.Sleep(3 * time.Second)

	// 发送消息并进行验证
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		if err := topics[i%20].Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 让负分数的主机尝试发送 gossip
	time.Sleep(2 * time.Second)

	// 验证第一个主机只能接收到自己的消息，其他主机不应接收到第一个主机的消息
	collectAll := func(sub *Subscription) []*Message {
		var res []*Message
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				break
			}
			res = append(res, msg)
		}

		return res
	}

	// 验证第一个主机的消息接收
	count := len(collectAll(subs[0]))
	if count != 1 {
		t.Fatalf("expected 1 message but got %d instead", count)
	}

	// 验证其他主机没有收到第一个主机的消息
	for _, sub := range subs[1:] {
		all := collectAll(sub)
		for _, m := range all {
			if m.ReceivedFrom == hosts[0].ID() {
				t.Fatal("received message from sinkholed peer")
			}
		}
	}
}

// TestGossipsubScoreValidatorEx 测试消息验证器的负分处理。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 3 个主机。
//  2. 配置 GossipSub 实例，设置分数验证器。
//  3. 注册主题验证器，并设置不同的验证结果。
//  4. 发送消息并验证各主机的分数是否符合预期。
// TestGossipsubScoreValidatorEx 测试 GossipSub 的分数验证器，验证主机在消息被接受、忽略或拒绝时的分数变化。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 3 个主机。
//  2. 配置 GossipSub 实例，设置分数参数和阈值。
//  3. 连接所有主机。
//  4. 注册主题验证器以根据发送主机决定消息的处理方式。
//  5. 订阅第一个 GossipSub 实例的 "test" 主题。
//  6. 发送消息并验证第一个主机没有接收到消息。
//  7. 检查分数变化，确保主机 1 的分数为 0，主机 2 的分数为负。

func TestGossipsubScoreValidatorEx(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 3 个默认的主机实例
	hosts := getDefaultHosts(t, 3)

	// 配置 GossipSub 实例，设置分数验证器
	psubs := getGossipsubs(ctx, hosts, WithPeerScore(
		&PeerScoreParams{
			// 设置应用程序特定的分数函数，返回固定的分数 0
			AppSpecificScore: func(p peer.ID) float64 { return 0 },
			// 设置分数衰减间隔和衰减至零的因子
			DecayInterval: time.Second,
			DecayToZero:   0.01,
			// 为 "test" 主题设置分数参数
			Topics: map[string]*TopicScoreParams{
				"test": {
					TopicWeight:                    1,           // 主题权重
					TimeInMeshQuantum:              time.Second, // mesh 中时间的量子
					InvalidMessageDeliveriesWeight: -1,          // 无效消息的权重
					InvalidMessageDeliveriesDecay:  0.9999,      // 无效消息的衰减因子
				},
			},
		},
		// 设置分数阈值
		&PeerScoreThresholds{
			GossipThreshold:   -10,
			PublishThreshold:  -100,
			GraylistThreshold: -10000,
		}))

	// 连接所有主机
	connectAll(t, hosts)

	// 注册主题验证器，定义消息的验证规则
	err := psubs[0].RegisterTopicValidator("test", func(ctx context.Context, p peer.ID, msg *Message) ValidationResult {
		// 忽略来自 host1 的消息
		if p == hosts[1].ID() {
			return ValidationIgnore
		}
		// 拒绝来自 host2 的消息
		if p == hosts[2].ID() {
			return ValidationReject
		}
		// 接受其他消息
		return ValidationAccept
	})
	if err != nil {
		t.Fatal(err)
	}

	// 订阅第一个 GossipSub 实例的 "test" 主题
	topic, err := psubs[0].Join("test")
	if err != nil {
		t.Fatal(err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// 等待一小段时间以确保订阅生效
	time.Sleep(100 * time.Millisecond)

	// 定义一个检查订阅者是否没有接收到消息的函数
	expectNoMessage := func(sub *Subscription) {
		// 创建一个超时上下文，用于等待消息
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel() // 确保超时后取消上下文，释放资源

		// 尝试接收消息
		m, err := sub.Next(ctx)
		// 如果收到消息，报告错误
		if err == nil {
			t.Fatal("expected no message, but got ", string(m.Data))
		}
	}

	// 发送来自 host1 和 host2 的消息
	if err := topic.Publish(ctx, []byte("i am not a walrus")); err != nil {
		t.Fatal(err)
	}
	if err := topic.Publish(ctx, []byte("i am not a walrus either")); err != nil {
		t.Fatal(err)
	}

	// 验证订阅者没有接收到消息
	expectNoMessage(sub)

	// 检查 host1 的分数是否为 0（消息被忽略），host2 的分数是否为负（消息被拒绝）
	res := make(chan float64, 1)

	// 获取 host1 的分数
	psubs[0].eval <- func() {
		res <- psubs[0].rt.(*GossipSubRouter).score.Score(hosts[1].ID())
	}
	score := <-res
	if score != 0 {
		t.Fatalf("expected 0 score for peer1, but got %f", score)
	}

	// 获取 host2 的分数
	psubs[0].eval <- func() {
		res <- psubs[0].rt.(*GossipSubRouter).score.Score(hosts[2].ID())
	}
	score = <-res
	if score >= 0 {
		t.Fatalf("expected negative score for peer2, but got %f", score)
	}
}

// TestGossipsubPiggybackControl 直接测试 GossipSubRouter 的 piggybackControl 函数。
// 由于在 Travis CI 上无法可靠地触发该功能，因此我们通过直接调用来进行测试。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取一个默认的主机。
//  2. 初始化 GossipSub 实例并模拟网络中的其它节点。
//  3. 使用 eval 函数来直接调用 piggybackControl 函数，模拟发送控制消息。
//  4. 验证控制消息中包含的 GRAFT 和 PRUNE 是否符合预期。

func TestGossipsubPiggybackControl(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取一个默认的主机实例
	h := getDefaultHosts(t, 1)[0]

	// 初始化 GossipSub 实例
	ps := getGossipsub(ctx, h)

	// 模拟一个无效的对等节点 ID（假设为另一个节点）
	blah := peer.ID("bogotr0n")

	// 创建一个通道用于接收 piggybackControl 的结果
	res := make(chan *RPC, 1)

	// 使用 eval 函数在 GossipSubRouter 的上下文中执行 piggybackControl 逻辑
	ps.eval <- func() {
		// 获取 GossipSubRouter 实例
		gs := ps.rt.(*GossipSubRouter)

		// 定义一些测试的主题
		test1 := "test1"
		test2 := "test2"
		test3 := "test3"

		// 在 gs.mesh 中为每个主题创建一个 map，用于保存连接的对等节点
		gs.mesh[test1] = make(map[peer.ID]struct{})
		gs.mesh[test2] = make(map[peer.ID]struct{})

		// 将模拟的对等节点 blah 添加到 test1 主题的 mesh 中
		gs.mesh[test1][blah] = struct{}{}

		// 创建一个新的 RPC 消息结构体
		rpc := &RPC{RPC: pb.RPC{}}

		// 调用 piggybackControl 函数，传递对等节点 ID 和控制消息
		gs.piggybackControl(blah, rpc, &pb.ControlMessage{
			Graft: []*pb.ControlGraft{
				{TopicID: test1},
				{TopicID: test2},
				{TopicID: test3},
			},
			Prune: []*pb.ControlPrune{
				{TopicID: test1},
				{TopicID: test2},
				{TopicID: test3},
			},
		})

		// 将生成的 RPC 消息发送到结果通道
		res <- rpc
	}

	// 从结果通道接收 RPC 消息
	rpc := <-res

	// 验证 RPC 控制消息是否非空
	if rpc.Control == nil {
		t.Fatal("expected non-nil control message")
	}

	// 验证 GRAFT 消息的数量是否为 1
	if len(rpc.Control.Graft) != 1 {
		t.Fatal("expected 1 GRAFT")
	}

	// 验证 GRAFT 消息的主题 ID 是否为 "test1"
	if rpc.Control.Graft[0].GetTopicID() != "test1" {
		t.Fatal("expected test1 as graft topic ID")
	}

	// 验证 PRUNE 消息的数量是否为 2
	if len(rpc.Control.Prune) != 2 {
		t.Fatal("expected 2 PRUNEs")
	}

	// 验证第一个 PRUNE 消息的主题 ID 是否为 "test2"
	if rpc.Control.Prune[0].GetTopicID() != "test2" {
		t.Fatal("expected test2 as prune topic ID")
	}

	// 验证第二个 PRUNE 消息的主题 ID 是否为 "test3"
	if rpc.Control.Prune[1].GetTopicID() != "test3" {
		t.Fatal("expected test3 as prune topic ID")
	}
}

// TestGossipsubMultipleGraftTopics 测试在多个主题上同时进行 GRAFT 操作。
// 验证在多个主题下，GossipSub 网络的拓扑是否正确更新，确保一个节点可以成功加入多个主题的 mesh。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取 2 个默认的主机。
//  2. 初始化 GossipSub 实例，并将两个主机连接起来。
//  3. 为第二个主机添加多个主题的 mesh。
//  4. 从第一个主机向第二个主机发送多个 GRAFT 消息，尝试将第一个主机加入多个主题的 mesh。
//  5. 验证第二个主机的 mesh 是否正确更新，确保第一个主机已成功加入多个主题的 mesh。

func TestGossipsubMultipleGraftTopics(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 2 个默认的主机实例
	hosts := getDefaultHosts(t, 2)

	// 初始化 GossipSub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 将两个主机进行稀疏连接
	sparseConnect(t, hosts)

	// 等待连接稳定
	time.Sleep(time.Second * 1)

	// 定义三个测试的主题
	firstTopic := "topic1"
	secondTopic := "topic2"
	thirdTopic := "topic3"

	// 获取两个主机的对等 ID
	firstPeer := hosts[0].ID()
	secondPeer := hosts[1].ID()

	// 获取第二个主机的 PubSub 实例和两个主机的 GossipSubRouter 实例
	p2Sub := psubs[1]
	p1Router := psubs[0].rt.(*GossipSubRouter)
	p2Router := psubs[1].rt.(*GossipSubRouter)

	// 用于同步的通道
	finChan := make(chan struct{})

	// 在第二个主机的 eval 线程中执行以下操作
	p2Sub.eval <- func() {
		// 为第二个主机的 GossipSubRouter 添加三个主题的 mesh
		p2Router.mesh[firstTopic] = map[peer.ID]struct{}{}
		p2Router.mesh[secondTopic] = map[peer.ID]struct{}{}
		p2Router.mesh[thirdTopic] = map[peer.ID]struct{}{}

		// 发送完成信号
		finChan <- struct{}{}
	}

	// 等待 eval 操作完成
	<-finChan

	// 从第一个主机向第二个主机发送 GRAFT 消息，将第一个主机加入三个主题的 mesh
	p1Router.sendGraftPrune(map[peer.ID][]string{
		secondPeer: {firstTopic, secondTopic, thirdTopic},
	}, map[peer.ID][]string{}, map[peer.ID]bool{})

	// 等待一段时间以确保 GRAFT 操作生效
	time.Sleep(time.Second * 1)

	// 在第二个主机的 eval 线程中执行以下操作
	p2Sub.eval <- func() {
		// 验证第一个主机是否已成功加入第一个主题的 mesh
		if _, ok := p2Router.mesh[firstTopic][firstPeer]; !ok {
			t.Errorf("First peer wasn't added to mesh of the second peer for the topic %s", firstTopic)
		}
		// 验证第一个主机是否已成功加入第二个主题的 mesh
		if _, ok := p2Router.mesh[secondTopic][firstPeer]; !ok {
			t.Errorf("First peer wasn't added to mesh of the second peer for the topic %s", secondTopic)
		}
		// 验证第一个主机是否已成功加入第三个主题的 mesh
		if _, ok := p2Router.mesh[thirdTopic][firstPeer]; !ok {
			t.Errorf("First peer wasn't added to mesh of the second peer for the topic %s", thirdTopic)
		}
		// 发送完成信号
		finChan <- struct{}{}
	}

	// 等待 eval 操作完成
	<-finChan
}

// TestGossipsubOpportunisticGrafting 测试 GossipSub 协议的机会性 GRAFTING 功能。
// 该测试通过设置网络中的节点，验证在一定时间内，节点是否会自动进行 GRAFT 操作以优化网络拓扑。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 调整 GossipSub 参数以加快测试速度。
//  2. 创建测试上下文，并初始化 50 个主机实例。
//  3. 初始化前 10 个主机的 GossipSub 实例，并设置分数参数和连接拓扑。
//  4. 为剩余的 40 个主机设置 Sybil 攻击模拟器，并将它们连接到前 10 个主机。
//  5. 让前 10 个主机加入主题 "test" 并发布消息。
//  6. 验证在多次机会性 GRAFT 循环后，mesh 中的诚实节点数量是否达到预期。

func TestGossipsubOpportunisticGrafting(t *testing.T) {
	// 保存原始的 GossipSub 参数，并在测试完成后恢复
	originalGossipSubPruneBackoff := GossipSubPruneBackoff
	GossipSubPruneBackoff = 500 * time.Millisecond
	originalGossipSubGraftFloodThreshold := GossipSubGraftFloodThreshold
	GossipSubGraftFloodThreshold = 100 * time.Millisecond
	originalGossipSubOpportunisticGraftTicks := GossipSubOpportunisticGraftTicks
	GossipSubOpportunisticGraftTicks = 2
	defer func() {
		GossipSubPruneBackoff = originalGossipSubPruneBackoff
		GossipSubGraftFloodThreshold = originalGossipSubGraftFloodThreshold
		GossipSubOpportunisticGraftTicks = originalGossipSubOpportunisticGraftTicks
	}()

	// 创建可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在测试结束时取消上下文以释放资源

	// 获取 50 个主机实例
	hosts := getDefaultHosts(t, 50)

	// 初始化前 10 个主机的 GossipSub 实例，设置分数参数和连接拓扑
	psubs := getGossipsubs(ctx, hosts[:10],
		WithFloodPublish(true),
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore:  func(peer.ID) float64 { return 0 },
				AppSpecificWeight: 0,
				DecayInterval:     time.Second,
				DecayToZero:       0.01,
				Topics: map[string]*TopicScoreParams{
					"test": {
						TopicWeight:                   1,
						TimeInMeshWeight:              0.0002777,
						TimeInMeshQuantum:             time.Second,
						TimeInMeshCap:                 3600,
						FirstMessageDeliveriesWeight:  1,
						FirstMessageDeliveriesDecay:   0.9997,
						FirstMessageDeliveriesCap:     100,
						InvalidMessageDeliveriesDecay: 0.99997,
					},
				},
			},
			&PeerScoreThresholds{
				GossipThreshold:             -10,
				PublishThreshold:            -100,
				GraylistThreshold:           -10000,
				OpportunisticGraftThreshold: 1,
			}))

	// 连接前 10 个主机，使每个主机具有 5 个连接
	connectSome(t, hosts[:10], 5)

	// 为后 40 个主机设置 Sybil 攻击模拟器
	for _, h := range hosts[10:] {
		squatter := &sybilSquatter{h: h}
		h.SetStreamHandler(GossipSubID_v10, squatter.handleStream)
	}

	// 将所有 Sybil 主机连接到每个真实的主机
	for _, squatter := range hosts[10:] {
		for _, real := range hosts[:10] {
			connect(t, squatter, real)
		}
	}

	// 等待一段时间以便连接事件传播到 pubsubs
	time.Sleep(time.Second)

	// 让前 10 个主机加入主题 "test"
	topics := make([]*Topic, 10)
	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err)
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		// 启动一个 Goroutine 来消费消息
		go func(sub *Subscription) {
			for {
				_, err := sub.Next(ctx)
				if err != nil {
					return
				}
			}
		}(sub)
	}

	// 从真实的主机发布大量消息
	for i := 0; i < 1000; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		// 使用正确的 Topic 实例发布消息
		if err := topics[i%10].Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}
		// 每发布一条消息等待 20 毫秒
		time.Sleep(20 * time.Millisecond)
	}

	// 等待几次机会性 GRAFT 循环
	time.Sleep(7 * time.Second)

	// 检查诚实节点的 mesh，确保每个节点的 mesh 中至少有 3 个诚实节点
	res := make(chan int, 1)
	for _, ps := range psubs {
		ps.eval <- func() {
			gs := ps.rt.(*GossipSubRouter)
			count := 0
			for _, h := range hosts[:10] {
				_, ok := gs.mesh["test"][h.ID()]
				if ok {
					count++
				}
			}
			res <- count
		}

		count := <-res
		if count < 3 {
			t.Fatalf("expected at least 3 honest peers, got %d", count)
		}
	}
}

// TestGossipSubLeaveTopic 测试从主题中离开后 GossipSub 的行为。
// 主要检查离开主题后，是否正确设置了回退时间(backoff time)，以防止在短时间内重新加入主题。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建一个可取消的上下文以管理测试的生命周期。
//  2. 获取两个默认的主机实例，并为它们分别创建 GossipSub 实例。
//  3. 连接两个主机，使它们能够进行网络通信。
//  4. 让两个主机加入 "test" 主题，并为每个 GossipSub 实例订阅该主题。
//  5. 等待一段时间，确保网络拓扑结构稳定，订阅生效。
//  6. 第一个主机离开 "test" 主题，检查是否正确设置了回退时间(backoff time)，并验证回退时间是否与预期值一致。
//  7. 确保第二个主机也正确地对第一个主机应用了回退时间。
//  8. 验证回退时间是否大致等于预期的 GossipSubUnsubscribeBackoff 时间（允许 1 秒的误差）。
func TestGossipSubLeaveTopic(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 2 个默认的主机实例
	h := getDefaultHosts(t, 2)
	// 为每个主机创建 GossipSub 实例
	psubs := []*PubSub{
		getGossipsub(ctx, h[0]),
		getGossipsub(ctx, h[1]),
	}

	// 连接两个主机实例
	connect(t, h[0], h[1])

	// 让每个主机加入 "test" 主题并订阅
	var subs []*Subscription
	topics := make([]*Topic, len(psubs))
	for i, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err)
		}
		topics[i] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待一段时间，确保网络拓扑结构稳定
	time.Sleep(time.Second)

	// 记录离开主题的时间
	leaveTime := time.Now()
	done := make(chan struct{})

	// 处理第一个主机的 GossipsubRouter 离开主题逻辑
	psubs[0].rt.(*GossipSubRouter).p.eval <- func() {
		defer close(done) // 确保在逻辑完成后关闭 done 通道
		// 第一个主机离开 "test" 主题
		psubs[0].rt.Leave("test")
		time.Sleep(time.Second) // 等待一段时间，确保逻辑完成

		// 获取第一个主机的回退（backoff）映射
		peerMap := psubs[0].rt.(*GossipSubRouter).backoff["test"]
		if len(peerMap) != 1 {
			t.Fatalf("No peer is populated in the backoff map for peer 0")
		}
		_, ok := peerMap[h[1].ID()]
		if !ok {
			t.Errorf("Expected peer does not exist in the backoff map")
		}

		// 计算实际的回退时间
		backoffTime := peerMap[h[1].ID()].Sub(leaveTime)
		// 检查回退时间是否大致等于预期的 GossipSubUnsubscribeBackoff 时间（允许 1 秒的误差）
		if backoffTime-GossipSubUnsubscribeBackoff > time.Second {
			t.Error("Backoff time should be set to GossipSubUnsubscribeBackoff.")
		}
	}
	<-done // 等待第一个主机的逻辑执行完成

	done = make(chan struct{})
	// 确保远程主机（peer 1）也正确地对 peer 0 应用回退时间
	psubs[1].rt.(*GossipSubRouter).p.eval <- func() {
		defer close(done)
		peerMap2 := psubs[1].rt.(*GossipSubRouter).backoff["test"]
		if len(peerMap2) != 1 {
			t.Fatalf("No peer is populated in the backoff map for peer 1")
		}
		_, ok := peerMap2[h[0].ID()]
		if !ok {
			t.Errorf("Expected peer does not exist in the backoff map")
		}

		// 计算实际的回退时间
		backoffTime := peerMap2[h[0].ID()].Sub(leaveTime)
		// 检查回退时间是否大致等于预期的 GossipSubUnsubscribeBackoff 时间（允许 1 秒的误差）
		if backoffTime-GossipSubUnsubscribeBackoff > time.Second {
			t.Error("Backoff time should be set to GossipSubUnsubscribeBackoff.")
		}
	}
	<-done // 等待第二个主机的逻辑执行完成
}

// TestGossipSubJoinTopic 测试在 GossipSub 中节点加入主题后的行为。
// 主要检查加入主题后，是否正确地处理了之前设置的回退时间(backoff time)，
// 以防止在短时间内将被回退的节点重新加入网状网络(mesh)。
//
// 功能描述：
//  1. 创建一个可取消的上下文以管理测试的生命周期。
//  2. 获取三个默认的主机实例，并为它们分别创建 GossipSub 实例。
//  3. 连接第一个主机与其他两个主机，使它们能够进行网络通信。
//  4. 设置第一个主机的 GossipSubRouter 的回退时间，使其对第二个主机应用回退(backoff)。
//  5. 让所有主机加入 "test" 主题，并订阅该主题。
//  6. 等待一段时间，确保网络拓扑结构稳定，订阅生效。
//  7. 验证第一个主机的网状网络中是否正确地排除了被回退的第二个主机。

func TestGossipSubJoinTopic(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 3 个默认的主机实例
	h := getDefaultHosts(t, 3)
	psubs := []*PubSub{
		getGossipsub(ctx, h[0]), // 第一个主机的 GossipSub 实例
		getGossipsub(ctx, h[1]), // 第二个主机的 GossipSub 实例
		getGossipsub(ctx, h[2]), // 第三个主机的 GossipSub 实例
	}

	// 连接第一个主机与其他两个主机，建立网络通信
	connect(t, h[0], h[1])
	connect(t, h[0], h[2])

	// 获取第一个主机的 GossipSubRouter
	router0 := psubs[0].rt.(*GossipSubRouter)

	// 为第二个主机设置回退时间
	peerMap := make(map[peer.ID]time.Time)
	peerMap[h[1].ID()] = time.Now().Add(router0.params.UnsubscribeBackoff) // 添加回退时间

	// 将回退时间应用于 "test" 主题
	router0.backoff["test"] = peerMap

	// 让所有主机加入 "test" 主题并订阅
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待一段时间，确保网络拓扑结构稳定
	time.Sleep(time.Second)

	// 获取第一个主机在 "test" 主题中的网状网络
	meshMap := router0.mesh["test"]
	if len(meshMap) != 1 {
		t.Fatalf("Unexpect peer included in the mesh") // 如果网状网络中的节点数量不为 1，报告错误
	}

	// 检查第二个主机是否被正确排除在网状网络之外
	_, ok := meshMap[h[1].ID()]
	if ok {
		t.Fatalf("Peer that was to be backed off is included in the mesh") // 如果第二个主机被错误地包含在网状网络中，报告错误
	}
}

// sybilSquatter 结构体定义了一个模拟 Sybil 攻击的节点，该节点会处理传入的 GossipSub 流，并尝试通过订阅一个主题来成为 GRAFT 候选节点。
type sybilSquatter struct {
	h host.Host // P2P 网络中的主机实例
}

// handleStream 处理传入的 GossipSub 流。
// 功能描述：
//  1. 当有一个 GossipSub 流连接到 sybilSquatter 时，它将启动一个新的流与对等方通信。
//  2. sybilSquatter 订阅 "test" 主题并发送订阅请求，以尝试成为 GRAFT 的候选节点。
//  3. 之后，sybilSquatter 进入一个循环，持续读取来自对等方的 RPC 消息，但不会对此作出回应，只是简单地忽略这些消息。
//  4. 在流关闭或遇到读取错误时，该函数将结束。
func (sq *sybilSquatter) handleStream(s network.Stream) {
	// 确保在函数结束时关闭传入的流
	defer s.Close()

	// 创建一个新的流，用于与远程对等方通信
	os, err := sq.h.NewStream(context.Background(), s.Conn().RemotePeer(), GossipSubID_v10)
	if err != nil {
		// 如果流创建失败，直接引发 panic 停止程序
		panic(err)
	}

	// 发送一个订阅 "test" 主题的请求，以尝试成为 GRAFT 候选节点
	// 然后只是读取并忽略传入的 RPC 消息
	r := protoio.NewDelimitedReader(s, 1<<20) // 创建一个用于读取 RPC 消息的流
	w := protoio.NewDelimitedWriter(os)       // 创建一个用于写入 RPC 消息的流

	// 准备发送订阅请求
	truth := true
	topic := "test"
	err = w.WriteMsg(&pb.RPC{
		Subscriptions: []*pb.RPC_SubOpts{
			{
				Subscribe: truth, // 表示订阅 "test" 主题
				Topicid:   topic, // 主题 ID
			},
		},
	})
	if err != nil {
		// 如果写入消息失败，直接引发 panic 停止程序
		panic(err)
	}

	// 创建一个空的 RPC 消息结构，用于循环中接收消息
	var rpc pb.RPC

	// 无限循环，读取来自对等方的 RPC 消息
	for {
		// 重置 rpc 结构体，准备接收新的消息
		rpc.Reset()

		// 从输入流中读取下一条消息
		err = r.ReadMsg(&rpc)
		if err != nil {
			// 如果读取过程中遇到 EOF，表示流已关闭，结束循环
			if err != io.EOF {
				// 如果是其他错误，重置流状态
				s.Reset()
			}
			// 结束处理函数
			return
		}
		// 读取的消息被忽略，不作任何处理
	}
}

// TestGossipsubPeerScoreInspect 测试 GossipSub 网络中节点分数的检查功能。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取默认的 2 个主机。
//  2. 为第一个主机创建 GossipSub 实例，并配置节点分数参数和检查器。
//  3. 连接两个主机。
//  4. 订阅 "test" 主题并开始发送消息。
//  5. 验证分数检查器的结果，确保主机 1 的分数在消息发送后达到预期值。
func TestGossipsubPeerScoreInspect(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 2 个默认的主机实例
	hosts := getDefaultHosts(t, 2)

	// 创建一个 mock 的分数检查器实例
	inspector := &mockPeerScoreInspector{}

	// 为第一个主机创建 GossipSub 实例，并配置分数参数和检查器
	psub1 := getGossipsub(ctx, hosts[0],
		WithPeerScore(
			&PeerScoreParams{
				Topics: map[string]*TopicScoreParams{
					"test": {
						TopicWeight:                    1, // 主题权重
						TimeInMeshQuantum:              time.Second,
						FirstMessageDeliveriesWeight:   1,      // 首次消息传递权重
						FirstMessageDeliveriesDecay:    0.999,  // 首次消息传递衰减因子
						FirstMessageDeliveriesCap:      100,    // 首次消息传递上限
						InvalidMessageDeliveriesWeight: -1,     // 无效消息传递的权重
						InvalidMessageDeliveriesDecay:  0.9999, // 无效消息传递衰减因子
					},
				},
				AppSpecificScore: func(peer.ID) float64 { return 0 }, // 应用特定的分数函数
				DecayInterval:    time.Second,                        // 衰减间隔
				DecayToZero:      0.01,                               // 衰减到零的阈值
			},
			&PeerScoreThresholds{
				GossipThreshold:   -1,    // Gossip 阈值
				PublishThreshold:  -10,   // 发布阈值
				GraylistThreshold: -1000, // 灰名单阈值
			}),
		WithPeerScoreInspect(inspector.inspect, time.Second)) // 配置分数检查器

	// 为第二个主机创建 GossipSub 实例
	psub2 := getGossipsub(ctx, hosts[1])
	// 将两个实例加入数组
	psubs := []*PubSub{psub1, psub2}

	// 连接两个主机
	connect(t, hosts[0], hosts[1])

	// 为两个主机订阅 "test" 主题
	var topics []*Topic
	for _, ps := range psubs {
		topic, err := ps.Join("test")
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, topic)

		_, err = topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}

	// 等待一段时间，以便订阅和网络连接稳定
	time.Sleep(time.Second)

	// 循环发送 20 条测试消息，交替从两个主机发送
	for i := 0; i < 20; i++ {
		// 生成测试消息
		msg := []byte(fmt.Sprintf("message %d", i))
		// 从交替的主机发布消息
		if err := topics[i%2].Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}
		// 短暂等待，以模拟实际的消息发送间隔
		time.Sleep(20 * time.Millisecond)
	}

	// 再次等待一段时间，以确保所有消息都能被处理和检查
	time.Sleep(time.Second + 200*time.Millisecond)

	// 检查第二个主机的分数，确保其达到预期值
	score2 := inspector.score(hosts[1].ID())
	if score2 < 9 {
		t.Fatalf("expected score to be at least 9, instead got %f", score2)
	}
}

// TestGossipsubPeerScoreResetTopicParams 测试 GossipSub 网络中重置主题分数参数的功能。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取一个默认的主机。
//  2. 为主机创建 GossipSub 实例，并配置初始的节点分数参数。
//  3. 加入 "test" 主题。
//  4. 使用新的分数参数重置该主题的分数设置。
func TestGossipsubPeerScoreResetTopicParams(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 确保在函数结束时取消上下文，释放资源
	defer cancel()

	// 获取 1 个默认的主机实例
	hosts := getDefaultHosts(t, 1)

	// 为主机创建 GossipSub 实例，并配置初始的分数参数
	ps := getGossipsub(ctx, hosts[0],
		WithPeerScore(
			&PeerScoreParams{
				Topics: map[string]*TopicScoreParams{
					// 配置 "test" 主题的分数参数
					"test": {
						TopicWeight:                    1,           // 主题权重
						TimeInMeshQuantum:              time.Second, // 时间量子
						FirstMessageDeliveriesWeight:   1,           // 首次消息传递的权重
						FirstMessageDeliveriesDecay:    0.999,       // 首次消息传递的衰减
						FirstMessageDeliveriesCap:      100,         // 首次消息传递的上限
						InvalidMessageDeliveriesWeight: -1,          // 无效消息传递的权重
						InvalidMessageDeliveriesDecay:  0.9999,      // 无效消息传递的衰减
					},
				},
				AppSpecificScore: func(peer.ID) float64 { return 0 }, // 应用特定的分数函数
				DecayInterval:    time.Second,                        // 衰减间隔
				DecayToZero:      0.01,                               // 衰减到零的阈值
			},
			&PeerScoreThresholds{
				GossipThreshold:   -1,    // Gossip 阈值
				PublishThreshold:  -10,   // 发布阈值
				GraylistThreshold: -1000, // 灰名单阈值
			}))

	// 加入 "test" 主题
	topic, err := ps.Join("test")
	if err != nil {
		t.Fatal(err) // 如果加入失败，则报告错误
	}

	// 重置该主题的分数参数
	err = topic.SetScoreParams(
		&TopicScoreParams{
			TopicWeight:                    1,           // 主题权重
			TimeInMeshQuantum:              time.Second, // 时间量子
			FirstMessageDeliveriesWeight:   1,           // 首次消息传递的权重
			FirstMessageDeliveriesDecay:    0.999,       // 首次消息传递的衰减
			FirstMessageDeliveriesCap:      200,         // 将上限设置为 200
			InvalidMessageDeliveriesWeight: -1,          // 无效消息传递的权重
			InvalidMessageDeliveriesDecay:  0.9999,      // 无效消息传递的衰减
		})
	if err != nil {
		t.Fatal(err) // 如果重置分数参数失败，则报告错误
	}
}

// mockPeerScoreInspector 是一个用于检查和记录节点分数的模拟结构体。
type mockPeerScoreInspector struct {
	mx     sync.Mutex          // 互斥锁，确保并发访问 scores 字段时的安全性
	scores map[peer.ID]float64 // 存储节点 ID 对应的分数
}

// inspect 更新并存储传入的节点分数映射。
//
// 参数：
//   - scores: map[peer.ID]float64 包含节点 ID 及其对应的分数。
func (ps *mockPeerScoreInspector) inspect(scores map[peer.ID]float64) {
	ps.mx.Lock()         // 加锁，确保线程安全
	defer ps.mx.Unlock() // 在函数结束时解锁
	ps.scores = scores   // 更新分数映射
}

// score 返回给定节点 ID 对应的分数。
//
// 参数：
//   - p: peer.ID 节点 ID。
//
// 返回值：
//   - float64 对应的节点分数。
func (ps *mockPeerScoreInspector) score(p peer.ID) float64 {
	ps.mx.Lock()         // 加锁，确保线程安全
	defer ps.mx.Unlock() // 在函数结束时解锁
	return ps.scores[p]  // 返回对应的节点分数
}

// TestGossipsubRPCFragmentation 测试 GossipSub 网络中的 RPC 消息分片行为。
//
// 参数：
//   - t: *testing.T 用于报告测试结果和错误。
//
// 功能描述：
//  1. 创建测试上下文，并获取 2 个默认的主机。
//  2. 为第一个主机创建 GossipSub 实例，并配置第二个主机为模拟的假 peer。
//  3. 第一个主机加入 "test" 主题，并发布大量较大的消息。
//  4. 验证假 peer 是否正确接收到所有消息，以及是否收到相应的 IHAVE 消息。
//  5. 验证消息的 RPC 消息分片行为是否符合预期。
func TestGossipsubRPCFragmentation(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在函数结束时取消上下文，释放资源

	// 获取 2 个默认的主机实例
	hosts := getDefaultHosts(t, 2)

	// 为第一个主机创建 GossipSub 实例
	ps := getGossipsub(ctx, hosts[0])

	// 创建一个模拟的假 peer，并设置其为请求所有消息的行为
	iwe := iwantEverything{h: hosts[1]}
	iwe.h.SetStreamHandler(GossipSubID_v10, iwe.handleStream)

	// 连接两个主机
	connect(t, hosts[0], hosts[1])

	// 第一个主机加入 "test" 主题
	topic, err := ps.Join("test")
	if err != nil {
		t.Fatal(err) // 如果加入主题失败，终止测试
	}

	_, err = topic.Subscribe()
	if err != nil {
		t.Fatal(err) // 如果订阅失败，终止测试
	}

	// 等待 GossipSub 连接并尝试与假 peer 建立关系
	time.Sleep(time.Second)

	// 从第一个主机发布大量较大的消息
	nMessages := 1000 // 要发布的消息数量
	msgSize := 20000  // 每条消息的大小（字节）
	for i := 0; i < nMessages; i++ {
		msg := make([]byte, msgSize)
		rand.Read(msg) // 随机生成消息内容
		err := topic.Publish(ctx, msg)
		if err != nil {
			t.Fatal(err) // 如果发布失败，终止测试
		}
		time.Sleep(20 * time.Millisecond) // 等待一小段时间，以便消息传播
	}

	// 等待一段时间，以便假 peer 通过 Gossip 协议接收消息
	time.Sleep(5 * time.Second)
	iwe.lk.Lock() // 加锁，确保线程安全
	defer iwe.lk.Unlock()

	// 验证假 peer 是否接收到所有消息
	if iwe.msgsReceived != nMessages {
		t.Fatalf("expected fake gossipsub peer to receive all messages, got %d / %d", iwe.msgsReceived, nMessages)
	}

	// 验证是否收到相应的 IHAVE 消息
	if iwe.ihavesReceived != nMessages {
		t.Fatalf("expected to get IHAVEs for every message, got %d / %d", iwe.ihavesReceived, nMessages)
	}

	// 计算期望的最小 RPC 消息数量
	minExpectedRPCS := (nMessages * msgSize) / ps.maxMessageSize
	if iwe.rpcsWithMessages < minExpectedRPCS {
		t.Fatalf("expected to receive at least %d RPCs containing messages, got %d", minExpectedRPCS, iwe.rpcsWithMessages)
	}
}

// iwantEverything 是一个简单的 gossipsub 客户端，它永远不会 graft 到一个网格上，
// 而是通过 IWANT gossip 消息请求所有内容。用于测试大型 IWANT 请求的响应是否被分割成多个 RPC 消息。
type iwantEverything struct {
	h                host.Host
	lk               sync.Mutex
	rpcsWithMessages int // 收到包含消息的 RPC 数量
	msgsReceived     int // 收到的消息数量
	ihavesReceived   int // 收到的 IHAVE 消息数量
}

// handleStream 处理 gossipsub 流量，包括订阅主题、处理接收到的消息以及发送 IWANT 和 PRUNE 控制消息。
func (iwe *iwantEverything) handleStream(s network.Stream) {
	defer s.Close() // 在函数退出时关闭流

	// 创建一个新流，以便在测试主题中成为候选人
	os, err := iwe.h.NewStream(context.Background(), s.Conn().RemotePeer(), GossipSubID_v10)
	if err != nil {
		panic(err) // 如果无法创建新流，则触发恐慌
	}

	msgIdsReceived := make(map[string]struct{})       // 用于跟踪已收到的消息 ID
	gossipMsgIdsReceived := make(map[string]struct{}) // 用于跟踪通过 gossip 收到的消息 ID

	// 发送一个对 "test" 主题的订阅消息，以成为 gossip 消息的候选人
	r := protoio.NewDelimitedReader(s, 1<<20) // 读取 RPC 消息
	w := protoio.NewDelimitedWriter(os)       // 写入 RPC 消息
	truth := true
	topic := "test"
	err = w.WriteMsg(&pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Subscribe: truth, Topicid: topic}}})
	if err != nil {
		panic(err) // 如果写入消息失败，则触发恐慌
	}

	var rpc pb.RPC
	for {
		// 重置 RPC 结构并读取新的消息
		rpc.Reset()
		err = r.ReadMsg(&rpc)
		if err != nil {
			if err != io.EOF {
				s.Reset() // 如果遇到非 EOF 错误，重置流
			}
			return // 结束循环
		}

		// 锁定结构，以便安全地更新计数器
		iwe.lk.Lock()
		if len(rpc.Publish) != 0 {
			iwe.rpcsWithMessages++ // 如果 RPC 包含发布的消息，增加计数器
		}

		// 跟踪已接收到的唯一消息 ID
		for _, msg := range rpc.Publish {
			id := string(msg.Seqno) // 使用消息的 Seqno 作为 ID
			if _, seen := msgIdsReceived[id]; !seen {
				iwe.msgsReceived++ // 如果是新消息，增加计数器
			}
			msgIdsReceived[id] = struct{}{} // 标记消息为已接收
		}

		if rpc.Control != nil {
			// 发送 PRUNE 消息，以防止接收直接的消息传递
			var prunes []*pb.ControlPrune
			for _, graft := range rpc.Control.Graft {
				prunes = append(prunes, &pb.ControlPrune{TopicID: graft.TopicID})
			}

			// 构建 IWANT 控制消息，请求所有 IHAVE 提供的消息 ID
			var iwants []*pb.ControlIWant
			for _, ihave := range rpc.Control.Ihave {
				iwants = append(iwants, &pb.ControlIWant{MessageIDs: ihave.MessageIDs})
				for _, msgId := range ihave.MessageIDs {
					if _, seen := gossipMsgIdsReceived[msgId]; !seen {
						iwe.ihavesReceived++ // 增加接收到的 IHAVE 消息数量
					}
					gossipMsgIdsReceived[msgId] = struct{}{} // 标记 gossip 消息为已接收
				}
			}

			// 构建并发送包含 IWANT 和 PRUNE 控制消息的 RPC
			out := rpcWithControl(nil, nil, iwants, nil, prunes)
			err = w.WriteMsg(out)
			if err != nil {
				panic(err) // 如果写入失败，触发恐慌
			}
		}
		iwe.lk.Unlock() // 解锁
	}
}

// validRPCSizes 验证 RPC 切片中的每个 RPC 是否都在指定的大小限制内
func validRPCSizes(slice []*RPC, limit int) bool {
	for _, rpc := range slice {
		if rpc.Size() > limit {
			return false
		}
	}
	return true
}

// TestFragmentRPCFunction 测试 `fragmentRPC` 函数，它将超出大小限制的 RPC 消息进行分片。
//
// 功能描述：
// 1. 定义 `fragmentRPC` 内联函数，用于分片 RPC 消息。
// 2. 使用 `mkMsg` 函数创建指定大小的消息，用于测试不同场景。
// 3. 验证 RPC 消息在不超过大小限制时是否能正确处理（不分片）。
// 4. 验证当消息大小超过限制时是否能正确返回错误。
// 5. 验证当多个消息合计超过限制时，是否能正确分片。
// 6. 验证包含控制消息的 RPC 消息是否能正确处理，确保控制消息在最后一个分片中。
// 7. 验证控制消息太大时是否能正确分片。
// 8. 验证处理包含巨大消息 ID 的情况，确保能够正确分片并处理异常。

func TestFragmentRPCFunction(t *testing.T) {
	// 定义一个用于分片 RPC 消息的内联函数
	fragmentRPC := func(rpc *RPC, limit int) ([]*RPC, error) {
		// 使用 `appendOrMergeRPC` 函数将 RPC 消息分片
		rpcs := appendOrMergeRPC(nil, limit, *rpc)
		// 验证分片后的所有 RPC 消息大小是否都在限制内
		if allValid := validRPCSizes(rpcs, limit); !allValid {
			return rpcs, fmt.Errorf("RPC size exceeds limit")
		}
		return rpcs, nil
	}

	// 创建一个假设的 Peer ID
	p := peer.ID("some-peer")
	// 设置测试主题
	topic := "test"
	// 创建一个空的 RPC 消息，并设置发件人
	rpc := &RPC{from: p}
	// 设置大小限制为 1024 字节
	limit := 1024

	// 定义一个函数，用于创建指定大小的 `pb.Message`
	mkMsg := func(size int) *pb.Message {
		msg := &pb.Message{}
		// 创建指定大小的消息数据，并减去 protobuf 的开销
		msg.Data = make([]byte, size-4)
		// 随机填充消息数据
		rand.Read(msg.Data)
		return msg
	}

	// 定义一个函数，用于确保 RPC 消息大小在限制内
	ensureBelowLimit := func(rpcs []*RPC) {
		for _, r := range rpcs {
			// 如果分片后的任何 RPC 消息超出限制，则报告错误
			if r.Size() > limit {
				t.Fatalf("expected fragmented RPC to be below %d bytes, was %d", limit, r.Size())
			}
		}
	}

	// 测试场景 1: 当所有消息大小在限制内时，不进行分片
	rpc.Publish = []*pb.Message{}
	// 创建两条小消息，大小在限制内
	rpc.Publish = []*pb.Message{mkMsg(10), mkMsg(10)}
	// 进行分片操作
	results, err := fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	// 验证结果是否只生成了一个 RPC 消息
	if len(results) != 1 {
		t.Fatalf("expected single RPC if input is < limit, got %d", len(results))
	}

	// 测试场景 2: 当单个消息超过限制时，应该返回错误
	// 创建一个小消息和一个超出限制的消息
	rpc.Publish = []*pb.Message{mkMsg(10), mkMsg(limit * 2)}
	results, err = fragmentRPC(rpc, limit)
	// 验证是否返回了错误，而不是生成多个 RPC 消息
	if err == nil {
		t.Fatalf("expected an error if a message exceeds limit, got %d RPCs instead", len(results))
	}

	// 测试场景 3: 当消息总大小超过限制时，应该进行分片
	nMessages := 100
	msgSize := 200
	truth := true
	// 添加一个订阅消息到 RPC 消息
	rpc.Subscriptions = []*pb.RPC_SubOpts{
		{
			Subscribe: truth,
			Topicid:   topic,
		},
	}
	// 生成多个消息，确保总大小超过限制
	rpc.Publish = make([]*pb.Message, nMessages)
	for i := 0; i < nMessages; i++ {
		rpc.Publish[i] = mkMsg(msgSize)
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	// 验证所有分片后的 RPC 消息是否在限制内
	ensureBelowLimit(results)
	msgsPerRPC := limit / msgSize
	expectedRPCs := nMessages / msgsPerRPC
	// 验证生成的 RPC 消息数量是否符合预期
	if len(results) != expectedRPCs {
		t.Fatalf("expected %d RPC messages in output, got %d", expectedRPCs, len(results))
	}

	// 测试场景 4: 包含控制消息时，控制消息应该在最后一个 RPC 中
	// 重用前面的 RPC 消息，并添加一个控制消息
	rpc.Control = &pb.ControlMessage{
		Graft: []*pb.ControlGraft{{TopicID: topic}},
		Prune: []*pb.ControlPrune{{TopicID: topic}},
		Ihave: []*pb.ControlIHave{{MessageIDs: []string{"foo"}}},
		Iwant: []*pb.ControlIWant{{MessageIDs: []string{"bar"}}},
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	// 验证所有分片后的 RPC 消息是否在限制内
	ensureBelowLimit(results)
	expectedCtrl := 1
	expectedRPCs = (nMessages / msgsPerRPC) + expectedCtrl
	// 验证生成的 RPC 消息数量是否符合预期
	if len(results) != expectedRPCs {
		t.Fatalf("expected %d RPC messages in output, got %d", expectedRPCs, len(results))
	}
	ctl := results[len(results)-1].Control
	if ctl == nil {
		t.Fatal("expected final fragmented RPC to contain control messages, but .Control was nil")
	}
	// 确保控制消息未被修改
	originalBytes, err := rpc.Control.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	receivedBytes, err := ctl.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(originalBytes, receivedBytes) {
		t.Fatal("expected control message to be unaltered if it fits within one RPC message")
	}

	// 测试场景 5: 当控制消息太大以致无法放入单个 RPC 时，应该将其分成多个 RPC
	nTopics := 5
	messageIdSize := 32
	msgsPerTopic := 100
	// 为多个主题创建 IHAVE 和 IWANT 控制消息，确保消息总大小超出限制
	rpc.Control.Ihave = make([]*pb.ControlIHave, nTopics)
	rpc.Control.Iwant = make([]*pb.ControlIWant, nTopics)
	for i := 0; i < nTopics; i++ {
		messageIds := make([]string, msgsPerTopic)
		for m := 0; m < msgsPerTopic; m++ {
			mid := make([]byte, messageIdSize)
			rand.Read(mid)
			messageIds[m] = string(mid)
		}
		rpc.Control.Ihave[i] = &pb.ControlIHave{MessageIDs: messageIds}
		rpc.Control.Iwant[i] = &pb.ControlIWant{MessageIDs: messageIds}
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	// 验证所有分片后的 RPC 消息是否在限制内
	ensureBelowLimit(results)
	minExpectedCtl := rpc.Control.Size() / limit
	minExpectedRPCs := (nMessages / msgsPerRPC) + minExpectedCtl
	// 验证生成的 RPC 消息数量是否符合预期
	if len(results) < minExpectedRPCs {
		t.Fatalf("expected at least %d total RPCs (at least %d with control messages), got %d total", expectedRPCs, expectedCtrl, len(results))
	}

	// 测试场景 6: 处理包含巨大消息 ID 的情况，确保能够正确分片并处理异常
	// 重置 RPC 并创建一个超大消息 ID
	rpc.Reset()
	giantIdBytes := make([]byte, limit*2)
	rand.Read(giantIdBytes)
	// 添加一个控制消息，其中包含一个普通消息 ID 和一个超大消息 ID
	rpc.Control = &pb.ControlMessage{
		Iwant: []*pb.ControlIWant{
			{MessageIDs: []string{"hello", string(giantIdBytes)}},
		},
	}
	results, _ = fragmentRPC(rpc, limit)

	// 过滤掉超过限制的 ID
	filtered := make([]*RPC, 0, len(results))
	for _, r := range results {
		if r.Size() < limit {
			filtered = append(filtered, r)
		}
	}
	results = filtered
	err = nil
	if !validRPCSizes(results, limit) {
		err = fmt.Errorf("RPC size exceeds limit")
	}

	if err != nil {
		t.Fatal(err)
	}
	// 验证结果中只有一个有效的 RPC 消息
	if len(results) != 1 {
		t.Fatalf("expected 1 RPC, got %d", len(results))
	}
	// 验证 IWANT 控制消息中只有一个小的消息 ID
	if len(results[0].Control.Iwant) != 1 {
		t.Fatalf("expected 1 IWANT, got %d", len(results[0].Control.Iwant))
	}
	if results[0].Control.Iwant[0].MessageIDs[0] != "hello" {
		t.Fatalf("expected small message ID to be included unaltered, got %s instead",
			results[0].Control.Iwant[0].MessageIDs[0])
	}
}

// FuzzAppendOrMergeRPC 对 `appendOrMergeRPC` 函数进行模糊测试，确保在不同数据输入下的行为稳定。
//
// 功能描述：
// 1. 设置最小和最大消息大小限制。
// 2. 使用模糊测试框架生成随机数据，并计算最大消息大小。
// 3. 根据生成的数据创建一个随机的 RPC 消息。
// 4. 使用 `appendOrMergeRPC` 函数将 RPC 消息分片或合并。
// 5. 验证所有生成的 RPC 消息是否在大小限制内。

func FuzzAppendOrMergeRPC(f *testing.F) {
	// 设置最小和最大消息大小
	minMaxMsgSize := 100
	maxMaxMsgSize := 2048

	// 使用模糊测试函数，接受随机数据输入
	f.Fuzz(func(t *testing.T, data []byte) {
		// 根据输入数据生成最大消息大小
		maxSize := int(generateU16(&data)) % maxMaxMsgSize
		if maxSize < minMaxMsgSize {
			maxSize = minMaxMsgSize
		}

		// 根据随机数据生成一个 RPC 消息
		rpc := generateRPC(data, maxSize)

		// 调用 `appendOrMergeRPC` 函数，进行分片或合并操作
		rpcs := appendOrMergeRPC(nil, maxSize, *rpc)

		// 验证生成的所有 RPC 消息是否符合大小限制
		if !validRPCSizes(rpcs, maxSize) {
			t.Fatalf("invalid RPC size")
		}
	})
}

// TestGossipsubManagesAnAddressBook 测试 Gossipsub 路由器是否正确管理地址簿（Address Book）。
//
// 功能描述：
// 1. 创建测试上下文，并生成两个主机实例。
// 2. 为每个主机创建 Gossipsub 实例，并连接它们。
// 3. 检查第一个主机的地址簿，确保它包含了第二个主机的地址记录。
// 4. 断开第二个主机的连接，并更新地址簿中的 TTL。
// 5. 验证地址簿是否正确移除了所有与第二个主机相关的地址。

func TestGossipsubManagesAnAddressBook(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建两个主机实例
	hosts := getDefaultHosts(t, 2)

	// 为每个主机创建 Gossipsub 实例
	psubs := getGossipsubs(ctx, hosts)

	// 连接所有主机
	connectAll(t, hosts)

	// 等待 identify 事件传播，以便地址簿得到更新
	time.Sleep(time.Second)

	// 检查第一个主机的地址簿，确保它包含了第二个主机的地址记录
	cab, ok := peerstore.GetCertifiedAddrBook(psubs[0].rt.(*GossipSubRouter).cab)
	if !ok {
		t.Fatalf("expected a certified address book")
	}

	// 获取第二个主机的地址记录
	env := cab.GetPeerRecord(hosts[1].ID())
	if env == nil {
		t.Fatalf("expected a peer record for host 1")
	}

	// 断开与第二个主机的连接
	for _, c := range hosts[1].Network().Conns() {
		c.Close()
	}
	time.Sleep(time.Second)

	// 更新地址簿中的 TTL，标记最近连接的地址为已断开
	psubs[0].rt.(*GossipSubRouter).cab.UpdateAddrs(hosts[1].ID(), peerstore.RecentlyConnectedAddrTTL, 0)

	// 获取与第二个主机相关的所有地址
	addrs := psubs[0].rt.(*GossipSubRouter).cab.Addrs(hosts[1].ID())

	// 验证地址簿是否移除了所有地址
	if len(addrs) != 0 {
		t.Fatalf("expected no addrs, got %d addrs", len(addrs))
	}
}
