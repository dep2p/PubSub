package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/dep2p/go-dep2p"
	"github.com/dep2p/go-dep2p/core/host"
	"github.com/dep2p/go-dep2p/core/network"
)

// getDefaultHosts 创建并返回指定数量的 libp2p 主机。
// t：测试对象
// n：需要创建的主机数量
func getDefaultHosts(t *testing.T, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		// 创建新的 libp2p 主机，使用 NullResourceManager 禁用资源管理。
		h, err := dep2p.New(dep2p.ResourceManager(&network.NullResourceManager{}))
		if err != nil {
			t.Fatal(err)
		}
		// 确保在测试结束时关闭主机。
		t.Cleanup(func() { h.Close() })
		out = append(out, h)
	}

	return out
}

// TestPubSubRemovesBlacklistedPeer 测试 PubSub 是否正确移除黑名单中的 peer。
func TestPubSubRemovesBlacklistedPeer(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())

	// 创建两个 libp2p 主机
	hosts := getDefaultHosts(t, 2)

	// 创建黑名单
	bl := NewMapBlacklist()

	// 创建 PubSub 实例，并将其中一个与黑名单关联
	psubs0 := getPubsub(ctx, hosts[0])
	psubs1 := getPubsub(ctx, hosts[1], WithBlacklist(bl))

	// 连接两个主机
	connect(t, hosts[0], hosts[1])

	// 将第一个主机加入黑名单
	bl.Add(hosts[0].ID())

	// 订阅主题 "test"
	_, err := psubs0.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	sub1, err := psubs1.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	// 等待一段时间以确保订阅成功
	time.Sleep(time.Millisecond * 100)

	// 在第一个主机上发布消息
	topic, err := psubs0.Join("test")
	if err != nil {
		t.Fatal(err)
	}

	topic.Publish(ctx, []byte("message"))

	// 设置一个带超时的上下文，用于接收消息
	wctx, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel2()

	_, _ = sub1.Next(wctx)

	// 显式取消上下文，以便 PubSub 清理 peer 通道
	cancel()
	time.Sleep(time.Millisecond * 100)
}
