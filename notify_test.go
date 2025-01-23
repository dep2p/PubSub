package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/dep2p/go-dep2p/p2p/protocol/identify"
)

// TestNotifyPeerProtocolsUpdated 测试更新后通知对等协议的功能。
func TestNotifyPeerProtocolsUpdated(t *testing.T) {
	// 创建一个上下文和取消函数，用于管理生命周期
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取两个默认的host实例
	hosts := getDefaultHosts(t, 2)

	// 初始化id服务
	{
		// 创建第一个host的ID服务
		ids1, err := identify.NewIDService(hosts[0])
		if err != nil {
			t.Fatal(err)
		}
		// 启动ID服务并确保在测试结束时关闭
		ids1.Start()
		defer ids1.Close()

		// 创建第二个host的ID服务
		ids2, err := identify.NewIDService(hosts[1])
		if err != nil {
			t.Fatal(err)
		}
		// 启动ID服务并确保在测试结束时关闭
		ids2.Start()
		defer ids2.Close()
	}

	// 获取第一个host的PubSub实例
	psubs0 := getPubsub(ctx, hosts[0])
	// 连接两个hosts
	connect(t, hosts[0], hosts[1])
	// 延迟以确保对等节点已连接
	<-time.After(time.Millisecond * 100)
	// 获取第二个host的PubSub实例
	psubs1 := getPubsub(ctx, hosts[1])

	// Pubsub 0 加入 "test" 主题
	topic0, err := psubs0.Join("test")
	if err != nil {
		t.Fatal(err)
	}
	defer topic0.Close()

	// 订阅第一个主题
	sub0, err := topic0.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub0.Cancel()

	// Pubsub 1 加入 "test" 主题
	topic1, err := psubs1.Join("test")
	if err != nil {
		t.Fatal(err)
	}
	defer topic1.Close()

	// 订阅第二个主题
	sub1, err := topic1.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Cancel()

	// 延迟以确保对等节点已连接并订阅
	<-time.After(time.Millisecond * 100)

	// 检查第一个主题的对等节点列表是否至少包含一个对等节点
	if len(topic0.ListPeers()) == 0 {
		t.Fatalf("topic0 should at least have 1 peer")
	}

	// 检查第二个主题的对等节点列表是否至少包含一个对等节点
	if len(topic1.ListPeers()) == 0 {
		t.Fatalf("topic1 should at least have 1 peer")
	}
}
