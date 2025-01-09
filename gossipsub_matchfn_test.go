package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dep2p/libp2p/core/protocol"
)

// TestGossipSubMatchingFn 测试使用自定义协议匹配函数的 Gossipsub 行为。
func TestGossipSubMatchingFn(t *testing.T) {
	// 定义一些自定义协议
	customsubA100 := protocol.ID("/customsub_a/1.0.0")
	customsubA101Beta := protocol.ID("/customsub_a/1.0.1-beta")
	customsubB100 := protocol.ID("/customsub_b/1.0.0")

	// 创建上下文，用于取消操作
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取默认的 host 列表
	h := getDefaultHosts(t, 4)
	psubs := []*PubSub{
		// 使用自定义协议匹配函数和自定义协议创建 Gossipsub
		getGossipsub(ctx, h[0], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{customsubA100, GossipSubID_v11}, GossipSubDefaultFeatures)),
		getGossipsub(ctx, h[1], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{customsubA101Beta}, GossipSubDefaultFeatures)),
		getGossipsub(ctx, h[2], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{GossipSubID_v11}, GossipSubDefaultFeatures)),
		getGossipsub(ctx, h[3], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{customsubB100}, GossipSubDefaultFeatures)),
	}

	// 连接第一个 host 与其他 hosts
	connect(t, h[0], h[1])
	connect(t, h[0], h[2])
	connect(t, h[0], h[3])

	// 验证 peers 是否连接
	time.Sleep(2 * time.Second)
	for i := 1; i < len(h); i++ {
		if len(h[0].Network().ConnsToPeer(h[i].ID())) == 0 {
			t.Fatal("expected a connection between peers")
		}
	}

	// 构建 mesh
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// 等待订阅生效
	time.Sleep(time.Second)

	topic, err := psubs[0].Join("test")
	if err != nil {
		t.Fatal(err)
	}

	// 发布消息
	msg := []byte("message")
	topic.Publish(ctx, msg)

	// 验证接收到的消息
	assertReceive(t, subs[0], msg)
	assertReceive(t, subs[1], msg) // 通过 semver 匹配 customsub 名称，应收到消息
	assertReceive(t, subs[2], msg) // 通过 GossipSubID_v11 匹配，应收到消息

	// customsubA 和 customsubB 名称不同，不应接收到消息
	ctxTimeout, timeoutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer timeoutCancel()
	received := false
	for {
		msg, err := subs[3].Next(ctxTimeout)
		if err != nil {
			break
		}
		if msg != nil {
			received = true
		}
	}
	if received {
		t.Fatal("Should not have received a message")
	}
}

// protocolNameMatch 返回一个函数，该函数用于匹配协议名称
func protocolNameMatch(base protocol.ID) func(protocol.ID) bool {
	return func(check protocol.ID) bool {
		// 提取基础协议和检查协议的名称部分
		baseName := strings.Split(string(base), "/")[1]
		checkName := strings.Split(string(check), "/")[1]
		// 比较名称部分是否相同
		return baseName == checkName
	}
}
