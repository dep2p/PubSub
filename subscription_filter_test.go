package pubsub

import (
	"context"
	"regexp"
	"testing"
	"time"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/libp2p/core/peer"
)

// TestBasicSubscriptionFilter 测试基本的订阅过滤功能
func TestBasicSubscriptionFilter(t *testing.T) {
	// 定义一个示例peerID
	peerA := peer.ID("A")

	// 定义三个订阅主题
	topic1 := "test1"
	topic2 := "test2"
	topic3 := "test3"

	// 定义订阅选项，yes表示订阅
	yes := true
	subs := []*pb.RPC_SubOpts{
		{
			Topicid:   topic1,
			Subscribe: yes,
		},
		{
			Topicid:   topic2,
			Subscribe: yes,
		},
		{
			Topicid:   topic3,
			Subscribe: yes,
		},
	}

	// 创建一个允许列表过滤器，允许 topic1 和 topic2
	filter := NewAllowlistSubscriptionFilter(topic1, topic2)

	// 检查能否订阅 topic1
	canSubscribe := filter.CanSubscribe(topic1)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}

	// 检查能否订阅 topic2
	canSubscribe = filter.CanSubscribe(topic2)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}

	// 检查能否订阅 topic3
	canSubscribe = filter.CanSubscribe(topic3)
	if canSubscribe {
		t.Fatal("expected disallowed subscription")
	}

	// 过滤传入的订阅
	allowedSubs, err := filter.FilterIncomingSubscriptions(peerA, subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowedSubs) != 2 {
		t.Fatalf("expected 2 allowed subscriptions but got %d", len(allowedSubs))
	}

	// 检查过滤后的订阅
	for _, sub := range allowedSubs {
		if sub.GetTopicid() == topic3 {
			t.Fatal("unexpected subscription to test3")
		}
	}

	// 测试订阅限制过滤器，限制最多2个订阅
	limitFilter := WrapLimitSubscriptionFilter(filter, 2)
	_, err = limitFilter.FilterIncomingSubscriptions(peerA, subs)
	if err != ErrTooManySubscriptions {
		t.Fatal("expected rejection because of too many subscriptions")
	}

	// 创建一个正则表达式过滤器，允许匹配 "test1" 和 "test2" 的主题
	filter = NewRegexpSubscriptionFilter(regexp.MustCompile("test[12]"))
	canSubscribe = filter.CanSubscribe(topic1)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}
	canSubscribe = filter.CanSubscribe(topic2)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}
	canSubscribe = filter.CanSubscribe(topic3)
	if canSubscribe {
		t.Fatal("expected disallowed subscription")
	}

	// 过滤传入的订阅
	allowedSubs, err = filter.FilterIncomingSubscriptions(peerA, subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowedSubs) != 2 {
		t.Fatalf("expected 2 allowed subscriptions but got %d", len(allowedSubs))
	}
	for _, sub := range allowedSubs {
		if sub.GetTopicid() == topic3 {
			t.Fatal("unexpected subscription")
		}
	}

	// 测试订阅限制过滤器，限制最多2个订阅
	limitFilter = WrapLimitSubscriptionFilter(filter, 2)
	_, err = limitFilter.FilterIncomingSubscriptions(peerA, subs)
	if err != ErrTooManySubscriptions {
		t.Fatal("expected rejection because of too many subscriptions")
	}
}

// TestSubscriptionFilterDeduplication 测试订阅过滤的去重功能
func TestSubscriptionFilterDeduplication(t *testing.T) {
	// 定义一个示例peerID
	peerA := peer.ID("A")

	// 定义三个订阅主题
	topic1 := "test1"
	topic2 := "test2"
	topic3 := "test3"
	yes := true
	no := false

	// 定义订阅选项
	subs := []*pb.RPC_SubOpts{
		{
			Topicid:   topic1,
			Subscribe: yes,
		},
		{
			Topicid:   topic1,
			Subscribe: yes,
		},
		{
			Topicid:   topic2,
			Subscribe: yes,
		},
		{
			Topicid:   topic2,
			Subscribe: no,
		},
		{
			Topicid:   topic3,
			Subscribe: yes,
		},
	}

	// 创建一个允许列表过滤器，允许 topic1 和 topic2
	filter := NewAllowlistSubscriptionFilter(topic1, topic2)

	// 过滤传入的订阅
	allowedSubs, err := filter.FilterIncomingSubscriptions(peerA, subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowedSubs) != 1 {
		t.Fatalf("expected 2 allowed subscriptions but got %d", len(allowedSubs))
	}

	// 检查过滤后的订阅
	for _, sub := range allowedSubs {
		if sub.GetTopicid() == topic3 || sub.GetTopicid() == topic2 {
			t.Fatal("unexpected subscription")
		}
	}
}

// TestSubscriptionFilterRPC 测试通过 RPC 进行订阅过滤
func TestSubscriptionFilterRPC(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建两个节点
	hosts := getDefaultHosts(t, 2)
	// 为每个节点创建 PubSub 实例，并应用订阅过滤器
	ps1 := getPubsub(ctx, hosts[0], WithSubscriptionFilter(NewAllowlistSubscriptionFilter("test1", "test2")))
	ps2 := getPubsub(ctx, hosts[1], WithSubscriptionFilter(NewAllowlistSubscriptionFilter("test2", "test3")))

	// 为每个 PubSub 实例订阅主题
	_ = mustSubscribe(t, ps1, "test1")
	_ = mustSubscribe(t, ps1, "test2")
	_ = mustSubscribe(t, ps2, "test2")
	_ = mustSubscribe(t, ps2, "test3")

	// 检查订阅的拒绝情况
	_, err := ps1.Join("test3")
	if err == nil {
		t.Fatal("expected subscription error")
	}

	// 连接两个节点
	connect(t, hosts[0], hosts[1])

	// 等待一段时间，确保连接建立
	time.Sleep(time.Second)

	var sub1, sub2, sub3 bool
	ready := make(chan struct{})

	// 检查 ps1 的订阅情况
	ps1.eval <- func() {
		_, sub1 = ps1.topics["test1"][hosts[1].ID()]
		_, sub2 = ps1.topics["test2"][hosts[1].ID()]
		_, sub3 = ps1.topics["test3"][hosts[1].ID()]
		ready <- struct{}{}
	}
	<-ready

	if sub1 {
		t.Fatal("expected no subscription for test1")
	}
	if !sub2 {
		t.Fatal("expected subscription for test2")
	}
	if sub3 {
		t.Fatal("expected no subscription for test1")
	}

	// 检查 ps2 的订阅情况
	ps2.eval <- func() {
		_, sub1 = ps2.topics["test1"][hosts[0].ID()]
		_, sub2 = ps2.topics["test2"][hosts[0].ID()]
		_, sub3 = ps2.topics["test3"][hosts[0].ID()]
		ready <- struct{}{}
	}
	<-ready

	if sub1 {
		t.Fatal("expected no subscription for test1")
	}
	if !sub2 {
		t.Fatal("expected subscription for test1")
	}
	if sub3 {
		t.Fatal("expected no subscription for test1")
	}
}
