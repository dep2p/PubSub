package pubsub

import (
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	connmgri "github.com/dep2p/go-dep2p/core/connmgr"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/p2p/net/connmgr"

	pb "github.com/dep2p/pubsub/pb"
)

// TestTagTracerMeshTags 测试 TagTracer 在 graft 和 prune 事件时是否正确应用标签
func TestTagTracerMeshTags(t *testing.T) {
	// 创建一个新的连接管理器
	cmgr, err := connmgr.NewConnManager(5, 10, connmgr.WithGracePeriod(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tt := newTagTracer(cmgr)

	// 定义一个 peer 和主题
	p := peer.ID("a-peer")
	topic := "a-topic"

	// 将 peer 加入主题
	tt.Join(topic)
	tt.Graft(p, topic)

	// 检查 peer 是否被保护
	tag := "pubsub:" + topic
	if !cmgr.IsProtected(p, tag) {
		t.Fatal("expected the mesh peer to be protected")
	}

	// 将 peer 从主题中移除
	tt.Prune(p, topic)
	if cmgr.IsProtected(p, tag) {
		t.Fatal("expected the former mesh peer to be unprotected")
	}
}

// TestTagTracerDirectPeerTags 测试 TagTracer 对直接 peers 的标签应用
func TestTagTracerDirectPeerTags(t *testing.T) {
	// 创建一个新的连接管理器
	cmgr, err := connmgr.NewConnManager(5, 10, connmgr.WithGracePeriod(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tt := newTagTracer(cmgr)

	// 定义三个 peer
	p1 := peer.ID("1")
	p2 := peer.ID("2")
	p3 := peer.ID("3")

	// 设置直接 peers
	tt.direct = make(map[peer.ID]struct{})
	tt.direct[p1] = struct{}{}

	// 添加 peers
	tt.AddPeer(p1, GossipSubID_v10)
	tt.AddPeer(p2, GossipSubID_v10)
	tt.AddPeer(p3, GossipSubID_v10)

	// 检查直接 peer 是否被保护
	tag := "pubsub:<direct>"
	if !cmgr.IsProtected(p1, tag) {
		t.Fatal("expected direct peer to be protected")
	}

	// 检查非直接 peer 是否未被保护
	for _, p := range []peer.ID{p2, p3} {
		if cmgr.IsProtected(p, tag) {
			t.Fatal("expected non-direct peer to be unprotected")
		}
	}
}

// TestTagTracerDeliveryTags 测试递送标签的衰减
func TestTagTracerDeliveryTags(t *testing.T) {
	t.Skip("flaky test temporarily disabled; TODO: fixme")
	// 使用假时间来测试标签衰减
	clk := clock.NewMock()
	decayCfg := &connmgr.DecayerCfg{
		Clock:      clk,
		Resolution: time.Minute,
	}
	cmgr, err := connmgr.NewConnManager(5, 10, connmgr.WithGracePeriod(time.Minute), connmgr.DecayerConfig(decayCfg))
	if err != nil {
		t.Fatal(err)
	}

	tt := newTagTracer(cmgr)

	topic1 := "topic-1"
	topic2 := "topic-2"

	p := peer.ID("a-peer")

	tt.Join(topic1)
	tt.Join(topic2)

	for i := 0; i < 20; i++ {
		// 仅向 topic 2 递送 5 条消息（少于上限）
		topic := topic1
		if i < 5 {
			topic = topic2
		}
		msg := &Message{
			ReceivedFrom: p,
			Message: &pb.Message{
				From:  []byte(p),
				Data:  []byte("hello"),
				Topic: topic,
			},
		}
		tt.DeliverMessage(msg)
	}

	// 必须推进假时钟一次以应用 bump
	clk.Add(time.Minute)

	tag1 := "pubsub-deliveries:topic-1"
	tag2 := "pubsub-deliveries:topic-2"

	// topic-1 的标签值应被限制在 GossipSubConnTagMessageDeliveryCap（默认值为 15）
	val := getTagValue(cmgr, p, tag1)
	expected := GossipSubConnTagMessageDeliveryCap
	if val != expected {
		t.Errorf("expected delivery tag to be capped at %d, was %d", expected, val)
	}

	// topic-2 的值应等于递送的消息数量（5），因为它少于上限
	val = getTagValue(cmgr, p, tag2)
	expected = 5
	if val != expected {
		t.Errorf("expected delivery tag value = %d, got %d", expected, val)
	}

	// 如果我们向前跳几分钟，我们应该看到标签每 10 分钟减少 1
	clk.Add(50 * time.Minute)
	time.Sleep(2 * time.Second)

	val = getTagValue(cmgr, p, tag1)
	expected = GossipSubConnTagMessageDeliveryCap - 5
	if val > expected+1 || val < expected-1 {
		t.Errorf("expected delivery tag value = %d ± 1, got %d", expected, val)
	}

	// topic-2 的标签现在应该已经重置为零，但考虑到 Travis 的速度，我们加 1
	val = getTagValue(cmgr, p, tag2)
	expected = 0
	if val > expected+1 || val < expected-1 {
		t.Errorf("expected delivery tag value = %d ± 1, got %d", expected, val)
	}

	// 离开主题应移除标签
	if !tagExists(cmgr, p, tag1) {
		t.Errorf("expected delivery tag %s to be applied to peer %s", tag1, p)
	}
	tt.Leave(topic1)
	time.Sleep(time.Second)
	if tagExists(cmgr, p, tag1) {
		t.Errorf("expected delivery tag %s to be removed after leaving the topic", tag1)
	}
}

// TestTagTracerDeliveryTagsNearFirst 测试接近首次递送标签的情况
func TestTagTracerDeliveryTagsNearFirst(t *testing.T) {
	// 使用假时间来测试标签衰减
	clk := clock.NewMock()
	decayCfg := &connmgr.DecayerCfg{
		Clock:      clk,
		Resolution: time.Minute,
	}
	cmgr, err := connmgr.NewConnManager(5, 10, connmgr.WithGracePeriod(time.Minute), connmgr.DecayerConfig(decayCfg))
	if err != nil {
		t.Fatal(err)
	}

	tt := newTagTracer(cmgr)

	topic := "test"

	p := peer.ID("a-peer")
	p2 := peer.ID("another-peer")
	p3 := peer.ID("slow-peer")

	tt.Join(topic)

	for i := 0; i < GossipSubConnTagMessageDeliveryCap+5; i++ {
		msg := &Message{
			ReceivedFrom: p,
			Message: &pb.Message{
				From:  []byte(p),
				Data:  []byte(fmt.Sprintf("msg-%d", i)),
				Topic: topic,
				Seqno: []byte(fmt.Sprintf("%d", i)),
			},
		}

		dup := &Message{
			ReceivedFrom: p2,
			Message:      msg.Message,
		}

		tt.ValidateMessage(msg)
		tt.DuplicateMessage(dup)
		tt.DeliverMessage(msg)

		dup.ReceivedFrom = p3
		tt.DuplicateMessage(dup)
	}

	clk.Add(time.Minute)

	tag := "pubsub-deliveries:test"
	val := getTagValue(cmgr, p, tag)
	if val != GossipSubConnTagMessageDeliveryCap {
		t.Errorf("expected tag %s to have val %d, was %d", tag, GossipSubConnTagMessageDeliveryCap, val)
	}
	val = getTagValue(cmgr, p2, tag)
	if val != GossipSubConnTagMessageDeliveryCap {
		t.Errorf("expected tag %s for near-first peer to have val %d, was %d", tag, GossipSubConnTagMessageDeliveryCap, val)
	}

	val = getTagValue(cmgr, p3, tag)
	if val != 0 {
		t.Errorf("expected tag %s for slow peer to have val %d, was %d", tag, 0, val)
	}
}

// getTagValue 获取连接管理器中 peer 的特定标签的值
func getTagValue(mgr connmgri.ConnManager, p peer.ID, tag string) int {
	info := mgr.GetTagInfo(p)
	if info == nil {
		return 0
	}
	val, ok := info.Tags[tag]
	if !ok {
		return 0
	}
	return val
}

// tagExists 检查连接管理器中 peer 是否存在特定标签
func tagExists(mgr connmgri.ConnManager, p peer.ID, tag string) bool {
	info := mgr.GetTagInfo(p)
	if info == nil {
		return false
	}
	_, exists := info.Tags[tag]
	return exists
}
