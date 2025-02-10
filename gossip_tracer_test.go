package pubsub

import (
	"testing"
	"time"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/go-dep2p/core/peer"
)

func TestBrokenPromises(t *testing.T) {
	// 测试未兑现的承诺是否被正确跟踪
	gt := newGossipTracer()
	gt.followUpTime = 100 * time.Millisecond

	peerA := peer.ID("A")
	peerB := peer.ID("B")
	peerC := peer.ID("C")

	// 创建100个测试消息
	var mids []string
	for i := 0; i < 100; i++ {
		m := makeTestMessage(i)
		// 创建并序列化 AddrInfo
		addrInfo := peer.AddrInfo{
			ID:    peerA,
			Addrs: nil,
		}
		addrInfoBytes, err := addrInfo.MarshalJSON()
		if err != nil {
			t.Fatalf("序列化 AddrInfo 失败: %v", err)
		}
		m.From = addrInfoBytes
		mid := DefaultMsgIdFn(m)
		mids = append(mids, mid)
	}

	// 添加承诺
	gt.AddPromise(peerA, mids)
	gt.AddPromise(peerB, mids)
	gt.AddPromise(peerC, mids)

	// 验证当前没有未兑现的承诺
	brokenPromises := gt.GetBrokenPromises()
	if brokenPromises != nil {
		t.Fatal("expected no broken promises")
	}

	// 对其中一个对等节点进行限速以保留其承诺
	gt.ThrottlePeer(peerC)

	// 使承诺失效
	time.Sleep(gt.followUpTime + time.Millisecond)

	// 检查未兑现的承诺
	brokenPromises = gt.GetBrokenPromises()
	if len(brokenPromises) != 2 {
		t.Fatalf("expected 2 broken promises, got %d", len(brokenPromises))
	}

	// 检查 peerA 和 peerB 的未兑现承诺
	brokenPromisesA := brokenPromises[peerA]
	if brokenPromisesA != 1 {
		t.Fatalf("expected 1 broken promise from A, got %d", brokenPromisesA)
	}

	brokenPromisesB := brokenPromises[peerB]
	if brokenPromisesB != 1 {
		t.Fatalf("expected 1 broken promise from B, got %d", brokenPromisesB)
	}

	// 验证 peerPromises 映射已被清空
	if len(gt.peerPromises) != 0 {
		t.Fatal("expected empty peerPromises map")
	}
}

func TestNoBrokenPromises(t *testing.T) {
	// 类似上面的测试，但这次我们交付消息以兑现承诺
	gt := newGossipTracer()
	gt.followUpTime = 100 * time.Millisecond

	peerA := peer.ID("A")
	peerB := peer.ID("B")

	// 创建100个测试消息
	var msgs []*pb.Message
	var mids []string
	for i := 0; i < 100; i++ {
		m := makeTestMessage(i)
		// 创建并序列化 AddrInfo
		addrInfo := peer.AddrInfo{
			ID:    peerA,
			Addrs: nil,
		}
		addrInfoBytes, err := addrInfo.MarshalJSON()
		if err != nil {
			t.Fatalf("序列化 AddrInfo 失败: %v", err)
		}
		m.From = addrInfoBytes
		msgs = append(msgs, m)
		mid := DefaultMsgIdFn(m)
		mids = append(mids, mid)
	}

	// 添加承诺
	gt.AddPromise(peerA, mids)
	gt.AddPromise(peerB, mids)

	// 交付消息以兑现承诺
	for _, m := range msgs {
		gt.DeliverMessage(&Message{Message: m})
	}

	time.Sleep(gt.followUpTime + time.Millisecond)

	// 验证当前没有未兑现的承诺
	brokenPromises := gt.GetBrokenPromises()
	if brokenPromises != nil {
		t.Fatal("expected no broken promises")
	}

	// 验证 peerPromises 映射已被清空
	if len(gt.peerPromises) != 0 {
		t.Fatal("expected empty peerPromises map")
	}
}
