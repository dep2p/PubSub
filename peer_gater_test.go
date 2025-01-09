package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/dep2p/libp2p/core/peer"
)

// TestPeerGater 测试 PeerGater 的功能。
func TestPeerGater(t *testing.T) {
	// 创建一个上下文和取消函数，用于管理生命周期
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 定义测试用的peer和IP地址
	peerA := peer.ID("A")
	peerAip := "1.2.3.4"

	// 创建并验证PeerGater参数
	params := NewPeerGaterParams(.1, .9, .999)
	err := params.validate()
	if err != nil {
		t.Fatal(err)
	}

	// 创建一个新的PeerGater实例
	pg := newPeerGater(ctx, nil, params)
	// 设置自定义的获取IP地址函数
	pg.getIP = func(p peer.ID) string {
		switch p {
		case peerA:
			return peerAip
		default:
			return "<wtf>"
		}
	}

	// 添加peerA
	pg.AddPeer(peerA, "")

	// 检查peerA的初始接受状态
	status := pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	// 创建一个测试消息
	msg := &Message{ReceivedFrom: peerA}

	// 验证消息并检查接受状态
	pg.ValidateMessage(msg)
	status = pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	// 拒绝消息并检查接受状态
	pg.RejectMessage(msg, RejectValidationQueueFull)
	status = pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	pg.RejectMessage(msg, RejectValidationThrottled)
	status = pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	// 多次拒绝消息并检查接受状态
	for i := 0; i < 100; i++ {
		pg.RejectMessage(msg, RejectValidationIgnored)
		pg.RejectMessage(msg, RejectValidationFailed)
	}

	// 检查是否进入AcceptControl状态
	accepted := false
	for i := 0; !accepted && i < 1000; i++ {
		status = pg.AcceptFrom(peerA)
		if status == AcceptControl {
			accepted = true
		}
	}
	if !accepted {
		t.Fatal("expected AcceptControl")
	}

	// 多次交付消息并检查接受状态
	for i := 0; i < 100; i++ {
		pg.DeliverMessage(msg)
	}

	// 检查是否返回到AcceptAll状态
	accepted = false
	for i := 0; !accepted && i < 1000; i++ {
		status = pg.AcceptFrom(peerA)
		if status == AcceptAll {
			accepted = true
		}
	}
	if !accepted {
		t.Fatal("expected to accept at least once")
	}

	// 衰减统计数据并检查接受状态
	for i := 0; i < 100; i++ {
		pg.decayStats()
	}

	status = pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	// 移除peerA并检查是否清除其统计记录
	pg.RemovePeer(peerA)
	pg.Lock()
	_, ok := pg.peerStats[peerA]
	pg.Unlock()
	if ok {
		t.Fatal("still have a stat record for peerA")
	}

	// 检查是否保留peerA的IP统计记录
	pg.Lock()
	_, ok = pg.ipStats[peerAip]
	pg.Unlock()
	if !ok {
		t.Fatal("expected to still have a stat record for peerA's ip")
	}

	// 设置IP记录的过期时间并等待
	pg.Lock()
	pg.ipStats[peerAip].expire = time.Now()
	pg.Unlock()

	time.Sleep(2 * time.Second)

	// 检查是否清除了过期的IP记录
	pg.Lock()
	_, ok = pg.ipStats["1.2.3.4"]
	pg.Unlock()
	if ok {
		t.Fatal("still have a stat record for peerA's ip")
	}
}
