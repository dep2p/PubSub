package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/dep2p/go-dep2p"
	"github.com/dep2p/go-dep2p/core/host"

	"github.com/dep2p/go-dep2p/core/network"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/p2p/net/connmgr"
)

func TestGossipsubConnTagMessageDeliveries(t *testing.T) {
	t.Skip("Test disabled with go-dep2p v0.22.0") // TODO: reenable test when updating to v0.23.0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 保存原始的 GossipSub 配置参数，以便在测试结束时还原
	oldGossipSubD := GossipSubD
	oldGossipSubDlo := GossipSubDlo
	oldGossipSubDHi := GossipSubDhi
	oldGossipSubConnTagDecayInterval := GossipSubConnTagDecayInterval
	oldGossipSubConnTagMessageDeliveryCap := GossipSubConnTagMessageDeliveryCap

	// 将 GossipSub D 参数设置为较低值，以便我们有一些节点在 mesh 外
	GossipSubDlo = 3
	GossipSubD = 3
	GossipSubDhi = 3
	// 将 tag decay 间隔设置为较短时间，以便测试不需等待太久
	GossipSubConnTagDecayInterval = time.Second

	// 将 deliveries 的 cap 设置为高于 GossipSubConnTagValueMeshPeer，以便 sybils 即使在某人的 mesh 中也会被驱逐
	GossipSubConnTagMessageDeliveryCap = 50

	// 测试结束后还原 globals
	defer func() {
		GossipSubD = oldGossipSubD
		GossipSubDlo = oldGossipSubDlo
		GossipSubDhi = oldGossipSubDHi
		GossipSubConnTagDecayInterval = oldGossipSubConnTagDecayInterval
		GossipSubConnTagMessageDeliveryCap = oldGossipSubConnTagMessageDeliveryCap
	}()

	decayClock := clock.NewMock()
	decayCfg := connmgr.DecayerCfg{
		Resolution: time.Second,
		Clock:      decayClock,
	}

	nHonest := 5
	nSquatter := 10
	connLimit := 10

	connmgrs := make([]*connmgr.BasicConnMgr, nHonest)
	honestHosts := make([]host.Host, nHonest)
	honestPeers := make(map[peer.ID]struct{})

	for i := 0; i < nHonest; i++ {
		var err error
		connmgrs[i], err = connmgr.NewConnManager(nHonest, connLimit,
			connmgr.WithGracePeriod(0),
			connmgr.WithSilencePeriod(time.Millisecond),
			connmgr.DecayerConfig(&decayCfg),
		)
		if err != nil {
			t.Fatal(err)
		}

		h, err := dep2p.New(
			dep2p.ResourceManager(&network.NullResourceManager{}),
			dep2p.ConnectionManager(connmgrs[i]),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Close() })
		honestHosts[i] = h
		honestPeers[h.ID()] = struct{}{}
	}

	// 使用 flood publishing，这样即使是非 mesh 节点也会向所有人发送消息
	psubs := getGossipsubs(ctx, honestHosts,
		WithFloodPublish(true))

	// sybil squatters 稍后连接
	sybilHosts := getDefaultHosts(t, nSquatter)
	for _, h := range sybilHosts {
		squatter := &sybilSquatter{h: h}
		h.SetStreamHandler(GossipSubID_v10, squatter.handleStream)
	}

	// 连接所有的 honest hosts
	connectAll(t, honestHosts)

	for _, h := range honestHosts {
		if len(h.Network().Conns()) != nHonest-1 {
			t.Errorf("expected to have conns to all honest peers, have %d", len(h.Network().Conns()))
		}
	}

	// 所有人订阅主题
	topic := "test"
	for _, ps := range psubs {
		_, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 休眠以允许 mesh 形成
	time.Sleep(2 * time.Second)

	// 让所有的 hosts 发布足够多的消息以确保他们得到一些 delivery credit
	nMessages := GossipSubConnTagMessageDeliveryCap * 2
	for _, ps := range psubs {
		tp, err := ps.Join(topic)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nMessages; i++ {
			tp.Publish(ctx, []byte("hello"))
		}
	}

	// 推进 fake 时间以进行 tag decay
	decayClock.Add(time.Second)

	// 验证他们是否相互给予了 delivery connection tags
	tag := "pubsub-deliveries:test"
	for _, h := range honestHosts {
		for _, h2 := range honestHosts {
			if h.ID() == h2.ID() {
				continue
			}
			val := getTagValue(h.ConnManager(), h2.ID(), tag)
			if val == 0 {
				t.Errorf("Expected non-zero delivery tag value for peer %s", h2.ID())
			}
		}
	}

	// 现在连接 sybils 以对真实 hosts 的 connection managers 施加压力
	allHosts := append(honestHosts, sybilHosts...)
	connectAll(t, allHosts)

	// 验证我们有大量的连接
	for _, h := range honestHosts {
		if len(h.Network().Conns()) != nHonest+nSquatter-1 {
			t.Errorf("expected to have conns to all peers, have %d", len(h.Network().Conns()))
		}
	}

	// 强制 connection managers 修剪，这样我们就不需要过多考虑时间问题
	for _, cm := range connmgrs {
		cm.TrimOpenConns(ctx)
	}

	// 我们应该仍然与所有 honest peers 保持连接，但不与 sybils 连接
	for _, h := range honestHosts {
		nHonestConns := 0
		nDishonestConns := 0
		for _, conn := range h.Network().Conns() {
			if _, ok := honestPeers[conn.RemotePeer()]; !ok {
				nDishonestConns++
			} else {
				nHonestConns++
			}
		}
		if nDishonestConns > connLimit-nHonest {
			t.Errorf("expected most dishonest conns to be pruned, have %d", nDishonestConns)
		}
		if nHonestConns != nHonest-1 {
			t.Errorf("expected all honest conns to be preserved, have %d", nHonestConns)
		}
	}
}
