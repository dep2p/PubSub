package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dep2p/libp2p/core/protocol"
)

func TestDefaultGossipSubFeatures(t *testing.T) {
	// 测试 FloodSub 是否支持 Mesh 特性
	if GossipSubDefaultFeatures(GossipSubFeatureMesh, FloodSubID) {
		t.Fatal("floodsub should not support Mesh")
	}
	// 测试 GossipSub v1.0 是否支持 Mesh 特性
	if !GossipSubDefaultFeatures(GossipSubFeatureMesh, GossipSubID_v10) {
		t.Fatal("gossipsub-v1.0 should support Mesh")
	}
	// 测试 GossipSub v1.1 是否支持 Mesh 特性
	if !GossipSubDefaultFeatures(GossipSubFeatureMesh, GossipSubID_v11) {
		t.Fatal("gossipsub-v1.1 should support Mesh")
	}

	// 测试 FloodSub 是否支持 PX 特性
	if GossipSubDefaultFeatures(GossipSubFeaturePX, FloodSubID) {
		t.Fatal("floodsub should not support PX")
	}
	// 测试 GossipSub v1.0 是否支持 PX 特性
	if GossipSubDefaultFeatures(GossipSubFeaturePX, GossipSubID_v10) {
		t.Fatal("gossipsub-v1.0 should not support PX")
	}
	// 测试 GossipSub v1.1 是否支持 PX 特性
	if !GossipSubDefaultFeatures(GossipSubFeaturePX, GossipSubID_v11) {
		t.Fatal("gossipsub-v1.1 should support PX")
	}
}

func TestGossipSubCustomProtocols(t *testing.T) {
	// 定义自定义协议和特性
	customsub := protocol.ID("customsub/1.0.0")
	protos := []protocol.ID{customsub, FloodSubID}
	features := func(feat GossipSubFeature, proto protocol.ID) bool {
		return proto == customsub
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getDefaultHosts(t, 3)

	// 使用自定义协议创建 Gossipsub
	gsubs := getGossipsubs(ctx, hosts[:2], WithGossipSubProtocols(protos, features))
	fsub := getPubsub(ctx, hosts[2])
	psubs := append(gsubs, fsub)

	// 连接所有的 hosts
	connectAll(t, hosts)

	topic := "test"
	var subs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, subch)
	}

	// 等待 heartbeats 构建 mesh
	time.Sleep(time.Second * 2)

	// 检查 gsubs 的 meshes，gossipsub 的 meshes 应包括彼此但不包括 floodsub peer
	gsubs[0].eval <- func() {
		gs := gsubs[0].rt.(*GossipSubRouter)

		_, ok := gs.mesh[topic][hosts[1].ID()]
		if !ok {
			t.Fatal("expected gs0 to have gs1 in its mesh")
		}

		_, ok = gs.mesh[topic][hosts[2].ID()]
		if ok {
			t.Fatal("expected gs0 to not have fs in its mesh")
		}
	}

	gsubs[1].eval <- func() {
		gs := gsubs[1].rt.(*GossipSubRouter)

		_, ok := gs.mesh[topic][hosts[0].ID()]
		if !ok {
			t.Fatal("expected gs1 to have gs0 in its mesh")
		}

		_, ok = gs.mesh[topic][hosts[2].ID()]
		if ok {
			t.Fatal("expected gs1 to not have fs in its mesh")
		}
	}

	// 发送一些消息并验证接收
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not quite a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		tp, err := psubs[owner].Join(topic)
		if err != nil {
			t.Fatal(err)
		}

		tp.Publish(ctx, msg)

		for _, sub := range subs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}
}
