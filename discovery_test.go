package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// mockDiscoveryServer 模拟的发现服务器，用于模拟对等节点的发现
type mockDiscoveryServer struct {
	mx sync.Mutex                                    // 互斥锁，保护 db 字典的并发访问
	db map[string]map[peer.ID]*discoveryRegistration // 存储对等节点注册信息的字典
}

// discoveryRegistration 代表一个对等节点的注册信息
type discoveryRegistration struct {
	info peer.AddrInfo // 对等节点的地址信息
	ttl  time.Duration // 注册信息的存活时间
}

// newDiscoveryServer 创建一个新的 mockDiscoveryServer 实例
func newDiscoveryServer() *mockDiscoveryServer {
	return &mockDiscoveryServer{
		db: make(map[string]map[peer.ID]*discoveryRegistration),
	}
}

// Advertise 广告一个对等节点
func (s *mockDiscoveryServer) Advertise(ns string, info peer.AddrInfo, ttl time.Duration) (time.Duration, error) {
	s.mx.Lock()         // 获取互斥锁，保护对 db 的并发访问
	defer s.mx.Unlock() // 方法结束时释放锁

	peers, ok := s.db[ns]
	if !ok {
		peers = make(map[peer.ID]*discoveryRegistration)
		s.db[ns] = peers
	}
	peers[info.ID] = &discoveryRegistration{info, ttl}
	return ttl, nil
}

// FindPeers 查找对等节点
func (s *mockDiscoveryServer) FindPeers(ns string, limit int) (<-chan peer.AddrInfo, error) {
	s.mx.Lock()         // 获取互斥锁，保护对 db 的并发访问
	defer s.mx.Unlock() // 方法结束时释放锁

	peers, ok := s.db[ns]
	if !ok || len(peers) == 0 {
		emptyCh := make(chan peer.AddrInfo)
		close(emptyCh)
		return emptyCh, nil
	}

	count := len(peers)
	if count > limit {
		count = limit
	}
	ch := make(chan peer.AddrInfo, count)
	numSent := 0
	for _, reg := range peers {
		if numSent == count {
			break
		}
		numSent++
		ch <- reg.info
	}
	close(ch)

	return ch, nil
}

// hasPeerRecord 检查指定的对等节点是否存在于指定的命名空间中
func (s *mockDiscoveryServer) hasPeerRecord(ns string, pid peer.ID) bool {
	s.mx.Lock()         // 获取互斥锁，保护对 db 的并发访问
	defer s.mx.Unlock() // 方法结束时释放锁

	if peers, ok := s.db[ns]; ok {
		_, ok := peers[pid]
		return ok
	}
	return false
}

// mockDiscoveryClient 模拟的发现客户端，用于向发现服务器注册和查找对等节点
type mockDiscoveryClient struct {
	host   host.Host            // 本地节点的主机
	server *mockDiscoveryServer // 关联的发现服务器
}

// Advertise 广告本地节点
func (d *mockDiscoveryClient) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	var options discovery.Options
	err := options.Apply(opts...)
	if err != nil {
		return 0, err
	}

	return d.server.Advertise(ns, *host.InfoFromHost(d.host), options.Ttl)
}

// FindPeers 查找对等节点
func (d *mockDiscoveryClient) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	var options discovery.Options
	err := options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	return d.server.FindPeers(ns, options.Limit)
}

// dummyDiscovery 模拟的发现实现，不做实际的发现工作
type dummyDiscovery struct{}

func (d *dummyDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	return time.Hour, nil
}

func (d *dummyDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	retCh := make(chan peer.AddrInfo)
	go func() {
		time.Sleep(time.Second)
		close(retCh)
	}()
	return retCh, nil
}

// TestSimpleDiscovery 测试简单的发现机制
func TestSimpleDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numHosts = 20
	const topic = "foobar"

	// 设置发现服务器和 pubsub 客户端
	server := newDiscoveryServer()
	discOpts := []discovery.Option{discovery.Limit(numHosts), discovery.TTL(1 * time.Minute)}

	hosts := getDefaultHosts(t, numHosts)
	psubs := make([]*PubSub, numHosts)
	topicHandlers := make([]*Topic, numHosts)

	for i, h := range hosts {
		disc := &mockDiscoveryClient{h, server}
		ps := getPubsub(ctx, h, WithDiscovery(disc, WithDiscoveryOpts(discOpts...)))
		psubs[i] = ps
		topicHandlers[i], _ = ps.Join(topic)
	}

	// 订阅所有 pubsub 实例，除了一个
	msgs := make([]*Subscription, numHosts)
	for i, th := range topicHandlers[1:] {
		subch, err := th.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		msgs[i+1] = subch
	}

	// 等待广告通过并检查广告是否成功
	for {
		server.mx.Lock()
		numPeers := len(server.db["floodsub:foobar"])
		server.mx.Unlock()
		if numPeers == numHosts-1 {
			break
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}

	for i, h := range hosts[1:] {
		if !server.hasPeerRecord("floodsub:"+topic, h.ID()) {
			t.Fatalf("Server did not register host %d with ID: %s", i+1, h.ID())
		}
	}

	// 订阅一个然后发布一条消息
	subch, err := topicHandlers[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	msgs[0] = subch

	msg := []byte("first message")
	if err := topicHandlers[0].Publish(ctx, msg, WithReadiness(MinTopicSize(numHosts-1))); err != nil {
		t.Fatal(err)
	}

	for _, sub := range msgs {
		got, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(sub.err)
		}
		if !bytes.Equal(msg, got.Data) {
			t.Fatal("got wrong message!")
		}
	}

	// 尝试随机对等节点发送消息并确保它们被接收
	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		if err := topicHandlers[owner].Publish(ctx, msg, WithReadiness(MinTopicSize(1))); err != nil {
			t.Fatal(err)
		}

		for _, sub := range msgs {
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

// TestGossipSubDiscoveryAfterBootstrap 测试在引导之后的发现机制
func TestGossipSubDiscoveryAfterBootstrap(t *testing.T) {
	t.Skip("flaky test disabled")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置发现服务器和 pubsub 客户端
	partitionSize := GossipSubDlo - 1
	numHosts := partitionSize * 2
	const ttl = 1 * time.Minute

	const topic = "foobar"

	server1, server2 := newDiscoveryServer(), newDiscoveryServer()
	discOpts := []discovery.Option{discovery.Limit(numHosts), discovery.TTL(ttl)}

	// 将 pubsub 客户端分成两个分区
	hosts := getDefaultHosts(t, numHosts)
	psubs := make([]*PubSub, numHosts)
	topicHandlers := make([]*Topic, numHosts)

	for i, h := range hosts {
		s := server1
		if i >= partitionSize {
			s = server2
		}
		disc := &mockDiscoveryClient{h, s}
		ps := getGossipsub(ctx, h, WithDiscovery(disc, WithDiscoveryOpts(discOpts...)))
		psubs[i] = ps
		topicHandlers[i], _ = ps.Join(topic)
	}

	msgs := make([]*Subscription, numHosts)
	for i, th := range topicHandlers {
		subch, err := th.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		msgs[i] = subch
	}

	// 等待网络形成完成，然后通过发现将分区连接起来
	for _, ps := range psubs {
		waitUntilGossipsubMeshCount(ps, topic, partitionSize-1)
	}

	for i := 0; i < partitionSize; i++ {
		if _, err := server1.Advertise("floodsub:"+topic, *host.InfoFromHost(hosts[i+partitionSize]), ttl); err != nil {
			t.Fatal(err)
		}
	}

	// 测试 mesh
	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(numHosts)

		if err := topicHandlers[owner].Publish(ctx, msg, WithReadiness(MinTopicSize(numHosts-1))); err != nil {
			t.Fatal(err)
		}

		for _, sub := range msgs {
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

// waitUntilGossipsubMeshCount 等待直到 gossipsub 的 mesh 达到指定的计数
func waitUntilGossipsubMeshCount(ps *PubSub, topic string, count int) {
	done := false
	doneCh := make(chan bool, 1)
	rt := ps.rt.(*GossipSubRouter)
	for !done {
		ps.eval <- func() {
			doneCh <- len(rt.mesh[topic]) == count
		}
		done = <-doneCh
		if !done {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
