package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/dep2p/go-dep2p/core/host"
	"github.com/dep2p/go-dep2p/core/network"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/multiformats/varint"
	pool "github.com/dep2p/go-dep2p/p2plib/buffer/pool"
	"github.com/dep2p/go-dep2p/p2plib/msgio"

	pb "github.com/dep2p/pubsub/pb"
)

// 初始化随机数生成器
var rng *rand.Rand

func init() {
	// 使用固定种子创建随机数生成器
	rng = rand.New(rand.NewSource(314159))
}

// TestBasicSeqnoValidator1 测试基本序列号验证器，TTL为1分钟
func TestBasicSeqnoValidator1(t *testing.T) {
	testBasicSeqnoValidator(t, time.Minute)
}

// TestBasicSeqnoValidator2 测试基本序列号验证器，TTL为1纳秒
func TestBasicSeqnoValidator2(t *testing.T) {
	testBasicSeqnoValidator(t, time.Nanosecond)
}

// testBasicSeqnoValidator 测试基本序列号验证器
func testBasicSeqnoValidator(t *testing.T, ttl time.Duration) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建20个默认主机
	hosts := getDefaultHosts(t, 20)
	// 使用选项创建pubsub实例
	psubs := getPubsubsWithOptionC(ctx, hosts,
		// 设置默认验证器
		func(i int) Option {
			return WithDefaultValidator(NewBasicSeqnoValidator(newMockPeerMetadataStore()))
		},
		// 设置消息TTL
		func(i int) Option {
			return WithSeenMessagesTTL(ttl)
		},
	)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}
		msgs = append(msgs, subch)
	}

	// 稀疏连接主机
	sparseConnect(t, hosts)

	// 等待连接稳定
	time.Sleep(time.Millisecond * 100)

	// 发布和验证消息
	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rng.Intn(len(psubs))

		topic, err := psubs[owner].Join("foobar")
		if err != nil {
			t.Fatal(err)
		}

		topic.Publish(ctx, msg)

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

// TestBasicSeqnoValidatorReplay 测试基本序列号验证器的重放功能
func TestBasicSeqnoValidatorReplay(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建20个默认主机
	hosts := getDefaultHosts(t, 20)
	// 使用选项创建pubsub实例
	psubs := getPubsubsWithOptionC(ctx, hosts[:19],
		func(i int) Option {
			return WithDefaultValidator(NewBasicSeqnoValidator(newMockPeerMetadataStore()))
		},
		func(i int) Option {
			return WithSeenMessagesTTL(time.Nanosecond)
		},
	)
	// 创建重放Actor
	_ = newReplayActor(t, ctx, hosts[19])

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}
		msgs = append(msgs, subch)
	}

	// 稀疏连接主机
	sparseConnect(t, hosts)

	// 等待连接稳定
	time.Sleep(time.Millisecond * 100)

	// 发布和验证消息
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rng.Intn(len(psubs))

		topic, err := psubs[owner].Join("foobar")
		if err != nil {
			t.Fatal(err)
		}

		topic.Publish(ctx, msg)

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

	for _, sub := range msgs {
		assertNeverReceives(t, sub, time.Second)
	}
}

// mockPeerMetadataStore 模拟对等元数据存储
type mockPeerMetadataStore struct {
	meta map[peer.ID][]byte
}

// newMockPeerMetadataStore 创建新的模拟对等元数据存储
func newMockPeerMetadataStore() *mockPeerMetadataStore {
	return &mockPeerMetadataStore{
		meta: make(map[peer.ID][]byte),
	}
}

// Get 获取对等元数据
func (m *mockPeerMetadataStore) Get(ctx context.Context, p peer.ID) ([]byte, error) {
	v, ok := m.meta[p]
	if !ok {
		return nil, nil
	}
	return v, nil
}

// Put 设置对等元数据
func (m *mockPeerMetadataStore) Put(ctx context.Context, p peer.ID, v []byte) error {
	m.meta[p] = v
	return nil
}

// replayActor 重放Actor结构
type replayActor struct {
	t   *testing.T
	ctx context.Context
	h   host.Host
	mx  sync.Mutex
	out map[peer.ID]network.Stream
}

// newReplayActor 创建新的重放Actor
func newReplayActor(t *testing.T, ctx context.Context, h host.Host) *replayActor {
	replay := &replayActor{t: t, ctx: ctx, h: h, out: make(map[peer.ID]network.Stream)}
	h.SetStreamHandler(FloodSubID, replay.handleStream)
	h.Network().Notify(&network.NotifyBundle{ConnectedF: replay.connected})
	return replay
}

// handleStream 处理流事件
func (r *replayActor) handleStream(s network.Stream) {
	defer s.Close()
	p := s.Conn().RemotePeer()
	rd := msgio.NewVarintReaderSize(s, 65536)
	for {
		msgbytes, err := rd.ReadMsg()
		if err != nil {
			s.Reset()
			rd.ReleaseMsg(msgbytes)
			return
		}
		rpc := new(pb.RPC)
		err = rpc.Unmarshal(msgbytes)
		rd.ReleaseMsg(msgbytes)
		if err != nil {
			s.Reset()
			return
		}
		// 订阅与对等方相同的主题
		subs := rpc.GetSubscriptions()
		if len(subs) != 0 {
			go r.send(p, &pb.RPC{Subscriptions: subs})
		}
		// 重放接收到的所有消息
		for _, pmsg := range rpc.GetPublish() {
			go r.replay(pmsg)
		}
	}
}

// send 发送消息
func (r *replayActor) send(p peer.ID, rpc *pb.RPC) {
	r.mx.Lock()
	defer r.mx.Unlock()
	s, ok := r.out[p]
	if !ok {
		r.t.Logf("cannot send message to %s: no stream", p)
		return
	}
	size := uint64(rpc.Size())
	buf := pool.Get(varint.UvarintSize(size) + int(size))
	defer pool.Put(buf)
	n := binary.PutUvarint(buf, size)
	_, err := rpc.MarshalTo(buf[n:])
	if err != nil {
		r.t.Logf("replay: error marshalling message: %s", err)
		return
	}
	_, err = s.Write(buf)
	if err != nil {
		r.t.Logf("replay: error sending message: %s", err)
	}
}

// replay 重放消息
func (r *replayActor) replay(msg *pb.Message) {
	for i := 0; i < 10; i++ {
		delay := time.Duration(1+rng.Intn(20)) * time.Millisecond
		time.Sleep(delay)
		var peers []peer.ID
		r.mx.Lock()
		for p := range r.out {
			if rng.Intn(2) > 0 {
				peers = append(peers, p)
			}
		}
		r.mx.Unlock()
		rpc := &pb.RPC{Publish: []*pb.Message{msg}}
		r.t.Logf("replaying msg to %d peers", len(peers))
		for _, p := range peers {
			r.send(p, rpc)
		}
	}
}

// handleConnected 处理连接事件
func (r *replayActor) handleConnected(p peer.ID) {
	s, err := r.h.NewStream(r.ctx, p, FloodSubID)
	if err != nil {
		r.t.Logf("replay: error opening stream: %s", err)
		return
	}
	r.mx.Lock()
	defer r.mx.Unlock()
	r.out[p] = s
}

// connected 连接通知
func (r *replayActor) connected(_ network.Network, conn network.Conn) {
	go r.handleConnected(conn.RemotePeer())
}
