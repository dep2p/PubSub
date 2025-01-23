package pubsub

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/go-dep2p/core/network"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/core/peerstore"

	"github.com/dep2p/go-dep2p/p2plib/msgio/protoio"
)

// testWithTracer 测试使用特定的事件追踪器
func testWithTracer(t *testing.T, tracer EventTracer) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建20个默认主机
	hosts := getDefaultHosts(t, 20)
	psubs := getGossipsubs(ctx, hosts,
		WithEventTracer(tracer), // 设置事件追踪器
		WithPeerExchange(true),  // 启用对等交换以构建星型拓扑
		WithPeerScore( // 设置对等评分参数和阈值
			&PeerScoreParams{
				TopicScoreCap:    100,
				AppSpecificScore: func(peer.ID) float64 { return 0 },
				DecayInterval:    time.Second,
				DecayToZero:      0.01,
			},
			&PeerScoreThresholds{
				GossipThreshold:             -1,
				PublishThreshold:            -2,
				GraylistThreshold:           -3,
				OpportunisticGraftThreshold: 1,
			}))

	// 为每个 pubsub 实例注册主题验证器
	for _, ps := range psubs {
		ps.RegisterTopicValidator("test", func(ctx context.Context, p peer.ID, msg *Message) bool {
			// 如果消息内容为 "invalid!"，则验证失败
			return string(msg.Data) != "invalid!"
		})
	}

	// 将所有 peer 地址添加到 peerstores
	for i := range hosts {
		for j := range hosts {
			if i == j {
				continue
			}
			hosts[i].Peerstore().AddAddrs(hosts[j].ID(), hosts[j].Addrs(), peerstore.PermanentAddrTTL)
		}
	}

	// 构建星型拓扑
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	// 加入主题并订阅
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		go func(sub *Subscription) {
			for {
				_, err := sub.Next(ctx)
				if err != nil {
					return
				}
			}
		}(sub)
		subs = append(subs, sub)
	}

	// 等待网格构建完成
	time.Sleep(5 * time.Second)

	// 发布一些消息
	for i := 0; i < 20; i++ {
		topic, err := psubs[i].Join("test")
		if err != nil {
			t.Fatal(err)
		}
		if i%7 == 0 {
			topic.Publish(ctx, []byte("invalid!"))
		} else {
			msg := []byte(fmt.Sprintf("message %d", i))
			topic.Publish(ctx, msg)
		}
	}

	// 等待消息传播
	time.Sleep(time.Second)

	// 关闭所有订阅以触发离开事件
	for _, sub := range subs {
		sub.Cancel()
	}

	// 等待离开事件生效
	time.Sleep(time.Second)
}

// traceStats 用于统计事件类型的结构
type traceStats struct {
	publish, reject, duplicate, deliver, add, remove, recv, send, drop, join, leave, graft, prune int
}

// process 处理追踪事件
func (t *traceStats) process(evt *pb.TraceEvent) {
	switch evt.GetType() {
	case pb.TraceEvent_PUBLISH_MESSAGE:
		t.publish++
	case pb.TraceEvent_REJECT_MESSAGE:
		t.reject++
	case pb.TraceEvent_DUPLICATE_MESSAGE:
		t.duplicate++
	case pb.TraceEvent_DELIVER_MESSAGE:
		t.deliver++
	case pb.TraceEvent_ADD_PEER:
		t.add++
	case pb.TraceEvent_REMOVE_PEER:
		t.remove++
	case pb.TraceEvent_RECV_RPC:
		t.recv++
	case pb.TraceEvent_SEND_RPC:
		t.send++
	case pb.TraceEvent_DROP_RPC:
		t.drop++
	case pb.TraceEvent_JOIN:
		t.join++
	case pb.TraceEvent_LEAVE:
		t.leave++
	case pb.TraceEvent_GRAFT:
		t.graft++
	case pb.TraceEvent_PRUNE:
		t.prune++
	}
}

// check 验证事件统计
func (ts *traceStats) check(t *testing.T) {
	if ts.publish == 0 {
		t.Fatal("expected non-zero count for publish")
	}
	if ts.duplicate == 0 {
		t.Fatal("expected non-zero count for duplicate")
	}
	if ts.deliver == 0 {
		t.Fatal("expected non-zero count for deliver")
	}
	if ts.reject == 0 {
		t.Fatal("expected non-zero count for reject")
	}
	if ts.add == 0 {
		t.Fatal("expected non-zero count for add")
	}
	if ts.recv == 0 {
		t.Fatal("expected non-zero count for recv")
	}
	if ts.send == 0 {
		t.Fatal("expected non-zero count for send")
	}
	if ts.join == 0 {
		t.Fatal("expected non-zero count for join")
	}
	if ts.leave == 0 {
		t.Fatal("expected non-zero count for leave")
	}
	if ts.graft == 0 {
		t.Fatal("expected non-zero count for graft")
	}
	if ts.prune == 0 {
		t.Fatal("expected non-zero count for prune")
	}
}

// TestJSONTracer 测试 JSON 格式的追踪器
func TestJSONTracer(t *testing.T) {
	tracer, err := NewJSONTracer("/tmp/trace.out.json")
	if err != nil {
		t.Fatal(err)
	}

	// 使用追踪器进行测试
	testWithTracer(t, tracer)
	time.Sleep(time.Second)
	tracer.Close()

	var stats traceStats
	var evt pb.TraceEvent

	// 打开 JSON 追踪文件
	f, err := os.Open("/tmp/trace.out.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// 解码 JSON 文件中的追踪事件
	dec := json.NewDecoder(f)
	for {
		evt.Reset()
		err := dec.Decode(&evt)
		if err != nil {
			break
		}

		// 处理事件
		stats.process(&evt)
	}

	// 检查事件统计
	stats.check(t)
}

// TestPBTracer 测试 Protobuf 格式的追踪器
func TestPBTracer(t *testing.T) {
	tracer, err := NewPBTracer("/tmp/trace.out.pb")
	if err != nil {
		t.Fatal(err)
	}

	// 使用追踪器进行测试
	testWithTracer(t, tracer)
	time.Sleep(time.Second)
	tracer.Close()

	var stats traceStats
	var evt pb.TraceEvent

	// 打开 Protobuf 追踪文件
	f, err := os.Open("/tmp/trace.out.pb")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// 解码 Protobuf 文件中的追踪事件
	r := protoio.NewDelimitedReader(f, 1<<20)
	for {
		evt.Reset()
		err := r.ReadMsg(&evt)
		if err != nil {
			break
		}

		// 处理事件
		stats.process(&evt)
	}

	// 检查事件统计
	stats.check(t)
}

// mockRemoteTracer 模拟远程追踪器
type mockRemoteTracer struct {
	mx sync.Mutex
	ts traceStats
}

// handleStream 处理流事件
func (mrt *mockRemoteTracer) handleStream(s network.Stream) {
	defer s.Close()

	// 使用 gzip 解压流
	gzr, err := gzip.NewReader(s)
	if err != nil {
		panic(err)
	}

	// 解码 Protobuf 格式的追踪事件批次
	r := protoio.NewDelimitedReader(gzr, 1<<24)
	var batch pb.TraceEventBatch
	for {
		batch.Reset()
		err := r.ReadMsg(&batch)
		if err != nil {
			if err != io.EOF {
				s.Reset()
			}
			return
		}

		// 处理每个批次中的事件
		mrt.mx.Lock()
		for _, evt := range batch.GetBatch() {
			mrt.ts.process(evt)
		}
		mrt.mx.Unlock()
	}
}

// check 检查追踪事件统计
func (mrt *mockRemoteTracer) check(t *testing.T) {
	mrt.mx.Lock()
	defer mrt.mx.Unlock()
	mrt.ts.check(t)
}

// TestRemoteTracer 测试远程追踪器
func TestRemoteTracer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建两个默认主机
	hosts := getDefaultHosts(t, 2)
	h1 := hosts[0]
	h2 := hosts[1]

	// 设置模拟远程追踪器
	mrt := &mockRemoteTracer{}
	h1.SetStreamHandler(RemoteTracerProtoID, mrt.handleStream)

	// 创建远程追踪器
	tracer, err := NewRemoteTracer(ctx, h2, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()})
	if err != nil {
		t.Fatal(err)
	}

	// 使用追踪器进行测试
	testWithTracer(t, tracer)
	time.Sleep(time.Second)
	tracer.Close()

	// 检查追踪事件统计
	mrt.check(t)
}
