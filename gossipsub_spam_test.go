package pubsub

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dep2p/go-dep2p/core/host"
	"github.com/dep2p/go-dep2p/core/network"
	"github.com/dep2p/go-dep2p/core/peer"
	"github.com/dep2p/go-dep2p/core/protocol"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/go-dep2p/p2plib/msgio/protoio"
)

// 测试当Gossipsub从同一消息ID的对等点接收到过多的IWANT消息时，是否会切断对等点
func TestGossipsubAttackSpamIWANT(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建合法和攻击者主机
	hosts := getDefaultHosts(t, 2)
	legit := hosts[0]    // 合法主机
	attacker := hosts[1] // 攻击者主机

	// 在合法主机上设置gossipsub
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// 在合法主机上订阅主题
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// 用于发布包含随机数据的消息
	publishMsg := func() {
		data := make([]byte, 16)
		rand.Read(data)

		topic, err := ps.Join(mytopic)
		if err != nil {
			t.Fatal(err)
		}

		if err = topic.Publish(ctx, data); err != nil {
			t.Fatal(err)
		}
	}

	// 在最后一条消息之后等待一段时间，然后检查收到的消息数量
	msgWaitMax := time.Second
	msgCount := 0
	msgTimer := time.NewTimer(msgWaitMax)

	// 检查收到的消息数量是否正确
	checkMsgCount := func() {
		// 在合法主机发送的原始消息之后，我们会不断发送IWANT，直到它停止回复。
		// 因此，消息数量为<原始消息> + GossipSubGossipRetransmission
		exp := 1 + GossipSubGossipRetransmission
		if msgCount != exp {
			t.Fatalf("Expected %d messages, got %d", exp, msgCount)
		}
	}

	// 等待计时器到期
	go func() {
		select {
		case <-msgTimer.C:
			checkMsgCount()
			cancel()
			return
		case <-ctx.Done():
			checkMsgCount()
		}
	}()

	// 设置攻击者的模拟Gossipsub行为
	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// 当合法主机连接时，它会向我们发送订阅信息
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// 回复订阅主题并向对等点GRAFT
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				go func() {
					// 等待短时间以确保合法主机收到并处理了订阅和GRAFT
					time.Sleep(100 * time.Millisecond)

					// 从合法主机发布消息
					publishMsg()
				}()
			}
		}

		// 每当合法主机发送消息时
		for _, msg := range irpc.GetPublish() {
			// 增加消息数量并重置计时器
			msgCount++
			msgTimer.Reset(msgWaitMax)

			// 消息数量不应超过预期数量
			exp := 1 + GossipSubGossipRetransmission
			if msgCount > exp {
				cancel()
				t.Fatal("Received too many responses")
			}

			// 发送包含消息ID的IWANT，促使合法主机发送另一条消息（直到它因过多请求而切断攻击者）
			iwantlst := []string{DefaultMsgIdFn(msg)}
			iwant := []*pb.ControlIWant{{MessageIDs: iwantlst}}
			orpc := rpcWithControl(nil, nil, iwant, nil, nil)
			writeMsg(&orpc.RPC)
		}
	})

	// 连接合法和攻击者主机
	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// 测试Gossipsub是否每个心跳周期内仅响应一次IHAVE消息的IWANT请求
func TestGossipsubAttackSpamIHAVE(t *testing.T) {
	// 保存原始的GossipSubIWantFollowupTime并在测试完成后恢复
	originalGossipSubIWantFollowupTime := GossipSubIWantFollowupTime
	GossipSubIWantFollowupTime = 10 * time.Second
	defer func() {
		GossipSubIWantFollowupTime = originalGossipSubIWantFollowupTime
	}()

	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建合法和攻击者主机
	hosts := getDefaultHosts(t, 2)
	legit := hosts[0]    // 合法主机
	attacker := hosts[1] // 攻击者主机

	// 在合法主机上设置gossipsub
	ps, err := NewGossipSub(ctx, legit,
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore:       func(peer.ID) float64 { return 0 },
				BehaviourPenaltyWeight: -1,
				BehaviourPenaltyDecay:  ScoreParameterDecay(time.Minute),
				DecayInterval:          DefaultDecayInterval,
				DecayToZero:            DefaultDecayToZero,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -100,
				PublishThreshold:  -500,
				GraylistThreshold: -1000,
			}))
	if err != nil {
		t.Fatal(err)
	}

	// 在合法主机上订阅主题
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	iWantCount := 0
	iWantCountMx := sync.Mutex{}
	getIWantCount := func() int {
		iWantCountMx.Lock()
		defer iWantCountMx.Unlock()
		return iWantCount
	}
	addIWantCount := func(i int) {
		iWantCountMx.Lock()
		defer iWantCountMx.Unlock()
		iWantCount += i
	}

	// 设置攻击者的模拟Gossipsub行为
	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// 当合法主机连接时，它会向我们发送订阅信息
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// 回复订阅主题并向对等点GRAFT
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				sub := sub
				go func() {
					defer cancel()

					// 等待短时间以确保合法主机收到并处理了订阅和GRAFT
					time.Sleep(20 * time.Millisecond)

					// 发送大量IHAVE消息
					for i := 0; i < 3*GossipSubMaxIHaveLength; i++ {
						ihavelst := []string{"someid" + strconv.Itoa(i)}
						ihave := []*pb.ControlIHave{{TopicID: sub.Topicid, MessageIDs: ihavelst}}
						orpc := rpcWithControl(nil, ihave, nil, nil, nil)
						writeMsg(&orpc.RPC)
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(GossipSubHeartbeatInterval):
					}

					// 应该达到每个对等点每个心跳的最大IWANT数量
					iwc := getIWantCount()
					if iwc > GossipSubMaxIHaveLength {
						t.Errorf("Expecting max %d IWANTs per heartbeat but received %d", GossipSubMaxIHaveLength, iwc)
						return
					}
					firstBatchCount := iwc

					// 分数应仍为0，因为尚未违反任何承诺
					score := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score != 0 {
						t.Errorf("Expected 0 score, but got %f", score)
						return
					}

					// 再次发送大量IHAVE消息
					for i := 0; i < 3*GossipSubMaxIHaveLength; i++ {
						ihavelst := []string{"someid" + strconv.Itoa(i+100)}
						ihave := []*pb.ControlIHave{{TopicID: sub.Topicid, MessageIDs: ihavelst}}
						orpc := rpcWithControl(nil, ihave, nil, nil, nil)
						writeMsg(&orpc.RPC)
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(GossipSubHeartbeatInterval):
					}

					// 在心跳之后应发送更多的IWANT
					iwc = getIWantCount()
					if iwc == firstBatchCount {
						t.Error("Expecting to receive more IWANTs after heartbeat but did not")
						return
					}
					// 不应超过每个心跳的最大数量
					if iwc-firstBatchCount > GossipSubMaxIHaveLength {
						t.Errorf("Expecting max %d IWANTs per heartbeat but received %d", GossipSubMaxIHaveLength, iwc-firstBatchCount)
						return
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(GossipSubIWantFollowupTime):
					}

					// 因为违反承诺，分数应变为负数
					score = ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score >= 0 {
						t.Errorf("Expected negative score, but got %f", score)
						return
					}
				}()
			}
		}

		// 记录收到的IWANT消息数量
		if ctl := irpc.GetControl(); ctl != nil {
			addIWantCount(len(ctl.GetIwant()))
		}
	})

	// 连接合法和攻击者主机
	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// 测试当Gossipsub接收到GRAFT请求，但主题不存在时，是否会忽略请求
func TestGossipsubAttackGRAFTNonExistentTopic(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建合法和攻击者主机
	hosts := getDefaultHosts(t, 2)
	legit := hosts[0]    // 合法主机
	attacker := hosts[1] // 攻击者主机

	// 在合法主机上设置gossipsub
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// 在合法主机上订阅主题
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// 检查是否未收到任何PRUNE消息
	pruneCount := 0
	checkForPrune := func() {
		// 我们发送GRAFT请求的主题不存在，因此不应收到PRUNE响应
		if pruneCount != 0 {
			t.Fatalf("Got %d unexpected PRUNE messages", pruneCount)
		}
	}

	// 设置攻击者的模拟Gossipsub行为
	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// 当合法主机连接时，它会向我们发送订阅信息
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// 回复订阅主题并向对等点GRAFT
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				// 在不存在的主题上向对等点GRAFT
				nonExistentTopic := "non-existent"
				writeMsg(&pb.RPC{
					Control: &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: nonExistentTopic}}},
				})

				go func() {
					// 等待短时间以确保合法主机收到并处理了订阅和GRAFT
					time.Sleep(100 * time.Millisecond)

					// 我们不应收到任何PRUNE消息，因为该主题不存在
					checkForPrune()
					cancel()
				}()
			}
		}

		// 记录收到的PRUNE消息数量
		if ctl := irpc.GetControl(); ctl != nil {
			pruneCount += len(ctl.GetPrune())
		}
	})

	// 连接合法和攻击者主机
	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// 测试当Gossipsub收到一个已经被PRUNED的对等节点的GRAFT请求时，
// 如果GRAFT请求发送得太快，通过P7进行惩罚，并最终将其列入灰名单并忽略请求。

func TestGossipsubAttackGRAFTDuringBackoff(t *testing.T) {
	originalGossipSubPruneBackoff := GossipSubPruneBackoff
	GossipSubPruneBackoff = 200 * time.Millisecond
	originalGossipSubGraftFloodThreshold := GossipSubGraftFloodThreshold
	GossipSubGraftFloodThreshold = 100 * time.Millisecond
	defer func() {
		GossipSubPruneBackoff = originalGossipSubPruneBackoff
		GossipSubGraftFloodThreshold = originalGossipSubGraftFloodThreshold
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建合法和攻击者主机
	hosts := getDefaultHosts(t, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// 在合法主机上设置gossipsub
	ps, err := NewGossipSub(ctx, legit,
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore:       func(peer.ID) float64 { return 0 },
				BehaviourPenaltyWeight: -100,
				BehaviourPenaltyDecay:  ScoreParameterDecay(time.Minute),
				DecayInterval:          DefaultDecayInterval,
				DecayToZero:            DefaultDecayToZero,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -100,
				PublishThreshold:  -500,
				GraylistThreshold: -1000,
			}))
	if err != nil {
		t.Fatal(err)
	}

	// 订阅合法主机上的主题
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	pruneCount := 0
	pruneCountMx := sync.Mutex{}
	getPruneCount := func() int {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		return pruneCount
	}
	addPruneCount := func(i int) {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		pruneCount += i
	}

	// 设置攻击者的模拟Gossipsub行为
	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// 当合法主机连接时，它会向我们发送其订阅信息
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// 回复订阅主题并向对等点GRAFT
				graft := []*pb.ControlGraft{{TopicID: sub.Topicid}}
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: graft},
				})

				sub := sub
				go func() {
					defer cancel()

					// 等待一段短时间以确保合法主机收到并处理了订阅和GRAFT
					time.Sleep(20 * time.Millisecond)

					// 此时不应发送任何PRUNE
					pc := getPruneCount()
					if pc != 0 {
						t.Errorf("期望收到 %d 条 PRUNE 消息，但收到了 %d 条", 0, pc)
						return // 非测试协程中无法调用 t.Fatalf
					}

					// 发送一个PRUNE以将攻击者节点从合法主机的mesh中移除
					var prune []*pb.ControlPrune
					prune = append(prune, &pb.ControlPrune{TopicID: sub.Topicid})
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Prune: prune},
					})

					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}

					// 此时不应发送任何PRUNE
					pc = getPruneCount()
					if pc != 0 {
						t.Errorf("期望收到 %d 条 PRUNE 消息，但收到了 %d 条", 0, pc)
						return // 非测试协程中无法调用 t.Fatalf
					}

					// 等待GossipSubGraftFloodThreshold时间间隔后再尝试再次进行GRAFT
					time.Sleep(GossipSubGraftFloodThreshold + time.Millisecond)

					// 发送一个GRAFT尝试重新加入mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}

					// 我们应该因为在backoff过期之前发送而受到对等方的惩罚，
					// 但由于尚未降到GraylistThreshold以下，仍然会收到PRUNE。
					pc = getPruneCount()
					if pc != 1 {
						t.Errorf("期望收到 %d 条 PRUNE 消息，但收到了 %d 条", 1, pc)
						return // 非测试协程中无法调用 t.Fatalf
					}

					score1 := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score1 >= 0 {
						t.Errorf("期望得分为负数，但得到了 %f", score1)
						return // 非测试协程中无法调用 t.Fatalf
					}

					// 再次发送一个GRAFT尝试重新加入mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}

					// 我们应该因为在洪水阈值之前发送而受到两次惩罚，
					// 但仍然会得到PRUNE，因为我们在洪水阈值之前。
					pc = getPruneCount()
					if pc != 2 {
						t.Errorf("期望收到 %d 条 PRUNE 消息，但收到了 %d 条", 2, pc)
						return // 非测试协程中无法调用 t.Fatalf
					}

					score2 := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score2 >= score1 {
						t.Errorf("期望得分低于 %f，但得到了 %f", score1, score2)
						return // 非测试协程中无法调用 t.Fatalf
					}

					// 再次发送一个GRAFT尝试重新加入mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}

					// 这应该让我们得到一个PRUNE，并惩罚我们低于GraylistThreshold
					pc = getPruneCount()
					if pc != 3 {
						t.Errorf("期望收到 %d 条 PRUNE 消息，但收到了 %d 条", 3, pc)
						return // 非测试协程中无法调用 t.Fatalf
					}

					score3 := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score3 >= score2 {
						t.Errorf("期望得分低于 %f，但得到了 %f", score2, score3)
						return // 非测试协程中无法调用 t.Fatalf
					}
					if score3 >= -1000 {
						t.Errorf("期望得分低于 %f，但得到了 %f", -1000.0, score3)
						return // 非测试协程中无法调用 t.Fatalf
					}

					// 等待PRUNE的backoff过期后再次尝试；这次我们应该失败，
					// 因为我们低于GraylistThreshold，所以我们的RPC应被忽略，
					// 并且我们不应该收到PRUNE
					select {
					case <-ctx.Done():
						return
					case <-time.After(GossipSubPruneBackoff + time.Millisecond):
					}

					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}

					pc = getPruneCount()
					if pc != 3 {
						t.Errorf("期望收到 %d 条 PRUNE 消息，但收到了 %d 条", 3, pc)
						return // 非测试协程中无法调用 t.Fatalf
					}

					// 确保我们不在合法主机的mesh中
					res := make(chan bool)
					ps.eval <- func() {
						mesh := ps.rt.(*GossipSubRouter).mesh[mytopic]
						_, inMesh := mesh[attacker.ID()]
						res <- inMesh
					}

					inMesh := <-res
					if inMesh {
						t.Error("期望不在合法主机的mesh中")
						return // 非测试协程中无法调用 t.Fatal
					}
				}()
			}
		}

		if ctl := irpc.GetControl(); ctl != nil {
			addPruneCount(len(ctl.GetPrune()))
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// 定义一个gsAttackInvalidMsgTracer结构体，用于跟踪无效消息拒绝次数
type gsAttackInvalidMsgTracer struct {
	rejectCount int
}

// Trace方法实现了pb.TraceEvent接口，用于处理TraceEvent事件
func (t *gsAttackInvalidMsgTracer) Trace(evt *pb.TraceEvent) {
	// 如果事件类型是REJECT_MESSAGE，则增加rejectCount计数
	if evt.GetType() == pb.TraceEvent_REJECT_MESSAGE {
		t.rejectCount++
	}
}

// Test that when Gossipsub receives a lot of invalid messages from
// a peer it should graylist the peer
func TestGossipsubAttackInvalidMessageSpam(t *testing.T) {
	// 创建测试上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建合法和攻击者主机
	hosts := getDefaultHosts(t, 2)
	legit := hosts[0]
	attacker := hosts[1]

	mytopic := "mytopic"

	// 创建具有合理默认值的参数
	params := &PeerScoreParams{
		AppSpecificScore:            func(peer.ID) float64 { return 0 },
		IPColocationFactorWeight:    0,
		IPColocationFactorThreshold: 1,
		DecayInterval:               5 * time.Second,
		DecayToZero:                 0.01,
		RetainScore:                 10 * time.Second,
		Topics:                      make(map[string]*TopicScoreParams),
	}
	params.Topics[mytopic] = &TopicScoreParams{
		TopicWeight:                     0.25,
		TimeInMeshWeight:                0.0027,
		TimeInMeshQuantum:               time.Second,
		TimeInMeshCap:                   3600,
		FirstMessageDeliveriesWeight:    0.664,
		FirstMessageDeliveriesDecay:     0.9916,
		FirstMessageDeliveriesCap:       1500,
		MeshMessageDeliveriesWeight:     -0.25,
		MeshMessageDeliveriesDecay:      0.97,
		MeshMessageDeliveriesCap:        400,
		MeshMessageDeliveriesThreshold:  100,
		MeshMessageDeliveriesActivation: 30 * time.Second,
		MeshMessageDeliveriesWindow:     5 * time.Minute,
		MeshFailurePenaltyWeight:        -0.25,
		MeshFailurePenaltyDecay:         0.997,
		InvalidMessageDeliveriesWeight:  -99,
		InvalidMessageDeliveriesDecay:   0.9994,
	}
	thresholds := &PeerScoreThresholds{
		GossipThreshold:   -100,
		PublishThreshold:  -200,
		GraylistThreshold: -300,
		AcceptPXThreshold: 0,
	}

	// 在合法主机上设置Gossipsub
	tracer := &gsAttackInvalidMsgTracer{}
	ps, err := NewGossipSub(ctx, legit,
		WithEventTracer(tracer),
		WithPeerScore(params, thresholds),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 获取攻击者的评分函数
	attackerScore := func() float64 {
		return ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
	}

	// 在合法主机上订阅mytopic
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// 初始化PRUNE计数
	pruneCount := 0
	pruneCountMx := sync.Mutex{}
	getPruneCount := func() int {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		return pruneCount
	}
	addPruneCount := func(i int) {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		pruneCount += i
	}

	// 创建攻击者的模拟Gossipsub
	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// 当合法主机连接时，它将发送其订阅
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// 回复订阅主题并graft到对等方
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				go func() {
					defer cancel()

					// 攻击者分数应该从零开始
					if attackerScore() != 0 {
						t.Errorf("Expected attacker score to be zero but it's %f", attackerScore())
						return // 无法在非测试goroutine中调用t.Fatalf
					}

					// 发送一堆没有签名的消息（这些消息将失败验证并降低攻击者的分数）
					for i := 0; i < 100; i++ {
						msg := &pb.Message{
							Data:  []byte("some data" + strconv.Itoa(i)),
							Topic: mytopic,
							From:  []byte(attacker.ID()),
							Seqno: []byte{byte(i + 1)},
						}
						writeMsg(&pb.RPC{
							Publish: []*pb.Message{msg},
						})
					}

					// 等待初始心跳，加上一点填充时间
					select {
					case <-ctx.Done():
						return
					case <-time.After(100*time.Millisecond + GossipSubHeartbeatInitialDelay):
					}

					// 攻击者的分数现在应该已经降到零以下
					if attackerScore() >= 0 {
						t.Errorf("Expected attacker score to be less than zero but it's %f", attackerScore())
						return // 无法在非测试goroutine中调用t.Fatalf
					}
					// 应该有几个被拒绝的消息（因为签名无效）
					if tracer.rejectCount == 0 {
						t.Error("Expected message rejection but got none")
						return // 无法在非测试goroutine中调用t.Fatal
					}
					// 合法节点应该发送了一个PRUNE消息
					pc := getPruneCount()
					if pc == 0 {
						t.Error("Expected attacker node to be PRUNED when score drops low enough")
						return // 无法在非测试goroutine中调用t.Fatal
					}
				}()
			}
		}

		// 统计PRUNE控制消息的数量
		if ctl := irpc.GetControl(); ctl != nil {
			addPruneCount(len(ctl.GetPrune()))
		}
	})

	// 连接两个主机
	connect(t, hosts[0], hosts[1])

	// 等待测试上下文结束
	<-ctx.Done()
}

type mockGSOnRead func(writeMsg func(*pb.RPC), irpc *pb.RPC)

// newMockGS 创建一个模拟的Gossipsub节点，用于处理来自攻击者节点的消息
func newMockGS(ctx context.Context, t *testing.T, attacker host.Host, onReadMsg mockGSOnRead) {
	// 监听gossipsub协议
	const gossipSubID = protocol.ID("/meshsub/1.0.0")
	const maxMessageSize = 1024 * 1024
	attacker.SetStreamHandler(gossipSubID, func(stream network.Stream) {
		// 当有传入流打开时，设置一个传出流
		p := stream.Conn().RemotePeer()
		ostream, err := attacker.NewStream(ctx, p, gossipSubID)
		if err != nil {
			t.Fatal(err)
		}

		// 创建消息读写器
		r := protoio.NewDelimitedReader(stream, maxMessageSize)
		w := protoio.NewDelimitedWriter(ostream)

		var irpc pb.RPC

		writeMsg := func(rpc *pb.RPC) {
			if err = w.WriteMsg(rpc); err != nil {
				t.Fatalf("error writing RPC: %s", err)
			}
		}

		// 持续读取消息并响应
		for {
			// 当测试结束时退出
			if ctx.Err() != nil {
				return
			}

			irpc.Reset()

			err := r.ReadMsg(&irpc)

			// 当测试结束时退出
			if ctx.Err() != nil {
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			// 调用onReadMsg处理接收到的消息
			onReadMsg(writeMsg, &irpc)
		}
	})
}
