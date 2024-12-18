package pubsub

import (
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestPeerScoreThreshold_AtomicValidation 测试 PeerScoreThreshold 的原子验证。
func TestPeerScoreThreshold_AtomicValidation(t *testing.T) {
	testPeerScoreThresholdsValidation(t, false)
}

// TestPeerScoreThreshold_SkipAtomicValidation 测试 PeerScoreThreshold 的非原子验证。
func TestPeerScoreThreshold_SkipAtomicValidation(t *testing.T) {
	testPeerScoreThresholdsValidation(t, true)
}

// testPeerScoreThresholdsValidation 根据参数 skipAtomicValidation 测试 PeerScoreThreshold 的验证。
// t: 测试对象
// skipAtomicValidation: 是否跳过原子验证
func testPeerScoreThresholdsValidation(t *testing.T, skipAtomicValidation bool) {
	// 验证 GossipThreshold 参数是否为 1
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:      1,                    // 设置 Gossip 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	// 验证 PublishThreshold 参数是否为 1
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		PublishThreshold:     1,                    // 设置发布阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	// 验证 GossipThreshold 和 PublishThreshold 参数的组合
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:      -1,                   // 设置 Gossip 阈值
		PublishThreshold:     0,                    // 设置发布阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:      -1,                   // 设置 Gossip 阈值
		PublishThreshold:     -2,                   // 设置发布阈值
		GraylistThreshold:    0,                    // 设置灰名单阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		AcceptPXThreshold:    -1,                   // 设置接受 PX 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		OpportunisticGraftThreshold: -1,                   // 设置机会性 graft 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:             -1,                   // 设置 Gossip 阈值
		PublishThreshold:            -2,                   // 设置发布阈值
		GraylistThreshold:           -3,                   // 设置灰名单阈值
		AcceptPXThreshold:           1,                    // 设置接受 PX 阈值
		OpportunisticGraftThreshold: 2,                    // 设置机会性 graft 阈值
	}).validate() != nil {
		t.Fatal("expected validation success")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:             math.Inf(-1),         // 设置 Gossip 阈值为负无穷大
		PublishThreshold:            -2,                   // 设置发布阈值
		GraylistThreshold:           -3,                   // 设置灰名单阈值
		AcceptPXThreshold:           1,                    // 设置接受 PX 阈值
		OpportunisticGraftThreshold: 2,                    // 设置机会性 graft 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:             -1,                   // 设置 Gossip 阈值
		PublishThreshold:            math.Inf(-1),         // 设置发布阈值为负无穷大
		GraylistThreshold:           -3,                   // 设置灰名单阈值
		AcceptPXThreshold:           1,                    // 设置接受 PX 阈值
		OpportunisticGraftThreshold: 2,                    // 设置机会性 graft 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:             -1,                   // 设置 Gossip 阈值
		PublishThreshold:            -2,                   // 设置发布阈值
		GraylistThreshold:           math.Inf(-1),         // 设置灰名单阈值为负无穷大
		AcceptPXThreshold:           1,                    // 设置接受 PX 阈值
		OpportunisticGraftThreshold: 2,                    // 设置机会性 graft 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:             -1,                   // 设置 Gossip 阈值
		PublishThreshold:            -2,                   // 设置发布阈值
		GraylistThreshold:           -3,                   // 设置灰名单阈值
		AcceptPXThreshold:           math.NaN(),           // 设置接受 PX 阈值为 NaN
		OpportunisticGraftThreshold: 2,                    // 设置机会性 graft 阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		GossipThreshold:             -1,                   // 设置 Gossip 阈值
		PublishThreshold:            -2,                   // 设置发布阈值
		GraylistThreshold:           -3,                   // 设置灰名单阈值
		AcceptPXThreshold:           1,                    // 设置接受 PX 阈值
		OpportunisticGraftThreshold: math.Inf(0),          // 设置机会性 graft 阈值为正无穷大
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
}

// TestTopicScoreParamsValidation_InvalidParams_AtomicValidation 测试 TopicScoreParams 的无效参数原子验证。
func TestTopicScoreParamsValidation_InvalidParams_AtomicValidation(t *testing.T) {
	testTopicScoreParamsValidationWithInvalidParameters(t, false)
}

// TestTopicScoreParamsValidation_InvalidParams_SkipAtomicValidation 测试 TopicScoreParams 的无效参数非原子验证。
func TestTopicScoreParamsValidation_InvalidParams_SkipAtomicValidation(t *testing.T) {
	testTopicScoreParamsValidationWithInvalidParameters(t, true)
}

// testTopicScoreParamsValidationWithInvalidParameters 根据参数 skipAtomicValidation 测试 TopicScoreParams 的无效参数验证。
// t: 测试对象
// skipAtomicValidation: 是否跳过原子验证
func testTopicScoreParamsValidationWithInvalidParameters(t *testing.T, skipAtomicValidation bool) {
	if skipAtomicValidation {
		if (&TopicScoreParams{
			SkipAtomicValidation: true}).validate() != nil {
			t.Fatal("expected validation success")
		}
	} else {
		if (&TopicScoreParams{}).validate() == nil {
			t.Fatal("expected validation failure")
		}
	}

	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		TopicWeight:          -1,                   // 设置主题权重
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshWeight:     -1,                   // 设置网内时间权重
		TimeInMeshQuantum:    time.Second,          // 设置网内时间量子
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshWeight:     1,                    // 设置网内时间权重
		TimeInMeshQuantum:    -1,                   // 设置网内时间量子为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshWeight:     1,                    // 设置网内时间权重
		TimeInMeshQuantum:    time.Second,          // 设置网内时间量子
		TimeInMeshCap:        -1,                   // 设置网内时间上限为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:            time.Second,          // 设置网内时间量子
		FirstMessageDeliveriesWeight: -1,                   // 设置首次消息传递权重为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:            time.Second,          // 设置网内时间量子
		FirstMessageDeliveriesWeight: 1,                    // 设置首次消息传递权重
		FirstMessageDeliveriesDecay:  -1,                   // 设置首次消息传递衰减为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:            time.Second,          // 设置网内时间量子
		FirstMessageDeliveriesWeight: 1,                    // 设置首次消息传递权重
		FirstMessageDeliveriesDecay:  2,                    // 设置首次消息传递衰减大于 1
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:            time.Second,          // 设置网内时间量子
		FirstMessageDeliveriesWeight: 1,                    // 设置首次消息传递权重
		FirstMessageDeliveriesDecay:  .5,                   // 设置首次消息传递衰减
		FirstMessageDeliveriesCap:    -1,                   // 设置首次消息传递上限为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:           time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight: 1,                    // 设置网内消息传递权重
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:           time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight: -1,                   // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:  -1,                   // 设置网内消息传递衰减为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:           time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight: -1,                   // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:  2,                    // 设置网内消息传递衰减大于 1
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:           time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight: -1,                   // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:  .5,                   // 设置网内消息传递衰减
		MeshMessageDeliveriesCap:    -1,                   // 设置网内消息传递上限为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:              time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight:    -1,                   // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:     .5,                   // 设置网内消息传递衰减
		MeshMessageDeliveriesCap:       5,                    // 设置网内消息传递上限
		MeshMessageDeliveriesThreshold: -3,                   // 设置网内消息传递阈值为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:              time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight:    -1,                   // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:     .5,                   // 设置网内消息传递衰减
		MeshMessageDeliveriesCap:       5,                    // 设置网内消息传递上限
		MeshMessageDeliveriesThreshold: 3,                    // 设置网内消息传递阈值
		MeshMessageDeliveriesWindow:    -1,                   // 设置网内消息传递窗口为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:            skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:               time.Second,          // 设置网内时间量子
		MeshMessageDeliveriesWeight:     -1,                   // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:      .5,                   // 设置网内消息传递衰减
		MeshMessageDeliveriesCap:        5,                    // 设置网内消息传递上限
		MeshMessageDeliveriesThreshold:  3,                    // 设置网内消息传递阈值
		MeshMessageDeliveriesWindow:     time.Millisecond,     // 设置网内消息传递窗口
		MeshMessageDeliveriesActivation: time.Millisecond}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:     skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:        time.Second,          // 设置网内时间量子
		MeshFailurePenaltyWeight: 1,                    // 设置网内消息传递失败惩罚权重
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:     skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:        time.Second,          // 设置网内时间量子
		MeshFailurePenaltyWeight: -1,                   // 设置网内消息传递失败惩罚权重为负数
		MeshFailurePenaltyDecay:  -1,                   // 设置网内消息传递失败惩罚衰减为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:     skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:        time.Second,          // 设置网内时间量子
		MeshFailurePenaltyWeight: -1,                   // 设置网内消息传递失败惩罚权重为负数
		MeshFailurePenaltyDecay:  2,                    // 设置网内消息传递失败惩罚衰减大于 1
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:              time.Second,          // 设置网内时间量子
		InvalidMessageDeliveriesWeight: 1,                    // 设置无效消息传递权重
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:              time.Second,          // 设置网内时间量子
		InvalidMessageDeliveriesWeight: -1,                   // 设置无效消息传递权重为负数
		InvalidMessageDeliveriesDecay:  -1,                   // 设置无效消息传递衰减为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation, // 是否跳过原子验证
		TimeInMeshQuantum:              time.Second,          // 设置网内时间量子
		InvalidMessageDeliveriesWeight: -1,                   // 设置无效消息传递权重为负数
		InvalidMessageDeliveriesDecay:  2,                    // 设置无效消息传递衰减大于 1
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
}

// TestTopicScoreParamsValidation_ValidParams_AtomicValidation 测试有效参数的 TopicScoreParams 原子验证。
// 不要在生产中使用这些参数！
func TestTopicScoreParamsValidation_ValidParams_AtomicValidation(t *testing.T) {
	// 使用无效的 TopicScoreParams 进行测试，不要在生产中使用这些参数！
	if (&TopicScoreParams{
		SkipAtomicValidation:            false,            // 是否跳过原子验证
		TopicWeight:                     1,                // 设置主题权重
		TimeInMeshWeight:                0.01,             // 设置网内时间权重
		TimeInMeshQuantum:               time.Second,      // 设置网内时间量子
		TimeInMeshCap:                   10,               // 设置网内时间上限
		FirstMessageDeliveriesWeight:    1,                // 设置首次消息传递权重
		FirstMessageDeliveriesDecay:     0.5,              // 设置首次消息传递衰减
		FirstMessageDeliveriesCap:       10,               // 设置首次消息传递上限
		MeshMessageDeliveriesWeight:     -1,               // 设置网内消息传递权重为负数
		MeshMessageDeliveriesDecay:      0.5,              // 设置网内消息传递衰减
		MeshMessageDeliveriesCap:        10,               // 设置网内消息传递上限
		MeshMessageDeliveriesThreshold:  5,                // 设置网内消息传递阈值
		MeshMessageDeliveriesWindow:     time.Millisecond, // 设置网内消息传递窗口
		MeshMessageDeliveriesActivation: time.Second,      // 设置网内消息传递激活时间
		MeshFailurePenaltyWeight:        -1,               // 设置网内消息传递失败惩罚权重
		MeshFailurePenaltyDecay:         0.5,              // 设置网内消息传递失败惩罚衰减
		InvalidMessageDeliveriesWeight:  -1,               // 设置无效消息传递权重为负数
		InvalidMessageDeliveriesDecay:   0.5,              // 设置无效消息传递衰减
	}).validate() != nil {
		t.Fatal("expected validation success")
	}
}

// TestTopicScoreParamsValidation_NonAtomicValidation 测试 TopicScoreParams 的非原子验证。
// 不要在生产中使用这些参数！
func TestTopicScoreParamsValidation_NonAtomicValidation(t *testing.T) {
	// 不要在生产中使用这些参数！
	// 在非原子（选择性）验证模式下，如果单个参数值通过验证，则参数子集通过验证。
	p := &TopicScoreParams{}
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.SkipAtomicValidation = true // 设置为跳过原子验证
	})
	// 包括主题权重
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.TopicWeight = 1 // 设置主题权重
	})
	// 包括网内时间参数
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.TimeInMeshWeight = 0.01         // 设置网内时间权重
		params.TimeInMeshQuantum = time.Second // 设置网内时间量子
		params.TimeInMeshCap = 10              // 设置网内时间上限
	})
	// 包括首次消息传递参数
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.FirstMessageDeliveriesWeight = 1  // 设置首次消息传递权重
		params.FirstMessageDeliveriesDecay = 0.5 // 设置首次消息传递衰减
		params.FirstMessageDeliveriesCap = 10    // 设置首次消息传递上限
	})
	// 包括网内消息传递参数
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.MeshMessageDeliveriesWeight = -1               // 设置网内消息传递权重为负数
		params.MeshMessageDeliveriesDecay = 0.5               // 设置网内消息传递衰减
		params.MeshMessageDeliveriesCap = 10                  // 设置网内消息传递上限
		params.MeshMessageDeliveriesThreshold = 5             // 设置网内消息传递阈值
		params.MeshMessageDeliveriesWindow = time.Millisecond // 设置网内消息传递窗口
		params.MeshMessageDeliveriesActivation = time.Second  // 设置网内消息传递激活时间
	})
	// 包括网内消息传递失败惩罚参数
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.MeshFailurePenaltyWeight = -1 // 设置网内消息传递失败惩罚权重
		params.MeshFailurePenaltyDecay = 0.5 // 设置网内消息传递失败惩罚衰减
	})
	// 包括无效消息传递参数
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.InvalidMessageDeliveriesWeight = -1 // 设置无效消息传递权重为负数
		params.InvalidMessageDeliveriesDecay = 0.5 // 设置无效消息传递衰减
	})
}

// TestPeerScoreParamsValidation_InvalidParams_AtomicValidation 测试 PeerScoreParams 的无效参数原子验证。
func TestPeerScoreParamsValidation_InvalidParams_AtomicValidation(t *testing.T) {
	testPeerScoreParamsValidationWithInvalidParams(t, false)
}

// TestPeerScoreParamsValidation_InvalidParams_SkipAtomicValidation 测试 PeerScoreParams 的无效参数非原子验证。
func TestPeerScoreParamsValidation_InvalidParams_SkipAtomicValidation(t *testing.T) {
	testPeerScoreParamsValidationWithInvalidParams(t, true)
}

// testPeerScoreParamsValidationWithInvalidParams 根据参数 skipAtomicValidation 测试 PeerScoreParams 的无效参数验证。
// t: 测试对象
// skipAtomicValidation: 是否跳过原子验证
func testPeerScoreParamsValidationWithInvalidParams(t *testing.T, skipAtomicValidation bool) {
	appScore := func(peer.ID) float64 { return 0 } // 定义应用特定评分函数

	if (&PeerScoreParams{
		SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:        -1,                   // 设置主题评分上限为负数
		AppSpecificScore:     appScore,             // 设置应用特定评分函数
		DecayInterval:        time.Second,          // 设置衰减间隔
		DecayToZero:          0.01,                 // 设置衰减到零的比例
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if skipAtomicValidation {
		if (&PeerScoreParams{
			SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
			TopicScoreCap:        1,                    // 设置主题评分上限
			DecayInterval:        time.Second,          // 设置衰减间隔
			DecayToZero:          0.01,                 // 设置衰减到零的比例
		}).validate() != nil {
			t.Fatal("expected validation success")
		}
	} else {
		if (&PeerScoreParams{
			SkipAtomicValidation: skipAtomicValidation, // 是否跳过原子验证
			TopicScoreCap:        1,                    // 设置主题评分上限
			DecayInterval:        time.Second,          // 设置衰减间隔
			DecayToZero:          0.01,                 // 设置衰减到零的比例
		}).validate() == nil {
			t.Fatal("expected validation error")
		}
	}

	if (&PeerScoreParams{
		SkipAtomicValidation:     skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:            1,                    // 设置主题评分上限
		AppSpecificScore:         appScore,             // 设置应用特定评分函数
		DecayInterval:            time.Second,          // 设置衰减间隔
		DecayToZero:              0.01,                 // 设置衰减到零的比例
		IPColocationFactorWeight: 1,                    // 设置 IP 共置因子权重
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:               1,                    // 设置主题评分上限
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Second,          // 设置衰减间隔
		DecayToZero:                 0.01,                 // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,                   // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:               1,                    // 设置主题评分上限
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Millisecond,     // 设置衰减间隔
		DecayToZero:                 0.01,                 // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,                   // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,                    // 设置 IP 共置因子阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:               1,                    // 设置主题评分上限
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Second,          // 设置衰减间隔
		DecayToZero:                 -1,                   // 设置衰减到零的比例为负数
		IPColocationFactorWeight:    -1,                   // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,                    // 设置 IP 共置因子阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:               1,                    // 设置主题评分上限
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Second,          // 设置衰减间隔
		DecayToZero:                 2,                    // 设置衰减到零的比例大于 1
		IPColocationFactorWeight:    -1,                   // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,                    // 设置 IP 共置因子阈值
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:   skipAtomicValidation, // 是否跳过原子验证
		AppSpecificScore:       appScore,             // 设置应用特定评分函数
		DecayInterval:          time.Second,          // 设置衰减间隔
		DecayToZero:            0.01,                 // 设置衰减到零的比例
		BehaviourPenaltyWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:   skipAtomicValidation, // 是否跳过原子验证
		AppSpecificScore:       appScore,             // 设置应用特定评分函数
		DecayInterval:          time.Second,          // 设置衰减间隔
		DecayToZero:            0.01,                 // 设置衰减到零的比例
		BehaviourPenaltyWeight: -1,                   // 设置行为惩罚权重为负数
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:   skipAtomicValidation, // 是否跳过原子验证
		AppSpecificScore:       appScore,             // 设置应用特定评分函数
		DecayInterval:          time.Second,          // 设置衰减间隔
		DecayToZero:            0.01,                 // 设置衰减到零的比例
		BehaviourPenaltyWeight: -1,                   // 设置行为惩罚权重为负数
		BehaviourPenaltyDecay:  2,                    // 设置行为惩罚衰减大于 1
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	// 检查无效值（例如无穷大和 NaN）的主题参数。
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:               1,                    // 设置主题评分上限
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Second,          // 设置衰减间隔
		DecayToZero:                 0.01,                 // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,                   // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,                    // 设置 IP 共置因子阈值
		Topics: map[string]*TopicScoreParams{ // 设置主题评分参数
			"test": {
				TopicWeight:                     math.Inf(0),      // 设置主题权重为正无穷大
				TimeInMeshWeight:                math.NaN(),       // 设置网内时间权重为 NaN
				TimeInMeshQuantum:               time.Second,      // 设置网内时间量子
				TimeInMeshCap:                   10,               // 设置网内时间上限
				FirstMessageDeliveriesWeight:    math.Inf(1),      // 设置首次消息传递权重为正无穷大
				FirstMessageDeliveriesDecay:     0.5,              // 设置首次消息传递衰减
				FirstMessageDeliveriesCap:       10,               // 设置首次消息传递上限
				MeshMessageDeliveriesWeight:     math.Inf(-1),     // 设置网内消息传递权重为负无穷大
				MeshMessageDeliveriesDecay:      math.NaN(),       // 设置网内消息传递衰减为 NaN
				MeshMessageDeliveriesCap:        math.Inf(0),      // 设置网内消息传递上限为正无穷大
				MeshMessageDeliveriesThreshold:  5,                // 设置网内消息传递阈值
				MeshMessageDeliveriesWindow:     time.Millisecond, // 设置网内消息传递窗口
				MeshMessageDeliveriesActivation: time.Second,      // 设置网内消息传递激活时间
				MeshFailurePenaltyWeight:        -1,               // 设置网内消息传递失败惩罚权重
				MeshFailurePenaltyDecay:         math.NaN(),       // 设置网内消息传递失败惩罚衰减为 NaN
				InvalidMessageDeliveriesWeight:  math.Inf(0),      // 设置无效消息传递权重为正无穷大
				InvalidMessageDeliveriesDecay:   math.NaN(),       // 设置无效消息传递衰减为 NaN
			},
		},
	}).validate() == nil {
		t.Fatal("expected validation failure")
	}

	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Second,          // 设置衰减间隔
		DecayToZero:                 math.Inf(0),          // 设置衰减到零的比例为正无穷大
		IPColocationFactorWeight:    math.Inf(-1),         // 设置 IP 共置因子权重为负无穷大
		IPColocationFactorThreshold: 1,                    // 设置 IP 共置因子阈值
		BehaviourPenaltyWeight:      math.Inf(0),          // 设置行为惩罚权重为正无穷大
		BehaviourPenaltyDecay:       math.NaN(),           // 设置行为惩罚衰减为 NaN
	}).validate() == nil {
		t.Fatal("expected validation failure")
	}

	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation, // 是否跳过原子验证
		TopicScoreCap:               1,                    // 设置主题评分上限
		AppSpecificScore:            appScore,             // 设置应用特定评分函数
		DecayInterval:               time.Second,          // 设置衰减间隔
		DecayToZero:                 0.01,                 // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,                   // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,                    // 设置 IP 共置因子阈值
		Topics: map[string]*TopicScoreParams{ // 设置主题评分参数
			"test": {
				TopicWeight:                     -1,               // 设置主题权重为负数
				TimeInMeshWeight:                0.01,             // 设置网内时间权重
				TimeInMeshQuantum:               time.Second,      // 设置网内时间量子
				TimeInMeshCap:                   10,               // 设置网内时间上限
				FirstMessageDeliveriesWeight:    1,                // 设置首次消息传递权重
				FirstMessageDeliveriesDecay:     0.5,              // 设置首次消息传递衰减
				FirstMessageDeliveriesCap:       10,               // 设置首次消息传递上限
				MeshMessageDeliveriesWeight:     -1,               // 设置网内消息传递权重为负数
				MeshMessageDeliveriesDecay:      0.5,              // 设置网内消息传递衰减
				MeshMessageDeliveriesCap:        10,               // 设置网内消息传递上限
				MeshMessageDeliveriesThreshold:  5,                // 设置网内消息传递阈值
				MeshMessageDeliveriesWindow:     time.Millisecond, // 设置网内消息传递窗口
				MeshMessageDeliveriesActivation: time.Second,      // 设置网内消息传递激活时间
				MeshFailurePenaltyWeight:        -1,               // 设置网内消息传递失败惩罚权重
				MeshFailurePenaltyDecay:         0.5,              // 设置网内消息传递失败惩罚衰减
				InvalidMessageDeliveriesWeight:  -1,               // 设置无效消息传递权重为负数
				InvalidMessageDeliveriesDecay:   0.5,              // 设置无效消息传递衰减
			},
		},
	}).validate() == nil {
		t.Fatal("expected validation failure")
	}
}

// TestPeerScoreParamsValidation_ValidParams_AtomicValidation 测试 PeerScoreParams 的有效参数原子验证。
// 不要在生产中使用这些参数！
func TestPeerScoreParamsValidation_ValidParams_AtomicValidation(t *testing.T) {
	appScore := func(peer.ID) float64 { return 0 } // 定义应用特定评分函数

	// 不要在生产中使用这些参数！
	if (&PeerScoreParams{
		AppSpecificScore:            appScore,    // 设置应用特定评分函数
		DecayInterval:               time.Second, // 设置衰减间隔
		DecayToZero:                 0.01,        // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,          // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,           // 设置 IP 共置因子阈值
		BehaviourPenaltyWeight:      -1,          // 设置行为惩罚权重为负数
		BehaviourPenaltyDecay:       0.999,       // 设置行为惩罚衰减
	}).validate() != nil {
		t.Fatal("expected validation success")
	}

	if (&PeerScoreParams{
		TopicScoreCap:               1,           // 设置主题评分上限
		AppSpecificScore:            appScore,    // 设置应用特定评分函数
		DecayInterval:               time.Second, // 设置衰减间隔
		DecayToZero:                 0.01,        // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,          // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,           // 设置 IP 共置因子阈值
		BehaviourPenaltyWeight:      -1,          // 设置行为惩罚权重为负数
		BehaviourPenaltyDecay:       0.999,       // 设置行为惩罚衰减
	}).validate() != nil {
		t.Fatal("expected validation success")
	}

	if (&PeerScoreParams{
		TopicScoreCap:               1,           // 设置主题评分上限
		AppSpecificScore:            appScore,    // 设置应用特定评分函数
		DecayInterval:               time.Second, // 设置衰减间隔
		DecayToZero:                 0.01,        // 设置衰减到零的比例
		IPColocationFactorWeight:    -1,          // 设置 IP 共置因子权重为负数
		IPColocationFactorThreshold: 1,           // 设置 IP 共置因子阈值
		Topics: map[string]*TopicScoreParams{ // 设置主题评分参数
			"test": {
				TopicWeight:                     1,                // 设置主题权重
				TimeInMeshWeight:                0.01,             // 设置网内时间权重
				TimeInMeshQuantum:               time.Second,      // 设置网内时间量子
				TimeInMeshCap:                   10,               // 设置网内时间上限
				FirstMessageDeliveriesWeight:    1,                // 设置首次消息传递权重
				FirstMessageDeliveriesDecay:     0.5,              // 设置首次消息传递衰减
				FirstMessageDeliveriesCap:       10,               // 设置首次消息传递上限
				MeshMessageDeliveriesWeight:     -1,               // 设置网内消息传递权重为负数
				MeshMessageDeliveriesDecay:      0.5,              // 设置网内消息传递衰减
				MeshMessageDeliveriesCap:        10,               // 设置网内消息传递上限
				MeshMessageDeliveriesThreshold:  5,                // 设置网内消息传递阈值
				MeshMessageDeliveriesWindow:     time.Millisecond, // 设置网内消息传递窗口
				MeshMessageDeliveriesActivation: time.Second,      // 设置网内消息传递激活时间
				MeshFailurePenaltyWeight:        -1,               // 设置网内消息传递失败惩罚权重
				MeshFailurePenaltyDecay:         0.5,              // 设置网内消息传递失败惩罚衰减
				InvalidMessageDeliveriesWeight:  -1,               // 设置无效消息传递权重为负数
				InvalidMessageDeliveriesDecay:   0.5,              // 设置无效消息传递衰减
			},
		},
	}).validate() != nil {
		t.Fatal("expected validation success")
	}
}

// TestPeerScoreParamsValidation_ValidParams_SkipAtomicValidation 测试 PeerScoreParams 的有效参数非原子验证。
// 不要在生产中使用这些参数！
func TestPeerScoreParamsValidation_ValidParams_SkipAtomicValidation(t *testing.T) {
	appScore := func(peer.ID) float64 { return 0 } // 定义应用特定评分函数

	// 不要在生产中使用这些参数！
	p := &PeerScoreParams{}
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.SkipAtomicValidation = true // 设置为跳过原子验证
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.AppSpecificScore = appScore // 设置应用特定评分函数
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.DecayInterval = time.Second // 设置衰减间隔
		params.DecayToZero = 0.01          // 设置衰减到零的比例
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.IPColocationFactorWeight = -1   // 设置 IP 共置因子权重为负数
		params.IPColocationFactorThreshold = 1 // 设置 IP 共置因子阈值
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.BehaviourPenaltyWeight = -1   // 设置行为惩罚权重为负数
		params.BehaviourPenaltyDecay = 0.999 // 设置行为惩罚衰减
	})

	p = &PeerScoreParams{SkipAtomicValidation: true, AppSpecificScore: appScore}
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.TopicScoreCap = 1 // 设置主题评分上限
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.DecayInterval = time.Second // 设置衰减间隔
		params.DecayToZero = 0.01          // 设置衰减到零的比例
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.IPColocationFactorWeight = -1   // 设置 IP 共置因子权重为负数
		params.IPColocationFactorThreshold = 1 // 设置 IP 共置因子阈值
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.BehaviourPenaltyWeight = -1   // 设置行为惩罚权重为负数
		params.BehaviourPenaltyDecay = 0.999 // 设置行为惩罚衰减
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.Topics = map[string]*TopicScoreParams{ // 设置主题评分参数
			"test": {
				TopicWeight:                     1,                // 设置主题权重
				TimeInMeshWeight:                0.01,             // 设置网内时间权重
				TimeInMeshQuantum:               time.Second,      // 设置网内时间量子
				TimeInMeshCap:                   10,               // 设置网内时间上限
				FirstMessageDeliveriesWeight:    1,                // 设置首次消息传递权重
				FirstMessageDeliveriesDecay:     0.5,              // 设置首次消息传递衰减
				FirstMessageDeliveriesCap:       10,               // 设置首次消息传递上限
				MeshMessageDeliveriesWeight:     -1,               // 设置网内消息传递权重为负数
				MeshMessageDeliveriesDecay:      0.5,              // 设置网内消息传递衰减
				MeshMessageDeliveriesCap:        10,               // 设置网内消息传递上限
				MeshMessageDeliveriesThreshold:  5,                // 设置网内消息传递阈值
				MeshMessageDeliveriesWindow:     time.Millisecond, // 设置网内消息传递窗口
				MeshMessageDeliveriesActivation: time.Second,      // 设置网内消息传递激活时间
				MeshFailurePenaltyWeight:        -1,               // 设置网内消息传递失败惩罚权重
				MeshFailurePenaltyDecay:         0.5,              // 设置网内消息传递失败惩罚衰减
				InvalidMessageDeliveriesWeight:  -1,               // 设置无效消息传递权重为负数
				InvalidMessageDeliveriesDecay:   0.5,              // 设置无效消息传递衰减
			},
		}
	})
}

// TestScoreParameterDecay 测试 ScoreParameterDecay 函数。
func TestScoreParameterDecay(t *testing.T) {
	decay1hr := ScoreParameterDecay(time.Hour) // 计算一小时的衰减
	if decay1hr != .9987216039048303 {         // 检查计算结果是否正确
		t.Fatalf("expected .9987216039048303, got %f", decay1hr)
	}
}

// setParamAndValidate 设置 PeerScoreParams 参数并进行验证。
// t: 测试对象
// params: PeerScoreParams 对象
// set: 设置函数
func setParamAndValidate(t *testing.T, params *PeerScoreParams, set func(*PeerScoreParams)) {
	set(params)                               // 设置参数
	if err := params.validate(); err != nil { // 验证参数
		t.Fatalf("expected validation success, got: %s", err)
	}
}

// setTopicParamAndValidate 设置 TopicScoreParams 参数并进行验证。
// t: 测试对象
// params: TopicScoreParams 对象
// set: 设置函数
func setTopicParamAndValidate(t *testing.T, params *TopicScoreParams, set func(topic *TopicScoreParams)) {
	set(params)                               // 设置参数
	if err := params.validate(); err != nil { // 验证参数
		t.Fatalf("expected validation success, got: %s", err)
	}
}
