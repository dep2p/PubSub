// 作用：定义评分参数。
// 功能：定义对等节点评分的参数和配置，用于评估对等节点的行为和信誉。

package pubsub

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerScoreThresholds 包含用于控制对等节点分数的参数
type PeerScoreThresholds struct {
	SkipAtomicValidation        bool    // 是否允许仅设置某些参数而不是所有参数
	GossipThreshold             float64 // 低于该分数时抑制 Gossip 传播，应为负数
	PublishThreshold            float64 // 低于该分数时不应发布消息，应为负数且 <= GossipThreshold
	GraylistThreshold           float64 // 低于该分数时完全抑制消息处理，应为负数且 <= PublishThreshold
	AcceptPXThreshold           float64 // 低于该分数时将忽略 PX，应为正数，限于启动器和其他可信节点
	OpportunisticGraftThreshold float64 // 低于该分数时触发机会性 grafting，应为正数且值小
}

// validate 验证 PeerScoreThresholds 参数
// 返回值:
//   - error: 错误信息
func (p *PeerScoreThresholds) validate() error {
	// 如果没有跳过原子验证，或者 PublishThreshold、GossipThreshold、GraylistThreshold 不为 0
	if !p.SkipAtomicValidation || p.PublishThreshold != 0 || p.GossipThreshold != 0 || p.GraylistThreshold != 0 {
		// 验证 GossipThreshold 是否大于 0 或无效
		if p.GossipThreshold > 0 || isInvalidNumber(p.GossipThreshold) {
			return fmt.Errorf("invalid gossip threshold; it must be <= 0 and a valid number")
		}
		// 验证 PublishThreshold 是否大于 0 或大于 GossipThreshold 或无效
		if p.PublishThreshold > 0 || p.PublishThreshold > p.GossipThreshold || isInvalidNumber(p.PublishThreshold) {
			return fmt.Errorf("invalid publish threshold; it must be <= 0 and <= gossip threshold and a valid number")
		}
		// 验证 GraylistThreshold 是否大于 0 或大于 PublishThreshold 或无效
		if p.GraylistThreshold > 0 || p.GraylistThreshold > p.PublishThreshold || isInvalidNumber(p.GraylistThreshold) {
			return fmt.Errorf("invalid graylist threshold; it must be <= 0 and <= publish threshold and a valid number")
		}
	}
	// 如果没有跳过原子验证，或者 AcceptPXThreshold 不为 0
	if !p.SkipAtomicValidation || p.AcceptPXThreshold != 0 {
		// 验证 AcceptPXThreshold 是否小于 0 或无效
		if p.AcceptPXThreshold < 0 || isInvalidNumber(p.AcceptPXThreshold) {
			return fmt.Errorf("invalid accept PX threshold; it must be >= 0 and a valid number")
		}
	}
	// 如果没有跳过原子验证，或者 OpportunisticGraftThreshold 不为 0
	if !p.SkipAtomicValidation || p.OpportunisticGraftThreshold != 0 {
		// 验证 OpportunisticGraftThreshold 是否小于 0 或无效
		if p.OpportunisticGraftThreshold < 0 || isInvalidNumber(p.OpportunisticGraftThreshold) {
			return fmt.Errorf("invalid opportunistic grafting threshold; it must be >= 0 and a valid number")
		}
	}
	return nil // 所有验证通过，返回 nil 表示没有错误
}

// PeerScoreParams 包含用于控制对等节点分数的参数
type PeerScoreParams struct {
	SkipAtomicValidation        bool                         // 是否允许仅设置某些参数而不是所有参数
	Topics                      map[string]*TopicScoreParams // 每个主题的分数参数
	TopicScoreCap               float64                      // 主题分数上限
	AppSpecificScore            func(p peer.ID) float64      // 应用程序特定的对等节点分数
	AppSpecificWeight           float64                      // 应用程序特定分数的权重
	IPColocationFactorWeight    float64                      // IP 同位因素的权重
	IPColocationFactorThreshold int                          // IP 同位因素阈值
	IPColocationFactorWhitelist []*net.IPNet                 // IP 同位因素白名单
	BehaviourPenaltyWeight      float64                      // 行为模式处罚的权重
	BehaviourPenaltyThreshold   float64                      // 行为模式处罚的阈值
	BehaviourPenaltyDecay       float64                      // 行为模式处罚的衰减
	DecayInterval               time.Duration                // 参数计数器的衰减间隔
	DecayToZero                 float64                      // 计数器值低于该值时被视为 0
	RetainScore                 time.Duration                // 断开连接的对等节点记住计数器的时间
	SeenMsgTTL                  time.Duration                // 记住消息传递时间
}

// validate 验证 PeerScoreParams 参数
// 返回值:
//   - error: 错误信息
func (p *PeerScoreParams) validate() error {
	// 遍历每个主题及其对应的参数，验证其有效性
	for topic, params := range p.Topics {
		err := params.validate() // 调用主题参数的 validate 方法
		if err != nil {
			return fmt.Errorf("invalid score parameters for topic %s: %w", topic, err)
		}
	}
	// 如果没有跳过原子验证，或者 TopicScoreCap 不为 0
	if !p.SkipAtomicValidation || p.TopicScoreCap != 0 {
		// 验证 TopicScoreCap 是否小于 0 或无效
		if p.TopicScoreCap < 0 || isInvalidNumber(p.TopicScoreCap) {
			return fmt.Errorf("invalid topic score cap; must be positive (or 0 for no cap) and a valid number")
		}
	}
	// 验证 AppSpecificScore 是否为 nil
	if p.AppSpecificScore == nil {
		if p.SkipAtomicValidation {
			// 如果跳过了原子验证，设置一个默认的应用特定评分函数
			p.AppSpecificScore = func(p peer.ID) float64 {
				return 0
			}
		} else {
			return fmt.Errorf("missing application specific score function")
		}
	}
	// 如果没有跳过原子验证，或者 IPColocationFactorWeight 不为 0
	if !p.SkipAtomicValidation || p.IPColocationFactorWeight != 0 {
		// 验证 IPColocationFactorWeight 是否大于 0 或无效
		if p.IPColocationFactorWeight > 0 || isInvalidNumber(p.IPColocationFactorWeight) {
			return fmt.Errorf("invalid IPColocationFactorWeight; must be negative (or 0 to disable) and a valid number")
		}
		// 验证 IPColocationFactorThreshold 是否小于 1
		if p.IPColocationFactorWeight != 0 && p.IPColocationFactorThreshold < 1 {
			return fmt.Errorf("invalid IPColocationFactorThreshold; must be at least 1")
		}
	}
	// 如果没有跳过原子验证，或者 BehaviourPenaltyWeight 或 BehaviourPenaltyThreshold 不为 0
	if !p.SkipAtomicValidation || p.BehaviourPenaltyWeight != 0 || p.BehaviourPenaltyThreshold != 0 {
		// 验证 BehaviourPenaltyWeight 是否大于 0 或无效
		if p.BehaviourPenaltyWeight > 0 || isInvalidNumber(p.BehaviourPenaltyWeight) {
			return fmt.Errorf("invalid BehaviourPenaltyWeight; must be negative (or 0 to disable) and a valid number")
		}
		// 验证 BehaviourPenaltyDecay 是否不在 (0, 1) 区间内或无效
		if p.BehaviourPenaltyWeight != 0 && (p.BehaviourPenaltyDecay <= 0 || p.BehaviourPenaltyDecay >= 1 || isInvalidNumber(p.BehaviourPenaltyDecay)) {
			return fmt.Errorf("invalid BehaviourPenaltyDecay; must be between 0 and 1")
		}
		// 验证 BehaviourPenaltyThreshold 是否小于 0 或无效
		if p.BehaviourPenaltyThreshold < 0 || isInvalidNumber(p.BehaviourPenaltyThreshold) {
			return fmt.Errorf("invalid BehaviourPenaltyThreshold; must be >= 0 and a valid number")
		}
	}
	// 如果没有跳过原子验证，或者 DecayInterval 或 DecayToZero 不为 0
	if !p.SkipAtomicValidation || p.DecayInterval != 0 || p.DecayToZero != 0 {
		// 验证 DecayInterval 是否小于 1 秒
		if p.DecayInterval < time.Second {
			return fmt.Errorf("invalid DecayInterval; must be at least 1s")
		}
		// 验证 DecayToZero 是否不在 (0, 1) 区间内或无效
		if p.DecayToZero <= 0 || p.DecayToZero >= 1 || isInvalidNumber(p.DecayToZero) {
			return fmt.Errorf("invalid DecayToZero; must be between 0 and 1")
		}
	}
	return nil // 所有验证通过，返回 nil 表示没有错误
}

// TopicScoreParams 包含用于控制主题分数的参数
type TopicScoreParams struct {
	SkipAtomicValidation            bool          // 是否允许仅设置某些参数而不是所有参数
	TopicWeight                     float64       // 主题权重
	TimeInMeshWeight                float64       // 在 mesh 中的时间权重
	TimeInMeshQuantum               time.Duration // 在 mesh 中的时间量子
	TimeInMeshCap                   float64       // 在 mesh 中的时间上限
	FirstMessageDeliveriesWeight    float64       // 首次消息传递的权重
	FirstMessageDeliveriesDecay     float64       // 首次消息传递的衰减
	FirstMessageDeliveriesCap       float64       // 首次消息传递的上限
	MeshMessageDeliveriesWeight     float64       // mesh 消息传递的权重
	MeshMessageDeliveriesDecay      float64       // mesh 消息传递的衰减
	MeshMessageDeliveriesCap        float64       // mesh 消息传递的上限
	MeshMessageDeliveriesThreshold  float64       // mesh 消息传递的阈值
	MeshMessageDeliveriesWindow     time.Duration // mesh 消息传递的窗口
	MeshMessageDeliveriesActivation time.Duration // mesh 消息传递的激活时间
	MeshFailurePenaltyWeight        float64       // mesh 失败处罚的权重
	MeshFailurePenaltyDecay         float64       // mesh 失败处罚的衰减
	InvalidMessageDeliveriesWeight  float64       // 无效消息传递的权重
	InvalidMessageDeliveriesDecay   float64       // 无效消息传递的衰减
}

// validate 验证 TopicScoreParams 参数
// 返回值:
//   - error: 错误信息
func (p *TopicScoreParams) validate() error {
	if p.TopicWeight < 0 || isInvalidNumber(p.TopicWeight) {
		return fmt.Errorf("invalid topic weight; must be >= 0 and a valid number")
	}
	if err := p.validateTimeInMeshParams(); err != nil {
		return err
	}
	if err := p.validateMessageDeliveryParams(); err != nil {
		return err
	}
	if err := p.validateMeshMessageDeliveryParams(); err != nil {
		return err
	}
	if err := p.validateMessageFailurePenaltyParams(); err != nil {
		return err
	}
	if err := p.validateInvalidMessageDeliveryParams(); err != nil {
		return err
	}
	return nil
}

// validateTimeInMeshParams 验证 TimeInMesh 参数
// 返回值:
//   - error: 错误信息
func (p *TopicScoreParams) validateTimeInMeshParams() error {
	if p.SkipAtomicValidation {
		if p.TimeInMeshWeight == 0 && p.TimeInMeshQuantum == 0 && p.TimeInMeshCap == 0 {
			return nil
		}
	}
	if p.TimeInMeshQuantum == 0 {
		return fmt.Errorf("invalid TimeInMeshQuantum; must be non zero")
	}
	if p.TimeInMeshWeight < 0 || isInvalidNumber(p.TimeInMeshWeight) {
		return fmt.Errorf("invalid TimeInMeshWeight; must be positive (or 0 to disable) and a valid number")
	}
	if p.TimeInMeshWeight != 0 && p.TimeInMeshQuantum <= 0 {
		return fmt.Errorf("invalid TimeInMeshQuantum; must be positive")
	}
	if p.TimeInMeshWeight != 0 && (p.TimeInMeshCap <= 0 || isInvalidNumber(p.TimeInMeshCap)) {
		return fmt.Errorf("invalid TimeInMeshCap; must be positive and a valid number")
	}
	return nil
}

// validateMessageDeliveryParams 验证 FirstMessageDeliveries 参数
// 返回值:
//   - error: 错误信息
func (p *TopicScoreParams) validateMessageDeliveryParams() error {
	if p.SkipAtomicValidation {
		if p.FirstMessageDeliveriesWeight == 0 && p.FirstMessageDeliveriesCap == 0 && p.FirstMessageDeliveriesDecay == 0 {
			return nil
		}
	}
	if p.FirstMessageDeliveriesWeight < 0 || isInvalidNumber(p.FirstMessageDeliveriesWeight) {
		return fmt.Errorf("invalid FirstMessageDeliveriesWeight; must be positive (or 0 to disable) and a valid number")
	}
	if p.FirstMessageDeliveriesWeight != 0 && (p.FirstMessageDeliveriesDecay <= 0 || p.FirstMessageDeliveriesDecay >= 1 || isInvalidNumber(p.FirstMessageDeliveriesDecay)) {
		return fmt.Errorf("invalid FirstMessageDeliveriesDecay; must be between 0 and 1")
	}
	if p.FirstMessageDeliveriesWeight != 0 && (p.FirstMessageDeliveriesCap <= 0 || isInvalidNumber(p.FirstMessageDeliveriesCap)) {
		return fmt.Errorf("invalid FirstMessageDeliveriesCap; must be positive and a valid number")
	}
	return nil
}

// validateMeshMessageDeliveryParams 验证 MeshMessageDeliveries 参数
// 返回值:
//   - error: 错误信息
func (p *TopicScoreParams) validateMeshMessageDeliveryParams() error {
	if p.SkipAtomicValidation {
		if p.MeshMessageDeliveriesWeight == 0 &&
			p.MeshMessageDeliveriesCap == 0 &&
			p.MeshMessageDeliveriesDecay == 0 &&
			p.MeshMessageDeliveriesThreshold == 0 &&
			p.MeshMessageDeliveriesWindow == 0 &&
			p.MeshMessageDeliveriesActivation == 0 {
			return nil
		}
	}
	if p.MeshMessageDeliveriesWeight > 0 || isInvalidNumber(p.MeshMessageDeliveriesWeight) {
		return fmt.Errorf("invalid MeshMessageDeliveriesWeight; must be negative (or 0 to disable) and a valid number")
	}
	if p.MeshMessageDeliveriesWeight != 0 && (p.MeshMessageDeliveriesDecay <= 0 || p.MeshMessageDeliveriesDecay >= 1 || isInvalidNumber(p.MeshMessageDeliveriesDecay)) {
		return fmt.Errorf("invalid MeshMessageDeliveriesDecay; must be between 0 and 1")
	}
	if p.MeshMessageDeliveriesWeight != 0 && (p.MeshMessageDeliveriesCap <= 0 || isInvalidNumber(p.MeshMessageDeliveriesCap)) {
		return fmt.Errorf("invalid MeshMessageDeliveriesCap; must be positive and a valid number")
	}
	if p.MeshMessageDeliveriesWeight != 0 && (p.MeshMessageDeliveriesThreshold <= 0 || isInvalidNumber(p.MeshMessageDeliveriesThreshold)) {
		return fmt.Errorf("invalid MeshMessageDeliveriesThreshold; must be positive and a valid number")
	}
	if p.MeshMessageDeliveriesWindow < 0 {
		return fmt.Errorf("invalid MeshMessageDeliveriesWindow; must be non-negative")
	}
	if p.MeshMessageDeliveriesWeight != 0 && p.MeshMessageDeliveriesActivation < time.Second {
		return fmt.Errorf("invalid MeshMessageDeliveriesActivation; must be at least 1s")
	}
	return nil
}

// validateMessageFailurePenaltyParams 验证 MeshFailurePenalty 参数
// 返回值:
//   - error: 错误信息
func (p *TopicScoreParams) validateMessageFailurePenaltyParams() error {
	if p.SkipAtomicValidation {
		if p.MeshFailurePenaltyDecay == 0 && p.MeshFailurePenaltyWeight == 0 {
			return nil
		}
	}
	if p.MeshFailurePenaltyWeight > 0 || isInvalidNumber(p.MeshFailurePenaltyWeight) {
		return fmt.Errorf("invalid MeshFailurePenaltyWeight; must be negative (or 0 to disable) and a valid number")
	}
	if p.MeshFailurePenaltyWeight != 0 && (isInvalidNumber(p.MeshFailurePenaltyDecay) || p.MeshFailurePenaltyDecay <= 0 || p.MeshFailurePenaltyDecay >= 1) {
		return fmt.Errorf("invalid MeshFailurePenaltyDecay; must be between 0 and 1")
	}
	return nil
}

// validateInvalidMessageDeliveryParams 验证 InvalidMessageDeliveries 参数
// 返回值:
//   - error: 错误信息
func (p *TopicScoreParams) validateInvalidMessageDeliveryParams() error {
	if p.SkipAtomicValidation {
		if p.InvalidMessageDeliveriesDecay == 0 && p.InvalidMessageDeliveriesWeight == 0 {
			return nil
		}
	}
	if p.InvalidMessageDeliveriesWeight > 0 || isInvalidNumber(p.InvalidMessageDeliveriesWeight) {
		return fmt.Errorf("invalid InvalidMessageDeliveriesWeight; must be negative (or 0 to disable) and a valid number")
	}
	if p.InvalidMessageDeliveriesDecay <= 0 || p.InvalidMessageDeliveriesDecay >= 1 || isInvalidNumber(p.InvalidMessageDeliveriesDecay) {
		return fmt.Errorf("invalid InvalidMessageDeliveriesDecay; must be between 0 and 1")
	}
	return nil
}

const (
	DefaultDecayInterval = time.Second // 默认的衰减间隔
	DefaultDecayToZero   = 0.01        // 默认的衰减到零值
)

// ScoreParameterDecay 计算参数的衰减因子，假设 DecayInterval 为 1s 并且值在低于 0.01 时衰减到零
// 参数:
//   - decay: 衰减时间
//
// 返回值:
//   - float64: 衰减因子
func ScoreParameterDecay(decay time.Duration) float64 {
	return ScoreParameterDecayWithBase(decay, DefaultDecayInterval, DefaultDecayToZero)
}

// ScoreParameterDecayWithBase 使用基准 DecayInterval 计算参数的衰减因子
// 参数:
//   - decay: 衰减时间
//   - base: 基准衰减间隔
//   - decayToZero: 衰减到零值
//
// 返回值:
//   - float64: 衰减因子
func ScoreParameterDecayWithBase(decay time.Duration, base time.Duration, decayToZero float64) float64 {
	ticks := float64(decay / base)
	return math.Pow(decayToZero, 1/ticks)
}

// isInvalidNumber 检查提供的浮点数是否为 NaN 或无穷大
// 参数:
//   - num: 要检查的浮点数
//
// 返回值:
//   - bool: 是否为无效数字
func isInvalidNumber(num float64) bool {
	return math.IsNaN(num) || math.IsInf(num, 0)
}
