// 作用：定义和管理GossipSub协议的特性。
// 功能：管理GossipSub协议中的可选特性和扩展。

package pubsub

import (
	"fmt"

	"github.com/dep2p/pubsub/logger"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// GossipSubFeatureTest 是一个功能测试函数；它接受一个功能和协议ID，并且如果协议支持该功能则返回 true
type GossipSubFeatureTest = func(GossipSubFeature, protocol.ID) bool

// GossipSubFeature 是一个功能判别枚举
type GossipSubFeature int

const (
	// 协议支持基本的 GossipSub 网格 -- 与 gossipsub-v1.0 兼容
	GossipSubFeatureMesh = iota
	// 协议支持在修剪时的对等节点交换 -- 与 gossipsub-v1.1 兼容
	GossipSubFeaturePX
)

// GossipSubDefaultProtocols 是默认的 gossipsub 路由器协议列表
var GossipSubDefaultProtocols = []protocol.ID{GossipSubID_v11, GossipSubID_v10, FloodSubID}

// GossipSubDefaultFeatures 是默认 gossipsub 协议的功能测试函数
// 参数:
//   - feat: 要测试的功能
//   - proto: 协议ID
//
// 返回值:
//   - bool: 如果协议支持该功能，则返回 true
func GossipSubDefaultFeatures(feat GossipSubFeature, proto protocol.ID) bool {
	switch feat {
	case GossipSubFeatureMesh:
		return proto == GossipSubID_v11 || proto == GossipSubID_v10
	case GossipSubFeaturePX:
		return proto == GossipSubID_v11
	default:
		return false
	}
}

// WithGossipSubProtocols 是一个 gossipsub 路由器选项，用于配置自定义协议列表和功能测试函数
// 参数:
//   - protos: 协议列表
//   - feature: 功能测试函数
//
// 返回值:
//   - Option: 配置 gossipsub 协议和功能的选项函数
func WithGossipSubProtocols(protos []protocol.ID, feature GossipSubFeatureTest) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			logger.Errorf("pubsub 路由器不是 gossipsub")     // 返回错误，如果路由器不是 gossipsub
			return fmt.Errorf("pubsub 路由器不是 gossipsub") // 返回错误，如果路由器不是 gossipsub
		}

		gs.protos = protos   // 设置 gossipsub 路由器的协议列表
		gs.feature = feature // 设置 gossipsub 路由器的功能测试函数

		return nil
	}
}
