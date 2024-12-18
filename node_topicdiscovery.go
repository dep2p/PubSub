package pubsub

// import (
// 	"context"
// 	"time"

// 	dht "github.com/libp2p/go-libp2p-kad-dht"
// 	"github.com/libp2p/go-libp2p/core/discovery"
// 	"github.com/libp2p/go-libp2p/core/host"
// 	"github.com/libp2p/go-libp2p/core/peer"
// 	discrouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
// 	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
// )

// // 创建轻量级发现服务
// func createDiscoveryService(ctx context.Context, host host.Host) (discovery.Discovery, error) {
// 	// 创建新的 DHT 实例
// 	kadDHT, err := dht.New(ctx, host)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// 启动 DHT
// 	if err := kadDHT.Bootstrap(ctx); err != nil {
// 		return nil, err
// 	}

// 	// 创建基于 DHT 的路由发现服务
// 	routingDiscovery := discrouting.NewRoutingDiscovery(kadDHT)

// 	// 广告自己的存在
// 	dutil.Advertise(ctx, routingDiscovery, "your-rendezvous-string")

// 	// 创建一个简单的发现服务包装器
// 	return &simpleDiscovery{
// 		host:      host,
// 		discovery: routingDiscovery,
// 	}, nil
// }

// // simpleDiscovery 是一个简单的发现服务包装器
// type simpleDiscovery struct {
// 	host      host.Host
// 	discovery *discrouting.RoutingDiscovery
// }

// // Advertise 实现 discovery.Discovery 接口
// func (sd *simpleDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
// 	return sd.discovery.Advertise(ctx, ns, opts...)
// }

// // FindPeers 实现 discovery.Discovery 接口
// func (sd *simpleDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
// 	return sd.discovery.FindPeers(ctx, ns, opts...)
// }
