// 作用：处理对等节点的发现。
// 功能：实现对等节点发现的机制，帮助节点在网络中找到其他对等节点。

package pubsub

import (
	"context"
	"math/rand"
	"time"

	"github.com/dep2p/go-dep2p/core/discovery"
	"github.com/dep2p/go-dep2p/core/host"
	"github.com/dep2p/go-dep2p/core/peer"
	discimpl "github.com/dep2p/go-dep2p/p2p/discovery/backoff"
)

var (
	// 轮询间隔

	// DiscoveryPollInitialDelay 是发现系统在首次启动后等待的时间
	DiscoveryPollInitialDelay = 0 * time.Millisecond
	// DiscoveryPollInterval 是发现系统在检查是否需要更多对等点之间等待的大致时间
	DiscoveryPollInterval = 1 * time.Second
)

// 重试广告失败时的间隔
const discoveryAdvertiseRetryInterval = 2 * time.Minute

// DiscoverOpt 是一个函数类型，用于配置发现选项
type DiscoverOpt func(*discoverOptions) error

// discoverOptions 是发现配置的选项
type discoverOptions struct {
	connFactory BackoffConnectorFactory // 退避连接器工厂
	opts        []discovery.Option      // 发现选项
}

// defaultDiscoverOptions 返回默认的发现选项
func defaultDiscoverOptions() *discoverOptions {
	rngSrc := rand.NewSource(rand.Int63()) // 创建一个新的随机数源，使用随机的种子初始化

	minBackoff, maxBackoff := time.Second*10, time.Hour // 定义最小和最大的退避时间，分别为10秒和1小时
	cacheSize := 100                                    // 设置缓存大小为100
	dialTimeout := time.Minute * 2                      // 设置拨号超时时间为2分钟

	discoverOpts := &discoverOptions{ // 初始化discoverOptions结构体
		// connFactory是一个函数，用于创建新的BackoffConnector
		connFactory: func(host host.Host) (*discimpl.BackoffConnector, error) {
			// 创建一个新的指数退避策略
			backoff := discimpl.NewExponentialBackoff(
				minBackoff,          // 最小退避时间
				maxBackoff,          // 最大退避时间
				discimpl.FullJitter, // 使用全抖动算法
				time.Second,         // 基础时间单位
				5.0,                 // 倍数
				0,                   // 初始退避时间
				rand.New(rngSrc),    // 随机数生成器
			)
			// 使用退避策略、缓存大小和拨号超时时间创建一个新的BackoffConnector
			return discimpl.NewBackoffConnector(host, cacheSize, dialTimeout, backoff)
		},
	}

	return discoverOpts // 返回初始化的discoverOptions
}

// discover 表示发现管道
// 发现管道处理对等节点的广告和发现
type discover struct {
	p *PubSub

	// discovery 协助发现和广告主题的对等节点
	discovery discovery.Discovery

	// advertising 跟踪正在广告的主题
	advertising map[string]context.CancelFunc

	// discoverQ 处理持续的对等节点发现
	discoverQ chan *discoverReq

	// ongoing 跟踪正在进行的发现请求
	ongoing map[string]struct{}

	// done 处理发现请求的完成
	done chan string

	// connector 处理通过发现找到的新对等节点连接
	connector *discimpl.BackoffConnector

	// options 是在 Start 中完成结构体构造的一组选项
	options *discoverOptions
}

// MinTopicSize 返回一个函数，该函数根据主题大小检查路由器是否准备好发布。
// 参数:
//   - size: 建议的主题大小（对等节点数）
//
// 返回值:
//   - RouterReady: 一个函数类型，接收路由器和主题名称，返回布尔值和错误
func MinTopicSize(size int) RouterReady {
	return func(rt PubSubRouter, topic string) (bool, error) {
		// 调用路由器的EnoughPeers方法，检查主题的对等节点数是否达到给定的大小
		// EnoughPeers 方法返回布尔值，指示是否有足够的对等节点
		// 该函数始终返回nil错误
		return rt.EnoughPeers(topic, size), nil
	}
}

// Start 将发现管道附加到 pubsub 实例，初始化发现并启动事件循环
// 参数:
//   - p: PubSub 实例
//   - opts: 可选的 DiscoverOpt 配置项
//
// 返回值:
//   - error: 如果初始化失败，返回错误
func (d *discover) Start(p *PubSub, opts ...DiscoverOpt) error {
	if d.discovery == nil || p == nil { // 检查发现服务和pubsub实例是否为空
		return nil // 如果为空，则返回nil
	}

	d.p = p                                             // 将pubsub实例赋值给discover结构体的p字段
	d.advertising = make(map[string]context.CancelFunc) // 初始化广告映射，用于取消广告
	d.discoverQ = make(chan *discoverReq, 32)           // 初始化发现请求通道，缓冲区大小为32
	d.ongoing = make(map[string]struct{})               // 初始化进行中的请求映射
	d.done = make(chan string)                          // 初始化完成通道，用于通知完成的发现请求

	conn, err := d.options.connFactory(p.host) // 使用连接器工厂创建连接器
	if err != nil {                            // 如果创建连接器时出错
		return err // 返回错误
	}
	d.connector = conn // 将创建的连接器赋值给discover结构体的connector字段

	go d.discoverLoop() // 启动发现循环，处理发现请求
	go d.pollTimer()    // 启动轮询计时器，定期执行发现操作

	return nil // 返回nil表示成功启动
}

// pollTimer 是一个定时器，用于定期触发发现请求
func (d *discover) pollTimer() {
	select {
	case <-time.After(DiscoveryPollInitialDelay): // 等待初始延迟时间
	case <-d.p.ctx.Done(): // 上下文完成，退出
		return
	}

	select {
	case d.p.eval <- d.requestDiscovery: // 发送发现请求
	case <-d.p.ctx.Done(): // 上下文完成，退出
		return
	}

	ticker := time.NewTicker(DiscoveryPollInterval) // 创建新的定时器
	defer ticker.Stop()                             // 函数结束时停止定时器

	for {
		select {
		case <-ticker.C:
			select {
			case d.p.eval <- d.requestDiscovery: // 发送发现请求
			case <-d.p.ctx.Done(): // 上下文完成，退出
				return
			}
		case <-d.p.ctx.Done(): // 上下文完成，退出
			return
		}
	}
}

// requestDiscovery 发送发现请求
func (d *discover) requestDiscovery() {
	for t := range d.p.myTopics { // 遍历所有主题
		if !d.p.rt.EnoughPeers(t, 0) { // 检查是否有足够的对等节点
			d.discoverQ <- &discoverReq{topic: t, done: make(chan struct{}, 1)} // 发送发现请求
		}
	}
}

// discoverLoop 处理发现请求的循环
func (d *discover) discoverLoop() {
	for {
		select {
		case discover := <-d.discoverQ: // 从发现请求通道中读取一个发现请求
			topic := discover.topic

			if _, ok := d.ongoing[topic]; ok { // 如果该主题的发现请求已经在进行中
				discover.done <- struct{}{} // 向完成通道发送信号，表示当前请求已完成
				continue                    // 跳过当前请求
			}

			d.ongoing[topic] = struct{}{} // 将该主题标记为正在进行发现请求

			go func() {
				d.handleDiscovery(d.p.ctx, topic, discover.opts) // 启动协程处理发现请求
				select {
				case d.done <- topic: // 将已完成的主题发送到done通道
				case <-d.p.ctx.Done(): // 如果上下文完成，退出
				}
				discover.done <- struct{}{} // 向完成通道发送信号，表示当前请求已完成
			}()
		case topic := <-d.done: // 从done通道中读取一个已完成的主题
			delete(d.ongoing, topic) // 从进行中的请求映射中删除该主题
		case <-d.p.ctx.Done(): // 如果上下文完成
			return // 退出循环
		}
	}
}

// Advertise 在发现服务中广告该节点对某个主题的兴趣。Advertise 不是线程安全的。
// 参数:
//   - topic: 主题
func (d *discover) Advertise(topic string) {
	if d.discovery == nil { // 如果发现服务为空，直接返回
		return
	}

	advertisingCtx, cancel := context.WithCancel(d.p.ctx) // 创建带有取消功能的上下文

	if _, ok := d.advertising[topic]; ok { // 如果该主题已经在广告中
		cancel() // 取消广告上下文
		return
	}
	d.advertising[topic] = cancel // 将取消函数存储到广告映射中

	go func() { // 启动一个新的协程处理广告过程
		next, err := d.discovery.Advertise(advertisingCtx, topic) // 在发现服务中广告该主题
		if err != nil {                                           // 如果广告过程中出现错误
			logger.Warnf("bootstrap: 为主题 %s 提供集合点时发生错误: %s", topic, err.Error()) // 记录警告日志
			if next == 0 {                                                       // 如果下一次广告间隔为0
				next = discoveryAdvertiseRetryInterval // 使用默认的广告重试间隔
			}
		}

		t := time.NewTimer(next) // 创建定时器，用于定时进行下一次广告
		defer t.Stop()           // 函数结束时停止定时器

		for advertisingCtx.Err() == nil { // 当广告上下文未结束时循环
			select {
			case <-t.C: // 定时器触发
				next, err = d.discovery.Advertise(advertisingCtx, topic) // 再次尝试广告该主题
				if err != nil {                                          // 如果再次广告过程中出现错误
					logger.Warnf("提供对等节点发现服务失败: %s", err.Error()) // 记录警告日志
					if next == 0 {                                // 如果下一次广告间隔为0
						next = discoveryAdvertiseRetryInterval // 使用默认的广告重试间隔
					}
				}
				t.Reset(next) // 重置定时器，根据下一次广告间隔
			case <-advertisingCtx.Done(): // 如果广告上下文结束
				return // 结束协程
			}
		}
	}()
}

// StopAdvertise 停止广告该节点对某个主题的兴趣。StopAdvertise 不是线程安全的。
// 参数:
//   - topic: 主题
func (d *discover) StopAdvertise(topic string) {
	if d.discovery == nil { // 如果发现服务为空，直接返回
		return
	}

	if advertiseCancel, ok := d.advertising[topic]; ok { // 如果该主题在广告中存在
		advertiseCancel()            // 调用取消广告的函数
		delete(d.advertising, topic) // 从广告映射中删除该主题
	}
}

// Discover 搜索对某个主题感兴趣的其他对等节点
// 参数:
//   - topic: 主题
//   - opts: 可选的发现选项
func (d *discover) Discover(topic string, opts ...discovery.Option) {
	if d.discovery == nil {
		return
	}

	d.discoverQ <- &discoverReq{
		topic,                  // 设置主题
		opts,                   // 设置发现选项
		make(chan struct{}, 1), // 初始化完成通道
	} // 发送发现请求
}

// Bootstrap 尝试引导某个主题。引导成功返回 true，否则返回 false。
// 参数:
//   - ctx: 上下文
//   - topic: 主题
//   - ready: 路由器准备情况检查函数
//   - opts: 可选的发现选项
//
// 返回值:
//   - bool: 是否引导成功
func (d *discover) Bootstrap(ctx context.Context, topic string, ready RouterReady, opts ...discovery.Option) bool {
	if d.discovery == nil { // 如果发现服务为空，直接返回成功
		return true
	}

	t := time.NewTimer(time.Hour) // 创建一个1小时的定时器
	if !t.Stop() {                // 确保定时器已经停止
		<-t.C
	}
	defer t.Stop() // 函数结束时停止定时器

	for {
		// 检查是否准备好发布
		bootstrapped := make(chan bool, 1)
		select {
		case d.p.eval <- func() { // 将路由器准备情况检查函数放入评估通道
			done, _ := ready(d.p.rt, topic) // 调用准备情况检查函数
			bootstrapped <- done            // 将检查结果发送到bootstrapped通道
		}:
			if <-bootstrapped { // 如果准备好发布
				return true // 引导成功，返回true
			}
		case <-d.p.ctx.Done(): // 如果上下文被取消，返回false
			return false
		case <-ctx.Done(): // 如果上下文被取消，返回false
			return false
		}

		// 如果未准备好，发现更多对等节点
		disc := &discoverReq{topic, opts, make(chan struct{}, 1)} // 创建一个新的发现请求
		select {
		case d.discoverQ <- disc: // 发送发现请求到发现请求通道
		case <-d.p.ctx.Done(): // 如果上下文被取消，返回false
			return false
		case <-ctx.Done(): // 如果上下文被取消，返回false
			return false
		}

		select {
		case <-disc.done: // 等待发现请求完成
		case <-d.p.ctx.Done(): // 如果上下文被取消，返回false
			return false
		case <-ctx.Done(): // 如果上下文被取消，返回false
			return false
		}

		t.Reset(time.Millisecond * 100) // 重置定时器为100毫秒
		select {
		case <-t.C: // 等待定时器触发
		case <-d.p.ctx.Done(): // 如果上下文被取消，返回false
			return false
		case <-ctx.Done(): // 如果上下文被取消，返回false
			return false
		}
	}
}

// handleDiscovery 处理发现过程
// 参数:
//   - ctx: 上下文
//   - topic: 主题
//   - opts: 发现选项
func (d *discover) handleDiscovery(ctx context.Context, topic string, opts []discovery.Option) {
	discoverCtx, cancel := context.WithTimeout(ctx, time.Second*90) // 创建带有10秒超时的上下文
	defer cancel()                                                  // 函数结束时取消上下文

	peerCh, err := d.discovery.FindPeers(discoverCtx, topic, opts...) // 使用发现服务查找对应主题的对等节点
	if err != nil {
		logger.Debugf("发现对等节点失败: %v", err) // 记录发现错误
		return
	}

	d.connector.Connect(ctx, peerCh) // 使用连接器连接发现到的对等节点
}

// discoverReq 表示发现请求
type discoverReq struct {
	topic string             // topic 表示发现请求的主题
	opts  []discovery.Option // opts 表示发现请求的选项列表
	done  chan struct{}      // done 是一个信号通道，用于通知发现请求完成
}

// pubSubDiscovery 是一个实现了发现接口的结构体
type pubSubDiscovery struct {
	discovery.Discovery                    // pubSubDiscovery 包含一个 Discovery 接口的实现
	opts                []discovery.Option // opts 表示发现实例的配置选项列表
}

// Advertise 广告对某个命名空间的兴趣
// 参数:
//   - ctx: 上下文
//   - ns: 命名空间
//   - opts: 发现选项
//
// 返回值:
//   - time.Duration: 下次广告的间隔时间
//   - error: 广告失败时返回错误
func (d *pubSubDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	return d.Discovery.Advertise(ctx, "floodsub:"+ns, append(opts, d.opts...)...)
}

// FindPeers 查找对某个命名空间感兴趣的对等节点
// 参数:
//   - ctx: 上下文
//   - ns: 命名空间
//   - opts: 发现选项
//
// 返回值:
//   - <-chan peer.AddrInfo: 对等节点地址信息的通道
//   - error: 查找失败时返回错误
func (d *pubSubDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	return d.Discovery.FindPeers(ctx, "floodsub:"+ns, append(opts, d.opts...)...)
}

// WithDiscoveryOpts 传递 libp2p 发现选项到 PubSub 发现子系统
// 参数:
//   - opts: 发现选项，这些选项将被应用到发现子系统中
//
// 返回值:
//   - DiscoverOpt: 配置发现选项的函数，用于设置发现配置
func WithDiscoveryOpts(opts ...discovery.Option) DiscoverOpt {
	return func(d *discoverOptions) error {
		d.opts = opts // 将传入的发现选项设置到发现配置中
		return nil    // 返回 nil 表示配置成功
	}
}

// BackoffConnectorFactory 创建一个附加到给定主机的 BackoffConnector
type BackoffConnectorFactory func(host host.Host) (*discimpl.BackoffConnector, error)

// WithDiscoverConnector 添加一个自定义连接器，该连接器处理发现子系统如何连接到对等节点
// 参数:
//   - connFactory: 退避连接器工厂，用于创建一个自定义的连接器
//
// 返回值:
//   - DiscoverOpt: 配置发现选项的函数，用于设置发现配置
func WithDiscoverConnector(connFactory BackoffConnectorFactory) DiscoverOpt {
	return func(d *discoverOptions) error {
		d.connFactory = connFactory // 设置传入的连接器工厂到发现配置中
		return nil                  // 返回 nil 表示配置成功
	}
}
