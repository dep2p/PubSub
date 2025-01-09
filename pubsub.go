// 作用：实现pubsub系统的核心逻辑。
// 功能：定义和实现发布-订阅系统的核心功能，包括消息发布、订阅和传递。

package pubsub

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dep2p/pubsub/timecache"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/discovery"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"
)

// DefaultMaxMessageSize 定义默认的最大消息大小为1MB
const DefaultMaxMessageSize = 1 << 20

var (
	// TimeCacheDuration 指定消息ID会被记住多长时间
	TimeCacheDuration = 120 * time.Second

	// TimeCacheStrategy 指定 seen messages cache 的查找/清理策略
	TimeCacheStrategy = timecache.Strategy_FirstSeen

	// ErrSubscriptionCancelled 当订阅被取消后调用 Next() 时可能返回的错误
	ErrSubscriptionCancelled = errors.New("subscription cancelled")
)

// 定义日志记录器
// var log = logging.Logger("pubsub")

// ProtocolMatchFn 是一个函数类型，用于匹配协议ID
type ProtocolMatchFn = func(protocol.ID) func(protocol.ID) bool

// PubSub 实现了发布-订阅系统。
type PubSub struct {
	// 用于消息序列号的原子计数器
	// 注意:必须声明在结构体的顶部，因为我们对这个字段进行原子操作。
	//
	// 参见：https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	counter uint64 // 原子计数器，用于生成唯一的消息序列号

	// 本地主机
	host host.Host // libp2p 主机实例，表示当前节点

	// 发布-订阅路由器
	rt PubSubRouter // 实际执行消息路由的路由器接口

	// 消息验证模块
	val *validation // 负责消息验证的模块，用于确保消息的有效性和完整性

	// 节点发现模块
	disc *discover // 负责发现新对等节点的模块，帮助节点在网络中找到其他对等节点

	// 追踪器，用于记录和分析消息流
	tracer *pubsubTracer // 用于追踪和记录消息流的追踪器，帮助分析消息的传递路径和性能

	// 对等节点过滤器
	peerFilter PeerFilter // 过滤不可信对等节点的过滤器，用于防止与不可信或恶意节点通信

	// 最大消息大小，全局适用于所有主题
	maxMessageSize int // 允许的最大消息大小，适用于所有主题，防止消息过大导致的资源浪费或攻击

	// 每个对等节点的出站消息队列大小
	peerOutboundQueueSize int // 每个对等节点的出站消息队列大小，控制消息的并发发送量

	// 来自其他对等节点的传入消息
	incoming chan *RPC // 处理其他对等节点传入消息的通道，用于接收网络中的消息

	// 用于添加和移除订阅的控制通道
	addSub chan *addSubReq // 添加和移除订阅的控制通道，用于动态管理订阅

	// 用于添加和移除中继的控制通道
	addRelay chan *addRelayReq // 添加和移除中继的控制通道，用于动态管理消息中继

	// 中继取消通道
	rmRelay chan string // 中继取消的控制通道，用于取消中继操作

	// 获取我们订阅的主题列表
	getTopics chan *topicReq // 获取订阅主题列表的请求通道，用于查询当前订阅的主题

	// 获取我们连接的对等节点列表
	getPeers chan *listPeerReq // 获取连接的对等节点列表的请求通道，用于查询当前连接的对等节点

	// 取消订阅的通道
	cancelCh chan *Subscription // 取消订阅的控制通道，用于取消现有订阅

	// 添加主题的通道
	addTopic chan *addTopicReq // 添加主题的控制通道，用于动态创建新的主题

	// 主题取消通道
	rmTopic chan *rmTopicReq // 取消主题的控制通道，用于删除现有主题

	// 新对等节点连接的通知通道
	newPeers       chan struct{}        // 新对等节点连接的通知通道，用于通知有新对等节点连接
	newPeersPrioLk sync.RWMutex         // 保护 newPeersPend 的读写锁，确保并发安全
	newPeersMx     sync.Mutex           // 保护 newPeersPend 的互斥锁，确保并发安全
	newPeersPend   map[peer.ID]struct{} // 新对等节点待处理连接的集合，用于记录待处理的新连接

	// 新出站对等节点流的通知通道
	newPeerStream chan network.Stream // 新出站对等节点流的通知通道，用于通知有新的出站流建立

	// 打开新对等节点流时出错的通知通道
	newPeerError chan peer.ID // 打开新对等节点流时出错的通知通道，用于通知建立出站流时的错误

	// 对等节点断开的通知通道
	peerDead       chan struct{}        // 对等节点断开的通知通道，用于通知有对等节点断开
	peerDeadPrioLk sync.RWMutex         // 保护 peerDeadPend 的读写锁，确保并发安全
	peerDeadMx     sync.Mutex           // 保护 peerDeadPend 的互斥锁，确保并发安全
	peerDeadPend   map[peer.ID]struct{} // 断开对等节点待处理集合，用于记录待处理的断开连接
	// 重新连接到已断开对等节点的重试策略
	deadPeerBackoff *backoff // 重试已断开对等节点连接的退避策略，用于控制重试频率

	// 我们订阅的主题集合
	mySubs map[string]map[*Subscription]struct{} // 当前节点订阅的主题集合，用于管理主题订阅

	// 我们中继的主题集合
	myRelays map[string]int // 当前节点中继的主题集合，用于管理消息中继

	// 我们感兴趣的主题集合
	myTopics map[string]*Topic // 当前节点感兴趣的主题集合，用于跟踪关注的主题

	// 跟踪每个对等节点订阅的主题
	topics map[string]map[peer.ID]struct{} // 跟踪每个对等节点订阅的主题，用于管理对等节点的订阅情况

	// 处理已验证的消息
	sendMsg chan *Message // 处理已验证消息的通道，用于发送经过验证的消息

	// 处理验证器注册请求
	addVal chan *addValReq // 验证器注册请求的控制通道，用于动态添加消息验证器

	// 处理验证器取消注册请求
	rmVal chan *rmValReq // 验证器取消注册请求的控制通道，用于动态移除消息验证器

	// 在事件循环中执行函数
	eval chan func() // 在事件循环中执行的函数通道，用于处理异步任务

	// 对等节点黑名单
	blacklist     Blacklist    // 对等节点黑名单，用于记录被禁用的对等节点
	blacklistPeer chan peer.ID // 对等节点黑名单添加请求的通道，用于动态添加黑名单节点

	// 对等节点的消息通道
	peers map[peer.ID]chan *RPC // 对等节点的消息通道集合，用于管理与每个对等节点的消息传递

	// 入站流互斥锁
	inboundStreamsMx sync.Mutex // 保护入站流的互斥锁，确保并发安全
	// 入站流
	inboundStreams map[peer.ID]network.Stream // 入站流集合，用于记录每个对等节点的入站流

	// 已见消息缓存
	seenMessages timecache.TimeCache // 已见消息缓存，用于存储已处理过的消息，防止重复处理
	// 已见消息缓存的存活时间
	seenMsgTTL time.Duration // 已见消息缓存的存活时间，用于控制消息缓存的有效期
	// 已见消息缓存的策略
	seenMsgStrategy timecache.Strategy // 已见消息缓存的策略，用于定义消息缓存的行为

	// 用于生成消息 ID 的生成器
	idGen *msgIDGenerator // 消息 ID 生成器，用于生成唯一的消息标识符

	// 用于签名消息的密钥，如果签名被禁用则为 nil
	signKey crypto.PrivKey // 签名消息的私钥，用于对消息进行签名，确保消息的真实性
	// 签名消息的源 ID，对应于 signKey，如果签名被禁用则为空。
	// 如果为空，则消息中完全省略作者和序列号。
	signID peer.ID // 签名消息的对等节点 ID，用于标识消息的签名者
	// 严格模式在验证之前拒绝所有未签名的消息
	signPolicy MessageSignaturePolicy // 签名消息的策略，用于控制如何处理未签名的消息

	// 用于跟踪感兴趣主题订阅的过滤器；如果为 nil，则跟踪所有订阅
	subFilter SubscriptionFilter // 订阅过滤器，用于跟踪特定主题的订阅

	// 协议选择的匹配函数
	protoMatchFunc ProtocolMatchFn // 协议选择的匹配函数，用于选择合适的协议处理消息

	// 上下文，用于控制发布-订阅系统的生命周期
	ctx context.Context // 发布-订阅系统的上下文，用于控制系统的生命周期

	// 应用程序特定的 RPC 检查器，用于在处理之前检查传入的 RPC。
	// 检查器函数返回一个错误，指示是否应处理 RPC。
	// 如果错误为 nil，则正常处理 RPC。如果错误为非 nil，则丢弃 RPC。
	appSpecificRpcInspector func(peer.ID, *RPC) error // 应用程序特定的 RPC 检查器，用于在处理之前检查传入的 RPC

	// 回复通道保护锁
	repliesMx sync.Mutex // 锁，用于保护 replies 的并发访问
	// 保存每个消息 ID 对应的回复通道
	replies map[string]chan []byte
	// 等待回复的超时时间
	timeout time.Duration
	// 消息发送的重试次数
	retry int
}

// PubSubRouter 是 PubSub 的消息路由组件
type PubSubRouter interface {
	// Protocols 返回路由器支持的协议列表
	Protocols() []protocol.ID
	// Attach 被 PubSub 构造函数调用以将路由器附加到一个新初始化的 PubSub 实例
	Attach(*PubSub)
	// AddPeer 通知路由器有新的 peer 已连接
	AddPeer(peer.ID, protocol.ID)
	// RemovePeer 通知路由器有 peer 已断开
	RemovePeer(peer.ID)
	// EnoughPeers 返回路由器是否需要更多的 peers 才能准备好发布新记录
	// 参数:
	//   - topic: 主题
	//   - suggested: 建议的 peer 数量（如果大于 0）
	EnoughPeers(topic string, suggested int) bool
	// AcceptFrom 在将消息推送到验证管道或处理控制信息之前调用，用于判断是否接受来自指定 peer 的消息
	// 参数:
	//   - peer.ID: 发送消息的 peer ID
	// 返回值:
	//   - AcceptStatus: 接受状态（全部接受、仅接受控制消息或不接受）
	AcceptFrom(peer.ID) AcceptStatus
	// HandleRPC 处理 RPC 包裹中的控制消息
	// 参数:
	//   - rpc: 要处理的 RPC
	HandleRPC(*RPC)
	// Publish 转发已验证的新消息
	// 参数:
	//   - msg: 要转发的消息
	Publish(*Message)
	// Join 通知路由器我们要接收和转发主题中的消息
	// 参数:
	//   - topic: 要加入的主题
	Join(topic string)
	// Leave 通知路由器我们不再对主题感兴趣
	// 参数:
	//   - topic: 要离开的主题
	Leave(topic string)
}

// AcceptStatus 是表示是否接受传入 RPC 的枚举
type AcceptStatus int

const (
	// AcceptNone 表示丢弃传入的 RPC
	AcceptNone AcceptStatus = iota
	// AcceptControl 表示仅接受传入 RPC 的控制消息
	AcceptControl
	// AcceptAll 表示接受传入 RPC 的全部内容
	AcceptAll
)

// Message 表示一个消息
type Message struct {
	*pb.Message               // 嵌入的 Protocol Buffers 生成的消息结构体
	ID            string      // 消息的唯一标识符
	ReceivedFrom  peer.ID     // 发送该消息的节点ID
	ValidatorData interface{} // 验证器相关数据，可能包含验证消息的元数据
	Local         bool        // 指示消息是否是本地生成的
}

// GetFrom 获取消息的发送者
// 返回值:
//   - peer.ID: 消息的发送者 ID
func (m *Message) GetFrom() peer.ID {
	return peer.ID(m.Message.GetFrom())
}

// RPC 表示一个 RPC 消息
type RPC struct {
	pb.RPC

	// from 是发送此消息的 peer ID，不会通过网络发送
	from peer.ID
}

// Option 是用于配置 PubSub 的选项函数类型
type Option func(*PubSub) error

// NewPubSub 返回一个新的 PubSub 管理对象。
// 参数:
//   - ctx: 用于控制 PubSub 生命周期的上下文。
//   - h: libp2p 主机。
//   - rt: PubSub 路由器。
//   - opts: 可选配置项。
//
// 返回值:
//   - *PubSub: 新的 PubSub 对象。
//   - error: 如果有错误发生，返回错误。
func NewPubSub(ctx context.Context, h host.Host, rt PubSubRouter, opts ...Option) (*PubSub, error) {
	// 初始化 PubSub 对象
	ps := &PubSub{
		host:                  h,                                                                 // libp2p 主机
		ctx:                   ctx,                                                               // 上下文
		rt:                    rt,                                                                // 路由器
		val:                   newValidation(),                                                   // 验证模块
		peerFilter:            DefaultPeerFilter,                                                 // 默认的 peer 过滤器
		disc:                  &discover{},                                                       // 发现模块
		maxMessageSize:        DefaultMaxMessageSize,                                             // 最大消息大小
		peerOutboundQueueSize: 32,                                                                // 出站消息队列大小
		signID:                h.ID(),                                                            // 签名 ID
		signKey:               nil,                                                               // 签名密钥
		signPolicy:            StrictSign,                                                        // 签名策略
		incoming:              make(chan *RPC, 32),                                               // 传入消息通道
		newPeers:              make(chan struct{}, 1),                                            // 新 peer 通知通道
		newPeersPend:          make(map[peer.ID]struct{}),                                        // 待处理的新 peer
		newPeerStream:         make(chan network.Stream),                                         // 新 peer 流通知通道
		newPeerError:          make(chan peer.ID),                                                // 新 peer 错误通知通道
		peerDead:              make(chan struct{}, 1),                                            // peer 死亡通知通道
		peerDeadPend:          make(map[peer.ID]struct{}),                                        // 待处理的死亡 peer
		deadPeerBackoff:       newBackoff(ctx, 1000, BackoffCleanupInterval, MaxBackoffAttempts), // 断开 peer 的重试机制
		cancelCh:              make(chan *Subscription),                                          // 取消订阅通道
		getPeers:              make(chan *listPeerReq),                                           // 获取 peer 列表通道
		addSub:                make(chan *addSubReq),                                             // 添加订阅通道
		addRelay:              make(chan *addRelayReq),                                           // 添加中继通道
		rmRelay:               make(chan string),                                                 // 删除中继通道
		addTopic:              make(chan *addTopicReq),                                           // 添加主题通道
		rmTopic:               make(chan *rmTopicReq),                                            // 删除主题通道
		getTopics:             make(chan *topicReq),                                              // 获取主题列表通道
		sendMsg:               make(chan *Message, 32),                                           // 发送消息通道
		addVal:                make(chan *addValReq),                                             // 添加验证器通道
		rmVal:                 make(chan *rmValReq),                                              // 删除验证器通道
		eval:                  make(chan func()),                                                 // 评估通道
		myTopics:              make(map[string]*Topic),                                           // 我们感兴趣的主题
		mySubs:                make(map[string]map[*Subscription]struct{}),                       // 我们的订阅
		myRelays:              make(map[string]int),                                              // 我们的中继
		topics:                make(map[string]map[peer.ID]struct{}),                             // 主题到 peer 的映射
		peers:                 make(map[peer.ID]chan *RPC),                                       // peer 到 RPC 通道的映射
		inboundStreams:        make(map[peer.ID]network.Stream),                                  // inbound 流
		blacklist:             NewMapBlacklist(),                                                 // 黑名单
		blacklistPeer:         make(chan peer.ID),                                                // 黑名单 peer 通道
		seenMsgTTL:            TimeCacheDuration,                                                 // 已看到消息的生存时间
		seenMsgStrategy:       TimeCacheStrategy,                                                 // 已看到消息的策略
		idGen:                 newMsgIdGenerator(),                                               // 消息 ID 生成器
		counter:               uint64(time.Now().UnixNano()),                                     // 计数器
		replies:               make(map[string]chan []byte),                                      // 保存每个消息 ID 对应的回复通道
		timeout:               30 * time.Second,                                                  // 等待回复的超时时间
		retry:                 3,                                                                 // 消息发送的重试次数
	}

	// 应用所有选项配置
	for _, opt := range opts {
		err := opt(ps)
		if err != nil {
			// 如果配置选项应用失败，返回错误
			return nil, err
		}
	}

	// 检查签名策略是否必须签名
	if ps.signPolicy.mustSign() {
		// 如果签名策略要求消息必须签名，但签名 ID 未设置，则返回错误
		if ps.signID == "" {
			return nil, fmt.Errorf("strict signature usage enabled but message author was disabled")
		}
		// 从主机的 Peerstore 中获取对应签名 ID 的私钥
		ps.signKey = ps.host.Peerstore().PrivKey(ps.signID)
		if ps.signKey == nil {
			// 如果无法获取签名私钥，则返回错误
			return nil, fmt.Errorf("can't sign for peer %s: no private key", ps.signID)
		}
	}

	// 初始化已看到消息的缓存
	ps.seenMessages = timecache.NewTimeCacheWithStrategy(ps.seenMsgStrategy, ps.seenMsgTTL)

	// 启动发现模块
	if err := ps.disc.Start(ps); err != nil {
		// 如果发现模块启动失败，返回错误
		return nil, err
	}

	// 附加路由器
	rt.Attach(ps)

	// 设置流处理器
	for _, id := range rt.Protocols() {
		// 如果设置了协议匹配函数，使用该函数设置流处理器
		if ps.protoMatchFunc != nil {
			h.SetStreamHandlerMatch(id, ps.protoMatchFunc(id), ps.handleNewStream) // SetStreamHandlerMatch 允许使用自定义匹配函数，在协议 ID 匹配的基础上，进一步筛选是否应用处理器。
			/**
			SetStreamHandlerMatch
			用途: 为指定的协议 ID 设置一个协议处理器，但使用一个匹配函数来选择是否应用该处理器。
			工作方式: 在协议匹配时，不仅根据协议 ID 判断，还可以使用自定义的匹配函数 (match function)。该函数可以实现更复杂的逻辑，比如根据协议版本号或其他协议字段来决定是否使用指定的协议处理器。
			示例:
				h.SetStreamHandlerMatch("/myprotocol/1.0.0", myMatchFunc, myHandler)
				这里，当主机接收到协议 ID 为 "/myprotocol/1.0.0" 的流时，libp2p 将首先调用 myMatchFunc 来决定是否使用 myHandler 处理该流。
			*/
		} else {
			// 否则，直接设置流处理器
			h.SetStreamHandler(id, ps.handleNewStream) // SetStreamHandler 仅基于协议 ID 的直接匹配。
			/**
			SetStreamHandler
			用途: 直接为指定的协议 ID 设置一个协议处理器。
			工作方式: 当主机接收到一个传入的流 (stream)，并且流的协议与设置的协议 ID 相匹配时，libp2p 主机将使用指定的协议处理器来处理该流。
			示例:
				h.SetStreamHandler("/myprotocol/1.0.0", myHandler)
				这里，当主机接收到协议 ID 为 "/myprotocol/1.0.0" 的流时，将调用 myHandler 来处理该流。
			*/
		}
	}

	// 监视新 peer
	go ps.watchForNewPeers(ctx)

	// 启动验证模块
	ps.val.Start(ps)

	// 启动处理循环
	go ps.processLoop(ctx)

	return ps, nil
}

// MsgIdFunction 返回传递的消息的唯一 ID，PubSub 可以通过配置 Option from WithMessageIdFn 使用任何此函数的实现。
// 参数:
//   - pmsg: 要计算 ID 的消息。
//
// 返回值:
//   - string: 消息 ID。
type MsgIdFunction func(pmsg *pb.Message) string

// WithMessageIdFn 是一个选项，用于自定义为 pubsub 消息计算消息 ID 的方式。
// 默认的 ID 函数是 DefaultMsgIdFn（连接源和序列号），但它可以自定义为消息的哈希值。
// 参数:
//   - fn: 自定义的消息 ID 函数。
//
// 返回值:
//   - Option: 配置选项。
func WithMessageIdFn(fn MsgIdFunction) Option {
	return func(p *PubSub) error {
		p.idGen.Default = fn
		return nil
	}
}

// PeerFilter 用于过滤 pubsub peers。对于给定的主题，它应该返回 true 表示接受。
// 参数:
//   - pid: peer 的 ID。
//   - topic: 主题。
//
// 返回值:
//   - bool: 是否接受该 peer。
type PeerFilter func(pid peer.ID, topic string) bool

// WithPeerFilter 是一个选项，用于设置 pubsub peers 的过滤器。
// 默认的 peer 过滤器是 DefaultPeerFilter（总是返回 true），但它可以自定义为任何自定义实现。
// 参数:
//   - filter: 自定义的 peer 过滤器。
//
// 返回值:
//   - Option: 配置选项。
func WithPeerFilter(filter PeerFilter) Option {
	return func(p *PubSub) error {
		p.peerFilter = filter
		return nil
	}
}

// WithPeerOutboundQueueSize 是一个选项，用于设置对 peer 的出站消息缓冲区大小。
// 当出站队列已满时，我们开始丢弃消息。
// 参数:
//   - size: 出站消息队列的大小。
//
// 返回值:
//   - Option: 配置选项。
func WithPeerOutboundQueueSize(size int) Option {
	return func(p *PubSub) error {
		if size <= 0 {
			return errors.New("outbound queue size must always be positive")
		}
		p.peerOutboundQueueSize = size
		return nil
	}
}

// WithMessageSignaturePolicy 设置生产和验证消息签名的操作模式。
// 参数:
//   - policy: 签名策略。
//
// 返回值:
//   - Option: 配置选项。
func WithMessageSignaturePolicy(policy MessageSignaturePolicy) Option {
	return func(p *PubSub) error {
		p.signPolicy = policy
		return nil
	}
}

// WithMessageSigning 启用或禁用消息签名（默认启用）。
// 不推荐在没有消息签名或没有验证的情况下使用。
// 参数:
//   - enabled: 是否启用消息签名。
//
// 返回值:
//   - Option: 配置选项。
func WithMessageSigning(enabled bool) Option {
	return func(p *PubSub) error {
		if enabled {
			p.signPolicy |= msgSigning
		} else {
			p.signPolicy &^= msgSigning
		}
		return nil
	}
}

// WithMessageAuthor 设置出站消息的作者为给定的 peer ID（默认为主机的 ID）。
// 如果启用了消息签名，则私钥必须在主机的 peerstore 中可用。
// 参数:
//   - author: 消息作者的 ID。
//
// 返回值:
//   - Option: 配置选项。
func WithMessageAuthor(author peer.ID) Option {
	return func(p *PubSub) error {
		if author == "" {
			author = p.host.ID()
		}
		p.signID = author
		return nil
	}
}

// WithNoAuthor 省略消息的作者和序列号数据，并禁用签名的使用。
// 不推荐与默认的消息 ID 函数一起使用，请参阅 WithMessageIdFn。
// 返回值:
//   - Option: 配置选项。
func WithNoAuthor() Option {
	return func(p *PubSub) error {
		p.signID = ""
		p.signPolicy &^= msgSigning
		return nil
	}
}

// WithStrictSignatureVerification 是一个选项，用于启用或禁用严格的消息签名验证。
// 当启用时（这是默认设置），未签名的消息将被丢弃。
// 不推荐在没有消息签名或没有验证的情况下使用。
// 参数:
//   - required: 是否要求严格签名验证。
//
// 返回值:
//   - Option: 配置选项。
func WithStrictSignatureVerification(required bool) Option {
	return func(p *PubSub) error {
		if required {
			p.signPolicy |= msgVerification
		} else {
			p.signPolicy &^= msgVerification
		}
		return nil
	}
}

// WithBlacklist 提供黑名单的实现；默认是 MapBlacklist。
// 参数:
//   - b: 黑名单实现。
//
// 返回值:
//   - Option: 配置选项。
func WithBlacklist(b Blacklist) Option {
	return func(p *PubSub) error {
		p.blacklist = b
		return nil
	}
}

// WithDiscovery 提供用于引导和提供 peers 到 PubSub 的发现机制。
// 参数:
//   - d: 发现机制。
//   - opts: 可选的发现配置。
//
// 返回值:
//   - Option: 配置选项。
func WithDiscovery(d discovery.Discovery, opts ...DiscoverOpt) Option {
	return func(p *PubSub) error {
		discoverOpts := defaultDiscoverOptions()
		for _, opt := range opts {
			err := opt(discoverOpts)
			if err != nil {
				return err
			}
		}

		p.disc.discovery = &pubSubDiscovery{Discovery: d, opts: discoverOpts.opts}
		p.disc.options = discoverOpts
		return nil
	}
}

// WithEventTracer 提供 pubsub 系统的事件追踪器。
// 参数:
//   - tracer: 事件追踪器。
//
// 返回值:
//   - Option: 配置选项。
func WithEventTracer(tracer EventTracer) Option {
	return func(p *PubSub) error {
		if p.tracer != nil {
			p.tracer.tracer = tracer
		} else {
			p.tracer = &pubsubTracer{tracer: tracer, pid: p.host.ID(), idGen: p.idGen}
		}
		return nil
	}
}

// WithRawTracer 添加一个原始追踪器到 pubsub 系统。
// 可以使用多次调用选项添加多个追踪器。
// 参数:
//   - tracer: 原始追踪器。
//
// 返回值:
//   - Option: 配置选项。
func WithRawTracer(tracer RawTracer) Option {
	return func(p *PubSub) error {
		if p.tracer != nil {
			p.tracer.raw = append(p.tracer.raw, tracer)
		} else {
			p.tracer = &pubsubTracer{raw: []RawTracer{tracer}, pid: p.host.ID(), idGen: p.idGen}
		}
		return nil
	}
}

// WithMaxMessageSize 设置 pubsub 消息的全局最大消息大小。默认值是 1MiB (DefaultMaxMessageSize)。
// 警告 #1：确保更改 floodsub (FloodSubID) 和 gossipsub (GossipSubID) 的默认协议前缀。
// 警告 #2：减少默认的最大消息限制是可以的，但要确保您的应用程序消息不会超过新的限制。
// 参数:
//   - maxMessageSize: 最大消息大小。
//
// 返回值:
//   - Option: 配置选项。
func WithMaxMessageSize(maxMessageSize int) Option {
	return func(ps *PubSub) error {
		ps.maxMessageSize = maxMessageSize
		return nil
	}
}

// WithProtocolMatchFn 设置用于协议选择的自定义匹配函数。
// 参数:
//   - m: 协议匹配函数。
//
// 返回值:
//   - Option: 配置选项。
func WithProtocolMatchFn(m ProtocolMatchFn) Option {
	return func(ps *PubSub) error {
		ps.protoMatchFunc = m
		return nil
	}
}

// WithSeenMessagesTTL 配置之前看到的消息 ID 可以被遗忘的时间。
// 参数:
//   - ttl: 生存时间。
//
// 返回值:
//   - Option: 配置选项。
func WithSeenMessagesTTL(ttl time.Duration) Option {
	return func(ps *PubSub) error {
		ps.seenMsgTTL = ttl
		return nil
	}
}

// WithSeenMessagesStrategy 配置已看到消息缓存使用的查找/清理策略。
// 参数:
//   - strategy: 策略。
//
// 返回值:
//   - Option: 配置选项。
func WithSeenMessagesStrategy(strategy timecache.Strategy) Option {
	return func(ps *PubSub) error {
		ps.seenMsgStrategy = strategy
		return nil
	}
}

// WithAppSpecificRpcInspector 设置一个钩子，用于在处理传入的 RPC 之前检查它们。
// 检查器在处理已接受的 RPC 之前调用。如果检查器的错误为 nil，则按常规处理 RPC。否则，RPC 将被丢弃。
// 参数:
//   - inspector: 检查函数。
//
// 返回值:
//   - Option: 配置选项。
func WithAppSpecificRpcInspector(inspector func(peer.ID, *RPC) error) Option {
	return func(ps *PubSub) error {
		ps.appSpecificRpcInspector = inspector
		return nil
	}
}

// WithTimeout 设置等待回复的超时时间
// 参数:
// - d: time.Duration 表示超时时间
//
// 返回值:
//   - Option: 配置选项。
func WithTimeout(d time.Duration) Option {
	return func(ps *PubSub) error {
		ps.timeout = d
		return nil
	}
}

// WithRetry 设置发送消息的重试次数
// 参数:
// - retries: int 表示重试次数
//
// 返回值:
//   - Option: 配置选项。
func WithRetry(retries int) Option {
	return func(ps *PubSub) error {
		ps.retry = retries
		return nil
	}
}

// processLoop 处理所有到达通道的输入
// 参数:
//   - ctx: 上下文，用于控制生命周期
func (p *PubSub) processLoop(ctx context.Context) {
	defer func() {
		// 清理 go 例程
		for _, ch := range p.peers { // 遍历所有 peers
			close(ch) // 关闭 peer 通道
		}
		p.peers = nil         // 清空 peers
		p.topics = nil        // 清空 topics
		p.seenMessages.Done() // 标记 seenMessages 完成
	}()

	for { // 处理所有到达通道的输入
		select {
		case <-p.newPeers: // 处理新加入的 peers
			p.handlePendingPeers() // 处理待处理的 peers

		case s := <-p.newPeerStream: // 处理新流事件
			pid := s.Conn().RemotePeer() // 获取新流的 peer ID

			ch, ok := p.peers[pid] // 检查是否为已知 peer
			if !ok {
				logger.Warnf("新流来自未知节点: %s", pid) // 记录未知 peer 的新流
				s.Reset()                         // 重置流
				continue
			}

			if p.blacklist.Contains(pid) { // 如果 peer 在黑名单中
				logger.Warnf("关闭黑名单节点 %s 的流", pid) // 记录黑名单 peer 的流
				close(ch)                          // 关闭通道
				delete(p.peers, pid)               // 从 peers 中删除
				s.Reset()                          // 重置流
				continue
			}

			p.rt.AddPeer(pid, s.Protocol()) // 添加 peer 到路由器

		case pid := <-p.newPeerError: // 处理新 peer 错误事件
			delete(p.peers, pid) // 删除发生错误的 peer

		case <-p.peerDead: // 处理死亡的 peers 事件
			p.handleDeadPeers() // 处理死亡的 peers

		case treq := <-p.getTopics: // 处理获取主题请求
			var out []string          // 初始化输出主题列表
			for t := range p.mySubs { // 获取我们订阅的所有主题
				out = append(out, t)
			}
			treq.resp <- out // 发送主题列表响应

		case topic := <-p.addTopic: // 处理添加主题请求
			p.handleAddTopic(topic) // 处理添加主题请求

		case topic := <-p.rmTopic: // 处理删除主题请求
			p.handleRemoveTopic(topic) // 处理删除主题请求

		case sub := <-p.cancelCh: // 处理取消订阅请求
			p.handleRemoveSubscription(sub) // 处理取消订阅请求

		case sub := <-p.addSub: // 处理添加订阅请求
			p.handleAddSubscription(sub) // 处理添加订阅请求

		case relay := <-p.addRelay: // 处理添加中继请求
			p.handleAddRelay(relay) // 处理添加中继请求

		case topic := <-p.rmRelay: // 处理删除中继请求
			p.handleRemoveRelay(topic) // 处理删除中继请求

		case preq := <-p.getPeers: // 处理获取 peers 请求
			tmap, ok := p.topics[preq.topic] // 获取主题对应的 peers
			if preq.topic != "" && !ok {     // 如果主题不存在，返回 nil
				preq.resp <- nil
				continue
			}
			var peers []peer.ID      // 初始化 peers 列表
			for p := range p.peers { // 遍历所有 peers
				if preq.topic != "" {
					_, ok := tmap[p]
					if !ok {
						continue
					}
				}
				peers = append(peers, p) // 获取所有与主题相关的 peers
			}
			preq.resp <- peers // 发送 peers 列表响应

		case rpc := <-p.incoming: // 处理传入的 RPC 请求
			p.handleIncomingRPC(rpc) // 处理传入的 RPC

		case msg := <-p.sendMsg: // 处理发送消息请求
			p.publishMessage(msg) // 发布消息

		case req := <-p.addVal: // 处理添加验证器请求
			p.val.AddValidator(req) // 添加验证器

		case req := <-p.rmVal: // 处理删除验证器请求
			p.val.RemoveValidator(req) // 删除验证器

		case thunk := <-p.eval: // 处理评估函数请求
			thunk() // 执行评估函数

		case pid := <-p.blacklistPeer: // 处理黑名单 peer 请求
			logger.Infof("将节点 %s 加入黑名单", pid) // 记录黑名单操作
			p.blacklist.Add(pid)              // 添加到黑名单

			ch, ok := p.peers[pid] // 检查 peer 是否存在
			if ok {
				close(ch)                       // 关闭黑名单 peer 的通道
				delete(p.peers, pid)            // 从 peers 中删除
				for t, tmap := range p.topics { // 遍历所有主题
					if _, ok := tmap[pid]; ok {
						delete(tmap, pid)     // 从主题中删除 peer
						p.notifyLeave(t, pid) // 通知离开事件
					}
				}
				p.rt.RemovePeer(pid) // 从路由器中移除 peer
			}

		case <-ctx.Done(): // 处理上下文完成事件
			logger.Info("pubsub 进程循环关闭") // 记录进程循环关闭
			return                       // 退出函数
		}
	}
}

// handlePendingPeers 处理待处理的 peers
func (p *PubSub) handlePendingPeers() {
	p.newPeersPrioLk.Lock() // 锁定 newPeersPrioLk 以防止并发修改

	if len(p.newPeersPend) == 0 { // 如果没有待处理的 peers
		p.newPeersPrioLk.Unlock() // 解锁 newPeersPrioLk
		return                    // 退出函数
	}

	newPeers := p.newPeersPend                  // 复制待处理的 peers
	p.newPeersPend = make(map[peer.ID]struct{}) // 清空待处理的 peers
	p.newPeersPrioLk.Unlock()                   // 解锁 newPeersPrioLk

	for pid := range newPeers { // 遍历每个新的 peer
		// 确保我们有一个非受限的连接
		if p.host.Network().Connectedness(pid) != network.Connected {
			continue // 如果没有连接，跳过
		}

		if _, ok := p.peers[pid]; ok { // 检查 peer 是否已存在
			logger.Debugf("已连接到节点 %s", pid) // 记录已连接的 peer
			continue                        // 如果已连接，跳过
		}

		if p.blacklist.Contains(pid) { // 检查 peer 是否在黑名单中
			logger.Warnf("忽略来自黑名单节点 %s 的连接", pid) // 记录黑名单 peer
			continue                              // 如果在黑名单中，跳过
		}

		messages := make(chan *RPC, p.peerOutboundQueueSize) // 创建消息通道，大小为 peerOutboundQueueSize
		messages <- p.getHelloPacket()                       // 发送 hello 包
		go p.handleNewPeer(p.ctx, pid, messages)             // 启动新的 goroutine 处理新 peer
		p.peers[pid] = messages                              // 将消息通道添加到 peers
	}
}

// handleDeadPeers 处理死亡的 peers
func (p *PubSub) handleDeadPeers() {
	p.peerDeadPrioLk.Lock() // 锁定 peerDeadPrioLk 以防止并发修改

	if len(p.peerDeadPend) == 0 { // 如果没有死亡的 peers
		p.peerDeadPrioLk.Unlock() // 解锁 peerDeadPrioLk
		return                    // 退出函数
	}

	deadPeers := p.peerDeadPend                 // 复制死亡的 peers
	p.peerDeadPend = make(map[peer.ID]struct{}) // 清空死亡的 peers
	p.peerDeadPrioLk.Unlock()                   // 解锁 peerDeadPrioLk

	for pid := range deadPeers { // 遍历每个死亡的 peer
		ch, ok := p.peers[pid] // 获取 peer 的消息通道
		if !ok {               // 如果消息通道不存在
			continue // 跳过
		}

		close(ch)            // 关闭死亡 peer 的通道
		delete(p.peers, pid) // 从 peers 中删除

		for t, tmap := range p.topics { // 遍历所有主题
			if _, ok := tmap[pid]; ok { // 检查主题中是否包含该 peer
				delete(tmap, pid)     // 从主题中删除 peer
				p.notifyLeave(t, pid) // 通知离开
			}
		}

		p.rt.RemovePeer(pid) // 从路由器中移除 peer

		if p.host.Network().Connectedness(pid) == network.Connected { // 如果 peer 仍然连接
			backoffDelay, err := p.deadPeerBackoff.updateAndGet(pid) // 获取退避延迟时间
			if err != nil {
				logger.Debug(err) // 记录错误
				continue          // 跳过
			}

			// 仍然连接，必须是重复连接被关闭。
			// 我们重新启动 writer，因为我们需要确保有一个活动的流
			logger.Debugf("节点 %s 声明死亡但仍然连接; 重新生成 writer", pid)                // 记录重新生成 writer 的操作
			messages := make(chan *RPC, p.peerOutboundQueueSize)              // 创建新的消息通道
			messages <- p.getHelloPacket()                                    // 发送 hello 包
			p.peers[pid] = messages                                           // 将消息通道添加到 peers
			go p.handleNewPeerWithBackoff(p.ctx, pid, backoffDelay, messages) // 启动新的 goroutine 处理带退避延迟的新 peer
		}
	}
}

// handleAddTopic 添加一个主题的跟踪器。
// 只从 processLoop 调用。
// 参数:
//   - req: addTopicReq 请求，包含要添加的主题
func (p *PubSub) handleAddTopic(req *addTopicReq) {
	topic := req.topic     // 获取请求中的主题
	topicID := topic.topic // 获取主题 ID

	t, ok := p.myTopics[topicID] // 检查主题是否已经存在
	if ok {
		req.resp <- t // 如果主题已存在，返回该主题
		return
	}

	p.myTopics[topicID] = topic // 添加新主题到 myTopics
	req.resp <- topic           // 返回新添加的主题
}

// handleRemoveTopic 从账本中移除主题跟踪器。
// 只从 processLoop 调用。
// 参数:
//   - req: rmTopicReq 请求，包含要移除的主题
func (p *PubSub) handleRemoveTopic(req *rmTopicReq) {
	topic := p.myTopics[req.topic.topic] // 获取要移除的主题

	if topic == nil {
		req.resp <- nil // 如果主题不存在，返回 nil
		return
	}

	// 如果没有事件处理程序、订阅和中继，则删除主题
	if len(topic.evtHandlers) == 0 &&
		len(p.mySubs[req.topic.topic]) == 0 &&
		p.myRelays[req.topic.topic] == 0 {
		delete(p.myTopics, topic.topic) // 从 myTopics 中删除主题
		req.resp <- nil
		return
	}

	req.resp <- fmt.Errorf("无法关闭主题: 存在事件处理程序或订阅") // 不能关闭主题的错误
}

// handleRemoveSubscription 从账本中移除订阅。
// 如果这是最后一个订阅且没有更多的中继存在于给定的主题中，
// 它还将宣布该节点不再订阅此主题。
// 只从 processLoop 调用。
// 参数:
//   - sub: 要移除的订阅
func (p *PubSub) handleRemoveSubscription(sub *Subscription) {
	subs := p.mySubs[sub.topic] // 获取主题的所有订阅

	if subs == nil {
		return
	}

	sub.err = ErrSubscriptionCancelled // 设置订阅取消错误
	sub.close()                        // 关闭订阅
	delete(subs, sub)                  // 从订阅列表中删除订阅

	if len(subs) == 0 {
		delete(p.mySubs, sub.topic) // 如果订阅列表为空，删除主题订阅

		// 仅当没有更多订阅和中继时停止广告
		if p.myRelays[sub.topic] == 0 {
			p.disc.StopAdvertise(sub.topic) // 停止广告
			p.announce(sub.topic, false)    // 宣布离开主题
			p.rt.Leave(sub.topic)           // 从路由器中离开主题
		}
	}
}

// handleAddSubscription 添加一个特定主题的订阅。
// 如果这是第一个订阅且目前没有中继存在于主题中，
// 它将宣布该节点订阅此主题。
// 只从 processLoop 调用。
// 参数:
//   - req: addSubReq 请求，包含要添加的订阅
func (p *PubSub) handleAddSubscription(req *addSubReq) {
	sub := req.sub              // 获取请求中的订阅
	subs := p.mySubs[sub.topic] // 获取主题的所有订阅

	// 如果订阅和中继不存在，则宣布我们需要此主题
	if len(subs) == 0 && p.myRelays[sub.topic] == 0 {
		p.disc.Advertise(sub.topic) // 广告此主题
		p.announce(sub.topic, true) // 宣布订阅主题
		p.rt.Join(sub.topic)        // 加入主题
	}

	// 如果订阅列表不存在，则创建新订阅列表
	if subs == nil {
		p.mySubs[sub.topic] = make(map[*Subscription]struct{})
	}

	sub.cancelCh = p.cancelCh // 设置订阅的取消通道

	p.mySubs[sub.topic][sub] = struct{}{} // 添加订阅到订阅列表

	req.resp <- sub // 返回新添加的订阅
}

// handleAddRelay 添加一个特定主题的中继。
// 如果这是第一个中继且目前没有订阅存在于主题中，
// 它将宣布该节点为主题中继。
// 只从 processLoop 调用。
// 参数:
//   - req: addRelayReq 请求，包含要添加的中继
func (p *PubSub) handleAddRelay(req *addRelayReq) {
	topic := req.topic // 获取请求中的主题

	p.myRelays[topic]++ // 增加主题的中继计数

	// 如果中继和订阅不存在，则宣布我们需要此主题
	if p.myRelays[topic] == 1 && len(p.mySubs[topic]) == 0 {
		p.disc.Advertise(topic) // 广告此主题
		p.announce(topic, true) // 宣布订阅主题
		p.rt.Join(topic)        // 加入主题
	}

	// 标志用于防止多次调用取消函数
	isCancelled := false

	relayCancelFunc := func() {
		if isCancelled {
			return
		}

		select {
		case p.rmRelay <- topic: // 移除中继
			isCancelled = true
		case <-p.ctx.Done(): // 上下文完成
		}
	}

	req.resp <- relayCancelFunc // 返回中继取消函数
}

// handleRemoveRelay 从账本中移除一个中继引用。
// 如果这是最后一个中继引用且没有更多的订阅存在于给定的主题中，
// 它还将宣布该节点不再为此主题进行中继。
// 只从 processLoop 调用。
// 参数:
//   - topic: 要移除中继的主题
func (p *PubSub) handleRemoveRelay(topic string) {
	if p.myRelays[topic] == 0 {
		return // 如果没有中继，直接返回
	}

	p.myRelays[topic]-- // 减少中继计数

	if p.myRelays[topic] == 0 {
		delete(p.myRelays, topic) // 如果中继计数为零，从 myRelays 中删除该主题

		// 仅当没有更多的中继和订阅时停止广告
		if len(p.mySubs[topic]) == 0 {
			p.disc.StopAdvertise(topic) // 停止广告
			p.announce(topic, false)    // 宣布离开主题
			p.rt.Leave(topic)           // 从路由器中离开主题
		}
	}
}

// announce 宣布该节点是否对给定主题感兴趣
// 只从 processLoop 调用。
// 参数:
//   - topic: 要宣布的主题
//   - sub: 是否订阅该主题
func (p *PubSub) announce(topic string, sub bool) {
	subopt := &pb.RPC_SubOpts{
		Topicid:   topic, // 设置主题 ID
		Subscribe: sub,   // 设置订阅标志
	}

	out := rpcWithSubs(subopt) // 创建包含订阅选项的 RPC
	for pid, peer := range p.peers {
		select {
		case peer <- out: // 发送 RPC 给 peer
			p.tracer.SendRPC(out, pid) // 追踪发送的 RPC
		default:
			logger.Infof("无法发送宣布消息到节点 %s: 队列已满; 调度重试", pid)
			p.tracer.DropRPC(out, pid)          // 追踪丢弃的 RPC
			go p.announceRetry(pid, topic, sub) // 调度重试
		}
	}
}

// announceRetry 在队列已满时调度重试
// 参数:
//   - pid: 要重试的 peer ID
//   - topic: 主题
//   - sub: 是否订阅该主题
func (p *PubSub) announceRetry(pid peer.ID, topic string, sub bool) {
	time.Sleep(time.Duration(1+rand.Intn(1000)) * time.Millisecond) // 随机延迟

	retry := func() {
		_, okSubs := p.mySubs[topic]
		_, okRelays := p.myRelays[topic]

		ok := okSubs || okRelays

		if (ok && sub) || (!ok && !sub) {
			p.doAnnounceRetry(pid, topic, sub) // 执行重试
		}
	}

	select {
	case p.eval <- retry: // 调度重试函数
	case <-p.ctx.Done():
	}
}

// doAnnounceRetry 执行实际的重试逻辑
// 参数:
//   - pid: 要重试的 peer ID
//   - topic: 主题
//   - sub: 是否订阅该主题
func (p *PubSub) doAnnounceRetry(pid peer.ID, topic string, sub bool) {
	peer, ok := p.peers[pid]
	if !ok {
		return // 如果 peer 不存在，直接返回
	}

	subopt := &pb.RPC_SubOpts{
		Topicid:   topic, // 设置主题 ID
		Subscribe: sub,   // 设置订阅标志
	}

	out := rpcWithSubs(subopt) // 创建包含订阅选项的 RPC
	select {
	case peer <- out: // 发送 RPC 给 peer
		p.tracer.SendRPC(out, pid) // 追踪发送的 RPC
	default:
		logger.Infof("无法发送宣布消息到节点 %s: 队列已满; 调度重试", pid)
		p.tracer.DropRPC(out, pid)          // 追踪丢弃的 RPC
		go p.announceRetry(pid, topic, sub) // 调度重试
	}
}

// notifySubs 向所有对应的订阅者发送给定消息。
// 只从 processLoop 调用。
// 参数:
//   - msg: 要通知的消息
func (p *PubSub) notifySubs(msg *Message) {
	// 如果消息具有元数据且为响应类型
	if msg.Metadata != nil && msg.Metadata.Type == pb.MessageMetadata_RESPONSE {
		p.handleResponse(msg)
		return
	}

	topic := msg.GetTopic() // 获取消息的主题
	subs := p.mySubs[topic] // 获取主题的所有订阅
	for f := range subs {
		// 检查消息是否来自订阅者自己
		if msg.ReceivedFrom == p.host.ID() {
			continue // 如果是，跳过这个订阅者
		}

		select {
		case f.ch <- msg: // 发送消息给订阅者
		default:
			p.tracer.UndeliverableMessage(msg) // 追踪未能递送的消息
			logger.Infof("无法递送消息到主题 %s 的订阅者; 订阅者处理速度过慢", topic)
		}
	}
}

// handleResponse 处理响应类型的消息，将其发送到对应的回复通道。
// 参数:
//   - msg: 要处理的响应消息
func (p *PubSub) handleResponse(msg *Message) {
	if msg.Metadata == nil || msg.Metadata.MessageID == "" {
		logger.Warn("收到的响应消息缺少有效的元数据或消息ID")
		return
	}

	// 查找对应消息ID的回复通道
	// 使用锁保护对 replies 的访问
	p.repliesMx.Lock()
	replyChan, ok := p.replies[msg.Metadata.MessageID]
	if ok {
		select {
		case replyChan <- msg.Data: // 将响应消息的数据发送到通道
			// 如果成功发送数据到通道，删除这个通道，以忽略后续响应
			delete(p.replies, msg.Metadata.MessageID)
		default:
			// 如果通道已关闭或已经接收到数据，直接忽略后续响应
			// 不执行任何操作
		}
	}
	p.repliesMx.Unlock()
}

// seenMessage 返回我们之前是否已经看到此消息
// 参数:
//   - id: 消息 ID
//
// 返回值:
//   - bool: 是否已看到此消息
func (p *PubSub) seenMessage(id string) bool {
	return p.seenMessages.Has(id) // 检查消息是否在缓存中
}

// markSeen 标记消息为已看到，使得 seenMessage 为给定 ID 返回 `true`
// 返回是否消息是新标记的
// 参数:
//   - id: 消息 ID
//
// 返回值:
//   - bool: 是否新标记的消息
func (p *PubSub) markSeen(id string) bool {
	return p.seenMessages.Add(id) // 添加消息到缓存
}

// subscribedToMsg 返回我们是否订阅了给定消息的一个主题
// 参数:
//   - msg: 要检查的消息
//
// 返回值:
//   - bool: 是否订阅了消息的主题
func (p *PubSub) subscribedToMsg(msg *pb.Message) bool {
	if len(p.mySubs) == 0 {
		return false // 如果没有订阅，返回 false
	}

	topic := msg.GetTopic() // 获取消息的主题
	_, ok := p.mySubs[topic]

	return ok
}

// canRelayMsg 返回我们是否能够为给定消息的一个主题进行中继
// 参数:
//   - msg: 要检查的消息
//
// 返回值:
//   - bool: 是否能够中继消息
func (p *PubSub) canRelayMsg(msg *pb.Message) bool {
	if len(p.myRelays) == 0 {
		return false // 如果没有中继，返回 false
	}

	topic := msg.GetTopic() // 获取消息的主题
	relays := p.myRelays[topic]

	return relays > 0
}

// notifyLeave 通知主题的离开事件
// 参数:
//   - topic: 主题
//   - pid: 离开的 peer ID
func (p *PubSub) notifyLeave(topic string, pid peer.ID) {
	if t, ok := p.myTopics[topic]; ok {
		t.sendNotification(PeerEvent{PeerLeave, pid}) // 发送离开通知
	}
}

// handleIncomingRPC 处理传入的 RPC 消息
// 参数:
//   - rpc: 传入的 RPC 消息指针
func (p *PubSub) handleIncomingRPC(rpc *RPC) {
	// 通过应用程序特定的验证（如果有）。
	if p.appSpecificRpcInspector != nil {
		// 检查 RPC 是否被外部检查器允许
		if err := p.appSpecificRpcInspector(rpc.from, rpc); err != nil {
			// 如果检查失败，记录调试信息并拒绝该 RPC
			logger.Debugf("应用程序特定的检查失败，拒绝传入的 RPC: %s", err)
			return // 拒绝该 RPC
		}
	}

	// 追踪接收到的 RPC，用于调试或监控目的
	p.tracer.RecvRPC(rpc)

	// 获取 RPC 消息中的订阅信息
	subs := rpc.GetSubscriptions()
	// 如果存在订阅且设置了订阅过滤器，则应用订阅过滤器
	if len(subs) != 0 && p.subFilter != nil {
		var err error
		// 过滤传入的订阅，确保其符合规则
		subs, err = p.subFilter.FilterIncomingSubscriptions(rpc.from, subs)
		if err != nil {
			// 如果过滤器发生错误，记录调试信息并忽略该 RPC
			logger.Debugf("订阅过滤器错误: %s; 忽略 RPC", err)
			return
		}
	}

	// 遍历所有订阅选项，更新订阅状态
	for _, subopt := range subs {
		t := subopt.GetTopicid() // 获取订阅的主题 ID

		if subopt.GetSubscribe() {
			// 如果是订阅请求，处理新的订阅
			tmap, ok := p.topics[t] // 获取当前主题的订阅者集合
			if !ok {
				// 如果该主题没有订阅者，创建一个新的订阅者集合
				tmap = make(map[peer.ID]struct{})
				p.topics[t] = tmap
			}

			if _, ok = tmap[rpc.from]; !ok {
				// 如果 peer 尚未订阅该主题，将其加入订阅者集合
				tmap[rpc.from] = struct{}{}
				if topic, ok := p.myTopics[t]; ok {
					// 如果该主题存在于当前节点的主题集合中，发送通知
					peer := rpc.from
					topic.sendNotification(PeerEvent{PeerJoin, peer})
				}
			}
		} else {
			// 如果是取消订阅请求，处理取消订阅
			tmap, ok := p.topics[t] // 获取当前主题的订阅者集合
			if !ok {
				continue // 如果没有订阅者，跳过该主题
			}

			if _, ok := tmap[rpc.from]; ok {
				// 如果 peer 已订阅该主题，将其从订阅者集合中删除
				delete(tmap, rpc.from)
				p.notifyLeave(t, rpc.from) // 通知其他节点该 peer 已离开
			}
		}
	}

	// 请求路由器验证 peer，确保资源不被浪费
	switch p.rt.AcceptFrom(rpc.from) {
	case AcceptNone:
		// 如果路由器将该 peer 标记为灰名单，丢弃该 RPC
		logger.Debugf("接收到来自路由器灰名单 peer %s 的 RPC; 丢弃 RPC", rpc.from)
		return

	case AcceptControl:
		// 如果路由器只接受控制消息，忽略负载消息
		if len(rpc.GetPublish()) > 0 {
			logger.Debugf("peer %s 被路由器限制; 忽略 %d 个负载消息", rpc.from, len(rpc.GetPublish()))
		}
		// 对该 peer 进行流量限制，防止滥用
		p.tracer.ThrottlePeer(rpc.from)

	case AcceptAll:
		// 如果路由器接受所有消息，处理发布的消息
		for _, pmsg := range rpc.GetPublish() {
			// 检查消息是否属于已订阅的主题，或是否可以中继消息
			if !(p.subscribedToMsg(pmsg) || p.canRelayMsg(pmsg)) {
				// 如果未订阅或无法中继，忽略该消息
				logger.Debug("接收到我们未订阅主题的消息; 忽略消息")
				continue
			}

			// 推送消息到消息处理队列
			p.pushMsg(&Message{pmsg, "", rpc.from, nil, false})
		}
	}

	// 让路由器处理 RPC 消息的控制部分
	p.rt.HandleRPC(rpc)
}

// DefaultMsgIdFn 返回传入消息的唯一 ID
// 参数:
//   - pmsg: 传入的消息
//
// 返回值:
//   - string: 消息的唯一 ID
func DefaultMsgIdFn(pmsg *pb.Message) string {
	return string(pmsg.GetFrom()) + string(pmsg.GetSeqno())
}

// DefaultPeerFilter 接受所有主题的所有 peers
// 参数:
//   - pid: peer ID
//   - topic: 主题
//
// 返回值:
//   - bool: 是否接受该 peer
func DefaultPeerFilter(pid peer.ID, topic string) bool {
	return true
}

// pushMsg 推送消息，并根据需要进行验证
// 参数:
//   - msg: 要推送的消息
func (p *PubSub) pushMsg(msg *Message) {
	// 获取消息的来源节点 ID
	src := msg.ReceivedFrom

	// 拒绝来自黑名单 peers 的消息
	// 如果消息的来源节点在黑名单中，直接丢弃消息，并记录此操作
	if p.blacklist.Contains(src) {
		logger.Debugf("丢弃来自黑名单 peer %s 的消息", src)
		// 记录被拒绝的消息
		p.tracer.RejectMessage(msg, RejectBlacklstedPeer)
		return
	}

	// 即使消息是由良好 peers 转发的
	// 如果消息的原始发送者在黑名单中，也丢弃消息
	if p.blacklist.Contains(msg.GetFrom()) {
		logger.Debugf("丢弃来自黑名单来源 %s 的消息", src)
		// 记录被拒绝的消息
		p.tracer.RejectMessage(msg, RejectBlacklistedSource)
		return
	}

	// 检查消息的签名策略是否符合要求
	err := p.checkSigningPolicy(msg)
	if err != nil {
		// 如果签名检查失败，丢弃消息，并记录此操作
		logger.Debugf("丢弃来自 %s 的消息: %s", src, err)
		return
	}

	// 拒绝声称是从我们自己发出的但实际上是转发的消息
	// 如果消息声称来自本节点，但实际是由其他节点转发的，则丢弃消息
	self := p.host.ID()
	if peer.ID(msg.GetFrom()) == self && src != self {
		logger.Debugf("丢弃声称来自自己但由 %s 转发的消息", src)
		// 记录被拒绝的消息
		p.tracer.RejectMessage(msg, RejectSelfOrigin)
		return
	}

	// 我们是否已经看到并验证了这条消息？
	// 使用消息 ID 生成器生成消息 ID，并检查消息是否已经处理过
	id := p.idGen.ID(msg)
	if p.seenMessage(id) {
		// 如果消息是重复的，记录此操作
		p.tracer.DuplicateMessage(msg)
		return
	}

	// 将消息推送到验证队列进行进一步验证
	if !p.val.Push(src, msg) {
		// 如果验证不通过，直接返回，不做进一步处理
		return
	}

	// 如果消息之前未被看到，标记为已看到并进行发布
	if p.markSeen(id) {
		// 发布消息到订阅者
		p.publishMessage(msg)
	}
}

// checkSigningPolicy 检查消息的签名策略
// 参数:
//   - msg: 要检查的消息
//
// 返回值:
//   - error: 如果消息不符合签名策略，返回错误
func (p *PubSub) checkSigningPolicy(msg *Message) error {
	// 在严格模式下，拒绝未签名的消息
	if p.signPolicy.mustVerify() {
		if p.signPolicy.mustSign() {
			if msg.Signature == nil {
				p.tracer.RejectMessage(msg, RejectMissingSignature)
				return ValidationError{Reason: RejectMissingSignature}
			}
			// 实际的签名验证在验证管道中进行，
			// 在检查消息是否已被看到之前，以避免不必要的签名验证处理成本。
		} else {
			if msg.Signature != nil {
				p.tracer.RejectMessage(msg, RejectUnexpectedSignature)
				return ValidationError{Reason: RejectUnexpectedSignature}
			}
			// 如果我们期望签名的消息，并且不自己创建消息，
			// 则不接受序列号、来源数据或密钥数据。
			// 默认的 msgID 函数仍然依赖于 Seqno 和 From，
			// 但如果我们不自己创建消息，则不会使用它。
			if p.signID == "" {
				if msg.Seqno != nil || msg.From != nil || msg.Key != nil {
					p.tracer.RejectMessage(msg, RejectUnexpectedAuthInfo)
					return ValidationError{Reason: RejectUnexpectedAuthInfo}
				}
			}
		}
	}

	return nil
}

// publishMessage 发布消息
// 参数:
//   - msg: 要发布的消息
func (p *PubSub) publishMessage(msg *Message) {
	// 通知 tracer 已投递消息
	p.tracer.DeliverMessage(msg)

	// 如果没有设置目标节点，直接通知订阅者，并继续转发消息
	if msg.GetTargets() == nil || len(msg.GetTargets()) == 0 {
		p.notifySubs(msg) // 通知所有订阅者
		// 如果消息不是本地的，调用路由器发布消息
		if !msg.Local {
			p.rt.Publish(msg) // 转发消息
		}
		return
	}

	// 如果设置了目标节点，处理逻辑如下
	allTargetsReceived := true   // 标志位，表示是否所有目标节点均已接收到消息
	currentNodeIsTarget := false // 标志位，表示当前节点是否是目标节点

	for _, target := range msg.GetTargets() {
		if peer.ID(target.GetPeerId()).String() == p.host.ID().String() {
			// 当前节点是目标节点
			currentNodeIsTarget = true

			// 检查当前节点是否已经接收过该消息
			if target.GetReceived() {
				// 如果已经接收到过消息，不再处理
				return
			}

			// 标记当前节点已接收到消息
			target.Received = true

			// 通知订阅者消息已到达
			p.notifySubs(msg)
		}

		// 检查是否所有目标节点均已接收到消息
		if !target.GetReceived() {
			allTargetsReceived = false
		}
	}

	// 如果消息不是本地的，并且存在未接收的目标节点，继续转发消息
	if !msg.Local && (!currentNodeIsTarget || !allTargetsReceived) {
		p.rt.Publish(msg) // 转发消息
	}
}

// addTopicReq 添加主题请求结构体
type addTopicReq struct {
	topic *Topic      // 主题
	resp  chan *Topic // 响应通道
}

// rmTopicReq 删除主题请求结构体
type rmTopicReq struct {
	topic *Topic     // 主题
	resp  chan error // 响应通道
}

// TopicOptions 主题选项结构体（占位符）
type TopicOptions struct{}

// TopicOpt 主题选项函数类型
type TopicOpt func(t *Topic) error

// WithTopicMessageIdFn 设置自定义 MsgIdFunction 用于生成消息 ID
// 参数:
//   - msgId: 消息 ID 函数
//
// 返回值:
//   - TopicOpt: 主题选项函数
func WithTopicMessageIdFn(msgId MsgIdFunction) TopicOpt {
	return func(t *Topic) error {
		t.p.idGen.Set(t.topic, msgId)
		return nil
	}
}

// Join 加入主题并返回 Topic 句柄。
// 每个主题应该只有一个 Topic 句柄，如果主题句柄已存在，Join 将返回错误。
// 参数:
//   - topic: 主题名称
//   - opts: 主题选项
//
// 返回值:
//   - *Topic: 主题句柄
//   - error: 如果发生错误，返回错误
func (p *PubSub) Join(topic string, opts ...TopicOpt) (*Topic, error) {
	// 尝试加入主题，并返回主题句柄、是否为新创建的主题以及错误信息。
	t, ok, err := p.tryJoin(topic, opts...)
	if err != nil {
		return nil, err // 如果发生错误，直接返回
	}

	// 如果主题已经存在，则返回错误。
	if !ok {
		return nil, fmt.Errorf("topic already exists")
	}

	// 返回主题句柄。
	return t, nil
}

// tryJoin 是一个内部函数，用于尝试加入一个主题
// 返回值:
//   - *Topic: 如果可以创建或找到主题，返回主题
//   - bool: 如果主题是新创建的，则返回 true，否则返回 false
//   - error: 如果发生错误，返回错误
func (p *PubSub) tryJoin(topic string, opts ...TopicOpt) (*Topic, bool, error) {
	// 检查订阅过滤器，确保允许订阅该主题。
	if p.subFilter != nil && !p.subFilter.CanSubscribe(topic) {
		return nil, false, fmt.Errorf("topic is not allowed by the subscription filter ")
	}

	// 创建一个新的 Topic 结构体，用于表示该主题。
	t := &Topic{
		p:           p,                                     // 指向 PubSub 实例
		topic:       topic,                                 // 主题名称
		evtHandlers: make(map[*TopicEventHandler]struct{}), // 事件处理函数
	}

	// 应用主题选项。
	for _, opt := range opts {
		if err := opt(t); err != nil {
			return nil, false, err
		}
	}

	// 发送一个请求给 PubSub 实例，要求加入该主题。
	resp := make(chan *Topic, 1)
	select {
	case t.p.addTopic <- &addTopicReq{
		topic: t,
		resp:  resp,
	}:
	case <-t.p.ctx.Done():
		return nil, false, t.p.ctx.Err() // 如果上下文被取消，则返回错误
	}

	// 等待响应，获取返回的主题句柄。
	returnedTopic := <-resp

	// 如果返回的主题与创建的主题不同，说明主题已经存在。
	if returnedTopic != t {
		return returnedTopic, false, nil
	}

	// 返回主题句柄和创建状态。
	return t, true, nil
}

// addSubReq 添加订阅请求结构体
type addSubReq struct {
	sub  *Subscription      // 订阅
	resp chan *Subscription // 响应通道
}

// SubOpt 订阅选项函数类型
type SubOpt func(sub *Subscription) error

// Subscribe 返回给定主题的新订阅。
// 请注意，订阅不是即时操作。可能需要一些时间，订阅才能被 pubsub 主循环处理并传播给我们的对等节点。
//
// 已弃用：请使用 pubsub.Join() 和 topic.Subscribe()
func (p *PubSub) Subscribe(topic string, opts ...SubOpt) (*Subscription, error) {
	// 忽略主题是新创建的还是已有的，因为无论哪种方式我们都有一个有效的主题可以使用
	topicHandle, _, err := p.tryJoin(topic)
	if err != nil {
		return nil, err
	}

	return topicHandle.Subscribe(opts...)
}

// WithBufferSize 是一个订阅选项，用于自定义订阅输出缓冲区的大小。
// 默认长度为 32，但可以配置以避免消费者读取速度不够快时丢失消息。
func WithBufferSize(size int) SubOpt {
	return func(sub *Subscription) error {
		sub.ch = make(chan *Message, size)
		return nil
	}
}

// topicReq 请求订阅的主题结构体
type topicReq struct {
	resp chan []string // 响应通道
}

// GetTopics 返回此节点订阅的主题。
func (p *PubSub) GetTopics() []string {
	out := make(chan []string, 1)
	select {
	case p.getTopics <- &topicReq{resp: out}:
	case <-p.ctx.Done():
		return nil
	}
	return <-out
}

// Publish 向给定主题发布数据。
//
// 已弃用：请使用 pubsub.Join() 和 topic.Publish()
// func (p *PubSub) Publish(topic string, data []byte, opts ...PubOpt) error {
// 	// 忽略主题是新创建的还是已有的，因为无论哪种方式我们都有一个有效的主题可以使用
// 	t, _, err := p.tryJoin(topic)
// 	if err != nil {
// 		return err
// 	}

// 	return t.Publish(context.TODO(), data, opts...)
// }

// nextSeqno 返回下一个消息的序列号
func (p *PubSub) nextSeqno() []byte {
	seqno := make([]byte, 8)
	counter := atomic.AddUint64(&p.counter, 1)
	binary.BigEndian.PutUint64(seqno, counter)
	return seqno
}

// listPeerReq 列出对等节点请求结构体
type listPeerReq struct {
	resp  chan []peer.ID // 响应通道
	topic string         // 主题
}

// ListPeers 返回我们在给定主题中连接的对等节点列表。
func (p *PubSub) ListPeers(topic string) []peer.ID {
	out := make(chan []peer.ID)
	select {
	case p.getPeers <- &listPeerReq{
		resp:  out,
		topic: topic,
	}:
	case <-p.ctx.Done():
		return nil
	}
	return <-out
}

// BlacklistPeer 将一个对等节点列入黑名单；所有来自此对等节点的消息将无条件丢弃。
func (p *PubSub) BlacklistPeer(pid peer.ID) {
	select {
	case p.blacklistPeer <- pid:
	case <-p.ctx.Done():
	}
}

// RegisterTopicValidator 为主题注册一个验证器。
// 默认情况下，验证器是异步的，这意味着它们将在单独的 goroutine 中运行。
// 活动 goroutine 的数量由全局和每个主题验证器的节流控制；如果超过节流阈值，消息将被丢弃。
func (p *PubSub) RegisterTopicValidator(topic string, val interface{}, opts ...ValidatorOpt) error {
	addVal := &addValReq{
		topic:    topic,
		validate: val,
		resp:     make(chan error, 1),
	}

	for _, opt := range opts {
		err := opt(addVal)
		if err != nil {
			return err
		}
	}

	select {
	case p.addVal <- addVal:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	return <-addVal.resp
}

// UnregisterTopicValidator 从主题中移除一个验证器。
// 如果没有验证器注册到该主题，则返回错误。
func (p *PubSub) UnregisterTopicValidator(topic string) error {
	rmVal := &rmValReq{
		topic: topic,
		resp:  make(chan error, 1),
	}

	select {
	case p.rmVal <- rmVal:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	return <-rmVal.resp
}

// RelayCancelFunc 中继取消函数类型
type RelayCancelFunc func()

// addRelayReq 添加中继请求结构体
type addRelayReq struct {
	topic string               // 主题
	resp  chan RelayCancelFunc // 响应通道
}
