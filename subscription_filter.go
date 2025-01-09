// 作用：实现订阅过滤器。
// 功能：管理和过滤对等节点的订阅请求，防止恶意或不合理的订阅行为。

package pubsub

import (
	"errors"
	"regexp"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/libp2p/core/peer"
)

// ErrTooManySubscriptions 可能由 SubscriptionFilter 返回，以表示订阅过多无法处理的错误
var ErrTooManySubscriptions = errors.New("too many subscriptions")

// SubscriptionFilter 是一个函数，用于告诉我们是否有兴趣允许和跟踪给定主题的订阅。
//
// 每当收到另一个对等点的订阅通知时，都会咨询过滤器；
// 如果过滤器返回 false，则忽略通知。
//
// 当加入主题时，也会咨询过滤器；如果过滤器返回 false，则 Join 操作将导致错误。
type SubscriptionFilter interface {
	// CanSubscribe 返回 true 如果主题是感兴趣的并且我们可以订阅它
	CanSubscribe(topic string) bool

	// FilterIncomingSubscriptions 用于所有包含订阅通知的 RPC。
	// 它应仅过滤感兴趣的订阅，并且如果订阅过多（例如）可能会返回错误。
	// 参数:
	// - from: 订阅来源的 peer.ID
	// - subs: 包含订阅通知的 RPC_SubOpts 列表
	// 返回值:
	// - []*pb.RPC_SubOpts: 过滤后的订阅列表
	// - error: 错误信息，如果有的话
	FilterIncomingSubscriptions(from peer.ID, subs []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error)
}

// WithSubscriptionFilter 是一个 pubsub 选项，用于指定感兴趣主题的订阅过滤器。
// 参数:
// - subFilter: 要应用的 SubscriptionFilter
// 返回值:
// - Option: 一个设置 SubscriptionFilter 的选项
func WithSubscriptionFilter(subFilter SubscriptionFilter) Option {
	return func(ps *PubSub) error {
		ps.subFilter = subFilter
		return nil
	}
}

// NewAllowlistSubscriptionFilter 创建一个订阅过滤器，该过滤器仅允许显式指定的主题用于本地订阅和传入对等订阅。
// 参数:
// - topics: 允许订阅的主题列表
// 返回值:
// - SubscriptionFilter: 一个新的允许列表订阅过滤器
func NewAllowlistSubscriptionFilter(topics ...string) SubscriptionFilter {
	allow := make(map[string]struct{})
	for _, topic := range topics {
		allow[topic] = struct{}{}
	}

	return &allowlistSubscriptionFilter{allow: allow}
}

// allowlistSubscriptionFilter 是一个结构体，保存允许订阅的主题列表
type allowlistSubscriptionFilter struct {
	allow map[string]struct{} // 允许订阅的主题集合
}

// 确保 allowlistSubscriptionFilter 实现了 SubscriptionFilter 接口
var _ SubscriptionFilter = (*allowlistSubscriptionFilter)(nil)

// CanSubscribe 返回 true 如果主题在允许列表中
// 参数:
// - topic: 要检查的主题
// 返回值:
// - bool: 是否允许订阅该主题
func (f *allowlistSubscriptionFilter) CanSubscribe(topic string) bool {
	_, ok := f.allow[topic]
	return ok
}

// FilterIncomingSubscriptions 过滤传入的订阅，仅保留感兴趣的订阅，并返回过滤后的订阅列表。
// 参数:
// - from: 订阅来源的 peer.ID
// - subs: 包含订阅通知的 RPC_SubOpts 列表
// 返回值:
// - []*pb.RPC_SubOpts: 过滤后的订阅列表
// - error: 错误信息，如果有的话
func (f *allowlistSubscriptionFilter) FilterIncomingSubscriptions(from peer.ID, subs []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error) {
	return FilterSubscriptions(subs, f.CanSubscribe), nil
}

// NewRegexpSubscriptionFilter 创建一个订阅过滤器，该过滤器仅允许与正则表达式匹配的主题用于本地订阅和传入对等订阅。
// 参数:
// - rx: 用于匹配主题的正则表达式
// 返回值:
// - SubscriptionFilter: 一个新的正则表达式订阅过滤器
func NewRegexpSubscriptionFilter(rx *regexp.Regexp) SubscriptionFilter {
	return &rxSubscriptionFilter{allow: rx}
}

// rxSubscriptionFilter 是一个结构体，保存允许订阅的正则表达式
type rxSubscriptionFilter struct {
	allow *regexp.Regexp // 允许订阅的正则表达式
}

// 确保 rxSubscriptionFilter 实现了 SubscriptionFilter 接口
var _ SubscriptionFilter = (*rxSubscriptionFilter)(nil)

// CanSubscribe 返回 true 如果主题与正则表达式匹配
// 参数:
// - topic: 要检查的主题
// 返回值:
// - bool: 是否允许订阅该主题
func (f *rxSubscriptionFilter) CanSubscribe(topic string) bool {
	return f.allow.MatchString(topic)
}

// FilterIncomingSubscriptions 过滤传入的订阅，仅保留感兴趣的订阅，并返回过滤后的订阅列表。
// 参数:
// - from: 订阅来源的 peer.ID
// - subs: 包含订阅通知的 RPC_SubOpts 列表
// 返回值:
// - []*pb.RPC_SubOpts: 过滤后的订阅列表
// - error: 错误信息，如果有的话
func (f *rxSubscriptionFilter) FilterIncomingSubscriptions(from peer.ID, subs []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error) {
	return FilterSubscriptions(subs, f.CanSubscribe), nil
}

// FilterSubscriptions 过滤并去重订阅列表。
// filter 应返回 true 如果一个主题是感兴趣的。
// 参数:
// - subs: 包含订阅通知的 RPC_SubOpts 列表
// - filter: 用于确定是否感兴趣的过滤函数
// 返回值:
// - []*pb.RPC_SubOpts: 过滤后的订阅列表
func FilterSubscriptions(subs []*pb.RPC_SubOpts, filter func(string) bool) []*pb.RPC_SubOpts {
	accept := make(map[string]*pb.RPC_SubOpts)

	for _, sub := range subs {
		topic := sub.GetTopicid()

		if !filter(topic) {
			continue
		}

		otherSub, ok := accept[topic]
		if ok {
			if sub.GetSubscribe() != otherSub.GetSubscribe() {
				delete(accept, topic)
			}
		} else {
			accept[topic] = sub
		}
	}

	if len(accept) == 0 {
		return nil
	}

	result := make([]*pb.RPC_SubOpts, 0, len(accept))
	for _, sub := range accept {
		result = append(result, sub)
	}

	return result
}

// WrapLimitSubscriptionFilter 包装一个订阅过滤器，在 RPC 消息中允许的订阅数量有硬限制。
// 参数:
// - filter: 内部使用的 SubscriptionFilter
// - limit: 订阅数量限制
// 返回值:
// - SubscriptionFilter: 包装后的订阅过滤器
func WrapLimitSubscriptionFilter(filter SubscriptionFilter, limit int) SubscriptionFilter {
	return &limitSubscriptionFilter{filter: filter, limit: limit}
}

// limitSubscriptionFilter 是一个结构体，保存订阅过滤器和订阅数量限制
type limitSubscriptionFilter struct {
	filter SubscriptionFilter // 内部使用的订阅过滤器
	limit  int                // 订阅数量限制
}

// 确保 limitSubscriptionFilter 实现了 SubscriptionFilter 接口
var _ SubscriptionFilter = (*limitSubscriptionFilter)(nil)

// CanSubscribe 返回 true 如果可以通过内部过滤器订阅主题
// 参数:
// - topic: 要检查的主题
// 返回值:
// - bool: 是否允许订阅该主题
func (f *limitSubscriptionFilter) CanSubscribe(topic string) bool {
	return f.filter.CanSubscribe(topic)
}

// FilterIncomingSubscriptions 过滤传入的订阅，仅保留感兴趣的订阅，并返回过滤后的订阅列表。
// 如果订阅数量超过限制，则返回错误。
// 参数:
// - from: 订阅来源的 peer.ID
// - subs: 包含订阅通知的 RPC_SubOpts 列表
// 返回值:
// - []*pb.RPC_SubOpts: 过滤后的订阅列表
// - error: 错误信息，如果有的话
func (f *limitSubscriptionFilter) FilterIncomingSubscriptions(from peer.ID, subs []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error) {
	if len(subs) > f.limit {
		logger.Warnf("订阅数量超过限制: %d > %d", len(subs), f.limit)
		return nil, ErrTooManySubscriptions
	}

	return f.filter.FilterIncomingSubscriptions(from, subs)
}
