// 作用：管理订阅。
// 功能：处理订阅请求和维护订阅状态，确保消息能正确地发送给订阅者。

package pubsub

import (
	"context"
	"sync"
)

// Subscription 处理特定主题订阅的详细信息。
// 对于给定的主题可能有多个订阅。
type Subscription struct {
	topic    string               // 订阅的主题
	ch       chan *Message        // 消息通道
	cancelCh chan<- *Subscription // 取消订阅的通道
	ctx      context.Context      // 上下文，用于取消操作
	err      error                // 错误信息
	once     sync.Once            // 确保某些操作只执行一次的机制
}

// Topic 返回与订阅关联的主题字符串。
// 返回值:
// - string: 订阅的主题字符串
func (sub *Subscription) Topic() string {
	return sub.topic
}

// Next 返回订阅中的下一条消息。
// 参数:
// - ctx: context.Context 上下文，用于取消操作
// 返回值:
// - *Message: 下一条消息，如果有的话
// - error: 错误信息，如果有的话
func (sub *Subscription) Next(ctx context.Context) (*Message, error) {
	select {
	case msg, ok := <-sub.ch: // 从消息通道读取消息
		if !ok { // 如果通道已关闭
			return msg, sub.err // 返回消息和错误信息
		}
		return msg, nil // 返回消息和空错误信息
	case <-ctx.Done(): // 如果上下文已取消
		return nil, ctx.Err() // 返回空消息和上下文错误
	}
}

// Cancel 关闭订阅。如果这是最后一个活动订阅，那么 pubsub 将向网络发送取消订阅公告。
func (sub *Subscription) Cancel() {
	select {
	case sub.cancelCh <- sub: // 将订阅发送到取消通道
	case <-sub.ctx.Done(): // 如果上下文已取消
	}
}

// close 关闭订阅的消息通道。
// 确保该操作只执行一次。
func (sub *Subscription) close() {
	sub.once.Do(func() {
		close(sub.ch) // 关闭消息通道
	})
}
