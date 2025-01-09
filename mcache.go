// 作用：实现消息缓存。
// 功能：管理最近的消息缓存，防止重复处理和发送相同的消息。

package pubsub

import (
	"fmt"

	"github.com/dep2p/libp2p/core/peer"
)

// NewMessageCache 创建一个滑动窗口缓存，记住消息长达 `history` 个插槽。
// 当查询要通告的消息时，缓存仅返回最后 `gossip` 个插槽中的消息。
// `gossip` 参数必须小于或等于 `history`，否则该函数会引发 panic。
// 在通过 IHAVE gossip 通告消息和通过 IWANT 命令获取消息之间的反应时间之间存在松弛。
func NewMessageCache(gossip, history int) *MessageCache {
	if gossip > history {
		err := fmt.Errorf("invalid parameters for message cache; gossip slots (%d) cannot be larger than history slots (%d)",
			gossip, history)
		panic(err) // 如果 gossip 插槽大于 history 插槽，则引发错误
	}
	return &MessageCache{
		msgs:    make(map[string]*Message),        // 消息 ID 到消息的映射
		peertx:  make(map[string]map[peer.ID]int), // 消息 ID 到对等节点事务计数的映射
		history: make([][]CacheEntry, history),    // 历史缓存
		gossip:  gossip,                           // gossip 插槽数量
		msgID: func(msg *Message) string { // 默认的消息 ID 生成函数
			return DefaultMsgIdFn(msg.Message)
		},
	}
}

// MessageCache 是一个滑动窗口缓存，记住消息长达一定的历史长度。
type MessageCache struct {
	msgs    map[string]*Message        // 消息 ID 到消息的映射
	peertx  map[string]map[peer.ID]int // 消息 ID 到对等节点事务计数的映射
	history [][]CacheEntry             // 历史缓存
	gossip  int                        // gossip 插槽数量
	msgID   func(*Message) string      // 消息 ID 生成函数
}

// SetMsgIdFn 设置消息 ID 生成函数。
// 参数:
//   - msgID: 消息 ID 生成函数
func (mc *MessageCache) SetMsgIdFn(msgID func(*Message) string) {
	mc.msgID = msgID
}

// CacheEntry 表示消息缓存条目。
type CacheEntry struct {
	mid   string // 消息 ID
	topic string // 主题名称
}

// Put 将消息放入缓存。
// 参数:
//   - msg: 要放入缓存的消息
func (mc *MessageCache) Put(msg *Message) {
	mid := mc.msgID(msg)                                                               // 生成消息 ID
	mc.msgs[mid] = msg                                                                 // 将消息存储到消息映射中
	mc.history[0] = append(mc.history[0], CacheEntry{mid: mid, topic: msg.GetTopic()}) // 将缓存条目添加到历史的第一个插槽中
}

// Get 从缓存中获取消息。
// 参数:
//   - mid: 消息 ID
//
// 返回值:
//   - *Message: 消息指针
//   - bool: 是否存在该消息
func (mc *MessageCache) Get(mid string) (*Message, bool) {
	m, ok := mc.msgs[mid] // 从消息映射中获取消息
	return m, ok
}

// GetForPeer 从缓存中获取对等节点的消息。
// 参数:
//   - mid: 消息 ID
//   - p: 对等节点 ID
//
// 返回值:
//   - *Message: 消息指针
//   - int: 对等节点事务计数
//   - bool: 是否存在该消息
func (mc *MessageCache) GetForPeer(mid string, p peer.ID) (*Message, int, bool) {
	m, ok := mc.msgs[mid] // 从消息映射中获取消息
	if !ok {
		return nil, 0, false
	}

	tx, ok := mc.peertx[mid] // 获取消息 ID 对应的对等节点事务映射
	if !ok {
		tx = make(map[peer.ID]int)
		mc.peertx[mid] = tx
	}
	tx[p]++ // 增加对等节点的事务计数

	return m, tx[p], true
}

// GetGossipIDs 获取给定主题的 gossip 消息 ID 列表。
// 参数:
//   - topic: 主题名称
//
// 返回值:
//   - []string: gossip 消息 ID 列表
func (mc *MessageCache) GetGossipIDs(topic string) []string {
	var mids []string
	for _, entries := range mc.history[:mc.gossip] {
		for _, entry := range entries {
			if entry.topic == topic {
				mids = append(mids, entry.mid)
			}
		}
	}
	return mids
}

// Shift 移动缓存窗口，丢弃最旧的插槽并腾出空间存储新的消息。
func (mc *MessageCache) Shift() {
	last := mc.history[len(mc.history)-1] // 获取最旧的插槽
	for _, entry := range last {
		delete(mc.msgs, entry.mid)   // 从消息映射中删除消息
		delete(mc.peertx, entry.mid) // 从对等节点事务映射中删除消息
	}
	for i := len(mc.history) - 2; i >= 0; i-- {
		mc.history[i+1] = mc.history[i] // 将较新的插槽向后移动
	}
	mc.history[0] = nil // 清空第一个插槽，准备存储新的消息
}
