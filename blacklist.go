// 作用：实现对等节点的黑名单机制。
// 功能：管理被黑名单的对等节点，防止与不可信或恶意节点的通信。

package pubsub

import (
	"time"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/pubsub/timecache"
)

// Blacklist 是一个接口，定义了对等节点黑名单的方法
type Blacklist interface {
	Add(peer.ID) bool      // 将节点添加到黑名单
	Contains(peer.ID) bool // 检查节点是否在黑名单中
}

// MapBlacklist 是一种使用map实现的黑名单
type MapBlacklist map[peer.ID]struct{}

// NewMapBlacklist 创建一个新的 MapBlacklist 实例
func NewMapBlacklist() Blacklist {
	return MapBlacklist(make(map[peer.ID]struct{})) // 初始化并返回一个 MapBlacklist
}

// Add 将节点添加到 MapBlacklist 中
// 参数:
//   - p: 需要添加到黑名单的节点ID
//
// 返回值:
//   - bool: 添加操作是否成功
func (b MapBlacklist) Add(p peer.ID) bool {
	b[p] = struct{}{} // 将节点ID作为键添加到map中，值为空结构体
	return true       // 返回true表示添加成功
}

// Contains 检查节点是否在 MapBlacklist 中
// 参数:
//   - p: 需要检查的节点ID
//
// 返回值:
//   - bool: 节点是否在黑名单中
func (b MapBlacklist) Contains(p peer.ID) bool {
	_, ok := b[p] // 在map中查找节点ID
	return ok     // 返回查找结果
}

// TimeCachedBlacklist 是一种使用时间缓存实现的黑名单
type TimeCachedBlacklist struct {
	tc timecache.TimeCache // 时间缓存实例
}

// NewTimeCachedBlacklist 创建一个带有指定过期时间的新 TimeCachedBlacklist 实例
// 参数:
//   - expiry: 黑名单条目的过期时间
//
// 返回值:
//   - Blacklist: 初始化后的 TimeCachedBlacklist 实例
//   - error: 如果有错误发生则返回错误
func NewTimeCachedBlacklist(expiry time.Duration) (Blacklist, error) {
	b := &TimeCachedBlacklist{tc: timecache.NewTimeCache(expiry)} // 创建时间缓存并初始化 TimeCachedBlacklist
	return b, nil
}

// Add 将节点添加到 TimeCachedBlacklist 中
// 参数:
//   - p: 需要添加到黑名单的节点ID
//
// 返回值:
//   - bool: 添加操作是否成功
func (b *TimeCachedBlacklist) Add(p peer.ID) bool {
	s := p.String()  // 获取节点ID的字符串表示
	if b.tc.Has(s) { // 检查时间缓存中是否已有该节点ID
		return false // 如果已有则返回false表示添加失败
	}
	b.tc.Add(s) // 将节点ID添加到时间缓存中
	return true // 返回true表示添加成功
}

// Contains 检查节点是否在 TimeCachedBlacklist 中
// 参数:
//   - p: 需要检查的节点ID
//
// 返回值:
//   - bool: 节点是否在黑名单中
func (b *TimeCachedBlacklist) Contains(p peer.ID) bool {
	return b.tc.Has(p.String()) // 在时间缓存中查找节点ID的字符串表示
}
