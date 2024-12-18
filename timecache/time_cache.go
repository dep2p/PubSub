package timecache

import (
	"time"
)

// Strategy 表示 TimeCache 过期策略
type Strategy uint8

const (
	// Strategy_FirstSeen 表示从添加条目时开始计算过期时间
	Strategy_FirstSeen Strategy = iota
	// Strategy_LastSeen 表示从上次添加或检查条目时开始计算过期时间
	Strategy_LastSeen
)

// TimeCache 是一个最近看到的消息（通过 id）的缓存接口
type TimeCache interface {
	// Add 将 id 添加到缓存中，如果该 id 不存在
	// 如果 id 是新添加的，返回 true
	// 根据实现策略，可能会或不会更新现有条目的过期时间
	Add(string) bool
	// Has 检查缓存中是否存在 id
	// 根据实现策略，可能会或不会更新现有条目的过期时间
	Has(string) bool
	// Done 表示用户不再使用此缓存，可以停止后台线程并释放资源
	Done()
}

// NewTimeCache 创建一个默认的 ("first seen") 缓存实现
// 参数:
// - ttl: 条目的存活时间
// 返回值：
// - TimeCache: 一个实现了 TimeCache 接口的缓存实例
func NewTimeCache(ttl time.Duration) TimeCache {
	return NewTimeCacheWithStrategy(Strategy_FirstSeen, ttl)
}

// NewTimeCacheWithStrategy 根据指定的策略创建一个 TimeCache 实例
// 参数:
// - strategy: 使用的过期策略
// - ttl: 条目的存活时间
// 返回值：
// - TimeCache: 一个实现了 TimeCache 接口的缓存实例
func NewTimeCacheWithStrategy(strategy Strategy, ttl time.Duration) TimeCache {
	switch strategy {
	case Strategy_FirstSeen:
		return newFirstSeenCache(ttl)
	case Strategy_LastSeen:
		return newLastSeenCache(ttl)
	default:
		// 默认使用原始的时间缓存实现
		return newFirstSeenCache(ttl)
	}
}
