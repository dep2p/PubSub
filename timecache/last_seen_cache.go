package timecache

import (
	"context"
	"sync"
	"time"
)

// LastSeenCache 是一个时间缓存，在添加或检查消息存在时，延长其过期时间。
type LastSeenCache struct {
	lk  sync.Mutex           // 互斥锁，用于保护缓存数据
	m   map[string]time.Time // 存储消息及其过期时间的映射
	ttl time.Duration        // 消息的存活时间

	done func() // 用于停止后台清理协程的函数
}

// 确保 LastSeenCache 实现了 TimeCache 接口
var _ TimeCache = (*LastSeenCache)(nil)

// newLastSeenCache 创建一个新的 LastSeenCache。
// 参数:
// - ttl: 消息的存活时间
// 返回值:
// - *LastSeenCache: 新的 LastSeenCache 实例
func newLastSeenCache(ttl time.Duration) *LastSeenCache {
	tc := &LastSeenCache{
		m:   make(map[string]time.Time),
		ttl: ttl,
	}

	// 创建上下文，用于控制后台清理协程的生命周期
	ctx, done := context.WithCancel(context.Background())
	tc.done = done
	// 启动后台清理协程
	go background(ctx, &tc.lk, tc.m)

	return tc
}

// Done 停止后台清理协程
func (tc *LastSeenCache) Done() {
	tc.done()
}

// Add 将消息添加到缓存中，并延长其过期时间。
// 参数:
// - s: 消息字符串
// 返回值:
// - bool: 是否成功添加消息（如果消息已存在，返回 false）
func (tc *LastSeenCache) Add(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	// 将消息添加到缓存，并设置其过期时间
	tc.m[s] = time.Now().Add(tc.ttl)

	return !ok
}

// Has 检查消息是否存在于缓存中，并延长其过期时间。
// 参数:
// - s: 消息字符串
// 返回值:
// - bool: 消息是否存在
func (tc *LastSeenCache) Has(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	if ok {
		// 延长消息的过期时间
		tc.m[s] = time.Now().Add(tc.ttl)
	}

	return ok
}
