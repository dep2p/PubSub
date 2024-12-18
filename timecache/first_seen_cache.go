package timecache

import (
	"context"
	"sync"
	"time"
)

// FirstSeenCache 是一个时间缓存，仅在消息首次添加时标记其过期时间。
type FirstSeenCache struct {
	lk  sync.RWMutex         // 读写锁，用于保护缓存数据
	m   map[string]time.Time // 存储消息及其过期时间的映射
	ttl time.Duration        // 消息的存活时间

	done func() // 用于停止后台清理协程的函数
}

// 确保 FirstSeenCache 实现了 TimeCache 接口
var _ TimeCache = (*FirstSeenCache)(nil)

// newFirstSeenCache 创建一个新的 FirstSeenCache。
// 参数:
// - ttl: 消息的存活时间
// 返回值:
// - *FirstSeenCache: 新的 FirstSeenCache 实例
func newFirstSeenCache(ttl time.Duration) *FirstSeenCache {
	tc := &FirstSeenCache{
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
func (tc *FirstSeenCache) Done() {
	tc.done()
}

// Has 检查消息是否存在于缓存中
// 参数:
// - s: 消息字符串
// 返回值:
// - bool: 消息是否存在
func (tc *FirstSeenCache) Has(s string) bool {
	tc.lk.RLock()
	defer tc.lk.RUnlock()

	_, ok := tc.m[s]
	return ok
}

// Add 将消息添加到缓存中
// 参数:
// - s: 消息字符串
// 返回值:
// - bool: 是否成功添加消息
func (tc *FirstSeenCache) Add(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	if ok {
		// 如果消息已经存在，则返回 false
		return false
	}

	// 将消息添加到缓存，并设置其过期时间
	tc.m[s] = time.Now().Add(tc.ttl)
	return true
}
