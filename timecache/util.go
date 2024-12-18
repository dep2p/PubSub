package timecache

import (
	"context"
	"sync"
	"time"
)

// 定义后台清理间隔时间为 1 分钟
var backgroundSweepInterval = time.Minute

// background 在后台定期清理过期的缓存条目
// 参数:
// - ctx: 上下文，用于控制后台任务的生命周期
// - lk: 用于同步的锁
// - m: 存储条目的 map，键是条目的 ID，值是条目的过期时间
func background(ctx context.Context, lk sync.Locker, m map[string]time.Time) {
	// 创建一个定时器，每分钟触发一次
	ticker := time.NewTicker(backgroundSweepInterval)
	defer ticker.Stop() // 在函数退出时停止定时器

	for {
		select {
		case now := <-ticker.C: // 每当定时器触发时
			sweep(lk, m, now) // 执行清理操作

		case <-ctx.Done(): // 如果上下文被取消，退出循环
			return
		}
	}
}

// sweep 清理 map 中过期的条目
// 参数:
// - lk: 用于同步的锁
// - m: 存储条目的 map，键是条目的 ID，值是条目的过期时间
// - now: 当前时间，用于判断条目是否过期
func sweep(lk sync.Locker, m map[string]time.Time, now time.Time) {
	lk.Lock()         // 加锁以保证并发安全
	defer lk.Unlock() // 在函数退出时解锁

	// 遍历 map 中的所有条目
	for k, expiry := range m {
		if expiry.Before(now) { // 如果条目已过期
			delete(m, k) // 删除该条目
		}
	}
}
