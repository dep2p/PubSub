package pubsub

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/dep2p/libp2p/core/peer"
)

func TestBackoff_Update(t *testing.T) {
	id1 := peer.ID("peer-1") // 定义第一个peer ID
	id2 := peer.ID("peer-2") // 定义第二个peer ID

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保测试结束时取消上下文

	size := 10
	cleanupInterval := 5 * time.Second
	maxBackoffAttempts := 10

	// 创建一个新的backoff实例
	b := newBackoff(ctx, size, cleanupInterval, maxBackoffAttempts)

	if len(b.info) > 0 {
		t.Fatal("non-empty info map for backoff") // 初始info map不为空
	}

	// 初始情况下updateAndGet应该返回0持续时间和nil错误
	if d, err := b.updateAndGet(id1); d != time.Duration(0) || err != nil {
		t.Fatalf("invalid initialization: %v, \t, %s", d, err)
	}
	if d, err := b.updateAndGet(id2); d != time.Duration(0) || err != nil {
		t.Fatalf("invalid initialization: %v, \t, %s", d, err)
	}

	// 更新id1的backoff计数，并检查返回的backoff时间
	for i := 0; i < maxBackoffAttempts-1; i++ {
		got, err := b.updateAndGet(id1)
		if err != nil {
			t.Fatalf("unexpected error post update: %s", err)
		}

		expected := time.Duration(math.Pow(BackoffMultiplier, float64(i)) *
			float64(MinBackoffDelay+MaxBackoffJitterCoff*time.Millisecond))
		if expected > MaxBackoffDelay {
			expected = MaxBackoffDelay
		}

		if expected < got { // 考虑到抖动，预期的backoff必须大于或等于实际值
			t.Fatalf("invalid backoff result, expected: %v, got: %v", expected, got)
		}
	}

	// 尝试超过最大backoff尝试次数，应返回错误
	if _, err := b.updateAndGet(id1); err == nil {
		t.Fatalf("expected an error for going beyond threshold but got nil")
	}

	// 更新id2的backoff计数，并检查返回的backoff时间
	got, err := b.updateAndGet(id2)
	if err != nil {
		t.Fatalf("unexpected error post update: %s", err)
	}
	if got != MinBackoffDelay {
		t.Fatalf("invalid backoff result, expected: %v, got: %v", MinBackoffDelay, got)
	}

	// 将id2的最后尝试时间设置为很久以前，以便在下次尝试时重置
	b.info[id2].lastTried = time.Now().Add(-TimeToLive)
	got, err = b.updateAndGet(id2)
	if err != nil {
		t.Fatalf("unexpected error post update: %s", err)
	}
	if got != time.Duration(0) {
		t.Fatalf("invalid ttl expiration, expected: %v, got: %v", time.Duration(0), got)
	}

	if len(b.info) != 2 {
		t.Fatalf("pre-invalidation attempt, info map size mismatch, expected: %d, got: %d", 2, len(b.info))
	}
}

func TestBackoff_Clean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保测试结束时取消上下文

	size := 10
	cleanupInterval := 2 * time.Second
	maxBackoffAttempts := 100 // 设置一个很大的尝试次数以测试清理逻辑
	b := newBackoff(ctx, size, cleanupInterval, maxBackoffAttempts)

	// 为每个peer更新backoff计数并设置其为过期
	for i := 0; i < size; i++ {
		id := peer.ID(fmt.Sprintf("peer-%d", i))
		_, err := b.updateAndGet(id)
		if err != nil {
			t.Fatalf("unexpected error post update: %s", err)
		}
		b.info[id].lastTried = time.Now().Add(-TimeToLive) // 强制过期
	}

	if len(b.info) != size {
		t.Fatalf("info map size mismatch, expected: %d, got: %d", size, len(b.info))
	}

	// 等待清理循环启动
	time.Sleep(2 * cleanupInterval)

	// 下次更新应触发清理
	got, err := b.updateAndGet(peer.ID("some-new-peer"))
	if err != nil {
		t.Fatalf("unexpected error post update: %s", err)
	}
	if got != time.Duration(0) {
		t.Fatalf("invalid backoff result, expected: %v, got: %v", time.Duration(0), got)
	}

	// 除了"some-new-peer"，其他记录都应被清理
	if len(b.info) != 1 {
		t.Fatalf("info map size mismatch, expected: %d, got: %d", 1, len(b.info))
	}
}
