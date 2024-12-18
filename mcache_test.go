package pubsub

import (
	"encoding/binary"
	"fmt"
	"testing"

	pb "github.com/dep2p/pubsub/pb"
)

// TestMessageCache 测试消息缓存的行为，包括消息存储、检索、转移和丢弃。
func TestMessageCache(t *testing.T) {
	// 创建一个新的消息缓存，保留最近3个时间段的消息，每个时间段最多保留5条消息
	mcache := NewMessageCache(3, 5)
	msgID := DefaultMsgIdFn

	// 创建60条测试消息
	msgs := make([]*pb.Message, 60)
	for i := range msgs {
		msgs[i] = makeTestMessage(i)
	}

	// 将前10条消息放入缓存
	for i := 0; i < 10; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	// 检查前10条消息是否在缓存中
	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	// 获取 "test" 主题的所有 gossip IDs，检查是否有10个
	gids := mcache.GetGossipIDs("test")
	if len(gids) != 10 {
		t.Fatalf("Expected 10 gossip IDs; got %d", len(gids))
	}

	// 验证获取的 gossip IDs 是否与消息ID匹配
	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	// 转移缓存，模拟进入下一个时间段，并放入新的10条消息
	mcache.Shift()
	for i := 10; i < 20; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	// 检查前20条消息是否在缓存中
	for i := 0; i < 20; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	// 获取 "test" 主题的所有 gossip IDs，检查是否有20个
	gids = mcache.GetGossipIDs("test")
	if len(gids) != 20 {
		t.Fatalf("Expected 20 gossip IDs; got %d", len(gids))
	}

	// 验证获取的 gossip IDs 是否与消息ID匹配
	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		if mid != gids[10+i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		mid := msgID(msgs[i])
		if mid != gids[i-10] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	// 连续转移缓存并添加新的消息，模拟多个时间段的变化
	mcache.Shift()
	for i := 20; i < 30; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	mcache.Shift()
	for i := 30; i < 40; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	mcache.Shift()
	for i := 40; i < 50; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	mcache.Shift()
	for i := 50; i < 60; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	// 检查缓存中的消息数量是否为50
	if len(mcache.msgs) != 50 {
		t.Fatalf("Expected 50 messages in the cache; got %d", len(mcache.msgs))
	}

	// 检查前10条消息是否被正确丢弃
	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		_, ok := mcache.Get(mid)
		if ok {
			t.Fatalf("Message %d still in cache", i)
		}
	}

	// 检查剩余的消息是否都在缓存中
	for i := 10; i < 60; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	// 获取 "test" 主题的所有 gossip IDs，检查是否有30个
	gids = mcache.GetGossipIDs("test")
	if len(gids) != 30 {
		t.Fatalf("Expected 30 gossip IDs; got %d", len(gids))
	}

	// 验证获取的 gossip IDs 是否与消息ID匹配
	for i := 0; i < 10; i++ {
		mid := msgID(msgs[50+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		mid := msgID(msgs[30+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 20; i < 30; i++ {
		mid := msgID(msgs[10+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}
}

// makeTestMessage 创建一个测试消息，带有序列号、数据和主题
func makeTestMessage(n int) *pb.Message {
	seqno := make([]byte, 8)
	binary.BigEndian.PutUint64(seqno, uint64(n))
	data := []byte(fmt.Sprintf("%d", n))
	topic := "test"
	return &pb.Message{
		Data:  data,
		Topic: topic,
		From:  []byte("test"),
		Seqno: seqno,
	}
}
