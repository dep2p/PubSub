package pubsub

import (
	"testing"

	compat_pb "github.com/dep2p/pubsub/compat"

	pb "github.com/dep2p/pubsub/pb"
)

// TestMultitopicMessageCompatibility 测试新旧消息格式的兼容性
func TestMultitopicMessageCompatibility(t *testing.T) {
	topic1 := "topic1" // 定义第一个主题
	topic2 := "topic2" // 定义第二个主题

	// 创建一个新的消息，使用新格式
	newMessage1 := &pb.Message{
		From:      []byte("A"),           // 消息发送者
		Data:      []byte("blah"),        // 消息内容
		Seqno:     []byte("123"),         // 消息序列号
		Topic:     topic1,                // 消息主题
		Signature: []byte("a-signature"), // 消息签名
		Key:       []byte("a-key"),       // 消息公钥
	}

	// 创建一个旧格式的消息，只包含一个主题
	oldMessage1 := &compat_pb.Message{
		From:      []byte("A"),           // 消息发送者
		Data:      []byte("blah"),        // 消息内容
		Seqno:     []byte("123"),         // 消息序列号
		TopicIDs:  []string{topic1},      // 消息主题列表
		Signature: []byte("a-signature"), // 消息签名
		Key:       []byte("a-key"),       // 消息公钥
	}

	// 创建一个旧格式的消息，包含多个主题
	oldMessage2 := &compat_pb.Message{
		From:      []byte("A"),              // 消息发送者
		Data:      []byte("blah"),           // 消息内容
		Seqno:     []byte("123"),            // 消息序列号
		TopicIDs:  []string{topic1, topic2}, // 消息主题列表
		Signature: []byte("a-signature"),    // 消息签名
		Key:       []byte("a-key"),          // 消息公钥
	}

	// 将新格式的消息进行序列化
	newMessage1b, err := newMessage1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 将旧格式的消息进行序列化
	oldMessage1b, err := oldMessage1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	oldMessage2b, err := oldMessage2.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	newMessage := new(pb.Message)        // 创建一个新的消息实例用于反序列化
	oldMessage := new(compat_pb.Message) // 创建一个旧格式的消息实例用于反序列化

	// 将旧格式的消息反序列化为新格式的消息
	err = newMessage.Unmarshal(oldMessage1b)
	if err != nil {
		t.Fatal(err)
	}

	// 检查反序列化后的主题是否正确
	if newMessage.GetTopic() != topic1 {
		t.Fatalf("bad topic: expected %s, got %s", topic1, newMessage.GetTopic())
	}

	// 重置新格式的消息实例
	newMessage.Reset()

	// 将包含多个主题的旧格式消息反序列化为新格式消息
	err = newMessage.Unmarshal(oldMessage2b)
	if err != nil {
		t.Fatal(err)
	}

	// 检查反序列化后的主题是否正确
	if newMessage.GetTopic() != topic2 {
		t.Fatalf("bad topic: expected %s, got %s", topic2, newMessage.GetTopic())
	}

	// 将新格式的消息反序列化为旧格式的消息
	err = oldMessage.Unmarshal(newMessage1b)
	if err != nil {
		t.Fatal(err)
	}

	// 获取旧格式消息中的主题列表
	topics := oldMessage.GetTopicIDs()

	// 检查主题列表的长度是否正确
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}

	// 检查主题是否正确
	if topics[0] != topic1 {
		t.Fatalf("bad topic: expected %s, got %s", topic1, topics[0])
	}
}
