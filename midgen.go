// 作用：生成消息ID。
// 功能：定义和实现生成唯一消息ID的机制，确保每条消息都有唯一标识。

package pubsub

import (
	"sync"

	pb "github.com/dep2p/pubsub/pb"
)

// msgIDGenerator 处理消息的 ID 计算
// 它允许为每个主题设置自定义生成器 (MsgIdFunction)
type msgIDGenerator struct {
	Default MsgIdFunction // 默认的消息 ID 生成函数

	topicGensLk sync.RWMutex             // 读写锁，保护 topicGens 的并发访问
	topicGens   map[string]MsgIdFunction // 主题到消息 ID 生成函数的映射
}

// newMsgIdGenerator 创建一个新的 msgIDGenerator 实例
func newMsgIdGenerator() *msgIDGenerator {
	return &msgIDGenerator{
		Default:   DefaultMsgIdFn,                 // 设置默认的消息 ID 生成函数
		topicGens: make(map[string]MsgIdFunction), // 初始化主题生成器映射
	}
}

// Set 为特定主题设置自定义 ID 生成器 (MsgIdFunction)
// 参数:
//   - topic: 主题名称
//   - gen: 自定义 ID 生成函数
func (m *msgIDGenerator) Set(topic string, gen MsgIdFunction) {
	m.topicGensLk.Lock()     // 获取写锁
	m.topicGens[topic] = gen // 设置主题的 ID 生成函数
	m.topicGensLk.Unlock()   // 释放写锁
}

// ID 计算消息的 ID 或者直接返回缓存的值
// 参数:
//   - msg: 要计算 ID 的消息
//
// 返回值:
//   - string: 消息 ID
func (m *msgIDGenerator) ID(msg *Message) string {
	if msg.ID != "" { // 如果消息已有 ID，直接返回
		return msg.ID
	}

	msg.ID = m.RawID(msg.Message) // 计算消息的原始 ID
	return msg.ID
}

// RawID 计算 proto 消息的 ID
// 参数:
//   - msg: 要计算 ID 的 proto 消息
//
// 返回值:
//   - string: 消息 ID
func (m *msgIDGenerator) RawID(msg *pb.Message) string {
	m.topicGensLk.RLock()                  // 获取读锁
	gen, ok := m.topicGens[msg.GetTopic()] // 获取主题对应的 ID 生成函数
	m.topicGensLk.RUnlock()                // 释放读锁
	if !ok {                               // 如果没有自定义生成函数，使用默认生成函数
		gen = m.Default
	}

	return gen(msg) // 使用生成函数计算消息 ID
}
