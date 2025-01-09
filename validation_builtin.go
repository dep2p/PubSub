// 作用：内置的消息验证。
// 功能：实现默认的消息验证逻辑，确保消息的有效性和安全性。

package pubsub

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/dep2p/libp2p/core/peer"
)

// PeerMetadataStore 是一个接口，用于存储和检索每个对等节点的元数据
type PeerMetadataStore interface {
	// Get 获取与对等节点关联的元数据；
	// 如果没有与对等节点关联的元数据，应返回 nil，而不是错误。
	Get(context.Context, peer.ID) ([]byte, error)
	// Put 设置与对等节点关联的元数据。
	Put(context.Context, peer.ID, []byte) error
}

// BasicSeqnoValidator 是一个基本验证器，可用作默认验证器，忽略超出已见缓存窗口的重放消息。
// 验证器使用消息序号作为对等节点特定的 nonce 来决定是否应传播消息，比较对等节点元数据存储中的最大 nonce。
// 这有助于确保无论已见缓存跨度和网络直径如何，网络中都不会存在无限传播的消息。
// 它要求 pubsub 实例化时具有严格的消息签名策略，并且序号未被禁用，即不支持匿名模式。
//
// 警告：请参阅 https://github.com/libp2p/rust-libp2p/issues/3453
// 简而言之：rust 当前通过发出随机序号违反了规范，这带来了互操作性风险。
// 我们预计此问题将在不久的将来得到解决，但如果您处于与（较旧的）rust 节点混合的环境中，请牢记这一点。
type BasicSeqnoValidator struct {
	mx   sync.RWMutex      // 读写锁，保护对元数据存储的并发访问
	meta PeerMetadataStore // 对等节点元数据存储接口
}

// NewBasicSeqnoValidator 构造一个使用给定 PeerMetadataStore 的 BasicSeqnoValidator。
// 参数:
//   - meta: 用于存储对等节点元数据的接口实例
//
// 返回值:
//   - ValidatorEx: 返回一个扩展验证器函数
func NewBasicSeqnoValidator(meta PeerMetadataStore) ValidatorEx {
	val := &BasicSeqnoValidator{
		meta: meta,
	}
	return val.validate
}

// validate 是 BasicSeqnoValidator 的验证函数，检查消息的序号以决定是否接受消息。
// 参数:
//   - ctx: 上下文，用于控制请求的生命周期
//   - _ : 发送消息的对等节点 ID（未使用）
//   - m : 要验证的消息
//
// 返回值:
//   - ValidationResult: 返回验证结果，接受或忽略消息
func (v *BasicSeqnoValidator) validate(ctx context.Context, _ peer.ID, m *Message) ValidationResult {
	p := m.GetFrom() // 获取消息的发送者

	v.mx.RLock() // 获取读锁以保护并发访问
	nonceBytes, err := v.meta.Get(ctx, p)
	v.mx.RUnlock()

	if err != nil {
		logger.Warnf("获取对等节点 nonce 失败: %s", err)
		return ValidationIgnore
	}

	var nonce uint64
	if len(nonceBytes) > 0 {
		nonce = binary.BigEndian.Uint64(nonceBytes)
	}

	var seqno uint64
	seqnoBytes := m.GetSeqno()
	if len(seqnoBytes) > 0 {
		seqno = binary.BigEndian.Uint64(seqnoBytes)
	}

	// 比较当前消息序号与已知的最大 nonce
	if seqno <= nonce {
		return ValidationIgnore
	}

	// 获取写锁以防止并发验证冲突，并再次比较 nonce
	v.mx.Lock()
	defer v.mx.Unlock()

	nonceBytes, err = v.meta.Get(ctx, p)
	if err != nil {
		logger.Warnf("获取对等节点 nonce 失败: %s", err)
		return ValidationIgnore
	}

	if len(nonceBytes) > 0 {
		nonce = binary.BigEndian.Uint64(nonceBytes)
	}

	if seqno <= nonce {
		return ValidationIgnore
	}

	// 更新 nonce
	nonceBytes = make([]byte, 8)
	binary.BigEndian.PutUint64(nonceBytes, seqno)

	err = v.meta.Put(ctx, p, nonceBytes)
	if err != nil {
		logger.Warnf("存储对等节点 nonce 失败: %s", err)
	}

	return ValidationAccept
}
