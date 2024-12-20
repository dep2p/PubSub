// 作用：消息签名和验证。
// 功能：实现消息的签名和验证机制，确保消息的完整性和来源的可信度。

package pubsub

import (
	"fmt"

	"github.com/dep2p/pubsub/logger"
	pb "github.com/dep2p/pubsub/pb"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageSignaturePolicy 描述是否生成、期望和/或验证签名的策略。
type MessageSignaturePolicy uint8

// LaxSign 和 LaxNoSign 已弃用。未来消息签名和消息验证可以统一。
// 常量表示不同的签名策略
const (
	// msgSigning 表示本地产生的消息必须签名
	msgSigning MessageSignaturePolicy = 1 << iota
	// msgVerification 表示外部消息必须验证
	msgVerification
)

// 常量表示严格的签名和验证策略
const (
	// StrictSign 生成签名并期望验证传入的签名
	StrictSign = msgSigning | msgVerification
	// StrictNoSign 不生成签名，并丢弃和惩罚携带签名的传入消息
	StrictNoSign = msgVerification
	// LaxSign 生成签名，并仅在存在签名时验证传入的签名
	// 已弃用：建议严格启用或严格禁用签名。
	LaxSign = msgSigning
	// LaxNoSign 不生成签名，并仅在存在签名时验证传入的签名
	// 已弃用：建议严格启用或严格禁用签名。
	LaxNoSign = 0
)

// mustVerify 返回 true 当消息签名必须验证时。
// 如果不期望签名，则验证检查签名是否缺失。
func (policy MessageSignaturePolicy) mustVerify() bool {
	return policy&msgVerification != 0
}

// mustSign 返回 true 当消息应该签名，且传入消息被期望有签名时。
func (policy MessageSignaturePolicy) mustSign() bool {
	return policy&msgSigning != 0
}

// SignPrefix 是签名前缀常量
const SignPrefix = "libp2p-pubsub:"

// verifyMessageSignature 验证消息签名。
// 参数:
// - m: 要验证的 pb.Message 指针
// 返回值:
// - error: 错误信息，如果有的话
func verifyMessageSignature(m *pb.Message) error {
	pubk, err := messagePubKey(m) // 获取消息的公钥
	if err != nil {
		logger.Warnf("获取消息公钥失败: %s", err) // 获取消息公钥失败
		return err
	}

	xm := *m
	xm.Signature = nil
	xm.Key = nil
	bytes, err := xm.Marshal() // 序列化消息
	if err != nil {
		logger.Warnf("序列化消息失败: %s", err) // 序列化消息失败
		return err
	}

	bytes = withSignPrefix(bytes) // 添加签名前缀

	valid, err := pubk.Verify(bytes, m.Signature) // 验证签名
	if err != nil {
		logger.Warnf("验证签名失败: %s", err) // 验证签名失败
		return err
	}

	if !valid {
		logger.Warnf("签名无效")                   // 签名无效
		return fmt.Errorf("invalid signature") // 签名无效
	}

	return nil
}

// messagePubKey 获取消息的公钥。
// 参数:
// - m: 要获取公钥的 pb.Message 指针
// 返回值:
// - crypto.PubKey: 公钥
// - error: 错误信息，如果有的话
func messagePubKey(m *pb.Message) (crypto.PubKey, error) {
	var pubk crypto.PubKey

	pid, err := peer.IDFromBytes(m.From) // 从消息中提取 peer.ID
	if err != nil {
		return nil, err
	}

	if m.Key == nil {
		// 没有附加密钥，必须从源 ID 提取
		pubk, err = pid.ExtractPublicKey() // 提取公钥
		if err != nil {
			logger.Warnf("提取签名密钥失败: %s", err.Error()) // 提取签名密钥失败
			return nil, fmt.Errorf("无法提取签名密钥: %s", err.Error())
		}
		if pubk == nil {
			logger.Warnf("无法提取签名密钥") // 无法提取签名密钥
			return nil, fmt.Errorf("无法提取签名密钥")
		}
	} else {
		pubk, err = crypto.UnmarshalPublicKey(m.Key) // 解码公钥
		if err != nil {
			logger.Warnf("解码签名密钥失败: %s", err.Error()) // 解码签名密钥失败
			return nil, fmt.Errorf("无法解码签名密钥: %s", err.Error())
		}

		// 验证源 ID 与附加密钥是否匹配
		if !pid.MatchesPublicKey(pubk) {
			logger.Warnf("签名密钥与源ID不匹配: %s", pid) // 签名密钥与源ID不匹配
			return nil, fmt.Errorf("签名密钥与源ID不匹配: %s", pid)
		}
	}

	return pubk, nil
}

// signMessage 为消息生成签名。
// 参数:
// - pid: 消息的 peer.ID
// - key: 签名用的私钥
// - m: 要签名的 pb.Message 指针
// 返回值:
// - error: 错误信息，如果有的话
func signMessage(pid peer.ID, key crypto.PrivKey, m *pb.Message) error {
	bytes, err := m.Marshal() // 序列化消息
	if err != nil {
		logger.Warnf("序列化消息失败: %s", err) // 序列化消息失败
		return err
	}

	bytes = withSignPrefix(bytes) // 添加签名前缀

	sig, err := key.Sign(bytes) // 生成签名
	if err != nil {
		logger.Warnf("生成签名失败: %s", err) // 生成签名失败
		return err
	}

	m.Signature = sig // 设置消息签名

	pk, _ := pid.ExtractPublicKey() // 提取公钥
	if pk == nil {
		pubk, err := crypto.MarshalPublicKey(key.GetPublic()) // 编码公钥
		if err != nil {
			logger.Warnf("编码签名密钥失败: %s", err) // 编码签名密钥失败
			return err
		}
		m.Key = pubk // 设置消息公钥
	}

	return nil
}

// withSignPrefix 在字节数组前添加签名前缀。
// 参数:
// - bytes: 要添加前缀的字节数组
// 返回值:
// - []byte: 添加了签名前缀的字节数组
func withSignPrefix(bytes []byte) []byte {
	return append([]byte(SignPrefix), bytes...)
}
