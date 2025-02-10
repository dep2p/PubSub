package pubsub

import (
	"testing"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/go-dep2p/core/crypto"
	"github.com/dep2p/go-dep2p/core/peer"
)

// TestSigning 测试签名和验证功能。
func TestSigning(t *testing.T) {
	// 生成 RSA 密钥对并测试签名和验证
	privk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		t.Fatal(err) // 如果生成密钥对失败，则记录错误并终止测试
	}
	testSignVerify(t, privk) // 使用生成的私钥进行签名和验证测试

	// 生成 Ed25519 密钥对并测试签名和验证
	privk, _, err = crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		t.Fatal(err) // 如果生成密钥对失败，则记录错误并终止测试
	}
	testSignVerify(t, privk) // 使用生成的私钥进行签名和验证测试
}

// testSignVerify 使用提供的私钥进行签名和验证测试。
// t: 测试对象
// privk: 用于签名和验证的私钥
func testSignVerify(t *testing.T, privk crypto.PrivKey) {
	// 从公钥生成 peer ID
	id, err := peer.IDFromPublicKey(privk.GetPublic())
	if err != nil {
		t.Fatal(err) // 如果生成 peer ID 失败，则记录错误并终止测试
	}

	topic := "foo"
	// 创建并序列化 AddrInfo
	addrInfo := peer.AddrInfo{
		ID:    id,
		Addrs: nil,
	}
	addrInfoBytes, err := addrInfo.MarshalJSON()
	if err != nil {
		t.Fatalf("序列化 AddrInfo 失败: %v", err)
	}

	m := pb.Message{
		Data:  []byte("abc"), // 消息内容
		Topic: topic,         // 消息主题
		From:  addrInfoBytes, // 发送者的 peer ID
		Seqno: []byte("123"), // 消息序列号
	}
	// 对消息进行签名
	signMessage(id, privk, &m)
	// 验证消息签名
	err = verifyMessageSignature(&m)
	if err != nil {
		t.Fatal(err) // 如果验证签名失败，则记录错误并终止测试
	}
}
