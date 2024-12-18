package pubsub

import (
	"encoding/binary"

	pb "github.com/dep2p/pubsub/pb"
)

// generateU16 从字节切片中生成一个 uint16 值
func generateU16(data *[]byte) uint16 {
	// 检查数据长度是否足够
	if len(*data) < 2 {
		return 0
	}

	// 从字节切片的前两个字节生成 uint16 值，并移除已使用的字节
	out := binary.LittleEndian.Uint16((*data)[:2])
	*data = (*data)[2:]
	return out
}

// generateBool 从字节切片中生成一个布尔值
func generateBool(data *[]byte) bool {
	// 检查数据长度是否足够
	if len(*data) < 1 {
		return false
	}

	// 从字节切片的第一个字节生成布尔值，并移除已使用的字节
	out := (*data)[0]&1 == 1
	*data = (*data)[1:]
	return out
}

// generateMessage 生成一个 pb.Message 对象
func generateMessage(data []byte, limit int) *pb.Message {
	// 生成消息大小，并限制在指定范围内
	msgSize := int(generateU16(&data)) % limit
	// 创建指定大小的数据字节切片并生成 pb.Message 对象
	return &pb.Message{Data: make([]byte, msgSize)}
}

// generateSub 生成一个 pb.RPC_SubOpts 对象
func generateSub(data []byte, limit int) *pb.RPC_SubOpts {
	// 生成主题 ID 大小，并限制在指定范围内
	topicIDSize := int(generateU16(&data)) % limit
	// 生成订阅标志
	subscribe := generateBool(&data)

	// 创建指定大小的主题 ID 字符串并生成 pb.RPC_SubOpts 对象
	str := string(make([]byte, topicIDSize))
	return &pb.RPC_SubOpts{Subscribe: subscribe, Topicid: str}
}

// generateControl 生成一个 pb.ControlMessage 对象
func generateControl(data []byte, limit int) *pb.ControlMessage {
	// 生成控制消息的数量，并限制在指定范围内
	numIWANTMsgs := int(generateU16(&data)) % (limit / 2)
	numIHAVEMsgs := int(generateU16(&data)) % (limit / 2)

	ctl := &pb.ControlMessage{}

	// 生成 IWANT 消息
	ctl.Iwant = make([]*pb.ControlIWant, 0, numIWANTMsgs)
	for i := 0; i < numIWANTMsgs; i++ {
		msgSize := int(generateU16(&data)) % limit
		msgCount := int(generateU16(&data)) % limit
		ctl.Iwant = append(ctl.Iwant, &pb.ControlIWant{})
		ctl.Iwant[i].MessageIDs = make([]string, 0, msgCount)
		for j := 0; j < msgCount; j++ {
			ctl.Iwant[i].MessageIDs = append(ctl.Iwant[i].MessageIDs, string(make([]byte, msgSize)))
		}
	}
	// 检查大小是否超过限制
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	// 生成 IHAVE 消息
	ctl.Ihave = make([]*pb.ControlIHave, 0, numIHAVEMsgs)
	for i := 0; i < numIHAVEMsgs; i++ {
		msgSize := int(generateU16(&data)) % limit
		msgCount := int(generateU16(&data)) % limit
		topicSize := int(generateU16(&data)) % limit
		topic := string(make([]byte, topicSize))
		ctl.Ihave = append(ctl.Ihave, &pb.ControlIHave{TopicID: topic})

		ctl.Ihave[i].MessageIDs = make([]string, 0, msgCount)
		for j := 0; j < msgCount; j++ {
			ctl.Ihave[i].MessageIDs = append(ctl.Ihave[i].MessageIDs, string(make([]byte, msgSize)))
		}
	}
	// 检查大小是否超过限制
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	// 生成 GRAFT 消息
	numGraft := int(generateU16(&data)) % limit
	ctl.Graft = make([]*pb.ControlGraft, 0, numGraft)
	for i := 0; i < numGraft; i++ {
		topicSize := int(generateU16(&data)) % limit
		topic := string(make([]byte, topicSize))
		ctl.Graft = append(ctl.Graft, &pb.ControlGraft{TopicID: topic})
	}
	// 检查大小是否超过限制
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	// 生成 PRUNE 消息
	numPrune := int(generateU16(&data)) % limit
	ctl.Prune = make([]*pb.ControlPrune, 0, numPrune)
	for i := 0; i < numPrune; i++ {
		topicSize := int(generateU16(&data)) % limit
		topic := string(make([]byte, topicSize))
		ctl.Prune = append(ctl.Prune, &pb.ControlPrune{TopicID: topic})
	}
	// 检查大小是否超过限制
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	return ctl
}

// generateRPC 生成一个 RPC 对象
func generateRPC(data []byte, limit int) *RPC {
	rpc := &RPC{RPC: pb.RPC{}}
	sizeTester := RPC{RPC: pb.RPC{}}

	// 生成 PUBLISH 消息
	msgCount := int(generateU16(&data)) % (limit / 2)
	rpc.Publish = make([]*pb.Message, 0, msgCount)
	for i := 0; i < msgCount; i++ {
		msg := generateMessage(data, limit)

		sizeTester.Publish = []*pb.Message{msg}
		size := sizeTester.Size()
		sizeTester.Publish = nil
		if size > limit {
			continue
		}

		rpc.Publish = append(rpc.Publish, msg)
	}

	// 生成 SUBSCRIPTIONS 消息
	subCount := int(generateU16(&data)) % (limit / 2)
	rpc.Subscriptions = make([]*pb.RPC_SubOpts, 0, subCount)
	for i := 0; i < subCount; i++ {
		sub := generateSub(data, limit)

		sizeTester.Subscriptions = []*pb.RPC_SubOpts{sub}
		size := sizeTester.Size()
		sizeTester.Subscriptions = nil
		if size > limit {
			continue
		}

		rpc.Subscriptions = append(rpc.Subscriptions, sub)
	}

	// 生成 CONTROL 消息
	ctl := generateControl(data, limit)

	sizeTester.Control = ctl
	size := sizeTester.Size()
	sizeTester.Control = nil
	if size <= limit {
		rpc.Control = ctl
	}

	return rpc
}
