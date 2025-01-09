package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dep2p/libp2p/core/peer"
)

// 测试注册和注销主题验证器
func TestRegisterUnregisterValidator(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取一个默认主机
	hosts := getDefaultHosts(t, 1)
	// 创建Pubsub实例
	psubs := getPubsubs(ctx, hosts)

	// 注册主题验证器
	err := psubs[0].RegisterTopicValidator("foo", func(context.Context, peer.ID, *Message) bool {
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// 注销主题验证器
	err = psubs[0].UnregisterTopicValidator("foo")
	if err != nil {
		t.Fatal(err)
	}

	// 再次注销主题验证器，预期失败
	err = psubs[0].UnregisterTopicValidator("foo")
	if err == nil {
		t.Fatal("Unregistered bogus topic validator")
	}
}

// 测试注册不同类型的验证器
func TestRegisterValidatorEx(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取三个默认主机
	hosts := getDefaultHosts(t, 3)
	// 创建Pubsub实例
	psubs := getPubsubs(ctx, hosts)

	// 注册基本验证器
	err := psubs[0].RegisterTopicValidator("test",
		Validator(func(context.Context, peer.ID, *Message) bool {
			return true
		}))
	if err != nil {
		t.Fatal(err)
	}

	// 注册扩展验证器
	err = psubs[1].RegisterTopicValidator("test",
		ValidatorEx(func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationAccept
		}))
	if err != nil {
		t.Fatal(err)
	}

	// 注册错误的验证器，预期失败
	err = psubs[2].RegisterTopicValidator("test", "bogus")
	if err == nil {
		t.Fatal("expected error")
	}
}

// 测试消息验证
func TestValidate(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取两个默认主机
	hosts := getDefaultHosts(t, 2)
	// 创建Pubsub实例
	psubs := getPubsubs(ctx, hosts)

	// 连接两个主机
	connect(t, hosts[0], hosts[1])
	topic := "foobar"

	// 注册主题验证器
	err := psubs[1].RegisterTopicValidator(topic, func(ctx context.Context, from peer.ID, msg *Message) bool {
		return !bytes.Contains(msg.Data, []byte("illegal"))
	})
	if err != nil {
		t.Fatal(err)
	}

	// 订阅主题
	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	// 等待订阅稳定
	time.Sleep(time.Millisecond * 50)

	// 定义测试消息
	msgs := []struct {
		msg       []byte
		validates bool
	}{
		{msg: []byte("this is a legal message"), validates: true},
		{msg: []byte("there also is nothing controversial about this message"), validates: true},
		{msg: []byte("openly illegal content will be censored"), validates: false},
		{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
	}

	// 发布和验证消息
	for _, tc := range msgs {

		tp, err := psubs[0].Join(topic)
		if err != nil {
			t.Fatal(err)
		}

		err = tp.Publish(ctx, tc.msg)
		if err != nil {
			t.Fatal(err)
		}

		select {
		case msg := <-sub.ch:
			if !tc.validates {
				t.Log(msg)
				t.Error("expected message validation to filter out the message")
			}
		case <-time.After(333 * time.Millisecond):
			if tc.validates {
				t.Error("expected message validation to accept the message")
			}
		}
	}
}

// 测试消息验证的另一个用例
func TestValidate2(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取一个默认主机
	hosts := getDefaultHosts(t, 1)
	// 创建Pubsub实例
	psubs := getPubsubs(ctx, hosts)

	topic := "foobar"

	// 注册主题验证器
	err := psubs[0].RegisterTopicValidator(topic, func(ctx context.Context, from peer.ID, msg *Message) bool {
		return !bytes.Contains(msg.Data, []byte("illegal"))
	})
	if err != nil {
		t.Fatal(err)
	}

	// 定义测试消息
	msgs := []struct {
		msg       []byte
		validates bool
	}{
		{msg: []byte("this is a legal message"), validates: true},
		{msg: []byte("there also is nothing controversial about this message"), validates: true},
		{msg: []byte("openly illegal content will be censored"), validates: false},
		{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
	}

	// 发布和验证消息
	for _, tc := range msgs {
		tp, err := psubs[0].Join(topic)
		if err != nil {
			t.Fatal(err)
		}

		err = tp.Publish(ctx, tc.msg)
		if tc.validates {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if err == nil {
				t.Fatal("expected validation to fail for this message")
			}
		}
	}
}

// 测试验证过载情况
func TestValidateOverload(t *testing.T) {
	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type msg struct {
		msg       []byte
		validates bool
	}

	// 定义测试用例
	tcs := []struct {
		msgs           []msg
		maxConcurrency int
	}{
		{
			maxConcurrency: 10,
			msgs: []msg{
				{msg: []byte("this is a legal message"), validates: true},
				{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
				{msg: []byte("there also is nothing controversial about this message"), validates: true},
				{msg: []byte("also fine"), validates: true},
				{msg: []byte("still, all good"), validates: true},
				{msg: []byte("this is getting boring"), validates: true},
				{msg: []byte("foo"), validates: true},
				{msg: []byte("foobar"), validates: true},
				{msg: []byte("foofoo"), validates: true},
				{msg: []byte("barfoo"), validates: true},
				{msg: []byte("oh no!"), validates: false},
			},
		},
		{
			maxConcurrency: 2,
			msgs: []msg{
				{msg: []byte("this is a legal message"), validates: true},
				{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
				{msg: []byte("oh no!"), validates: false},
			},
		},
	}

	// 遍历测试用例
	for tci, tc := range tcs {
		t.Run(fmt.Sprintf("%d", tci), func(t *testing.T) {
			hosts := getDefaultHosts(t, 2)
			psubs := getPubsubs(ctx, hosts)

			connect(t, hosts[0], hosts[1])
			topic := "foobar"

			block := make(chan struct{})

			err := psubs[1].RegisterTopicValidator(topic,
				func(ctx context.Context, from peer.ID, msg *Message) bool {
					<-block
					return true
				},
				WithValidatorConcurrency(tc.maxConcurrency))

			if err != nil {
				t.Fatal(err)
			}

			sub, err := psubs[1].Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(time.Millisecond * 50)

			if len(tc.msgs) != tc.maxConcurrency+1 {
				t.Fatalf("expected number of messages sent to be maxConcurrency+1. Got %d, expected %d", len(tc.msgs), tc.maxConcurrency+1)
			}

			p := psubs[0]

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for _, tmsg := range tc.msgs {
					select {
					case msg := <-sub.ch:
						if !tmsg.validates {
							t.Log(msg)
							t.Error("expected message validation to drop the message because all validator goroutines are taken")
						}
					case <-time.After(time.Second):
						if tmsg.validates {
							t.Error("expected message validation to accept the message")
						}
					}
				}
				wg.Done()
			}()

			for _, tmsg := range tc.msgs {
				tp, err := p.Join(topic)
				if err != nil {
					t.Fatal(err)
				}

				err = tp.Publish(ctx, tmsg.msg)
				if err != nil {
					t.Fatal(err)
				}
			}

			// 等待一会儿再解除验证器的阻塞
			time.Sleep(500 * time.Millisecond)
			close(block)

			wg.Wait()
		})
	}
}

// 测试不同选项的验证
func TestValidateAssortedOptions(t *testing.T) {
	// 为各种未在其他测试中覆盖的选项添加覆盖
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 10)
	psubs := getPubsubs(ctx, hosts,
		WithValidateQueueSize(10),
		WithValidateThrottle(10),
		WithValidateWorkers(10))

	// 稀疏连接主机
	sparseConnect(t, hosts)

	for _, psub := range psubs {
		err := psub.RegisterTopicValidator("test1",
			func(context.Context, peer.ID, *Message) bool {
				return true
			},
			WithValidatorTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}

		err = psub.RegisterTopicValidator("test2",
			func(context.Context, peer.ID, *Message) bool {
				return true
			},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}
	}

	var subs1, subs2 []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test1")
		if err != nil {
			t.Fatal(err)
		}
		subs1 = append(subs1, sub)

		sub, err = ps.Subscribe("test2")
		if err != nil {
			t.Fatal(err)
		}
		subs2 = append(subs2, sub)
	}

	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))

		tp, err := psubs[i].Join("test1")
		if err != nil {
			t.Fatal(err)
		}

		err = tp.Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs1 {
			assertReceive(t, sub, msg)
		}

		tp1, err := psubs[i].Join("test2")
		if err != nil {
			t.Fatal(err)
		}

		err = tp1.Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs2 {
			assertReceive(t, sub, msg)
		}
	}
}
