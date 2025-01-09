// 作用：实现追踪器接口。
// 功能：定义和实现追踪器接口，用于追踪pubsub系统中的各种事件和消息。

package pubsub

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	pb "github.com/dep2p/pubsub/pb"

	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"
	"github.com/dep2p/libp2p/core/protocol"

	"github.com/libp2p/go-msgio/protoio"
)

// TraceBufferSize 设置追踪缓冲区大小，默认为 64K。
var TraceBufferSize = 1 << 16

// MinTraceBatchSize 设置最小批处理大小，默认为 16。
var MinTraceBatchSize = 16

// 拒绝消息的原因常量
const (
	RejectBlacklstedPeer      = "blacklisted peer"        // 被列入黑名单的对等节点
	RejectBlacklistedSource   = "blacklisted source"      // 被列入黑名单的来源
	RejectMissingSignature    = "missing signature"       // 缺少签名
	RejectUnexpectedSignature = "unexpected signature"    // 意外的签名
	RejectUnexpectedAuthInfo  = "unexpected auth info"    // 意外的身份验证信息
	RejectInvalidSignature    = "invalid signature"       // 无效的签名
	RejectValidationQueueFull = "validation queue full"   // 验证队列已满
	RejectValidationThrottled = "validation throttled"    // 验证被限制
	RejectValidationFailed    = "validation failed"       // 验证失败
	RejectValidationIgnored   = "validation ignored"      // 验证被忽略
	RejectSelfOrigin          = "self originated message" // 自己发起的消息
)

// basicTracer 是一个基本的追踪器，存储和管理追踪事件
type basicTracer struct {
	ch     chan struct{}    // 用于通知新事件的通道
	mx     sync.Mutex       // 互斥锁，保护缓冲区的并发访问
	buf    []*pb.TraceEvent // 存储追踪事件的缓冲区
	lossy  bool             // 是否允许丢失事件
	closed bool             // 追踪器是否已关闭
}

// Trace 向追踪器添加一个事件
// 参数:
//   - evt: 要添加的事件
func (t *basicTracer) Trace(evt *pb.TraceEvent) {
	t.mx.Lock()         // 加锁保护缓冲区
	defer t.mx.Unlock() // 函数结束时解锁

	if t.closed {
		return // 如果追踪器已关闭，直接返回
	}

	if t.lossy && len(t.buf) > TraceBufferSize {
		logger.Debug("追踪缓冲区溢出；丢弃追踪事件")
	} else {
		t.buf = append(t.buf, evt) // 添加事件到缓冲区
	}

	select {
	case t.ch <- struct{}{}: // 通知有新事件
	default: // 如果通道已满，忽略通知
	}
}

// Close 关闭追踪器
func (t *basicTracer) Close() {
	t.mx.Lock()         // 加锁保护缓冲区
	defer t.mx.Unlock() // 函数结束时解锁
	if !t.closed {
		t.closed = true
		close(t.ch) // 关闭通知通道
	}
}

// JSONTracer 是一个将事件写入文件的追踪器，事件以 ndjson 格式编码。
type JSONTracer struct {
	basicTracer                // 嵌入 basicTracer
	w           io.WriteCloser // 写入器，用于输出追踪事件
}

// NewJSONTracer 创建一个新的 JSONTracer，将追踪信息写入文件。
// 参数:
//   - file: 文件路径
//
// 返回值：
//   - *JSONTracer: JSONTracer 对象
//   - error: 错误信息
func NewJSONTracer(file string) (*JSONTracer, error) {
	return OpenJSONTracer(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

// OpenJSONTracer 创建一个新的 JSONTracer，可以显式控制文件打开的标志和权限。
// 参数:
//   - file: 文件路径
//   - flags: 文件打开标志
//   - perm: 文件权限
//
// 返回值：
//   - *JSONTracer: JSONTracer 对象
//   - error: 错误信息
func OpenJSONTracer(file string, flags int, perm os.FileMode) (*JSONTracer, error) {
	f, err := os.OpenFile(file, flags, perm)
	if err != nil {
		return nil, err
	}

	tr := &JSONTracer{w: f, basicTracer: basicTracer{ch: make(chan struct{}, 1)}}
	go tr.doWrite()

	return tr, nil
}

// doWrite 处理追踪事件的写入
func (t *JSONTracer) doWrite() {
	var buf []*pb.TraceEvent
	enc := json.NewEncoder(t.w) // 创建 JSON 编码器
	for {
		_, ok := <-t.ch

		t.mx.Lock()     // 加锁保护缓冲区
		tmp := t.buf    // 交换缓冲区
		t.buf = buf[:0] // 清空缓冲区
		buf = tmp
		t.mx.Unlock() // 解锁缓冲区

		for i, evt := range buf {
			err := enc.Encode(evt) // 编码事件并写入
			if err != nil {
				logger.Warnf("写入追踪事件失败: %s", err.Error())
			}
			buf[i] = nil // 清空缓冲区
		}

		if !ok {
			t.w.Close() // 关闭写入器
			return
		}
	}
}

// 确保 JSONTracer 实现了 EventTracer 接口
var _ EventTracer = (*JSONTracer)(nil)

// PBTracer 是一个将事件写入文件的追踪器，事件以 protobuf 格式编码。
type PBTracer struct {
	basicTracer                // 嵌入 basicTracer
	w           io.WriteCloser // 写入器，用于输出追踪事件
}

// NewPBTracer 创建一个新的 PBTracer，将追踪信息写入文件。
// 参数:
//   - file: 文件路径
//
// 返回值：
//   - *PBTracer: PBTracer 对象
//   - error: 错误信息
func NewPBTracer(file string) (*PBTracer, error) {
	return OpenPBTracer(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

// OpenPBTracer 创建一个新的 PBTracer，可以显式控制文件打开的标志和权限。
// 参数:
//   - file: 文件路径
//   - flags: 文件打开标志
//   - perm: 文件权限
//
// 返回值：
//   - *PBTracer: PBTracer 对象
//   - error: 错误信息
func OpenPBTracer(file string, flags int, perm os.FileMode) (*PBTracer, error) {
	f, err := os.OpenFile(file, flags, perm)
	if err != nil {
		return nil, err
	}

	tr := &PBTracer{w: f, basicTracer: basicTracer{ch: make(chan struct{}, 1)}}
	go tr.doWrite()

	return tr, nil
}

// doWrite 处理追踪事件的写入
func (t *PBTracer) doWrite() {
	var buf []*pb.TraceEvent
	w := protoio.NewDelimitedWriter(t.w) // 创建 Protobuf 编码器
	for {
		_, ok := <-t.ch

		t.mx.Lock()     // 加锁保护缓冲区
		tmp := t.buf    // 交换缓冲区
		t.buf = buf[:0] // 清空缓冲区
		buf = tmp
		t.mx.Unlock() // 解锁缓冲区

		for i, evt := range buf {
			err := w.WriteMsg(evt) // 编码事件并写入
			if err != nil {
				logger.Warnf("写入追踪事件失败: %s", err.Error())
			}
			buf[i] = nil // 清空缓冲区
		}

		if !ok {
			t.w.Close() // 关闭写入器
			return
		}
	}
}

// 确保 PBTracer 实现了 EventTracer 接口
var _ EventTracer = (*PBTracer)(nil)

// RemoteTracerProtoID 是远程追踪协议的 ID
const RemoteTracerProtoID = protocol.ID("/libp2p/pubsub/tracer/1.0.0")

// RemoteTracer 是一个将追踪事件发送到远程对等节点的追踪器
type RemoteTracer struct {
	basicTracer
	ctx  context.Context // 上下文
	host host.Host       // 本地主机
	peer peer.ID         // 远程对等节点 ID
}

// NewRemoteTracer 构建一个 RemoteTracer，将追踪信息发送到由 pi 标识的对等节点。
// 参数:
//   - ctx: 上下文，用于控制生命周期和取消操作
//   - host: 本地主机，表示当前节点
//   - pi: 远程对等节点的地址信息，包括节点ID和地址
//
// 返回值：
//   - *RemoteTracer: 新创建的 RemoteTracer 对象
//   - error: 如果发生错误，返回错误信息
func NewRemoteTracer(ctx context.Context, host host.Host, pi peer.AddrInfo) (*RemoteTracer, error) {
	// 创建一个新的 RemoteTracer 对象，并初始化其字段
	tr := &RemoteTracer{
		ctx:         ctx,                                                  // 设置上下文
		host:        host,                                                 // 设置本地主机
		peer:        pi.ID,                                                // 设置远程对等节点的ID
		basicTracer: basicTracer{ch: make(chan struct{}, 1), lossy: true}, // 初始化 basicTracer，带有一个带缓冲的通道和lossy标志
	}

	// 将远程对等节点的地址信息添加到本地主机的 Peerstore 中，设置地址的TTL（永久有效）
	host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL)

	// 启动一个 goroutine，调用 doWrite 方法处理追踪事件的发送
	go tr.doWrite()

	// 返回新创建的 RemoteTracer 对象和 nil 错误
	return tr, nil
}

// doWrite 处理追踪事件的发送
func (t *RemoteTracer) doWrite() {
	var buf []*pb.TraceEvent // 临时缓冲区，用于存储批量追踪事件

	// 打开一个流，用于发送追踪事件
	s, err := t.openStream()
	if err != nil {
		logger.Debugf("打开远程追踪器流时出错: %s", err.Error())
		return
	}

	var batch pb.TraceEventBatch // 批量追踪事件

	// 创建一个gzip写入器，将数据压缩后写入流
	gzipW := gzip.NewWriter(s)
	// 创建一个protobuf格式的写入器，用于写入追踪事件
	w := protoio.NewDelimitedWriter(gzipW)

	for {
		// 从通道中读取追踪事件，阻塞直到有事件或通道关闭
		_, ok := <-t.ch

		// 批量积累的截止时间
		deadline := time.Now().Add(time.Second)

		t.mx.Lock()
		// 等待追踪事件积累到最小批量大小或超过截止时间
		for len(t.buf) < MinTraceBatchSize && time.Now().Before(deadline) {
			t.mx.Unlock()
			time.Sleep(100 * time.Millisecond) // 睡眠一段时间以等待更多事件
			t.mx.Lock()
		}

		// 将缓冲区的内容赋给临时变量，并清空缓冲区
		tmp := t.buf
		t.buf = buf[:0]
		buf = tmp
		t.mx.Unlock()

		// 如果缓冲区为空，跳到循环结束部分
		if len(buf) == 0 {
			goto end
		}

		// 将追踪事件批次赋值
		batch.Batch = buf

		// 将批次写入流
		err = w.WriteMsg(&batch)
		if err != nil {
			logger.Debugf("写入追踪事件批次时出错: %s", err)
			goto end
		}

		// 刷新gzip写入器，确保数据被压缩并写入流
		err = gzipW.Flush()
		if err != nil {
			logger.Debugf("刷新gzip流时出错: %s", err)
			goto end
		}

	end:
		// 将缓冲区置空以回收已处理的事件
		for i := range buf {
			buf[i] = nil
		}

		// 如果通道关闭，处理结束
		if !ok {
			if err != nil {
				s.Reset() // 如果有错误，重置流
			} else {
				gzipW.Close() // 否则关闭gzip写入器
				s.Close()     // 关闭流
			}
			return
		}

		// 如果发生错误，重置流并重新打开
		if err != nil {
			s.Reset() // 重置流
			s, err = t.openStream()
			if err != nil {
				logger.Debugf("打开远程追踪器流时出错: %s", err.Error())
				return
			}

			gzipW.Reset(s) // 重置gzip写入器
		}
	}
}

// openStream 打开一个到远程对等节点的新流
// 返回值：
//   - network.Stream: 网络流
//   - error: 错误信息
func (t *RemoteTracer) openStream() (network.Stream, error) {
	for {
		ctx, cancel := context.WithTimeout(t.ctx, time.Minute)       // 创建一个带超时的上下文
		s, err := t.host.NewStream(ctx, t.peer, RemoteTracerProtoID) // 使用libp2p主机创建新流
		cancel()                                                     // 取消上下文
		if err != nil {
			if t.ctx.Err() != nil {
				return nil, err
			}

			// 等待一分钟后重试，以应对临时的服务器停机
			select {
			case <-time.After(time.Minute):
				continue
			case <-t.ctx.Done():
				return nil, t.ctx.Err()
			}
		}

		return s, nil
	}
}

// 确保 RemoteTracer 实现了 EventTracer 接口
var _ EventTracer = (*RemoteTracer)(nil)
