// 作用：消息验证。
// 功能：定义和实现消息的验证机制，确保只有有效的消息才能被处理和传播。

package pubsub

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
)

// 默认验证队列大小
const (
	defaultValidateQueueSize   = 32   // 默认验证队列大小为32
	defaultValidateConcurrency = 1024 // 默认并发验证限制为1024
	defaultValidateThrottle    = 8192 // 默认验证节流限制为8192
)

// ValidationError 表示消息验证失败时可能会发出的错误
type ValidationError struct {
	Reason string // 错误原因
}

// Error 返回验证错误的原因
func (e ValidationError) Error() string {
	return e.Reason
}

// Validator 是一个验证消息的函数，返回二元决策：接受或拒绝
type Validator func(context.Context, peer.ID, *Message) bool

// ValidatorEx 是一个扩展的验证函数，返回枚举决策
type ValidatorEx func(context.Context, peer.ID, *Message) ValidationResult

// ValidationResult 表示扩展验证器的决策结果
type ValidationResult int

const (
	// ValidationAccept 表示消息验证通过，应该被接受并交付给应用程序并转发到网络
	ValidationAccept = ValidationResult(0)
	// ValidationReject 表示消息验证失败，不应该交付给应用程序或转发到网络，并且转发该消息的对等节点应该被惩罚
	ValidationReject = ValidationResult(1)
	// ValidationIgnore 表示消息应该被忽略，不交付给应用程序或转发到网络，但与 ValidationReject 不同，转发该消息的对等节点不会被惩罚
	ValidationIgnore = ValidationResult(2)
	// internal 表示内部验证节流
	validationThrottled = ValidationResult(-1)
)

// ValidatorOpt 是 RegisterTopicValidator 的选项类型
type ValidatorOpt func(addVal *addValReq) error

// validation 表示验证管道
// 验证管道执行签名验证并运行每个主题的用户配置验证器。可以调整各种并发参数，如工作线程数量和最大并发验证数量。用户还可以附加内联验证器，这些验证器将同步执行，这对于轻量级任务可以防止不必要的上下文切换。
type validation struct {
	p *PubSub // 关联的 PubSub 实例

	tracer *pubsubTracer // 用于追踪验证过程的跟踪器

	// mx 保护验证器映射
	mx sync.Mutex
	// topicVals 跟踪每个主题的验证器
	topicVals map[string]*validatorImpl

	// defaultVals 跟踪适用于所有主题的默认验证器
	defaultVals []*validatorImpl

	// validateQ 是验证管道的前端
	validateQ chan *validateReq

	// validateThrottle 限制活动验证 goroutine 的数量
	validateThrottle chan struct{}

	// validateWorkers 是同步验证工作线程的数量
	validateWorkers int
}

// validateReq 表示验证请求
type validateReq struct {
	vals []*validatorImpl // 验证器列表
	src  peer.ID          // 消息来源的对等节点 ID
	msg  *Message         // 要验证的消息
}

// validatorImpl 表示主题验证器
type validatorImpl struct {
	topic            string        // 验证器所属的主题
	validate         ValidatorEx   // 验证函数
	validateTimeout  time.Duration // 验证超时时间
	validateThrottle chan struct{} // 验证节流通道
	validateInline   bool          // 是否内联验证
}

// addValReq 表示添加主题验证器的异步请求
type addValReq struct {
	topic    string        // 要添加验证器的主题
	validate interface{}   // 验证函数
	timeout  time.Duration // 验证超时时间
	throttle int           // 验证节流大小
	inline   bool          // 是否内联验证
	resp     chan error    // 响应通道，返回添加验证器的结果
}

// rmValReq 表示移除主题验证器的异步请求
type rmValReq struct {
	topic string     // 要移除验证器的主题
	resp  chan error // 响应通道，返回移除验证器的结果
}

// newValidation 创建一个新的验证管道
func newValidation() *validation {
	return &validation{
		topicVals:        make(map[string]*validatorImpl),                   // 初始化主题验证器映射
		validateQ:        make(chan *validateReq, defaultValidateQueueSize), // 初始化验证请求队列
		validateThrottle: make(chan struct{}, defaultValidateThrottle),      // 初始化验证节流
		validateWorkers:  runtime.NumCPU(),                                  // 设置同步验证工作线程数为 CPU 数量
	}
}

// Start 启动验证管道并附加到 pubsub 实例
// 参数:
//   - p: *PubSub 关联的 PubSub 实例
func (v *validation) Start(p *PubSub) {
	v.p = p                                  // 设置关联的 PubSub 实例
	v.tracer = p.tracer                      // 设置追踪器实例
	for i := 0; i < v.validateWorkers; i++ { // 启动指定数量的验证工作线程
		go v.validateWorker() // 启动验证工作线程
	}
}

// AddValidator 添加一个新的验证器
// 参数:
//   - req: *addValReq 添加验证器的请求
func (v *validation) AddValidator(req *addValReq) {
	val, err := v.makeValidator(req) // 创建验证器
	if err != nil {
		req.resp <- err // 如果出错，发送错误响应
		return
	}

	v.mx.Lock()         // 加锁以防止并发修改
	defer v.mx.Unlock() // 在函数返回前解锁

	topic := val.topic // 获取验证器的主题

	_, ok := v.topicVals[topic] // 检查是否已存在同主题的验证器
	if ok {
		req.resp <- fmt.Errorf("duplicate validator for topic %s", topic) // 如果主题已存在验证器，返回错误
		return
	}

	v.topicVals[topic] = val // 添加验证器到映射中
	req.resp <- nil          // 发送成功响应
}

// makeValidator 创建一个验证器
// 参数:
//   - req: *addValReq 添加验证器的请求
//
// 返回值：
//   - *validatorImpl 创建的验证器
//   - error 错误信息
func (v *validation) makeValidator(req *addValReq) (*validatorImpl, error) {
	// 将简单的 Validator 转换为扩展的 ValidatorEx
	makeValidatorEx := func(v Validator) ValidatorEx {
		return func(ctx context.Context, p peer.ID, msg *Message) ValidationResult {
			if v(ctx, p, msg) { // 调用简单的 Validator 函数
				return ValidationAccept // 如果验证通过，返回 ValidationAccept
			}
			return ValidationReject // 如果验证失败，返回 ValidationReject
		}
	}

	var validator ValidatorEx         // 声明扩展的 ValidatorEx 类型的验证器
	switch v := req.validate.(type) { // 根据请求中的验证器类型进行转换
	case func(ctx context.Context, p peer.ID, msg *Message) bool:
		validator = makeValidatorEx(Validator(v)) // 将简单的 Validator 转换为 ValidatorEx
	case Validator:
		validator = makeValidatorEx(v) // 将 Validator 转换为 ValidatorEx

	case func(ctx context.Context, p peer.ID, msg *Message) ValidationResult:
		validator = ValidatorEx(v) // 如果已经是 ValidatorEx 类型，则直接赋值
	case ValidatorEx:
		validator = v // 如果已经是 ValidatorEx 类型，则直接赋值

	default: // 如果验证器类型未知，返回错误
		topic := req.topic // 获取请求中的主题
		if req.topic == "" {
			topic = "(default)" // 如果主题为空，设置为默认值
		}
		return nil, fmt.Errorf("unknown validator type for topic %s; must be an instance of Validator or ValidatorEx", topic)
	}

	val := &validatorImpl{
		topic:            req.topic,                                       // 设置主题
		validate:         validator,                                       // 设置验证函数
		validateTimeout:  0,                                               // 设置验证超时时间为 0
		validateThrottle: make(chan struct{}, defaultValidateConcurrency), // 初始化验证节流通道，默认并发数
		validateInline:   req.inline,                                      // 设置是否内联验证
	}

	if req.timeout > 0 { // 如果请求中指定了超时时间
		val.validateTimeout = req.timeout // 设置验证超时时间
	}

	if req.throttle > 0 { // 如果请求中指定了节流值
		val.validateThrottle = make(chan struct{}, req.throttle) // 重新初始化验证节流通道，指定并发数
	}

	return val, nil // 返回创建的验证器和 nil 错误
}

// RemoveValidator 删除一个现有的验证器
// 参数:
//   - req: *rmValReq 删除验证器的请求
func (v *validation) RemoveValidator(req *rmValReq) {
	v.mx.Lock()         // 加锁以防止并发修改
	defer v.mx.Unlock() // 在函数返回前解锁

	topic := req.topic // 获取请求中的主题

	_, ok := v.topicVals[topic] // 检查主题是否存在验证器
	if ok {
		delete(v.topicVals, topic) // 删除主题验证器
		req.resp <- nil            // 发送成功响应
	} else {
		req.resp <- fmt.Errorf("no validator for topic %s", topic) // 发送错误响应，如果主题没有验证器
	}
}

// PushLocal 同步推送本地发布的消息并执行验证
// 参数:
//   - msg: *Message 要推送的消息
//
// 返回值：
//   - error 如果验证失败则返回错误
func (v *validation) PushLocal(msg *Message) error {
	v.p.tracer.PublishMessage(msg) // 追踪发布的消息

	err := v.p.checkSigningPolicy(msg) // 检查消息的签名策略
	if err != nil {
		return err // 如果签名检查失败，返回错误
	}

	vals := v.getValidators(msg)                         // 获取消息的验证器
	return v.validate(vals, msg.ReceivedFrom, msg, true) // 执行验证，返回验证结果
}

// Push 将消息推送到验证管道中
// 参数:
//   - src: peer.ID 消息来源的对等节点 ID
//   - msg: *Message 要推送的消息
//
// 返回值：
//   - bool 如果消息可以立即转发而无需验证，则返回 true
func (v *validation) Push(src peer.ID, msg *Message) bool {
	vals := v.getValidators(msg) // 获取消息的验证器

	if len(vals) > 0 || msg.Signature != nil { // 如果存在验证器或消息有签名
		select {
		case v.validateQ <- &validateReq{vals, src, msg}: // 将验证请求推送到验证队列
		default:
			logrus.Debugf("message validation throttled: queue full; dropping message from %s", src) // 验证队列已满，丢弃消息
			v.tracer.RejectMessage(msg, RejectValidationQueueFull)                                   // 记录消息被拒绝的原因
		}
		return false // 消息不能立即转发，需要验证
	}

	return true // 消息可以立即转发
}

// getValidators 返回适用于给定消息的所有验证器
// 参数:
//   - msg: *Message 要获取验证器的消息
//
// 返回值：
//   - []*validatorImpl 验证器列表
func (v *validation) getValidators(msg *Message) []*validatorImpl {
	v.mx.Lock()         // 加锁以防止并发修改
	defer v.mx.Unlock() // 在函数返回前解锁

	var vals []*validatorImpl
	vals = append(vals, v.defaultVals...) // 添加默认验证器

	topic := msg.GetTopic() // 获取消息的主题

	val, ok := v.topicVals[topic] // 检查主题是否有验证器
	if !ok {
		return vals // 如果没有主题验证器，返回默认验证器列表
	}

	return append(vals, val) // 添加主题验证器并返回
}

// validateWorker 是一个执行内联验证的 goroutine
func (v *validation) validateWorker() {
	for {
		select {
		case req := <-v.validateQ: // 从验证队列中接收验证请求
			v.validate(req.vals, req.src, req.msg, false) // 执行验证
		case <-v.p.ctx.Done(): // 如果上下文已关闭，退出循环
			return
		}
	}
}

// validate 执行验证，只有在所有验证器成功时才发送消息
// 参数:
//   - vals: []*validatorImpl 验证器列表
//   - src: peer.ID 消息来源的对等节点 ID
//   - msg: *Message 要验证的消息
//   - synchronous: bool 是否同步验证
//
// 返回值：
//   - error 验证错误信息
func (v *validation) validate(vals []*validatorImpl, src peer.ID, msg *Message, synchronous bool) error {
	// 如果启用了签名验证但禁用了签名，则接收消息时 Signature 应为 nil
	if msg.Signature != nil {
		if !v.validateSignature(msg) { // 验证消息签名
			logrus.Debugf("message signature validation failed; dropping message from %s", src) // 签名验证失败，丢弃消息
			v.tracer.RejectMessage(msg, RejectInvalidSignature)                                 // 记录消息被拒绝的原因
			return ValidationError{Reason: RejectInvalidSignature}                              // 返回验证错误
		}
	}

	// 现在我们已经验证了签名，可以标记消息为已看到，避免多次调用用户验证器
	id := v.p.idGen.ID(msg) // 生成消息的唯一 ID
	if !v.p.markSeen(id) {  // 标记消息为已看到
		v.tracer.DuplicateMessage(msg) // 记录重复消息
		return nil                     // 返回 nil 表示消息已处理
	} else {
		v.tracer.ValidateMessage(msg) // 记录消息验证成功
	}

	var inline, async []*validatorImpl // 声明内联和异步验证器列表
	for _, val := range vals {         // 遍历所有验证器
		if val.validateInline || synchronous {
			inline = append(inline, val) // 内联验证器
		} else {
			async = append(async, val) // 异步验证器
		}
	}

	// 应用内联（同步）验证器
	result := ValidationAccept // 初始化验证结果为接受
loop:
	for _, val := range inline { // 遍历所有内联验证器
		switch val.validateMsg(v.p.ctx, src, msg) { // 执行验证
		case ValidationAccept:
		case ValidationReject:
			result = ValidationReject // 验证失败，更新结果
			break loop                // 跳出循环
		case ValidationIgnore:
			result = ValidationIgnore // 忽略验证，更新结果
		}
	}

	if result == ValidationReject { // 如果验证结果为拒绝
		logrus.Debugf("message validation failed; dropping message from %s", src) // 验证失败，丢弃消息
		v.tracer.RejectMessage(msg, RejectValidationFailed)                       // 记录消息被拒绝的原因
		return ValidationError{Reason: RejectValidationFailed}                    // 返回验证错误
	}

	// 应用异步验证器
	if len(async) > 0 { // 如果存在异步验证器
		select {
		case v.validateThrottle <- struct{}{}: // 发送节流信号
			go func() { // 启动新的 goroutine 执行异步验证
				v.doValidateTopic(async, src, msg, result) // 执行异步验证
				<-v.validateThrottle                       // 验证完成后释放节流信号
			}()
		default:
			logrus.Debugf("message validation throttled; dropping message from %s", src) // 验证节流，丢弃消息
			v.tracer.RejectMessage(msg, RejectValidationThrottled)                       // 记录消息被拒绝的原因
		}
		return nil // 返回 nil 表示消息已处理
	}

	if result == ValidationIgnore { // 如果验证结果为忽略
		v.tracer.RejectMessage(msg, RejectValidationIgnored)    // 记录消息被忽略的原因
		return ValidationError{Reason: RejectValidationIgnored} // 返回验证错误
	}

	// 没有异步验证器，消息验证通过，发送消息
	select {
	case v.p.sendMsg <- msg: // 发送消息到发送通道
		return nil // 返回 nil 表示消息已发送
	case <-v.p.ctx.Done(): // 如果上下文已关闭
		return v.p.ctx.Err() // 返回上下文错误
	}
}

// validateSignature 验证消息签名
// 参数:
//   - msg: *Message 要验证的消息
//
// 返回值：
//   - bool 签名验证结果
func (v *validation) validateSignature(msg *Message) bool {
	err := verifyMessageSignature(msg.Message) // 验证消息签名
	if err != nil {
		logrus.Debugf("signature verification error: %s", err.Error()) // 签名验证错误，记录日志
		return false                                                   // 返回 false 表示签名验证失败
	}

	return true // 返回 true 表示签名验证成功
}

// doValidateTopic 执行主题验证
// 参数:
//   - vals: []*validatorImpl 验证器列表
//   - src: peer.ID 消息来源的对等节点 ID
//   - msg: *Message 要验证的消息
//   - r: ValidationResult 验证结果
func (v *validation) doValidateTopic(vals []*validatorImpl, src peer.ID, msg *Message, r ValidationResult) {
	result := v.validateTopic(vals, src, msg) // 执行主题验证

	if result == ValidationAccept && r != ValidationAccept {
		result = r // 使用之前的验证结果更新当前结果
	}

	switch result { // 根据验证结果执行相应操作
	case ValidationAccept:
		v.p.sendMsg <- msg // 发送消息到发送通道
	case ValidationReject:
		logrus.Debugf("message validation failed; dropping message from %s", src) // 验证失败，丢弃消息
		v.tracer.RejectMessage(msg, RejectValidationFailed)                       // 记录消息被拒绝的原因
		return
	case ValidationIgnore:
		logrus.Debugf("message validation punted; ignoring message from %s", src) // 验证忽略，记录日志
		v.tracer.RejectMessage(msg, RejectValidationIgnored)                      // 记录消息被忽略的原因
		return
	case validationThrottled:
		logrus.Debugf("message validation throttled; ignoring message from %s", src) // 验证节流，记录日志
		v.tracer.RejectMessage(msg, RejectValidationThrottled)                       // 记录消息被节流的原因

	default:
		panic(fmt.Errorf("unexpected validation result: %d", result)) // 内部编程错误，恐慌处理
	}
}

// validateTopic 执行主题验证
// 参数:
//   - vals: []*validatorImpl 验证器列表
//   - src: peer.ID 消息来源的对等节点 ID
//   - msg: *Message 要验证的消息
//
// 返回值：
//   - ValidationResult 验证结果
func (v *validation) validateTopic(vals []*validatorImpl, src peer.ID, msg *Message) ValidationResult {
	if len(vals) == 1 { // 如果只有一个验证器
		return v.validateSingleTopic(vals[0], src, msg) // 单一验证器的快速路径
	}

	ctx, cancel := context.WithCancel(v.p.ctx) // 创建可取消的上下文
	defer cancel()                             // 在函数返回前取消上下文

	rch := make(chan ValidationResult, len(vals)) // 创建一个缓冲通道，长度为验证器数量
	rcount := 0                                   // 初始化验证器计数

	for _, val := range vals { // 遍历所有验证器
		rcount++ // 计数增加

		select {
		case val.validateThrottle <- struct{}{}: // 发送节流信号
			go func(val *validatorImpl) { // 启动新的 goroutine 执行验证
				rch <- val.validateMsg(ctx, src, msg) // 执行验证并发送结果到通道
				<-val.validateThrottle                // 释放节流信号
			}(val)

		default:
			logrus.Debugf("validation throttled for topic %s", val.topic) // 验证节流，记录日志
			rch <- validationThrottled                                    // 发送节流结果到通道
		}
	}

	result := ValidationAccept // 初始化验证结果为接受
loop:
	for i := 0; i < rcount; i++ { // 遍历所有验证结果
		switch <-rch {
		case ValidationAccept:
		case ValidationReject:
			result = ValidationReject // 验证失败，更新结果
			break loop                // 跳出循环
		case ValidationIgnore:
			if result != validationThrottled {
				result = ValidationIgnore // 忽略验证，更新结果
			}
		case validationThrottled:
			result = validationThrottled // 节流验证，更新结果
		}
	}

	return result // 返回最终验证结果
}

// validateSingleTopic 执行单一验证器的验证
// 参数:
//   - val: *validatorImpl 验证器
//   - src: peer.ID 消息来源的对等节点 ID
//   - msg: *Message 要验证的消息
//
// 返回值：
//   - ValidationResult 验证结果
func (v *validation) validateSingleTopic(val *validatorImpl, src peer.ID, msg *Message) ValidationResult {
	select {
	case val.validateThrottle <- struct{}{}: // 发送节流信号
		res := val.validateMsg(v.p.ctx, src, msg) // 执行验证并获取结果
		<-val.validateThrottle                    // 释放节流信号
		return res                                // 返回验证结果

	default:
		logrus.Debugf("validation throttled for topic %s", val.topic) // 验证节流，记录日志
		return validationThrottled                                    // 返回节流结果
	}
}

// validateMsg 执行验证器的验证
// 参数:
//   - ctx: context.Context 上下文
//   - src: peer.ID 消息来源的对等节点 ID
//   - msg: *Message 要验证的消息
//
// 返回值：
//   - ValidationResult 验证结果
func (val *validatorImpl) validateMsg(ctx context.Context, src peer.ID, msg *Message) ValidationResult {
	start := time.Now() // 记录开始时间
	defer func() {
		logrus.Debugf("validation done; took %s", time.Since(start)) // 输出验证耗时
	}()

	if val.validateTimeout > 0 { // 如果设置了验证超时时间
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, val.validateTimeout) // 创建带超时的上下文
		defer cancel()                                              // 在函数返回前取消上下文
	}

	r := val.validate(ctx, src, msg) // 执行验证并获取结果
	switch r {                       // 根据验证结果返回相应的值
	case ValidationAccept:
		fallthrough
	case ValidationReject:
		fallthrough
	case ValidationIgnore:
		return r

	default:
		logrus.Warnf("Unexpected result from validator: %d; ignoring message", r) // 输出警告日志，记录意外的验证结果
		return ValidationIgnore                                                   // 返回忽略结果
	}
}

// WithDefaultValidator 添加一个默认验证器，适用于所有主题
// 参数:
//   - val: interface{} 验证器
//   - opts: ...ValidatorOpt 验证器选项
//
// 返回值：
//   - Option 配置选项
func WithDefaultValidator(val interface{}, opts ...ValidatorOpt) Option {
	return func(ps *PubSub) error {
		addVal := &addValReq{
			validate: val, // 设置验证器
		}

		for _, opt := range opts { // 遍历所有验证器选项并应用
			err := opt(addVal)
			if err != nil {
				return err // 如果应用选项出错，返回错误
			}
		}

		val, err := ps.val.makeValidator(addVal) // 创建验证器
		if err != nil {
			return err // 如果创建验证器出错，返回错误
		}

		ps.val.defaultVals = append(ps.val.defaultVals, val) // 添加验证器到默认验证器列表
		return nil                                           // 返回 nil 表示成功
	}
}

// WithValidateQueueSize 设置验证队列的大小，默认大小为 32
// 参数:
//   - n: int 队列大小
//
// 返回值：
//   - Option 配置选项
func WithValidateQueueSize(n int) Option {
	return func(ps *PubSub) error { // 返回一个配置选项函数
		if n > 0 { // 如果队列大小大于 0
			ps.val.validateQ = make(chan *validateReq, n) // 创建一个带缓冲的验证请求通道
			return nil                                    // 返回 nil 表示成功
		}
		return fmt.Errorf("validate queue size must be > 0") // 返回错误，队列大小必须大于 0
	}
}

// WithValidateThrottle 设置活动验证 goroutine 的上限，默认值为 8192
// 参数:
//   - n: int 上限值
//
// 返回值：
//   - Option 配置选项
func WithValidateThrottle(n int) Option {
	return func(ps *PubSub) error { // 返回一个配置选项函数
		ps.val.validateThrottle = make(chan struct{}, n) // 创建一个带缓冲的节流信号通道
		return nil                                       // 返回 nil 表示成功
	}
}

// WithValidateWorkers 设置同步验证工作线程的数量，默认值为 CPU 数量
// 参数:
//   - n: int 线程数量
//
// 返回值：
//   - Option 配置选项
func WithValidateWorkers(n int) Option {
	return func(ps *PubSub) error { // 返回一个配置选项函数
		if n > 0 { // 如果线程数量大于 0
			ps.val.validateWorkers = n // 设置验证工作线程的数量
			return nil                 // 返回 nil 表示成功
		}
		return fmt.Errorf("number of validation workers must be > 0") // 返回错误，线程数量必须大于 0
	}
}

// WithValidatorTimeout 是一个选项，用于设置异步主题验证器的超时时间，默认无超时
// 参数:
//   - timeout: time.Duration 超时时间
//
// 返回值：
//   - ValidatorOpt 验证器选项
func WithValidatorTimeout(timeout time.Duration) ValidatorOpt {
	return func(addVal *addValReq) error { // 返回一个验证器选项函数
		addVal.timeout = timeout // 设置验证器的超时时间
		return nil               // 返回 nil 表示成功
	}
}

// WithValidatorConcurrency 是一个选项，用于设置主题验证器的节流大小，默认值为 1024
// 参数:
//   - n: int 节流大小
//
// 返回值：
//   - ValidatorOpt 验证器选项
func WithValidatorConcurrency(n int) ValidatorOpt {
	return func(addVal *addValReq) error { // 返回一个验证器选项函数
		addVal.throttle = n // 设置验证器的节流大小
		return nil          // 返回 nil 表示成功
	}
}

// WithValidatorInline 是一个选项，用于设置验证器是否内联执行
// 参数:
//   - inline: bool 是否内联执行
//
// 返回值：
//   - ValidatorOpt 验证器选项
func WithValidatorInline(inline bool) ValidatorOpt {
	return func(addVal *addValReq) error { // 返回一个验证器选项函数
		addVal.inline = inline // 设置验证器是否内联执行
		return nil             // 返回 nil 表示成功
	}
}
