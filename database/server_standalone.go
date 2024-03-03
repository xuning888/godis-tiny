package database

import (
	"errors"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/logger"
	"godis-tiny/redis/protocol"
	"runtime"
	"sync/atomic"
)

var _ database.DBEngine = &Standalone{}

// Standalone 单机存储的存储引擎
type Standalone struct {
	dbSet       []*atomic.Value
	reqQueue    chan *database.CmdReq
	backUpQueue chan *database.CmdReq
	resQueue    chan *database.CmdRes
	cmdPushPool *ants.Pool
	lg          *zap.Logger
}

var multi = runtime.NumCPU() << 9

func MakeStandalone() *Standalone {
	server := &Standalone{
		resQueue:    make(chan *database.CmdRes, multi),
		reqQueue:    make(chan *database.CmdReq, multi),
		backUpQueue: make(chan *database.CmdReq, 1),
	}
	upLogger, err := logger.SetUpLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	server.lg = upLogger
	cmdPushPool, err := ants.NewPool(
		multi,
		ants.WithNonblocking(true),
		ants.WithPreAlloc(true),
	)
	if err != nil {
		server.lg.Sugar().Errorf("new ants pool failed with error: %v", err)
		panic(err)
	}
	server.lg.Sugar().Infof("cmdPushPool size: %v", cmdPushPool.Cap())
	server.cmdPushPool = cmdPushPool
	dbSet := make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		sdb := MakeSimpleDb(i, server, server)
		holder := &atomic.Value{}
		holder.Store(sdb)
		dbSet[i] = holder
	}
	server.dbSet = dbSet
	return server
}

func (s *Standalone) CheckIndex(index int) error {
	if index >= len(s.dbSet) || index < 0 {
		return errors.New("ERR DB index is out of range")
	}
	return nil
}

func (s *Standalone) PushReqEvent(req *database.CmdReq) {
	// 使用协程池控制协程的创建数量
	maxRetry, retry := 3, 0
	var err error = nil
	for err = s.cmdPushPool.Submit(func() {
		s.reqQueue <- req
	}); retry < maxRetry && err != nil; retry++ {
		if err != nil {
			s.lg.Sugar().Errorf("Attempt %d to push request event failed with error: %v", retry, err)
		}
	}
	if err != nil {
		s.lg.Sugar().Warnf("Command request could not be submitted after %d attempts, added to backup queue. Error: %v", retry, err)
		// 如果重试之后都没把命令放入队列，那么就把该命令放到一个缓冲区
		go func() {
			s.backUpQueue <- req
		}()
	}
	// 对协程池扩容
	if retry != 0 {
		oldCap := s.cmdPushPool.Cap()
		newCap := oldCap << 1
		s.cmdPushPool.Tune(newCap)
		s.lg.Sugar().Warnf("Increasing capacity of command execution pool from %d to %d due to past failures.", oldCap, newCap)
	}
}

func (s *Standalone) DeliverResEvent() <-chan *database.CmdRes {
	return s.resQueue
}

// doExec 调用db执行命令，将结果放到 resQueue 中
func (s *Standalone) doExec(req *database.CmdReq) *database.CmdRes {
	// 执行命令
	client := req.GetConn()
	lint := parseToLint(req.GetCmdLine())
	index := client.GetIndex()
	var reply redis.Reply = nil
	db, reply := s.selectDb(index)
	if reply != nil {
		return database.MakeCmdRes(client, reply)
	} else {
		// 每次执行指令的时候都尝试和检查和清理过期的key
		if lint.GetCmdName() != "ttlops" {
			db.RandomCheckTTLAndClearV1()
		}
		// 执行指令
		reply = db.Exec(client, lint)
		return database.MakeCmdRes(client, reply)
	}
}

func (s *Standalone) CheckAndClearDb() {
	for _, dbHolder := range s.dbSet {
		val := dbHolder.Load()
		if val != nil {
			db := dbHolder.Load().(*DB)
			db.RandomCheckTTLAndClearV1()
		}
	}
}

// selectDb 检查index是否在可选范围 0 ~ 15, 如果超过了范围返回错误的 reply, 否则返回一个对应index的db
func (s *Standalone) selectDb(index int) (*DB, redis.Reply) {
	if index >= len(s.dbSet) || index < 0 {
		return nil, protocol.MakeStandardErrReply("ERR DB index is out of range")
	}
	return s.dbSet[index].Load().(*DB), nil
}

// Close 关闭资源
func (s *Standalone) Close() error {
	// 关闭接收命令的队列
	close(s.reqQueue)
	// 关闭返回结果的队列
	close(s.resQueue)
	// 释放协程池
	s.cmdPushPool.Release()
	return nil
}

// Init 初始化 standalone，开启一个消费者消费cmdReqQueue中的命令
func (s *Standalone) Init() {
	initResister()
	// 开启一个协程来消费req队列
	s.startCmdConsumer()
	// 开启一个协程监听缓冲区的队列
	s.backUpQueueConsumer()
}

// startCmdConsumer 开启一个协程消费指令
func (s *Standalone) startCmdConsumer() {
	go func() {
		for {
			select {
			case cmdReq, ok := <-s.reqQueue:
				if !ok {
					return
				}
				s.resQueue <- s.doExec(cmdReq)
			}
		}
	}()
}

// backUpQueueConsumer 把缓冲区队列中的命令放到reqQueue中
func (s *Standalone) backUpQueueConsumer() {
	go func() {
		for {
			select {
			case cmdReq, ok := <-s.backUpQueue:
				if !ok {
					return
				}
				s.reqQueue <- cmdReq
			}
		}
	}()
}
