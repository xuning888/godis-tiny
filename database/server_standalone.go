package database

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"godis-tiny/config"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/persistence"
	"godis-tiny/pkg/logger"
	"godis-tiny/redis/protocol"
	"runtime"
	"sync/atomic"
)

var _ database.DBEngine = &Standalone{}

// Standalone 单机存储的存储引擎
type Standalone struct {
	dbSet    []*atomic.Value
	aof      persistence.Aof
	reqQueue chan *database.CmdReq
	resQueue chan *database.CmdRes
	lg       *zap.Logger
}

var multi = runtime.NumCPU() << 9

func MakeStandalone() *Standalone {
	server := &Standalone{
		resQueue: make(chan *database.CmdRes, multi),
		reqQueue: make(chan *database.CmdReq, 1),
	}
	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	server.lg = lg.Named("standalone")
	if err != nil {
		server.lg.Sugar().Errorf("new ants pool failed with error: %v", err)
		panic(err)
	}
	dbSet := make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		sdb := MakeSimpleDb(i, server, server)
		holder := &atomic.Value{}
		holder.Store(sdb)
		dbSet[i] = holder
	}
	server.dbSet = dbSet

	if config.Properties.AppendOnly {
		aofServer, err := persistence.NewAof(server, config.Properties.AppendFilename, config.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofServer)
	}
	return server
}

func (s *Standalone) CheckIndex(index int) error {
	if index >= len(s.dbSet) || index < 0 {
		return errors.New("ERR DB index is out of range")
	}
	return nil
}

func (s *Standalone) PushReqEvent(req *database.CmdReq) {
	go func() {
		s.reqQueue <- req
	}()
}

func (s *Standalone) DeliverResEvent() <-chan *database.CmdRes {
	return s.resQueue
}

func (s *Standalone) Exec(req *database.CmdReq) *database.CmdRes {
	return s.doExec(req)
}

// doExec 调用db执行命令，将结果放到 resQueue 中
func (s *Standalone) doExec(req *database.CmdReq) *database.CmdRes {
	// 执行命令
	client := req.GetConn()
	index := client.GetIndex()
	var reply redis.Reply = nil
	db, reply := s.selectDb(index)
	if reply != nil {
		return database.MakeCmdRes(client, reply)
	}
	cmdCtx := CtxPool.Get().(*CommandContext)
	cmdCtx.db = db
	cmdCtx.conn = client
	cmdCtx.cmdLine = req.GetCmdLine()
	defer func() {
		cmdCtx.Reset()
		CtxPool.Put(cmdCtx)
	}()
	// 每次执行指令的时候都尝试和检查和清理过期的key
	if cmdCtx.GetCmdName() != "ttlops" {
		db.RandomCheckTTLAndClearV1()
	}
	// 执行指令
	reply = db.Exec(context.Background(), cmdCtx)
	if reply != nil {
		return database.MakeCmdRes(client, reply)
	}
	return nil
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
	s.lg.Sugar().Infof("close standalone dbEngine")
	// 关闭接收命令的队列
	close(s.reqQueue)
	// 关闭返回结果的队列
	close(s.resQueue)
	return nil
}

// Init 初始化 standalone，开启一个消费者消费cmdReqQueue中的命令
func (s *Standalone) Init() {
	initResister()
	if config.Properties.AppendOnly {
		s.aof.LoadAof(0)
	}
	// 开启一个协程来消费req队列
	s.startCmdConsumer()
}

func (s *Standalone) bindPersister(aof persistence.Aof) {
	s.aof = aof
	for _, db := range s.dbSet {
		mDb := db.Load().(*DB)
		mDb.addAof = func(cmdLine database.CmdLine) {
			if config.Properties.AppendOnly {
				s.aof.AppendAof(mDb.index, cmdLine)
			}
		}
	}
}

// startCmdConsumer 开启一个协程消费指令
func (s *Standalone) startCmdConsumer() {
	go func() {
		for {
			select {
			case cmdReq, ok := <-s.reqQueue:
				if !ok {
					s.lg.Info("cmdReqQueue is closed")
					return
				}
				cmdRes := s.doExec(cmdReq)
				if cmdRes == nil || cmdRes.GetConn().IsInner() {
					continue
				}
				s.resQueue <- cmdRes
			}
		}
	}()
}
