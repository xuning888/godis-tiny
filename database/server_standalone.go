package database

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"godis-tiny/config"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/persistence"
	atomic2 "godis-tiny/pkg/atomic"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection"
	"godis-tiny/redis/protocol"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var _ database.DBEngine = &Standalone{}

// Standalone 单机存储的存储引擎
type Standalone struct {
	shutdown *atomic2.Boolean
	reqWait  sync.WaitGroup
	resWait  sync.WaitGroup
	ctx      context.Context
	cancel   func()
	dbSet    []*atomic.Value
	aof      *persistence.Aof
	reqQueue chan *database.CmdReq
	resQueue chan *database.CmdRes
	lg       *zap.Logger
}

func (s *Standalone) Cron() {
	cmdReq := database.MakeCmdReq(connection.SystemCon, util.ToCmdLine("ttlops"))
	s.PushReqEvent(cmdReq)

	// 触发aof重写
	s.aofRewrite()
}

func (s *Standalone) aofRewrite() {
	if !config.Properties.AppendOnly {
		return
	}
	defer s.lg.Sync()
	// 当前aof文件的大小
	currentAofFileSize, err := s.aof.CurrentAofSize()
	if err != nil {
		s.lg.Sugar().Errorf("check aof filesize failed with error: %v", err)
		return
	}
	// 上一次aof重写后的大小
	lastAofRewriteSize := s.aof.LasAofRewriteSize()
	// 计算aof文件的增长量
	aofSizeIncrease := currentAofFileSize - lastAofRewriteSize

	if aofSizeIncrease <= 0 {
		return
	}
	// 计算当前增长的百分比是否超过设置的阈值
	rewriteNeeded := (aofSizeIncrease >= int64(float64(lastAofRewriteSize)*float64(config.Properties.AofRewritePercentage)/100.0)) &&
		(currentAofFileSize >= int64(config.Properties.AofRewriteMinSize))

	if rewriteNeeded {
		go func() {
			err2 := s.aof.Rewrite()
			if err2 != nil {
				if !errors.Is(err2, persistence.ErrAofRewriteIsRunning) {
					s.lg.Sugar().Errorf("aof rewrite failed with error: %v", err2)
				}
			} else {
				s.lg.Sugar().Info("aof rewrite successfully")
			}
		}()
	}
}

func (s *Standalone) ForEach(dbIndex int, cb func(key string, entity *database.DataEntity, expiration *time.Time) bool) {
	db, _ := s.selectDb(dbIndex)
	db.ForEach(cb)
}

var multi = runtime.NumCPU()

func MakeStandalone() *Standalone {
	ctx, cancelFunc := context.WithCancel(context.Background())
	server := &Standalone{
		shutdown: new(atomic2.Boolean),
		resQueue: make(chan *database.CmdRes, multi),
		reqQueue: make(chan *database.CmdReq, multi),
	}
	server.ctx = ctx
	server.cancel = cancelFunc
	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	server.lg = lg.Named("standalone")
	if err != nil {
		server.lg.Sugar().Errorf("new ants pool failed with error: %v", err)
		panic(err)
	}
	dbSet := make([]*atomic.Value, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		sdb := MakeSimpleDb(i, server, server)
		holder := &atomic.Value{}
		holder.Store(sdb)
		dbSet[i] = holder
	}
	server.dbSet = dbSet

	if config.Properties.AppendOnly {
		aofServer, err := persistence.NewAof(
			server, config.AppendOnlyDir+config.Properties.AppendFilename, config.Properties.AppendFsync, MakeTemp)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofServer)
	}
	return server
}

func MakeTemp() database.DBEngine {
	server := &Standalone{}
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

func (s *Standalone) Rewrite() error {
	return s.aof.Rewrite()
}

var ErrorsShutdown = errors.New("shutdown")

func (s *Standalone) PushReqEvent(req *database.CmdReq) error {
	if s.shutdown.Get() {
		return ErrorsShutdown
	}
	s.reqWait.Add(1)
	go func() {
		defer s.reqWait.Done()
		reqq := req
		select {
		case <-s.ctx.Done():
			s.lg.Sugar().Errorf("push aborted: Standalone is shutting down")
			return
		case s.reqQueue <- reqq:
			return
		}
	}()
	return nil
}

func (s *Standalone) DeliverResEvent() <-chan *database.CmdRes {
	return s.resQueue
}

// Exec 这是一个无锁实现, 用于AofLoad
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

// Init 初始化 standalone，开启一个消费者消费cmdReqQueue中的命令
func (s *Standalone) Init() {
	initResister()
	if config.Properties.AppendOnly {
		s.aof.LoadAof(0)
	}
	// 开启一个协程来消费req队列
	s.startCmdConsumer()
}

func (s *Standalone) bindPersister(aof *persistence.Aof) {
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

// Shutdown 先处理AOF的落盘, 保证已经执行过的指令不丢。
// 随后拒绝未进入队列的的请求
func (s *Standalone) Shutdown(cancelCtx context.Context) (err error) {
	s.lg.Sugar().Info("shutdown dbEngine...")
	// 拒绝新的请求
	s.shutdown.Set(true)
	// 等待已经接收的请求执行
	s.reqWait.Wait()
	// 等待执行完的请求返回数据
	s.resWait.Wait()
	s.cancel()
	if config.Properties.AppendOnly {
		return s.aof.Shutdown(cancelCtx)
	}
	return
}

// startCmdConsumer 开启一个协程消费指令
func (s *Standalone) startCmdConsumer() {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				s.lg.Info("receive cmdRequeue closed")
				return
			case cmdReq, ok := <-s.reqQueue:
				if !ok {
					s.lg.Info("cmdReqQueue is closed")
					return
				}
				cmdRes := s.doExec(cmdReq)
				if cmdRes == nil || cmdRes.GetConn().IsInner() {
					continue
				}

				devl := func(res *database.CmdRes) {
					defer s.resWait.Done()
					s.resQueue <- res
				}
				s.resWait.Add(1)
				devl(cmdRes)
			}
		}
	}()
}
