package database

import (
	"context"
	"errors"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

var ErrorsShutdown = errors.New("shutdown")

var lock = sync.Mutex{}
var processWait sync.WaitGroup

// Server 单机存储的存储引擎
type Server struct {
	shutdown atomic.Bool
	dbSet    []*DB
	aof      *Aof
	lg       *zap.Logger
}

var systemClient = redis.NewClient(0, nil, true)

var ttlOpsCmdLine = util.ToCmdLine("ttlops")

func (s *Server) Cron() {
	systemClient.PushCmd(ttlOpsCmdLine)
	if err := s.Exec(context.Background(), systemClient); err != nil {
		s.lg.Sugar().Infof("Cron ttl fiald: %v", err)
	}
	// 触发aof重写
	s.aofRewrite()
}

func (s *Server) aofRewrite() {
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
				if !errors.Is(err2, ErrAofRewriteIsRunning) {
					s.lg.Sugar().Errorf("aof rewrite failed with error: %v", err2)
				}
			} else {
				s.lg.Sugar().Info("aof rewrite successfully")
			}
		}()
	}
}

func (s *Server) ForEach(dbIndex int, cb func(key string, entity *DataEntity, expiration *time.Time) bool) {
	mdb, _ := s.SelectDb(dbIndex)
	if mdb.Len() > 0 {
		mdb.ForEach(cb)
	}
}

func MakeServer() *Server {
	server := &Server{}
	server.shutdown = atomic.Bool{}
	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	server.lg = lg.Named("standalone")
	if err != nil {
		server.lg.Sugar().Errorf("new ants pool failed with error: %v", err)
		panic(err)
	}
	dbSet := make([]*DB, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		dbSet[i] = MakeSimpleDb(i, server)
	}
	server.dbSet = dbSet

	if config.Properties.AppendOnly {
		aofServer, err := NewAof(
			server.Exec, config.AppendOnlyDir+config.Properties.AppendFilename, config.Properties.AppendFsync, func() (Exec, ForEach) {
				tempServer := MakeTempServer()
				return tempServer.Exec, tempServer.ForEach
			})
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofServer)
	}
	return server
}

func MakeTempServer() *Server {
	server := &Server{}
	dbSet := make([]*DB, 16)
	for i := 0; i < 16; i++ {
		dbSet[i] = MakeSimpleDb(i, server)
	}
	server.dbSet = dbSet
	return server
}

func (s *Server) CheckIndex(index int) error {
	if index >= len(s.dbSet) || index < 0 {
		return errors.New("ERR DB index is out of range")
	}
	return nil
}

func (s *Server) Rewrite() error {
	return s.aof.Rewrite()
}

func (s *Server) Exec(ctx context.Context, conn *redis.Client) error {
	lock.Lock()
	processWait.Add(1)
	defer func() {
		processWait.Done()
		lock.Unlock()
	}()
	if s.shutdown.Load() {
		return ErrorsShutdown
	}
	for conn.HasRemaining() {
		index := conn.GetDbIndex()
		mdb, err := s.SelectDb(index)
		if err != nil {
			if err2 := redis.MakeStandardErrReply(err.Error()).WriteTo(conn); err2 != nil {
				return err2
			}
			conn.ResetQueryBuffer()
			return nil
		}
		if err = processCmd(mdb, conn); err != nil {
			return err
		}
	}
	return nil
}

func processCmd(db *DB, conn *redis.Client) error {
	cmdCtx := CtxPool.Get().(*CommandContext)
	defer func() {
		cmdCtx.Reset()
		CtxPool.Put(cmdCtx)
	}()
	cmdCtx.db = db
	cmdCtx.conn = conn
	cmdCtx.cmdLine = conn.PollCmd()
	// 每次执行指令的时候都尝试和检查和清理过期的key
	if cmdCtx.GetCmdName() != "ttlops" {
		db.RandomCheckTTLAndClearV1()
	}
	if err := db.Exec(context.Background(), cmdCtx); err != nil {
		return err
	}
	return nil
}

func (s *Server) CheckAndClearDb() {
	for _, mdb := range s.dbSet {
		if mdb != nil {
			mdb.RandomCheckTTLAndClearV1()
		}
	}
}

// SelectDb 检查index是否在可选范围 0 ~ 15, 如果超过了范围返回错误的 reply, 否则返回一个对应index的db
func (s *Server) SelectDb(index int) (*DB, error) {
	if index >= len(s.dbSet) || index < 0 {
		return nil, errors.New("ERR DB index is out of range")
	}
	return s.dbSet[index], nil
}

// Init 初始化 standalone，开启一个消费者消费cmdReqQueue中的命令
func (s *Server) Init() {
	if config.Properties.AppendOnly {
		s.aof.LoadAof(0)
	}
}

func (s *Server) bindPersister(aof *Aof) {
	s.aof = aof
	for _, ddb := range s.dbSet {
		mDb := ddb
		mDb.AddAof = func(cmdLine CmdLine) {
			if config.Properties.AppendOnly {
				s.aof.AppendAof(mDb.Index, cmdLine)
			}
		}
	}
}

// Shutdown 先处理AOF的落盘, 保证已经执行过的指令不丢。
// 随后拒绝未进入队列的的请求
func (s *Server) Shutdown(cancelCtx context.Context) (err error) {
	// 拒绝新的请求
	s.shutdown.Store(true)

	processDone := make(chan struct{})

	// 启动一个 goroutine 来等待所有现有请求处理完毕
	go func() {
		processWait.Wait()
		close(processDone)
	}()

	// 使用 select 等待所有请求处理完毕或上下文超时
	select {
	case <-processDone:
		// 所有请求处理完毕
		s.lg.Sugar().Info("User requested shutdown...")
		if config.Properties.AppendOnly {
			err = s.aof.Shutdown(cancelCtx)
		}
	case <-cancelCtx.Done():
		// 上下文取消或超时
		s.lg.Sugar().Error("Shutdown was canceled or timed out.")
		err = cancelCtx.Err()
	}
	return
}
