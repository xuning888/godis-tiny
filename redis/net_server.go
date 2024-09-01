package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/gnet/v2"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/pkg/datastruct/dict"
	"github.com/xuning888/godis-tiny/pkg/datastruct/ttl"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	errStatusNotRunning = errors.New("server is not running")
)

var defaultTimeout = 60

const (
	_ = iota
	statusInitialized
	statusRunning
	statusShutdown
	statusClosed
)

type RedisServer struct {
	shutdown                atomic.Bool
	dbs                     []*DB // dbs
	aof                     *Aof
	gnet.BuiltinEventEngine                            // eventHandler
	engine                  gnet.Engine                // network engine
	connManager             *Manager                   // conn manager
	status                  uint32                     // server status
	lg                      *zap.Logger                // log
	signalWaiter            func(err chan error) error // for shutdown
}

func waitSignal(errCh chan error) error {
	signalToNotify := []os.Signal{syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, signalToNotify...)
	select {
	case sig := <-signals:
		switch sig {
		case syscall.SIGTERM:
			// force shutdown
			return errors.New(sig.String())
		case syscall.SIGHUP, syscall.SIGINT:
			return errors.New(sig.String())
		}
	case err := <-errCh:
		// network engine error
		return err
	}
	return nil
}

func (r *RedisServer) Spin() {

	if atomic.LoadUint32(&r.status) != statusInitialized {
		return
	}

	if !atomic.CompareAndSwapUint32(&r.status, statusInitialized, statusRunning) {
		return
	}

	errCh := make(chan error)
	address := fmt.Sprintf("tcp://%s:%d", config.Properties.Bind, config.Properties.Port)
	go func() {
		errCh <- gnet.Run(
			r, address,
			gnet.WithMulticore(false), // 关闭多核心, 设置numEventLoop = 1, 用于模拟redis的单线程
			// 启用定时任务
			gnet.WithTicker(true),
			// socket 60不活跃就会被驱逐
			gnet.WithTCPKeepAlive(time.Second*time.Duration(defaultTimeout)),
			gnet.WithReusePort(true),
			// 使用最少连接的负载均衡算法为eventLoop分配conn
			gnet.WithLoadBalancing(gnet.LeastConnections),
			gnet.WithLogger(r.lg.Sugar().Named("tcp-server")),
		)
	}()

	signalWaiter := waitSignal
	if r.signalWaiter != nil {
		signalWaiter = r.signalWaiter
	}

	if err := signalWaiter(errCh); err != nil {
		logger.Infof("Received SIGINT %s scheduling shutdown...", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := r.Shutdown(ctx); err != nil {
		r.lg.Sugar().Error(err)
		return
	}
}

func (r *RedisServer) Shutdown(ctx context.Context) (err error) {

	if atomic.LoadUint32(&r.status) != statusRunning {
		return errStatusNotRunning
	}

	if !atomic.CompareAndSwapUint32(&r.status, statusRunning, statusShutdown) {
		return
	}

	// stop redis engine
	if err = r.shutdown0(ctx); err != nil {
		r.lg.Sugar().Errorf("stop dbEngine failed with error: %v", err)
	}

	// stop network engine
	if err = r.engine.Stop(ctx); err != nil {
		r.lg.Sugar().Errorf("stop network engine failed with error: %v", err)
	}

	r.lg.Sugar().Info("Redis is now ready to exit, bye bye...")

	atomic.StoreUint32(&r.status, statusClosed)
	return
}

func (r *RedisServer) shutdown0(ctx context.Context) (err error) {
	// 拒绝新的请求
	r.shutdown.Store(true)
	processDone := make(chan struct{})

	// 启动一个 goroutine 来等待所有现有请求处理完毕
	go func() {
		processWait.Wait()
		close(processDone)
	}()

	// 使用 select 等待所有请求处理完毕或上下文超时
	select {
	case <-processDone:
		r.lg.Sugar().Info("User requested shutdown...")
		if config.Properties.AppendOnly {
			err = r.aof.Shutdown(ctx)
		}
	case <-ctx.Done():
		r.lg.Sugar().Error("Shutdown was canceled or timed out.")
		err = ctx.Err()
	}
	return
}

func NewRedisServer() *RedisServer {
	server := &RedisServer{}
	server.connManager = NewManager()
	ConnCounter = server.connManager
	server.dbs = initDbs()

	if config.Properties.AppendOnly {
		aofServer, err := NewAof(
			server.process, config.AppendOnlyDir+config.Properties.AppendFilename, config.Properties.AppendFsync, func() (Exec, ForEach) {
				tempServer := makeTempServer()
				return tempServer.process, tempServer.ForEach
			})
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofServer)
	}

	server.status = statusInitialized
	lg, _ := logger.CreateLogger(logger.DefaultLevel)
	server.lg = lg.Named("redis-server")
	return server
}

func initDbs() []*DB {
	dbs := make([]*DB, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		dbs[i] = NewDB(i, dict.MakeSimpleDict(), ttl.MakeSimple())
	}
	return dbs
}

func makeTempServer() *RedisServer {
	server := &RedisServer{}
	server.dbs = initDbs()
	return server
}

func (r *RedisServer) bindPersister(aof *Aof) {
	r.aof = aof
	for _, ddb := range r.dbs {
		mDb := ddb
		mDb.AddAof = func(cmdLine [][]byte) {
			if config.Properties.AppendOnly {
				r.aof.AppendAof(mDb.Index, cmdLine)
			}
		}
	}
}
