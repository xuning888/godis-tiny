package tcp

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/gnet/v2"
	"github.com/xuning888/godis-tiny/config"
	database2 "github.com/xuning888/godis-tiny/database"
	"github.com/xuning888/godis-tiny/interface/database"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"github.com/xuning888/godis-tiny/redis/connection"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
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
	writeWg sync.WaitGroup
	gnet.BuiltinEventEngine
	status       uint32
	engine       gnet.Engine
	dbEngine     database.DBEngine
	connManager  redis.ConnManager
	stopChan     chan struct{}
	lg           *zap.Logger
	signalWaiter func(err chan error) error
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
			gnet.WithReadBufferCap(1<<18),
			// 启用多核心, 开启后NumEventLoops = coreSize
			gnet.WithMulticore(true),
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
	if err = r.dbEngine.Shutdown(ctx); err != nil {
		r.lg.Sugar().Errorf("stop dbEngine failed with error: %v", err)
	}

	// wait network engine async write to peer
	r.writeWg.Wait()

	// stop write res to peer
	close(r.stopChan)

	// stop network engine
	if err = r.engine.Stop(ctx); err != nil {
		r.lg.Sugar().Errorf("stop network engine failed with error: %v", err)
	}

	r.lg.Sugar().Info("Redis is now ready to exit, bye bye...")

	atomic.StoreUint32(&r.status, statusClosed)
	return
}

func NewRedisServer() *RedisServer {
	server := &RedisServer{}
	server.dbEngine = database2.MakeStandalone()
	connManager := connection.NewConnManager()
	server.connManager = connManager
	connection.ConnCounter = connManager
	server.stopChan = make(chan struct{})
	server.status = statusInitialized
	lg, _ := logger.CreateLogger(logger.DefaultLevel)
	server.lg = lg.Named("redis-server")
	return server
}
