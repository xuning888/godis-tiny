package mnetpoll

import (
	"context"
	"errors"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/cloudwego/netpoll"
	"godis-tiny/database"
	database2 "godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection/simple"
	"godis-tiny/redis/parser/mnetpoll"
	"godis-tiny/redis/protocol"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var dbEngine database2.DBEngine

var defaultTimeout = 60

var activateMap = sync.Map{}

type NetPollServer struct {
	eventLoop netpoll.EventLoop
	// 定时发送清理过期key的信号
	ttlTicker      *time.Ticker
	stopTTLChannel chan byte
}

// NewNetPollServer 使用netpoll作为网络库 https://github.com/cloudwego/netpoll
func NewNetPollServer() *NetPollServer {
	return &NetPollServer{
		eventLoop:      nil,
		ttlTicker:      time.NewTicker(time.Second),
		stopTTLChannel: make(chan byte, 1),
	}
}

// Serve 启动服务
func (n *NetPollServer) Serve(address string) error {
	// 监听关闭信号
	go func() {
		n.shutdownListener()
	}()
	activateMap = sync.Map{}
	dbEngine = database.MakeStandalone()
	dbEngine.Init()
	go func() {
		n.startTTLHandle()
	}()
	listener, err := netpoll.CreateListener("tcp", address)
	if err != nil {
		return err
	}
	n.eventLoop, err = netpoll.NewEventLoop(
		handle,
		netpoll.WithOnPrepare(prepare),
		netpoll.WithOnConnect(connect),
		netpoll.WithIdleTimeout(time.Second*time.Duration(defaultTimeout)),
	)
	if err != nil {
		return err
	}
	logger.Infof("bind: %s, start listening...", address)
	err = n.eventLoop.Serve(listener)
	if err != nil {
		return err
	}
	return nil
}

func (n *NetPollServer) shutdownListener() {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logger.Infof("sign: %v", sig)
			n.shutdown()
		}
	}()
}

func (n *NetPollServer) shutdown() {
	logger.Info("shutdown server...")
	// Stop the TTL ticker.
	n.stopTTLChannel <- 1
	close(n.stopTTLChannel)
	// 关闭 eventLoop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n.eventLoop != nil {
		if err := n.eventLoop.Shutdown(ctx); err != nil {
			logger.Error(err)
		}
	}
	// 关闭存储引擎
	_ = dbEngine.Close()

	// 关闭连接
	n.closeActiveMap()
}

func (n *NetPollServer) closeActiveMap() {
	keys := make([]netpoll.Connection, 0)
	activateMap.Range(func(conn, c interface{}) bool {
		connection := conn.(netpoll.Connection)
		keys = append(keys, connection)
		return true
	})
	for _, key := range keys {
		if key.IsActive() {
			_ = key.Close()
		}
	}
}

func (n *NetPollServer) startTTLHandle() {
	for {
		select {
		case <-n.ttlTicker.C:
			n.doTTLHandle()
		case <-n.stopTTLChannel:
			n.ttlTicker.Stop()
			logger.Debug("stop ttl check Handle")
			return
		}
	}
}

var systemConn = simple.NewConn(nil, true)

func (n *NetPollServer) doTTLHandle() {
	logger.Debug("doTTLHandle")
	dbEngine.Exec(systemConn, util.ToCmdLine("ttlops"))
}

var _ netpoll.OnPrepare = prepare

var _ netpoll.OnConnect = connect

var _ netpoll.CloseCallback = handleCloseCallBack

func prepare(conn netpoll.Connection) context.Context {
	return context.Background()
}

func connect(ctx context.Context, conn netpoll.Connection) context.Context {
	logger.Debugf("[%v] connection established", conn.RemoteAddr())
	// 建立连接的时候存储一下
	activateMap.Store(conn, simple.NewConn(conn, false))
	err := conn.AddCloseCallback(handleCloseCallBack)
	if err != nil {
		logger.Errorf("[%v] connection add close back has err: %v", conn.RemoteAddr(), err)
	}
	return ctx
}

func handleCloseCallBack(conn netpoll.Connection) error {
	logger.Infof("[%v] connection closed", conn.RemoteAddr())
	activateMap.Delete(conn)
	return nil
}

func handle(ctx context.Context, conn netpoll.Connection) error {
	reader := conn.Reader()
	writer := conn.Writer()
	ch := mnetpoll.ParseFromStream(ctx, conn)
	for payload := range ch {
		// 每次获取到一个payload 都要把之前读取过的slice释放
		err := reader.Release()
		if err != nil {
			logger.Errorf("release reader has error: %v", err)
			return err
		}
		if payload.Error != nil {
			if errors.Is(payload.Error, io.EOF) ||
				errors.Is(payload.Error, io.ErrUnexpectedEOF) {
				logger.Errorf("[%v] connection read payload has error", conn.RemoteAddr())
				return payload.Error
			}
			errReply := protocol.MakeStandardErrReply(payload.Error.Error())
			if conn.IsActive() {
				err2 := quickWrite(writer, errReply.ToBytes())
				if err2 != nil {
					return err2
				}
			} else {
				return nil
			}
			continue
		}
		if payload.Data == nil {
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			continue
		}
		value, _ := activateMap.LoadOrStore(conn, simple.NewConn(conn, false))
		c := value.(redis.Connection)
		cmdRes := dbEngine.Exec(c, r.Args)
		if conn.IsActive() {
			err = quickWrite(writer, cmdRes.GetReply().ToBytes())
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}
	return nil
}

func quickWrite(writer netpoll.Writer, bytes []byte) error {
	_, err := writer.WriteBinary(bytes)
	if err != nil {
		logger.Errorf("write bytes has error: %v", err)
		return err
	}
	err = writer.Flush()
	if err != nil {
		logger.Errorf("flush bytes has error: %v", err)
		return err
	}
	return nil
}