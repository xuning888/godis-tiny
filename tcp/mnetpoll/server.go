package mnetpoll

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/cloudwego/netpoll"
	"godis-tiny/database"
	database2 "godis-tiny/interface/database"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection/simple"
	"godis-tiny/redis/parser/mnetpoll"
	"godis-tiny/redis/protocol"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var dbEngine database2.DBEngine

type NetPollServer struct {
	eventLoop netpoll.EventLoop
	// 定时发送清理过期key的信号
	ttlTicker      *time.Ticker
	stopTTLChannel chan byte
}

func NewNetPollServer() *NetPollServer {
	return &NetPollServer{
		eventLoop:      nil,
		ttlTicker:      time.NewTicker(time.Second),
		stopTTLChannel: make(chan byte, 1),
	}
}

func (n *NetPollServer) Serve(address string) error {
	// 监听关闭信号
	go func() {
		n.shutdownListener()
	}()
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
	)
	if err != nil {
		return err
	}
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
	var ctx context.Context
	var cancel context.CancelFunc
	// 关闭 eventLoop
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n.eventLoop != nil {
		if err := n.eventLoop.Shutdown(ctx); err != nil {
			logger.Error(err)
		}
	}
	// 关闭存储引擎
	_ = dbEngine.Close()
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
	err := conn.AddCloseCallback(handleCloseCallBack)
	if err != nil {
		logger.Errorf("[%v] connection add close back has err: %v", conn.RemoteAddr(), err)
	}
	return ctx
}

func handleCloseCallBack(conn netpoll.Connection) error {
	logger.Debugf("[%v] connection closed", conn.RemoteAddr())
	return nil
}

func handle(ctx context.Context, conn netpoll.Connection) error {
	reader := conn.Reader()
	writer := conn.Writer()
	ch := mnetpoll.ParseFromStream(ctx, reader)
	for payload := range ch {
		err := reader.Release()
		if err != nil {
			logger.Errorf("release reader has error: %v", err)
			return err
		}
		if payload.Data == nil {
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			continue
		}
		c := simple.NewConn(nil, false)
		cmdRes := dbEngine.Exec(c, r.Args)
		_, err = writer.WriteBinary(cmdRes.GetReply().ToBytes())
		if err != nil {
			logger.Errorf("write bytes has error: %v", err)
			_ = conn.Close()
			return err
		}
		err = writer.Flush()
		if err != nil {
			logger.Errorf("flush bytes has error: %v", err)
			_ = conn.Close()
			return err
		}
	}
	return nil
}
