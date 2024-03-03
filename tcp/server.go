package tcp

import (
	"context"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
	"godis-tiny/database"
	database2 "godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection"
	"godis-tiny/redis/parser"
	"godis-tiny/redis/protocol"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var _ gnet.EventHandler = &GnetServer{}

var defaultTimeout = 60

// GnetServer 使用https://github.com/panjf2000/gnet作为网络实现
type GnetServer struct {
	activateMap map[gnet.Conn]redis.Conn
	dbEngine    database2.DBEngine
	logger      *zap.SugaredLogger
}

func (g *GnetServer) Serve(address string) error {
	err := gnet.Run(
		g,
		address,
		// 启用多核心, 开启后NumEventLoops = coreSize
		gnet.WithMulticore(true),
		// 启用定时任务
		gnet.WithTicker(true),
		// socket 60不活跃就会被驱逐
		gnet.WithTCPKeepAlive(time.Second*time.Duration(defaultTimeout)),
		gnet.WithReusePort(true),
		gnet.WithLogger(g.logger),
	)
	return err
}

func NewGnetServer(lg *zap.Logger) *GnetServer {
	dbEngine := database.MakeStandalone()
	return &GnetServer{
		activateMap: make(map[gnet.Conn]redis.Conn),
		dbEngine:    dbEngine,
		logger:      lg.Sugar(),
	}
}

func (g *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	g.logger.Info("on boot call back....")
	// 监听 kill -15 后关闭进程
	g.listenStopSignal(eng)
	// dbEngine做初始化
	g.dbEngine.Init()
	// 监听dbEngine返回的结果写回给客户端
	g.listenCmdResAndWrite2Peer()
	return gnet.None
}

func (g *GnetServer) OnShutdown(eng gnet.Engine) {
	g.logger.Info("on shutdown call back....")
	_ = g.dbEngine.Close()
}

func (g *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	g.logger.Infof("accept conn: %v", c.RemoteAddr())
	redisConn := connection.NewConn(c, false)
	g.activateMap[c] = redisConn
	return nil, gnet.None
}

func (g *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	g.logger.Infof("conn: %v, closed", c.RemoteAddr())
	delete(g.activateMap, c)
	return gnet.None
}

func (g *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	payload := parser.Decode(c)
	if payload.Error != nil {
		if errors.Is(payload.Error, io.EOF) ||
			errors.Is(payload.Error, io.ErrUnexpectedEOF) {
			g.logger.Errorf("[%v] connection read payload has error", c.RemoteAddr())
			return gnet.Close
		}
		// 协议中的错误
		errReply := protocol.MakeStandardErrReply(payload.Error.Error())
		err2 := g.quickWrite(c, errReply.ToBytes())
		if err2 != nil {
			return gnet.Close
		}
		return gnet.None
	}
	if payload.Data == nil {
		return gnet.None
	}
	r, ok := payload.Data.(*protocol.MultiBulkReply)
	if !ok {
		return gnet.None
	}
	conn, ok := g.activateMap[c]
	if !ok {
		conn = connection.NewConn(c, false)
		g.activateMap[c] = conn
	}
	cmdReq := database2.MakeCmdReq(conn, r.Args)
	// 推送命令到dbEngine
	g.dbEngine.PushReqEvent(cmdReq)
	return gnet.None
}

func (g *GnetServer) OnTick() (delay time.Duration, action gnet.Action) {
	g.ttlHandle()
	return time.Second * time.Duration(1), gnet.None
}

// listenCmdResAndWrite2Peer 监听dbEngine的命令消费结果, 然后写回去给客户端。
// 这个方法使用了gnet的 AsyncWrite方法。AsyncWrite方法会将写任务排入asyncTaskQueue，然后交给eventLoop进行轮询执行。
// 因此，这个方法在单协程上运行，其消费能力由eventLoop决定。
func (g *GnetServer) listenCmdResAndWrite2Peer() {
	g.logger.Info("start listen cmdResQueue")
	resEventChan := g.dbEngine.DeliverResEvent()
	go func() {
		for {
			select {
			case cmdRes, ok := <-resEventChan:
				if !ok {
					return
				}
				conn := cmdRes.GetConn()
				if conn.IsInner() {
					continue
				}
				g.asyncWrite(conn.GnetConn(), cmdRes.GetReply().ToBytes())
			}
		}
	}()
}

func callback(c gnet.Conn, err error) error {
	if err != nil {
		logger.Errorf("Async writing bytes to client has error: %v", err)
		if closeErr := c.Close(); closeErr != nil {
			logger.Errorf("Failed to close connection: %v", closeErr)
		}
		return err
	}
	return nil
}

func (g *GnetServer) quickWrite(conn gnet.Conn, bytes []byte) error {
	_, err := conn.Write(bytes)
	if err != nil {
		g.logger.Errorf("%v write bytes has error: %v", conn.RemoteAddr(), err)
		return err
	}
	err = conn.Flush()
	if err != nil {
		g.logger.Errorf("%v flush bytes has error: %v", conn.RemoteAddr(), err)
		return err
	}
	return nil
}

func (g *GnetServer) asyncWrite(c gnet.Conn, bytes []byte) {
	var err error = nil
	err = c.AsyncWrite(bytes, callback)
	maxRetry := 3
	for retry := 0; retry < maxRetry && err != nil; retry++ {
		g.logger.Warnf("Retry attempt #%d to async write to client", retry+1)
		err = c.AsyncWrite(bytes, callback)
	}
	if err != nil {
		g.logger.Errorf("Failed to async write to client after %d attempts", maxRetry)
	}
}

var systemCon = connection.NewConn(nil, true)

// ttlHandle 检查和清理所有数据库的过期key
// ttlops 指令是一个内部指令, 只能被内部client触发, 这个指令会检查并清理所有DB中的过期key
func (g *GnetServer) ttlHandle() {
	cmdReq := database2.MakeCmdReq(systemCon, util.ToCmdLine("ttlops"))
	g.dbEngine.PushReqEvent(cmdReq)
}

// listenStopSign 监听操作系统信号,关闭服务
func (g *GnetServer) listenStopSignal(eng gnet.Engine) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			g.logger.Infof("signal: %v", sig)
			err := eng.Stop(context.Background())
			if err != nil {
				g.logger.Error(err)
			}
		}
	}()
}
