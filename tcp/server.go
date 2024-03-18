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
	"golang.org/x/sys/unix"
	"io"
	"net"
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
	logger      *zap.Logger
	eng         gnet.Engine
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
		gnet.WithLogger(g.logger.Named("gnet").Sugar()),
		// 使用最少连接的负载均衡算法为eventLoop分配conn
		gnet.WithLoadBalancing(gnet.LeastConnections),
	)
	return err
}

func NewGnetServer() (*GnetServer, error) {
	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		return nil, err
	}
	dbEngine := database.MakeStandalone()
	server := &GnetServer{
		activateMap: make(map[gnet.Conn]redis.Conn),
		dbEngine:    dbEngine,
		logger:      lg.Named("tcp-Server"),
	}
	connection.ConnCounter = server
	return server, nil
}

func (g *GnetServer) CountConnections() int {
	return g.eng.CountConnections()
}

func (g *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	g.logger.Info("on boot call back....")
	g.eng = eng
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
	_ = g.logger.Sync()
}

func (g *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	g.logger.Sugar().Infof("accept conn: %v", c.RemoteAddr())
	c.SetContext(parser.NewCodecc())
	redisConn := connection.NewConn(c, false)
	g.activateMap[c] = redisConn
	return nil, gnet.None
}

func (g *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	defer g.logger.Sync()
	if err != nil {
		if errors.Is(err, unix.ECONNRESET) {
			g.logger.Sugar().Infof("conn: %v, closed", c.RemoteAddr())
		} else {
			g.logger.Sugar().Errorf("conn: %v, closed with error: %v", c.RemoteAddr(), err)
		}
	}
	delete(g.activateMap, c)
	return gnet.None
}

func (g *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	codecc := c.Context().(*parser.Codec)
	replies, err := codecc.Decode(c)
	if err != nil {
		if errors.Is(err, parser.ErrIncompletePacket) {
			return gnet.None
		}
		g.logger.Sugar().Errorf("decode falied with error: %v", err)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return gnet.Close
		}
		err = g.quickWrite(c, protocol.MakeStandardErrReply(err.Error()).ToBytes())
		if err != nil {
			g.logger.Sugar().Errorf("write to peer falied with error: %v", err)
			return gnet.Close
		}
		return gnet.None
	}
	conn, ok := g.activateMap[c]
	if !ok {
		conn = connection.NewConn(c, false)
		g.activateMap[c] = conn
	}
	for _, value := range replies {
		r, ok := value.(*protocol.MultiBulkReply)
		if !ok {
			reply := value.(*protocol.SimpleReply)
			errReply := protocol.MakeUnknownCommand(string(reply.Arg))
			_ = g.quickWrite(c, errReply.ToBytes())
			continue
		}
		cmdReq := database2.MakeCmdReq(conn, r.Args)
		g.dbEngine.PushReqEvent(cmdReq)
	}
	return
}

func (g *GnetServer) OnTick() (delay time.Duration, action gnet.Action) {
	g.ttlHandle()
	return time.Second * time.Duration(1), gnet.None
}

// listenCmdResAndWrite2Peer 监听dbEngine的命令消费结果, 然后写回去给客户端。
// 这个方法使用了gnet的 AsyncWrite方法。AsyncWrite方法会将写任务排入asyncTaskQueue，然后交给eventLoop进行轮询执行。
// 因此，这个方法在单协程上运行，其消费能力由eventLoop决定。
func (g *GnetServer) listenCmdResAndWrite2Peer() {
	g.logger.Sync()
	g.logger.Info("start listen cmdResQueue")
	resEventChan := g.dbEngine.DeliverResEvent()
	go func() {
		for {
			select {
			case cmdRes, ok := <-resEventChan:
				if !ok {
					g.logger.Sugar().Debug("stop listen cmdResQueue")
					return
				}
				conn := cmdRes.GetConn()
				if conn.IsInner() {
					continue
				}
				go func() {
					gnetConn, bytes := conn.GnetConn(), cmdRes.GetReply().ToBytes()
					g.asyncWrite(gnetConn, bytes)
				}()
			}
		}
	}()
}

func (g *GnetServer) callback(c gnet.Conn, err error) error {
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			g.logger.Sugar().Errorf("Async write failed. conn has closed, err: %v", err)
			return nil
		}
		g.logger.Sugar().Errorf("%v Async write failed with err: %v", c, err)
	}
	return nil
}

// quickWrite  放在OnTraffic方法里边同步会写用的
func (g *GnetServer) quickWrite(conn gnet.Conn, bytes []byte) error {
	_, err := conn.Write(bytes)
	if err != nil {
		g.logger.Sugar().Errorf("%v write bytes failed with error: %v", conn.RemoteAddr(), err)
		return err
	}
	err = conn.Flush()
	if err != nil {
		g.logger.Sugar().Errorf("%v flush bytes failed with error: %v", conn.RemoteAddr(), err)
		return err
	}
	return nil
}

func (g *GnetServer) asyncWrite(c gnet.Conn, bytes []byte) {
	var err error = nil
	err = c.AsyncWrite(bytes, g.callback)
	maxRetry := 3
	for retry := 0; retry < maxRetry && err != nil; retry++ {
		g.logger.Sugar().Infof("Retry attempt #%d to async write to client", retry+1)
		err = c.AsyncWrite(bytes, g.callback)
	}
	if err != nil {
		g.logger.Sugar().Errorf("Failed to async write to client after %d attempts", maxRetry)
	}
}

// ttlHandle 检查和清理所有数据库的过期key
// ttlops 指令是一个内部指令, 只能被内部client触发, 这个指令会检查并清理所有DB中的过期key
func (g *GnetServer) ttlHandle() {
	cmdReq := database2.MakeCmdReq(connection.SystemCon, util.ToCmdLine("ttlops"))
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
			g.logger.Sugar().Infof("signal: %v", sig)
			err := eng.Stop(context.Background())
			if err != nil {
				g.logger.Sugar().Error(err)
			}
		}
	}()
}
