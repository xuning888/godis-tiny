package tcp

import (
	"context"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
	"godis-tiny/config"
	"godis-tiny/database"
	database2 "godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection"
	"godis-tiny/redis/parser"
	"godis-tiny/redis/protocol"
	"golang.org/x/sys/unix"
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
	dbEngine    database2.DBEngine
	logger      *zap.Logger
	eng         gnet.Engine
	connManager redis.ConnManager
}

func (g *GnetServer) Serve(address string) error {
	err := gnet.Run(
		g,
		address,
		gnet.WithReadBufferCap(1<<18),
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
		dbEngine: dbEngine,
		logger:   lg.Named("tcp-Server"),
	}
	connManager := connection.NewConnManager()
	server.connManager = connManager
	connection.ConnCounter = connManager
	return server, nil
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
	defer g.logger.Sync()

	connectedClients := connection.ConnCounter.CountConnections()
	maxClients := config.Properties.MaxClients

	// 如果连接数达到了最大值, 就拒绝连接
	if connectedClients >= maxClients {
		g.logger.Sugar().Infof("max number of clients reached. clients_connected: %v, maxclinets: %v",
			connectedClients, maxClients)
		return protocol.MakeStandardErrReply("ERR max number of clients reached").ToBytes(), gnet.Close
	}
	g.logger.Sugar().Infof("accept conn: %v", c.RemoteAddr())
	codec := parser.NewCodec()
	c.SetContext(codec)
	g.connManager.RegisterConn(c.RemoteAddr().String(), connection.NewConn(c, false))
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
	g.connManager.RemoveConnByKey(c.RemoteAddr().String())
	return gnet.None
}

func (g *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := g.connManager.Get(c.RemoteAddr().String())
	codecc := c.Context().(*parser.Codec)
	data, err := c.Next(-1)
	if err != nil {
		g.logger.Sugar().Errorf("read data faild with error: %v", err)
		return gnet.Close
	}
	replies, err := codecc.Decode(data)
	if err != nil {
		if errors.Is(err, parser.ErrIncompletePacket) {
			return gnet.None
		}
		g.logger.Sugar().Errorf("decode falied with error: %v", err)
		err = g.quickWrite(c, protocol.MakeStandardErrReply(err.Error()).ToBytes())
		if err != nil {
			g.logger.Sugar().Errorf("write to peer falied with error: %v", err)
			return gnet.Close
		}
		return gnet.Close
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
func (g *GnetServer) listenCmdResAndWrite2Peer() {
	defer g.logger.Sync()
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
					finalCmdRes := cmdRes
					redisConn := finalCmdRes.GetConn()
					reply := finalCmdRes.GetReply()
					g.asyncWrite(redisConn.GnetConn(), reply.ToBytes())
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
