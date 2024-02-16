package mgnet

import (
	"errors"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/panjf2000/gnet/v2"
	"godis-tiny/database"
	database2 "godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection/simple"
	"godis-tiny/redis/parser/mgnet"
	"godis-tiny/redis/protocol"
	"io"
	"sync"
	"time"
)

var defaultTimeout = 60

type GnetServer struct {
	activateMap sync.Map
	dbEngine    database2.DBEngine
}

func (g *GnetServer) Serve(address string) error {
	err := gnet.Run(
		g,
		address,
		// 启用多核心
		gnet.WithMulticore(true),
		// 启用定时任务
		gnet.WithTicker(true),
		// socket 60不活跃就会被驱逐
		gnet.WithTCPKeepAlive(time.Second*time.Duration(defaultTimeout)),
	)
	return err
}

func NewGnetServer() *GnetServer {
	dbEngine := database.MakeStandalone()
	return &GnetServer{
		activateMap: sync.Map{},
		dbEngine:    dbEngine,
	}
}

func (g *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	g.dbEngine.Init()
	return gnet.None
}

func (g *GnetServer) OnShutdown(eng gnet.Engine) {
	_ = g.dbEngine.Close()
}

func (g *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	logger.Infof("connection: %v\n", c.RemoteAddr())
	g.activateMap.LoadOrStore(c, simple.NewConn(c, false))
	return nil, gnet.None
}

func (g *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	logger.Infof("conn: %v, closed", c.RemoteAddr())
	g.activateMap.Delete(c)
	return gnet.None
}

func (g *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	payload := mgnet.Parse(c)
	if payload.Error != nil {
		if errors.Is(payload.Error, io.EOF) ||
			errors.Is(payload.Error, io.ErrUnexpectedEOF) {
			// todo log
			return gnet.Close
		}
		// 协议中的错误
		errReply := protocol.MakeStandardErrReply(payload.Error.Error())
		_, err := c.Write(errReply.ToBytes())
		if err != nil {
			// todo log
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
	value, _ := g.activateMap.LoadOrStore(c, simple.NewConn(c, false))
	conn := value.(redis.Connection)
	cmdRes := g.dbEngine.Exec(conn, r.Args)
	err := g.quickWrite(c, cmdRes.GetReply().ToBytes())
	if err != nil {
		// todo
		return gnet.Close
	}
	return gnet.None
}

func (g *GnetServer) quickWrite(conn gnet.Conn, bytes []byte) error {
	_, err := conn.Write(bytes)
	if err != nil {
		// todo log
		return err
	}
	err = conn.Flush()
	if err != nil {
		// todo log
		return err
	}
	return nil
}

func (g *GnetServer) OnTick() (delay time.Duration, action gnet.Action) {
	g.ttlHandle()
	return time.Second * time.Duration(1), gnet.None
}

var systemCon = simple.NewConn(nil, true)

func (g *GnetServer) ttlHandle() {
	g.dbEngine.Exec(systemCon, util.ToCmdLine("ttlops"))
}
