package tcp

import (
	"errors"
	"github.com/panjf2000/gnet/v2"
	"godis-tiny/config"
	database2 "godis-tiny/interface/database"
	"godis-tiny/redis/connection"
	"godis-tiny/redis/parser"
	"godis-tiny/redis/protocol"
	"golang.org/x/sys/unix"
	"net"
	"time"
)

func (r *RedisServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	r.lg.Sugar().Info("on boot callback....")
	r.engine = eng
	r.dbEngine.Init()
	r.listen()
	return
}

func (r *RedisServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	connectedClients := connection.ConnCounter.CountConnections()
	maxClients := config.Properties.MaxClients

	// 如果连接数达到了最大值, 就拒绝连接
	if connectedClients >= maxClients {
		r.lg.Sugar().Infof("max number of clients reached. clients_connected: %v, maxclinets: %v",
			connectedClients, maxClients)
		return protocol.MakeStandardErrReply("ERR max number of clients reached").ToBytes(), gnet.Close
	}
	r.lg.Sugar().Infof("accept conn: %v", c.RemoteAddr())
	codec := parser.NewCodec()
	c.SetContext(codec)
	r.connManager.RegisterConn(c.RemoteAddr().String(), connection.NewConn(c, false))
	return nil, gnet.None
}

func (r *RedisServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		if errors.Is(err, unix.ECONNRESET) {
			r.lg.Sugar().Infof("conn: %v, closed", c.RemoteAddr())
		} else {
			r.lg.Sugar().Errorf("conn: %v, closed with error: %v", c.RemoteAddr(), err)
		}
	}
	r.connManager.RemoveConnByKey(c.RemoteAddr().String())
	return
}

func (r *RedisServer) OnTick() (delay time.Duration, action gnet.Action) {
	r.dbEngine.Cron()
	return time.Second * time.Duration(1), gnet.None
}

func (r *RedisServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := r.connManager.Get(c.RemoteAddr().String())
	codecc := c.Context().(*parser.Codec)
	data, err := c.Next(-1)
	if err != nil {
		r.lg.Sugar().Errorf("read data faild with error: %v", err)
		return gnet.Close
	}
	replies, err := codecc.Decode(data)
	if err != nil {
		if errors.Is(err, parser.ErrIncompletePacket) {
			return gnet.None
		}
		r.lg.Sugar().Errorf("decode falied with error: %v", err)
		err = r.quickWrite(c, protocol.MakeStandardErrReply(err.Error()).ToBytes())
		if err != nil {
			r.lg.Sugar().Errorf("write to peer falied with error: %v", err)
			return gnet.Close
		}
		return gnet.Close
	}
	for _, value := range replies {
		cmd, ok := value.(*protocol.MultiBulkReply)
		if !ok {
			reply := value.(*protocol.SimpleReply)
			errReply := protocol.MakeUnknownCommand(string(reply.Arg))
			_ = r.quickWrite(c, errReply.ToBytes())
			continue
		}
		cmdReq := database2.MakeCmdReq(conn, cmd.Args)
		r.dbEngine.PushReqEvent(cmdReq)
	}
	return
}

func (r *RedisServer) listen() {
	r.lg.Sugar().Info("start listen cmdResQueue")
	resEvent := r.dbEngine.DeliverResEvent()
	go func() {
		for {
			select {
			case cmdRes, ok := <-resEvent:
				if !ok {
					r.lg.Sugar().Info("stop listen cmdResQueue")
					return
				}
				conn := cmdRes.GetConn()
				if conn.IsInner() {
					return
				}
				go func() {
					finalCmdRes := cmdRes
					redisConn := finalCmdRes.GetConn()
					bytes := finalCmdRes.GetReply().ToBytes()
					r.asyncWrite(redisConn.GnetConn(), bytes)
				}()
			case <-r.stopChan:
				r.lg.Sugar().Info("stop listen due to shutdown signal")
				return
			}
		}
	}()
}

func (r *RedisServer) asyncWrite(c gnet.Conn, bytes []byte) {
	err := c.AsyncWrite(bytes, r.callback)
	if err != nil {
		r.lg.Sugar().Errorf("Async write failed with error: %v", err)
	}
}

func (r *RedisServer) callback(c gnet.Conn, err error) error {
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			r.lg.Sugar().Errorf("Async write failed. conn has closed, err: %v", err)
			return nil
		}
		r.lg.Sugar().Errorf("%v Async write failed with err: %v", c, err)
	}
	return nil
}

func (r *RedisServer) quickWrite(conn gnet.Conn, bytes []byte) error {
	_, err := conn.Write(bytes)
	if err != nil {
		r.lg.Sugar().Errorf("%v write bytes failed with error: %v", conn.RemoteAddr(), err)
		return err
	}
	err = conn.Flush()
	if err != nil {
		r.lg.Sugar().Errorf("%v flush bytes failed with error: %v", conn.RemoteAddr(), err)
		return err
	}
	return nil
}
