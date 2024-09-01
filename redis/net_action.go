package redis

import (
	"context"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"github.com/xuning888/godis-tiny/config"
	"io"
	"syscall"
	"time"
)

func (r *RedisServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	r.engine = eng
	r.Init()
	r.lg.Sugar().Info("Ready to accept connections tcp")
	return
}

func (r *RedisServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	if r.status == statusShutdown {
		return MakeStandardErrReply("ERR Server is shutting down").ToBytes(), gnet.Close
	}
	connectedClients := ConnCounter.CountConnections()
	maxClients := config.Properties.MaxClients

	// 如果连接数达到了最大值, 就拒绝连接
	if connectedClients >= maxClients {
		r.lg.Sugar().Infof("max number of clients reached. clients_connected: %v, maxclinets: %v",
			connectedClients, maxClients)
		return MakeStandardErrReply("ERR max number of clients reached").ToBytes(), gnet.Close
	}
	r.lg.Sugar().Debugf("accept conn: %v", c.RemoteAddr())
	r.connManager.RegisterConn(c.Fd(), NewClient(c.Fd(), c, false))
	return nil, gnet.None
}

func (r *RedisServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	remoteAddr := c.RemoteAddr()
	if err != nil {
		if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			r.lg.Sugar().Debugf("conn: %v, closed", remoteAddr)
		} else {
			r.lg.Sugar().Errorf("conn: %v closed, error: %v", remoteAddr, err)
		}
	} else {
		r.lg.Sugar().Debugf("conn: %v, closed", remoteAddr)
	}
	r.connManager.RemoveConnByKey(c.Fd())
	return
}

func (r *RedisServer) OnTick() (delay time.Duration, action gnet.Action) {
	r.cron()
	return time.Second * time.Duration(1), gnet.None
}

func (r *RedisServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := r.connManager.Get(c.Fd())
	err := conn.Decode()
	if err != nil && !conn.HasRemaining() {
		if errors.Is(err, ErrIncompletePacket) {
			return gnet.None
		}
		r.lg.Sugar().Errorf("decode falied with error: %v", err)
		_, err := conn.Write(MakeStandardErrReply(err.Error()).ToBytes())
		if err != nil {
			r.lg.Sugar().Errorf("write to peer falied with error: %v", err)
		}
		return gnet.Close
	} else if err != nil && conn.HasRemaining() {
		err2 := r.process(context.Background(), conn)
		if err2 != nil {
			if errors.Is(err2, ErrorsShutdown) {
				_ = MakeStandardErrReply("ERR Server is shutting down").WriteTo(conn)
				return gnet.Close
			}
			r.lg.Sugar().Errorf("process command failed: %v", err)
			return gnet.Close
		}
		if errors.Is(err, ErrIncompletePacket) {
			return gnet.None
		}
		r.lg.Sugar().Errorf("decode falied with error: %v", err)
		_, err = conn.Write(MakeStandardErrReply(err.Error()).ToBytes())
		if err != nil {
			r.lg.Sugar().Errorf("write to peer falied with error: %v", err)
		}
		return gnet.Close
	}
	err2 := r.process(context.Background(), conn)
	if err2 != nil {
		if errors.Is(err2, ErrorsShutdown) {
			_ = MakeStandardErrReply("ERR Server is shutting down").WriteTo(conn)
			return gnet.Close
		}
		r.lg.Sugar().Errorf("process command failed: %v", err)
		return gnet.Close
	}
	return
}
