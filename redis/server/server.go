package server

import (
	"context"
	"errors"
	"github.com/bytedance/gopkg/util/logger"
	database2 "godis-tiny/database"
	"godis-tiny/interface/database"
	"godis-tiny/pkg/atomic"
	"godis-tiny/pkg/util"
	simple2 "godis-tiny/redis/connection/simple"
	"godis-tiny/redis/parser/simple"
	"godis-tiny/redis/protocol"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Handler struct {
	// activate 持有活跃对连接
	activate sync.Map
	// closing 标记服务是否启动
	closing  atomic.Boolean
	dbEngine database.DBEngine
	// 定时发送清理过期key的信号
	ttlTicker      *time.Ticker
	stopTTLChannel chan byte
}

func MakeHandler() *Handler {
	dbEngin := database2.MakeStandalone()
	dbEngin.Init()
	handler := &Handler{
		activate:  sync.Map{},
		dbEngine:  dbEngin,
		ttlTicker: time.NewTicker(time.Second),
	}
	go func() {
		handler.startTTLHandle()
	}()
	return handler
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}
	client := simple2.NewConn(conn, false)
	h.activate.Store(client, struct{}{})
	ch := simple.ParseFromStream(conn)
	for payload := range ch {
		if payload.Error != nil {
			if payload.Error == io.EOF ||
				errors.Is(payload.Error, io.ErrUnexpectedEOF) ||
				strings.Contains(payload.Error.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Debugf("connection closed: %v", conn.RemoteAddr())
				return
			}
			errReply := protocol.MakeStandardErrReply(payload.Error.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Errorf("connection closed: %s, %v", client.RemoteAddr().String(), err)
				return
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
		cmdResult := h.dbEngine.Exec(client, r.Args)
		c := cmdResult.GetConn()
		_, err := c.Write(cmdResult.GetReply().ToBytes())
		if err != nil {
			h.closeClient(client)
			logger.Errorf("write reply to conn has err, close client %v, error: %v", conn.RemoteAddr(), err)
		}
	}
}

func (h *Handler) closeClient(client *simple2.Connection) {
	_ = client.Close()
	h.activate.Delete(client)
}

func (h *Handler) Close() error {
	if h.closing.Get() {
		return nil
	}
	h.closing.Set(true)
	h.activate.Range(func(key, value interface{}) bool {
		client := key.(*simple2.Connection)
		_ = client.Close()
		return true
	})
	h.stopTTLChannel <- 0
	close(h.stopTTLChannel)
	// 关闭存储
	_ = h.dbEngine.Close()
	return nil
}

func (h *Handler) startTTLHandle() {
	for {
		select {
		case <-h.ttlTicker.C:
			h.doTTLHandle()
		case <-h.stopTTLChannel:
			logger.Debug("stop ttl check Handle")
			return
		}
	}
}

var systemConn = simple2.NewConn(nil, true)

func (h *Handler) doTTLHandle() {
	logger.Debug("doTTLHandle")
	h.dbEngine.Exec(systemConn, util.ToCmdLine("ttlops"))
}
