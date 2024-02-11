package server

import (
	"context"
	"errors"
	database2 "g-redis/database"
	"g-redis/interface/database"
	"g-redis/pkg/atomic"
	"g-redis/redis/connection"
	"g-redis/redis/parser"
	"g-redis/redis/protocol"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type Handler struct {
	// activate 持有活跃对连接
	activate sync.Map
	// closing 标记服务是否启动
	closing  atomic.Boolean
	dbEngine database.DBEngine
}

func MakeHandler() *Handler {
	dbEngin := database2.MakeStandalone()
	dbEngin.Init()
	return &Handler{
		activate: sync.Map{},
		dbEngine: dbEngin,
	}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}
	client := connection.NewConn(conn)
	h.activate.Store(client, struct{}{})
	ch := parser.ParseFromStream(conn)
	for payload := range ch {
		if payload.Error != nil {
			// log.Println(fmt.Sprintf("input has error: %v", payload.Error))
			if payload.Error == io.EOF ||
				errors.Is(payload.Error, io.ErrUnexpectedEOF) ||
				strings.Contains(payload.Error.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				// log.Printf("connection closed: " + conn.RemoteAddr().String())
				return
			}
			errReply := protocol.MakeStandardErrReply(payload.Error.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				log.Println("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		// log.Println(fmt.Sprintf("input: %s", string(payload.Data.ToBytes())))
		if payload.Data == nil {
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			// log.Printf("require multi bulk protocol")
			continue
		}

		cmdResult := h.dbEngine.ExecV2(client, r.Args)
		// log.Println(fmt.Sprintf("output: %s", string(cmdResult.GetReply().ToBytes())))
		conn := cmdResult.GetConn()
		_, err := conn.Write(cmdResult.GetReply().ToBytes())
		if err != nil {
			h.closeClient(client)
			// log.Println("connection closed: " + client.RemoteAddr().String())
		}
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.activate.Delete(client)
}

func (h *Handler) Close() error {
	if h.closing.Get() {
		return nil
	}
	h.closing.Set(true)
	h.activate.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	// 关闭存储
	_ = h.dbEngine.Close()
	return nil
}
