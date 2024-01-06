package server

import (
	"context"
	"errors"
	"fmt"
	"g-redis/pkg/atomic"
	conn2 "g-redis/redis/conn"
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
	closing atomic.Boolean
}

func MakeHandler() *Handler {
	return &Handler{
		activate: sync.Map{},
	}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	client := conn2.NewConn(conn)
	h.activate.Store(client, struct{}{})
	ch := parser.ParseFromStream(conn)
	for payload := range ch {
		if payload.Error != nil {
			log.Println(fmt.Sprintf("input has error: %v", payload.Error))
			if payload.Error == io.EOF ||
				errors.Is(payload.Error, io.ErrUnexpectedEOF) ||
				strings.Contains(payload.Error.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				log.Printf("connection closed: " + conn.RemoteAddr().String())
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
		log.Println(fmt.Sprintf("input: %s", string(payload.Data.ToBytes())))
		if payload.Data == nil {
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			log.Printf("require multi bulk protocol")
			continue
		}
		log.Println(fmt.Sprintf("output: %s", string(r.ToBytes())))
		_, err := client.Write(protocol.MakePongReply().ToBytes())
		if err != nil {
			h.closeClient(client)
			log.Println("connection closed: " + client.RemoteAddr().String())
		}
	}
}

func (h *Handler) closeClient(client *conn2.Connection) {
	_ = client.Close()
	h.activate.Delete(client)
}

func (h *Handler) Close() error {

	h.closing.Set(true)
	h.activate.Range(func(key, value interface{}) bool {
		client := key.(*conn2.Connection)
		_ = client.Close()
		return true
	})
	return nil
}
