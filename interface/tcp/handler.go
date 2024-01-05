package tcp

import (
	"context"
	"net"
)

// Handler 应用层服务器的抽象
type Handler interface {

	// Handle 处理连接
	Handle(ctx context.Context, conn net.Conn)

	// Close 关闭应用层服务器
	Close() error
}
