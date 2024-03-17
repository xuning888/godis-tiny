package redis

import (
	"github.com/panjf2000/gnet/v2"
	"net"
)

type Conn interface {
	// Close 关闭连接
	Close() error
	// GetIndex 获取当前连接需要操作的数据库
	GetIndex() int
	// SetIndex 设置当前连接操作的数据库
	SetIndex(index int)
	// IsInner 是否是内建的客户端
	IsInner() bool
	// RemoteAddr 获取远程地址
	RemoteAddr() net.Addr
	// GnetConn gnet的conn
	GnetConn() gnet.Conn
}

var Counter ConnCounter = nil

type ConnCounter interface {
	CountConnections() int
}
