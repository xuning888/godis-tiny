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
	// Write
	Write(p []byte) (int, error)
	GnetConn() gnet.Conn
}

// ConnCounter conn的统计信息
type ConnCounter interface {

	// CountConnections connected_clients
	CountConnections() int
}

type ConnManager interface {

	// RegisterConn registerConn
	RegisterConn(key string, conn Conn)

	// RemoveConnByKey removeConn by key
	RemoveConnByKey(key string)

	// RemoveConn remove conn
	RemoveConn(conn Conn)

	Get(key string) Conn
}
