package redis

import "net"

// Connection 对连接的抽象
type Connection interface {
	// Close 关闭连接
	Close() error
	// RemoteAddr 获取连接的远程地址
	RemoteAddr() net.Addr
	// Write 向客户端发送数据
	Write(bytes []byte) (int, error)
	// GetIndex 获取当前连接需要操作的数据库
	GetIndex() int
}
