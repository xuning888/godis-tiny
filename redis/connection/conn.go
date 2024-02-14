package connection

import (
	"errors"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/wait"
	"net"
	"time"
)

var _ redis.Connection = &Connection{}

// Connection 简单的对客户端连接的描述
type Connection struct {
	// conn 客户端连接
	conn net.Conn
	// wait 发送数据时，不应该被关闭
	wait wait.Wait
	// index 需要操作的数据库
	index int
	inner bool
}

func (c *Connection) IsInner() bool {
	return c.inner
}

func NewConn(conn net.Conn, inner bool) *Connection {
	return &Connection{conn: conn, inner: inner}
}

func (c *Connection) Close() error {
	c.wait.WaitWithTimeout(10 * time.Second)
	return c.conn.Close()
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Write(bytes []byte) (int, error) {
	if bytes == nil || len(bytes) == 0 {
		return 0, nil
	}
	c.wait.Add(1)
	defer func() {
		c.wait.Done()
	}()
	if c.conn != nil {
		return c.conn.Write(bytes)
	}
	return 0, errors.New("conn is null")
}

func (c *Connection) GetIndex() int {
	return c.index
}

func (c *Connection) SetIndex(index int) {
	c.index = index
}
