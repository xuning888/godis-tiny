package connection

import (
	"github.com/panjf2000/gnet/v2"
	"godis-tiny/interface/redis"
	"net"
)

var _ redis.Conn = &Conn{}

var SystemCon = NewConn(nil, true)

var ConnCounter redis.ConnCounter = nil

type Conn struct {
	// index 正在操作的DB
	index int
	// inner 是否是内部链接
	inner bool
	// conn gnet.conn
	conn gnet.Conn
}

func (c *Conn) GnetConn() gnet.Conn {
	return c.conn
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) GetIndex() int {
	return c.index
}

func (c *Conn) SetIndex(index int) {
	c.index = index
}

func (c *Conn) IsInner() bool {
	return c.inner
}

func NewConn(conn gnet.Conn, inner bool) redis.Conn {
	return &Conn{
		conn:  conn,
		inner: inner,
	}
}
