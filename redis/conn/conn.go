package conn

import (
	"net"
	"sync"
)

type Connection struct {
	conn net.Conn
	wait sync.WaitGroup
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{conn: conn}
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Write(bytes []byte) (int, error) {
	return c.conn.Write(bytes)
}
