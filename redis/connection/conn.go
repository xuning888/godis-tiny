package connection

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/pkg/wait"
	"net"
	"sync"
	"time"
)

var _ redis.Conn = &Conn{}

var SystemCon = NewConn(nil, true)

var ConnCounter redis.ConnCounter = nil

type Conn struct {
	// index 正在操作的DB
	index int
	// inner 是否是内部链接
	inner     bool
	timestamp int64
	// conn gnet.conn
	conn gnet.Conn
	// sendWaiting 服务端正在发送数据时，阻止服务端关闭连接
	sendWaiting wait.Wait
	// mux 服务端向客户端发送数据时并不是线程安全的, 加锁保护一下。
	mux sync.Mutex
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) Close() error {
	if c.IsInner() {
		return nil
	}
	c.sendWaiting.WaitWithTimeout(10 * time.Second)
	return c.conn.Close()
}

func (c *Conn) GetIndex() int {
	return c.index
}

func (c *Conn) SetIndex(index int) {
	c.index = index
}

// Write 加锁保证线程安全
func (c *Conn) Write(p []byte) (int, error) {

	if p == nil {
		return 0, nil
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	c.sendWaiting.Add(1)
	defer func() {
		c.sendWaiting.Done()
	}()
	return c.conn.Write(p)
}

func (c *Conn) GnetConn() gnet.Conn {
	return c.conn
}

func (c *Conn) IsInner() bool {
	return c.inner
}

func NewConn(conn gnet.Conn, inner bool) redis.Conn {
	return &Conn{
		conn:        conn,
		inner:       inner,
		sendWaiting: wait.Wait{},
		timestamp:   time.Now().UnixMilli(),
	}
}
