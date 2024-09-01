package redis

import (
	"bufio"
	"container/list"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
	"net"
	"strings"
)

type DBRangeCheck func(index int) error

type Rewrite func() error

type ClearDatabase func()

type Client struct {
	Fd              int
	dbId            int
	db              *DB
	RangeCheck      DBRangeCheck
	Rewrite         Rewrite
	ClearDatabase   ClearDatabase
	inner           bool
	totalReplyBytes int
	conn            gnet.Conn
	writeBuffer     *bufio.Writer
	codec           *Codec
	curCommand      [][]byte
	queryBuffer     *list.List
	lg              *zap.Logger
}

func (c *Client) GetDbIndex() int {
	return c.dbId
}

func (c *Client) SetDbIndex(index int) {
	c.dbId = index
}

func (c *Client) SetDb(mdb *DB) {
	c.db = mdb
}

func (c *Client) GetDb() *DB {
	return c.db
}

func (c *Client) IsInner() bool {
	return c.inner
}

func (c *Client) Decode() error {
	return c.codec.Decode(c.conn, c.queryBuffer)
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Client) Write(bytes []byte) (int, error) {
	if c.conn == nil {
		return 0, nil
	}
	n, err := c.writeBuffer.Write(bytes)
	if err != nil {
		return 0, err
	}
	c.totalReplyBytes += n
	return n, err
}

func (c *Client) Flush() error {
	if c.writeBuffer.Buffered() > 0 {
		if err := c.writeBuffer.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) PollCmd() [][]byte {
	if c.queryBuffer.Len() > 0 {
		front := c.queryBuffer.Front()
		cmdLine := front.Value.([][]byte)
		c.queryBuffer.Remove(front)
		c.curCommand = cmdLine
		return cmdLine
	}
	return nil
}

func (c *Client) PushCmd(cmdline [][]byte) {
	c.queryBuffer.PushBack(cmdline)
}

func (c *Client) HasRemaining() bool {
	return c.queryBuffer.Len() > 0
}

func (c *Client) ResetQueryBuffer() {
	c.queryBuffer.Init()
}

func (c *Client) GetCmdName() string {
	if len(c.curCommand) == 0 {
		return ""
	}
	return strings.ToLower(string(c.curCommand[0]))
}

func (c *Client) GetArgs() [][]byte {
	if len(c.curCommand) <= 1 {
		return nil
	}
	return c.curCommand[1:]
}

func (c *Client) GetArgNum() int {
	if len(c.curCommand) <= 1 {
		return 0
	}
	return len(c.curCommand) - 1
}

func (c *Client) GetCmdLine() [][]byte {
	return c.curCommand
}

func NewClient(Fd int, conn gnet.Conn, inner bool) *Client {
	client := &Client{}
	client.Fd = Fd
	client.dbId = 0
	client.conn = conn
	client.writeBuffer = bufio.NewWriterSize(conn, 1<<16) // 64KB
	client.codec = NewCodec()
	client.inner = inner
	client.queryBuffer = list.New()
	return client
}
