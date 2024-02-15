package simple

import (
	"bufio"
	"context"
	"godis-tiny/pkg/atomic"
	"io"
	"log"
	"net"
	"sync"
)

// Client 客户端连接的抽象
type Client struct {
	// 连接的客户端
	conn net.Conn
	// 当服务器向客户端发送数据时应该阻塞goroutine
	wait sync.WaitGroup
}

type EchoHandler struct {

	// activateConn 保存所有活跃连接
	activateConn sync.Map

	// closing 表示echo服务器是否已经关闭
	closing atomic.Boolean
}

func (e *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 如果应用服务器已经关闭，那么就关闭待处理的连接
	if e.closing.Get() {
		_ = conn.Close()
		return
	}
	client := &Client{
		conn: conn,
		wait: sync.WaitGroup{},
	}
	e.activateConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		// ReadString 会一直阻塞到遇到分隔符 '\n'
		// 遇到分隔符后会返回上次遇到分隔符或连接建立后的所有数据，包括分隔符本身
		// 若在遇到分隔符之前就出现异常，ReadString 就会返回已收到的数据和错误信息
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 通常遇到的错误是连接中断或被关闭，用io.EOF 表示
			if err == io.EOF {
				log.Println("connection close")
			} else {
				log.Println(err)
			}
			return
		}
		// 向客户端发送数据时阻塞住 goroutine
		client.wait.Add(1)
		_, _ = client.Write([]byte(msg))
		// 向客户端发送完数据之后结束 wait
		client.wait.Done()
	}
}

func (e *EchoHandler) Close() error {
	log.Println("shutting down echo Handler")
	// 设置服务器关闭
	e.closing.Set(true)
	// 逐个关闭服务启上的连接
	e.activateConn.Range(func(key, value interface{}) bool {
		client := key.(*Client)
		_ = client.Close()
		return true
	})
	return nil
}

// Write 向客户端发送数据
func (c *Client) Write(bytes []byte) (int, error) {
	return c.conn.Write(bytes)
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.conn.Close()
}
