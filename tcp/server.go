package tcp

import (
	"context"
	"fmt"
	"g-redis/interface/tcp"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// listenAndServe 监听tcp连接， 并把连接分发给应用层服务器进行处理
// 当监听到 closeChan 发送的事件后，就关闭服务器
func listenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 开启一个 goroutine 来监听 closeChan
	go func() {
		// channel 阻塞在这里
		<-closeChan
		log.Printf("shutting down....")
		// 关闭 tcp listener, listener.Accept() 会立刻返回 io.EOF
		_ = listener.Close()
		// 关闭应用服务器
		_ = handler.Close()
	}()

	defer func() {
		// 当程序关闭时关闭资源
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	var waitDown sync.WaitGroup
	for {
		// Accept会一直阻塞到新的连接建立或者listener中断才会返回
		conn, err := listener.Accept()
		if err != nil {
			// 通常是由于listener 关闭无法继续监听导致的错误
			log.Printf("accept conn has error: %v", err)
			break
		}
		log.Printf("accept conn:%s", conn.RemoteAddr().String())
		waitDown.Add(1)
		// 开启新的 goroutine 处理该连接
		go func() {
			defer func() {
				waitDown.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDown.Wait()
}

// ListenAndServeWithSignal 监听指定的端口号，建立连接时派发给应用层服务器进行处理
// 并且携带关闭通知信息
func ListenAndServeWithSignal(address string, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// 绑定监听地址
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	log.Println(fmt.Sprintf("bind: %s, start listening...", address))
	listenAndServe(listener, handler, closeChan)
	return nil
}
