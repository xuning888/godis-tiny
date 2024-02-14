package tcp

import (
	"context"
	"errors"
	"godis-tiny/interface/tcp"
	"godis-tiny/logger"
	"godis-tiny/pkg/atomic"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var closing atomic.Boolean

// listenAndServe 监听tcp连接， 并把连接分发给应用层服务器进行处理
// 当监听到 closeChan 发送的事件后，就关闭服务器
func listenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 开启一个 goroutine 来监听 closeChan
	go func() {
		// channel 阻塞在这里
		<-closeChan
		shutdown(listener, handler)
	}()

	defer func() {
		// 当程序关闭时关闭资源
		shutdown(listener, handler)
	}()
	ctx := context.Background()
	var waitDown sync.WaitGroup
	for {
		// Accept会一直阻塞到新的连接建立或者listener中断才会返回
		conn, err := listener.Accept()
		if err != nil {
			// 检查是否因为监听器已关闭导致的错误
			var netErr net.Error
			if errors.As(err, &netErr) && !netErr.Timeout() {
				logger.InfoF("listener is closed, exiting accept loop: %v", err)
				break
			}
			logger.InfoF("accept conn has error: %v", err)
			continue
		}
		logger.InfoF("accept conn:%s", conn.RemoteAddr().String())
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

func shutdown(listener net.Listener, handler tcp.Handler) {
	if closing.Get() {
		return
	}
	closing.Set(true)
	logger.InfoF("shutting down....")
	// 关闭 tcp listener, listener.Accept() 会立刻返回 io.EOF
	err := listener.Close()
	if err != nil {
		logger.ErrorF("shut down listener has error: %v", err)
	}
	// 关闭应用服务器
	err = handler.Close()
	if err != nil {
		logger.ErrorF("shut down server has error: %v", err)
	}
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
	logger.InfoF("bind: %s, start listening...", address)
	listenAndServe(listener, handler, closeChan)
	return nil
}
