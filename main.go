package main

import (
	"github.com/bytedance/gopkg/util/logger"
	"godis-tiny/redis/server"
	"godis-tiny/tcp/mgnet"
	"godis-tiny/tcp/mnetpoll"
	"godis-tiny/tcp/simple"
)

type runFunc func(address string) error

func main() {
	logger.SetLevel(logger.LevelInfo)

	run(":8080", runGnetServer)
}

func run(address string, r runFunc) {
	if err := r(address); err != nil {
		logger.Errorf("start server has error: %v", err)
		panic(err)
	}
}

// 使用原生的go.net作为网络实现
func runGoNetServer(address string) error {
	logger.Info("run in go.net")
	return simple.ListenAndServeWithSignal(address, server.MakeHandler())
}

// 使用netpoll网路实现
func runNetPollServer(address string) error {
	logger.Info("run in netpoll")
	netPollServer := mnetpoll.NewNetPollServer()
	return netPollServer.Serve(address)
}

// 使用gnet作为网络实现
func runGnetServer(address string) error {
	logger.Info("run in gnet")
	gnetServer := mgnet.NewGnetServer()
	return gnetServer.Serve("tcp://" + address)
}
