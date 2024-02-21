package main

import (
	"github.com/bytedance/gopkg/util/logger"
	"godis-tiny/tcp"
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

// 使用gnet作为网络实现
func runGnetServer(address string) error {
	logger.Info("run in gnet")
	gnetServer := tcp.NewGnetServer()
	return gnetServer.Serve("tcp://" + address)
}
