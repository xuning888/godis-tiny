package main

import (
	"github.com/bytedance/gopkg/util/logger"
	"godis-tiny/tcp/mnetpoll"
)

func main() {
	logger.SetLevel(logger.LevelDebug)
	/*	logger.Configure(&logger.Configuration{
		Level:         logrus.DebugLevel,
		TimeFormat:    "2006-01-02 15:04:05.000",
		LogPath:       "logs",
		EnableFileLog: false,
	})*/
	server := mnetpoll.NewNetPollServer()
	if err := server.Serve(":8080"); err != nil {
		logger.Errorf("start server has error: %v", err)
		panic(err)
	}
}
