package main

import (
	"github.com/sirupsen/logrus"
	"godis-tiny/logger"
	"godis-tiny/redis/server"
	"godis-tiny/tcp"
)

func main() {
	logger.Configure(&logger.Configuration{
		Level:         logrus.DebugLevel,
		TimeFormat:    "2006-01-02 15:04:05.000",
		LogPath:       "logs",
		EnableFileLog: false,
	})
	handler := server.MakeHandler()
	err := tcp.ListenAndServeWithSignal(":8080", handler)
	if err != nil {
		panic(err)
	}
}
