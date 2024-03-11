package main

import (
	"fmt"
	"godis-tiny/config"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/tcp"
	"os"
)

var defaultConfig = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6279,
	AppendOnly:     false,
	AppendFilename: "",
	RunID:          util.RandStr(40),
}

func fileExists(filename string) bool {
	stat, err := os.Stat(filename)
	return err == nil && !stat.IsDir()
}

func main() {
	_, err := logger.SetUpLoggerv2(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	if fileExists("redis.conf") {
		config.SetUpConfig("redis.conf")
	} else {
		config.Properties = defaultConfig
	}
	gnetServer, err := tcp.NewGnetServer()
	if err != nil {
		logger.Errorf("start server failed with error: %v", err)
		return
	}
	address := fmt.Sprintf("tcp://%s:%d", config.Properties.Bind, config.Properties.Port)
	if err = gnetServer.Serve(address); err != nil {
		fmt.Printf("start server failed with error: %v\n", err)
	}
}
