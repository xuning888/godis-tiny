package main

import (
	"fmt"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis"
	"os"
)

var serverName = "godis-tiny"

var defaultConfig = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6389,
	AppendOnly:     false,
	AppendFilename: "",
	RunID:          util.RandStr(40),
}

func fileExists(filename string) bool {
	stat, err := os.Stat(filename)
	return err == nil && !stat.IsDir()
}

func setupConfiguration(configPath string) {
	if fileExists(configPath) {
		config.SetUpConfig(configPath)
		logger.Infof("Loaded configuration from %s", configPath)
	} else {
		logger.Infof("Config file '%s' not found. Using default settings.", configPath)
		config.Properties = defaultConfig
	}
}

func printHelp() {
	helpText := `Usage: ./` + serverName + ` [/path/to/redis.conf] [options] [-]
       ./` + serverName + ` -h or --help
`
	fmt.Print(helpText)
}

func main() {
	_, err := logger.SetUpLoggerv2(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	args := os.Args[1:]
	if len(args) > 0 {
		for _, arg := range args {
			switch arg {
			case "-h", "--help":
				printHelp()
				return
			default:
				setupConfiguration(arg)
			}
		}
	} else {
		setupConfiguration("redis.conf")
	}
	s := redis.NewRedisServer()
	s.Spin()
}
