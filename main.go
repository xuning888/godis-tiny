package main

import (
	"fmt"
	"godis-tiny/config"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/tcp"
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
		fmt.Printf("Loaded configuration from %s\n", configPath)
	} else {
		fmt.Printf("Config file '%s' not found. Using default settings.\n", configPath)
		config.Properties = defaultConfig
	}
}

func printHelp() {
	helpText := `
Usage: ./` + serverName + ` [/path/to/redis.conf] [options] [-]
       ./` + serverName + ` -h or --help`
	fmt.Print(helpText)
}

func main() {
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
	_, err := logger.SetUpLoggerv2(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	server := tcp.NewRedisServer()
	server.Spin()
}
