package database

import (
	"github.com/bytedance/gopkg/util/logger"
	"strings"
)

var cmdTable = make(map[string]*command)

type command struct {
	cmdName string
	exeFunc ExeFunc
}

func RegisterCmd(cmdName string, exeFunc ExeFunc) {
	lower := strings.ToLower(cmdName)
	cmd := &command{
		cmdName: lower,
		exeFunc: exeFunc,
	}
	logger.Debugf("register command %s", cmd.cmdName)
	cmdTable[lower] = cmd
}

func getCommand(cmdName string) *command {
	cmd, ok := cmdTable[cmdName]
	if ok {
		return cmd
	}
	return nil
}

func initResister() {
	registerSystemCmd()
	registerConnCmd()
	registerKeyCmd()
	registerStringCmd()
}
