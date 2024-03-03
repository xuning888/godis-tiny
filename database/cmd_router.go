package database

import (
	"go.uber.org/zap"
	"godis-tiny/pkg/logger"
	"strings"
)

var (
	cmdManager = makeCommandManager()
)

type command struct {
	cmdName string
	exeFunc ExeFunc
}

func initResister() {
	registerSystemCmd()
	registerConnCmd()
	registerKeyCmd()
	registerStringCmd()
	registerListCmd()
}

type commandManager struct {
	cmdTable map[string]*command
	lg       *zap.Logger
}

func (c *commandManager) registerCmd(cmdName string, execFunc ExeFunc) {
	lower := strings.ToLower(cmdName)
	cmd := &command{
		cmdName: lower,
		exeFunc: execFunc,
	}
	c.lg.Sugar().Debugf("register command: %s", cmd.cmdName)
	c.cmdTable[lower] = cmd
}

func (c *commandManager) getCmd(cmdName string) *command {
	cmd, ok := c.cmdTable[cmdName]
	if ok {
		return cmd
	}
	return nil
}

func makeCommandManager() *commandManager {
	lg, err := logger.SetUpLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	return &commandManager{
		cmdTable: make(map[string]*command),
		lg:       lg,
	}
}
