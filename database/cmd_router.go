package database

import (
	"github.com/xuning888/godis-tiny/pkg/logger"
	"go.uber.org/zap"
	"strings"
)

var (
	CmdManager = makeCommandManager()
)

type commandFlag int

const (
	// readOnly 对内存是只读操作
	readOnly commandFlag = 0
	// writeOnly 对内存是只写操作
	writeOnly commandFlag = 1
	// readWrite 对内存是存在读写操作  e
	readWrite commandFlag = 2
)

var flagNameMap = map[commandFlag]string{
	readOnly:  "readOnly",
	writeOnly: "writeOnly",
	readWrite: "readWrite",
}

type command struct {
	cmdName string
	exeFunc ExeFunc
	flag    commandFlag
}

/*func init() {
	registerSystemCmd()
	registerConnCmd()
	registerKeyCmd()
	registerStringCmd()
	registerListCmd()
}*/

type commandManager struct {
	cmdTable map[string]*command
	lg       *zap.Logger
}

func (c *commandManager) registerCmd(cmdName string, execFunc ExeFunc, flag commandFlag) {
	lower := strings.ToLower(cmdName)
	cmd := &command{
		cmdName: lower,
		exeFunc: execFunc,
		flag:    flag,
	}
	c.lg.Sugar().Infof("register command: %s, flag: %v", cmd.cmdName, flagNameMap[flag])
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
	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	return &commandManager{
		cmdTable: make(map[string]*command),
		lg:       lg.Named("command-manager"),
	}
}
