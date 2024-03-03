package persistence

import (
	"os"
)

type CmdLine [][]byte

type payload struct {
	dbIndex int
	cmdLine CmdLine
}

var _ Aof = &aof{}

// Aof aof的功能定义
type Aof interface {
	// AddAof 添加到Aof中
	AddAof(dbIndex int, cmdLine CmdLine)
}

// Aof persistence
type aof struct {
	// aofFilename aof文件名称
	aofFilename string
	// aofFile
	aofFile os.File
	// aofChan
	aofChan chan *payload
	// currentDb 后续命令操作的db
	currentDb int
}

func (a *aof) AddAof(dbIndex int, cmdLine CmdLine) {
	if a.aofChan == nil {
		return
	}
	a.aofChan <- &payload{
		dbIndex: dbIndex,
		cmdLine: cmdLine,
	}
}

func NewAof() Aof {
	return &aof{}
}
