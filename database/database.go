package database

import (
	"bytes"
	"fmt"
	"g-redis/datastruct/dict"
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"strings"
)

// cmdLint database 内部流转的结构体，包含了客户端发送的命令名称和数据
type cmdLint struct {
	cmdName    string
	cmdData    [][]byte
	dataString string
}

func (lint *cmdLint) GetCmdName() string {
	return lint.cmdName
}

func (lint *cmdLint) GetCmdData() [][]byte {
	return lint.cmdData
}

// parseToLint 将resp 协议的字节流转为为 database 内部流转的结构体
func parseToLint(cmdLine database.CmdLine) *cmdLint {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdData := cmdLine[1:]
	var buf bytes.Buffer
	for i := 0; i < len(cmdData); i++ {
		buf.Write(cmdData[i])
		if i != len(cmdData)-1 {
			buf.Write([]byte{'\r', '\n'})
		}
	}
	return &cmdLint{
		cmdName:    cmdName,
		cmdData:    cmdData,
		dataString: buf.String(),
	}
}

type ExeFunc func(db *DB, cmdLint *cmdLint) redis.Reply

// DB 存储
type DB struct {
	index  int
	Data   dict.Dict
	ttlMap dict.Dict
}

func MakeDB(index int) *DB {
	return &DB{
		index:  index,
		Data:   dict.MakeSimpleDict(),
		ttlMap: dict.MakeSimpleDict(),
	}
}

func (db *DB) Exec(c redis.Connection, lint *cmdLint) redis.Reply {
	return protocol.MakeStandardErrReply(fmt.Sprintf("ERR Unknown command %s", lint.cmdName))
}
