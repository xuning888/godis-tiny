package database

import (
	"fmt"
	"g-redis/datastruct/dict"
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"strings"
)

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

func (db *DB) Exec(c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	return protocol.MakeStandardErrReply(fmt.Sprintf("ERR Unknown command %s", cmdName))
}
