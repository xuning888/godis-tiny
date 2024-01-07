package database

import (
	"g-redis/interface/redis"
)

type CmdLine = [][]byte

// DBEngine 存储引擎的抽象
type DBEngine interface {
	// Exec client 上的命令
	Exec(client redis.Connection, cmdLine CmdLine) redis.Reply
	// Close 关闭
	Close() error
}
