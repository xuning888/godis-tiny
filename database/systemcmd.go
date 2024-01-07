package database

import (
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
)

// Ping ping -> pong
func Ping(c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	if len(cmdLine) == 1 {
		return protocol.MakePongReply()
	} else if len(cmdLine) == 2 {
		return protocol.MakeSimpleReply(cmdLine[1])
	} else {
		return protocol.MakeStandardErrReply("ERR wrong number of arguments for 'ping' command")
	}
}
