package database

import (
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
)

func Ping(c redis.Connection, lint *cmdLint) redis.Reply {
	length := len(lint.GetCmdData())
	if length == 0 {
		return protocol.MakePongReply()
	} else if length == 1 {
		return protocol.MakeSimpleReply(lint.GetCmdData()[0])
	} else {
		return protocol.MakeStandardErrReply("ERR wrong number of arguments for 'ping' command")
	}
}
