package database

import (
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"strings"
)

const (
	flushAsync = iota
	flushSync
)

func flushDb(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	var policy int
	// 默认同步刷新
	if argNum == 0 {
		policy = flushSync
	} else if argNum == 1 {
		args := strings.ToUpper(string(lint.GetCmdData()[0]))
		if args == "ASYNC" {
			policy = flushAsync
		} else if args == "SYNC" {
			policy = flushSync
		} else {
			return protocol.MakeSyntaxReply()
		}
	} else {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	db := ctx.GetDb()
	if policy == flushSync {
		db.Flush()
	} else {
		go func() {
			db.Flush()
		}()
	}
	return protocol.MakeOkReply()
}

func init() {
	RegisterCmd("ping", ping, 0)
	RegisterCmd("flushdb", flushDb, 0)
}
