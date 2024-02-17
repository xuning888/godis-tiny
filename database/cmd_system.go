package database

import (
	"fmt"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
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

func clearTTL(ctx *CommandContext, lint *cmdLint) redis.Reply {
	conn := ctx.GetConn()
	if !conn.IsInner() {
		return protocol.MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			lint.GetCmdName(), strings.Join(lint.cmdString, ", ")))
	}
	// 检查并清理所有数据库的过期key
	ctx.GetDb().ttlChecker.CheckAndClearDb()
	return protocol.MakeOkReply()
}

func registerSystemCmd() {
	RegisterCmd("flushdb", flushDb)
	RegisterCmd("ttlops", clearTTL)
}
