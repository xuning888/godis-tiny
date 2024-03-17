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
		ctx.GetDb().addAof(lint.GetCmdLine())
	} else {
		go func() {
			db.Flush()
			ctx.GetDb().addAof(lint.GetCmdLine())
		}()
	}
	return protocol.MakeOkReply()
}

func execMemory(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	option := strings.ToLower(string(lint.GetCmdData()[0]))
	key := string(lint.GetCmdData()[1])
	if option == "usage" {
		dataEntity, exists := ctx.GetDb().GetEntity(key)
		if !exists {
			return protocol.MakeNullBulkReply()
		}
		bytes := dataEntity.Data.([]byte)
		return protocol.MakeIntReply(int64(len(bytes)))
	}
	return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
}

func execQuit(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum != 0 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	/*redisConn := ctx.GetConn()
	err := redisConn.GnetConn().Close()
	if err != nil {
		return protocol.MakeStandardErrReply(err.Error())
	}*/
	// todo
	return protocol.MakeOkReply()
}

func clearTTL(ctx *CommandContext, lint *cmdLint) redis.Reply {
	conn := ctx.GetConn()
	if !conn.IsInner() {
		cmdData := lint.GetCmdData()
		with := make([]string, 0, len(cmdData))
		for _, data := range cmdData {
			with = append(with, "'"+string(data)+"'")
		}
		return protocol.MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			lint.GetCmdName(), strings.Join(with, ", ")))
	}
	// 检查并清理所有数据库的过期key
	ctx.GetDb().ttlChecker.CheckAndClearDb()
	return protocol.MakeOkReply()
}

// execInfo
func execInfo(ctx *CommandContext, lint *cmdLint) redis.Reply {

	s := fmt.Sprintf("# Clients\r\n"+
		"connected_clients:%d\r\n",
		//"client_recent_max_input_buffer:%d\r\n"+
		//"client_recent_max_output_buffer:%d\r\n"+
		//"blocked_clients:%d\n",
		redis.Counter.CountConnections(),
	)
	return protocol.MakeBulkReply([]byte(s))
}

func registerSystemCmd() {
	cmdManager.registerCmd("flushdb", flushDb, writeOnly)
	cmdManager.registerCmd("ttlops", clearTTL, readWrite)
	cmdManager.registerCmd("quit", execQuit, readOnly)
	cmdManager.registerCmd("memory", execMemory, readOnly)
	cmdManager.registerCmd("info", execInfo, readOnly)
}
