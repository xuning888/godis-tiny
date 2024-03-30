package database

import (
	"context"
	"fmt"
	"godis-tiny/config"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/connection"
	"godis-tiny/redis/protocol"
	"log"
	"strings"
)

const (
	flushAsync = iota
	flushSync
)

func flushDb(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	var policy int
	// 默认同步刷新
	if argNum == 0 {
		policy = flushSync
	} else if argNum == 1 {
		args := strings.ToUpper(string(ctx.GetArgs()[0]))
		if args == "ASYNC" {
			policy = flushAsync
		} else if args == "SYNC" {
			policy = flushSync
		} else {
			return protocol.MakeSyntaxReply()
		}
	} else {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	db := ctx.GetDb()
	cmdLine := ctx.GetCmdLine()
	if policy == flushSync {
		db.Flush()
		db.addAof(cmdLine)
	} else {
		go func() {
			db.Flush()
			db.addAof(cmdLine)
		}()
	}
	return protocol.MakeOkReply()
}

func execMemory(c context.Context, ctx *CommandContext) redis.Reply {
	// todo 计算的还不够准确
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	args := ctx.GetArgs()
	option := strings.ToLower(string(args[0]))
	key := string(args[1])
	if option == "usage" {
		dataEntity, exists := ctx.GetDb().GetEntity(key)
		if !exists {
			return protocol.MakeNullBulkReply()
		}
		memory := dataEntity.Memory()
		return protocol.MakeIntReply(int64(memory))
	}
	return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
}

func execType(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeUnknownCommand(ctx.GetCmdName())
	}
	key := string(ctx.GetArgs()[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeSimpleReply([]byte(entity.Type.ToLower()))
}

// execQuit wait write and close
func execQuit(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum != 0 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	return protocol.MakeOkReply()
}

func clearTTL(c context.Context, ctx *CommandContext) redis.Reply {
	conn := ctx.GetConn()
	if !conn.IsInner() {
		cmdData := ctx.GetArgs()
		with := make([]string, 0, len(cmdData))
		for _, data := range cmdData {
			with = append(with, "'"+string(data)+"'")
		}
		return protocol.MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			ctx.GetCmdName(), strings.Join(with, ", ")))
	}
	// 检查并清理所有数据库的过期key
	ctx.GetDb().ttlChecker.CheckAndClearDb()
	return protocol.MakeOkReply()
}

// execInfo
func execInfo(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	// todo
	cmdData := ctx.GetArgs()
	var logStr string
	for _, data := range cmdData {
		logStr += string(data)
	}
	log.Printf("info %s\n", logStr)
	return protocol.MakeBulkReply([]byte(infoClients()))
}

func infoClients() string {
	return fmt.Sprintf("# Clients\r\n"+
		"connected_clients:%d\r\n"+
		"maxclients:%d\r\n",
		connection.ConnCounter.CountConnections(),
		config.Properties.MaxClients,
	)
}

func registerSystemCmd() {
	cmdManager.registerCmd("flushdb", flushDb, writeOnly)
	cmdManager.registerCmd("ttlops", clearTTL, readWrite)
	cmdManager.registerCmd("quit", execQuit, readOnly)
	cmdManager.registerCmd("memory", execMemory, readOnly)
	cmdManager.registerCmd("info", execInfo, readOnly)
	cmdManager.registerCmd("type", execType, readOnly)
}
