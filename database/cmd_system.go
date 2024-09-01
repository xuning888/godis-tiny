package database

import (
	"context"
	"fmt"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/redis"
	"log"
	"runtime"
	"strings"
)

const (
	flushAsync = iota
	flushSync
)

// gc
// Note 并不一定能够百分之百的触发gc, 仅用于测试
func gc(c context.Context, ctx *CommandContext) error {
	go func() {
		runtime.GC()
	}()
	return redis.MakeOkReply().WriteTo(ctx.conn)
}

func flushDb(c context.Context, ctx *CommandContext) error {
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
			return redis.MakeSyntaxReply().WriteTo(ctx.conn)
		}
	} else {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	db := ctx.GetDb()
	cmdLine := ctx.GetCmdLine()
	// 这里不管是sync 还是 async 都走同一个逻辑, 因为有gc, 开一个协程没有啥意义
	if policy == flushSync || policy == flushAsync {
		db.Flush()
		db.AddAof(cmdLine)
	}
	return redis.MakeOkReply().WriteTo(ctx.conn)
}

func execMemory(c context.Context, ctx *CommandContext) error {
	// todo 计算的还不够准确
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	args := ctx.GetArgs()
	option := strings.ToLower(string(args[0]))
	key := string(args[1])
	if option == "usage" {
		dataEntity, exists := ctx.GetDb().GetEntity(key)
		if !exists {
			return redis.MakeNullBulkReply().WriteTo(ctx.conn)
		}
		memory := dataEntity.Memory()
		return redis.MakeIntReply(int64(memory)).WriteTo(ctx.conn)
	}
	return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
}

func execType(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	key := string(ctx.GetArgs()[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeNullBulkReply().WriteTo(ctx.conn)
	}
	return redis.MakeSimpleReply([]byte(entity.Type.ToLower())).WriteTo(ctx.conn)
}

// execQuit wait write and close
func execQuit(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum != 0 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	return redis.MakeOkReply().WriteTo(ctx.conn)
}

func clearTTL(c context.Context, ctx *CommandContext) error {
	conn := ctx.GetConn()
	if !conn.IsInner() {
		cmdData := ctx.GetArgs()
		with := make([]string, 0, len(cmdData))
		for _, data := range cmdData {
			with = append(with, "'"+string(data)+"'")
		}
		return redis.MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			ctx.GetCmdName(), strings.Join(with, ", "))).WriteTo(ctx.conn)
	}
	// 检查并清理所有数据库的过期key
	ctx.GetDb().server.CheckAndClearDb()
	return redis.MakeOkReply().WriteTo(ctx.conn)
}

func execRewriteAof(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum != 0 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	db := ctx.GetDb()
	if config.Properties.AppendOnly {
		go func() {
			err := db.server.Rewrite()
			if err != nil {
				log.Printf("start rewrite failed with err: %v", err)
			}
		}()
	}
	return redis.MakeSimpleReply([]byte("Background append only file rewriting started")).WriteTo(ctx.conn)
}

// execInfo
func execInfo(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	// todo
	cmdData := ctx.GetArgs()
	var logStr string
	for _, data := range cmdData {
		logStr += string(data)
	}
	return redis.MakeBulkReply([]byte(infoClients())).WriteTo(ctx.conn)
}

func infoClients() string {
	return fmt.Sprintf("# Clients\r\n"+
		"connected_clients:%d\r\n"+
		"maxclients:%d\r\n",
		redis.ConnCounter.CountConnections(),
		config.Properties.MaxClients,
	)
}

func registerSystemCmd() {
	CmdManager.registerCmd("flushdb", flushDb, writeOnly)
	CmdManager.registerCmd("ttlops", clearTTL, readWrite)
	CmdManager.registerCmd("quit", execQuit, readOnly)
	CmdManager.registerCmd("memory", execMemory, readOnly)
	CmdManager.registerCmd("info", execInfo, readOnly)
	CmdManager.registerCmd("type", execType, readOnly)
	CmdManager.registerCmd("bgrewriteaof", execRewriteAof, readOnly)
	CmdManager.registerCmd("gc", gc, writeOnly)
}
