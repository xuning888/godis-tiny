package redis

import (
	"context"
	"fmt"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"log"
	"runtime"
	"strconv"
	"strings"
)

func ping(ctx context.Context, conn *Client) error {
	args := conn.GetArgs()
	if len(args) == 0 {
		return MakePongReply().WriteTo(conn)
	} else if len(args) == 1 {
		return MakeBulkReply(args[0]).WriteTo(conn)
	} else {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
}

func selectDb(ctx context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	index, err := strconv.Atoi(string(cmdData[0]))
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	err = conn.RangeCheck(index)
	if err != nil {
		return MakeStandardErrReply(err.Error()).WriteTo(conn)
	}
	conn.SetDbIndex(index)
	return MakeOkReply().WriteTo(conn)
}

func execType(ctx context.Context, conn *Client) error {
	args := conn.GetArgs()
	if len(args) != 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	key := string(conn.GetArgs()[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeBulkReply([]byte("none")).WriteTo(conn)
	}
	typeName := obj.ObjectTypeName(redisObj.ObjType)
	return MakeSimpleReply([]byte(typeName)).WriteTo(conn)
}

// gc
// Note 并不一定能够百分之百的触发gc, 仅用于测试
func gc(c context.Context, conn *Client) error {
	go func() {
		runtime.GC()
	}()
	return MakeOkReply().WriteTo(conn)
}

const (
	flushAsync = iota
	flushSync
)

func flushDb(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	var policy int
	// 默认同步刷新
	if argNum == 0 {
		policy = flushSync
	} else if argNum == 1 {
		args := strings.ToUpper(string(conn.GetArgs()[0]))
		if args == "ASYNC" {
			policy = flushAsync
		} else if args == "SYNC" {
			policy = flushSync
		} else {
			return MakeSyntaxReply().WriteTo(conn)
		}
	} else {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	db := conn.GetDb()
	cmdLine := conn.GetCmdLine()
	// 这里不管是sync 还是 async 都走同一个逻辑, 因为有gc, 开一个协程没有啥意义
	if policy == flushSync || policy == flushAsync {
		db.Flush()
		db.AddAof(cmdLine)
	}
	return MakeOkReply().WriteTo(conn)
}

func clearTTL(c context.Context, conn *Client) error {
	if !conn.IsInner() {
		cmdData := conn.GetArgs()
		with := make([]string, 0, len(cmdData))
		for _, data := range cmdData {
			with = append(with, "'"+string(data)+"'")
		}
		return MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			conn.GetCmdName(), strings.Join(with, ", "))).WriteTo(conn)
	}
	conn.ClearDatabase()
	return MakeOkReply().WriteTo(conn)
}

func execRewriteAof(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum != 0 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	if config.Properties.AppendOnly {
		go func() {
			err := conn.Rewrite()
			if err != nil {
				log.Printf("start rewrite failed with err: %v", err)
			}
		}()
	}
	return MakeSimpleReply([]byte("Background append only file rewriting started")).WriteTo(conn)
}

func execQuit(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum != 0 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	return MakeOkReply().WriteTo(conn)
}

func execMemory(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	option := strings.ToLower(string(args[0]))
	key := string(args[1])
	if option == "usage" {
		redisObject, exists := conn.GetDb().GetEntity(key)
		if !exists {
			return MakeNullBulkReply().WriteTo(conn)
		}
		var mem = int64(len(args[1]))
		var err error
		switch redisObject.ObjType {
		case obj.RedisString:
			mem, err = obj.StringObjMem(redisObject)
			if err != nil {
				return MakeNullBulkReply().WriteTo(conn)
			}
			break
		case obj.RedisList:
			mem, err = obj.ListObjMem(redisObject)
			if err != nil {
				return MakeNullBulkReply().WriteTo(conn)
			}
			break
		default:
			// todo
			return MakeNullBulkReply().WriteTo(conn)
		}
		return MakeIntReply(mem).WriteTo(conn)
	}
	return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
}

// execInfo
func execInfo(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	// todo
	cmdData := conn.GetArgs()
	var logStr string
	for _, data := range cmdData {
		logStr += string(data)
	}
	return MakeBulkReply([]byte(infoClients())).WriteTo(conn)
}

func infoClients() string {
	return fmt.Sprintf("# Clients\r\n"+
		"connected_clients:%d\r\n"+
		"maxclients:%d\r\n",
		ConnCounter.CountConnections(),
		config.Properties.MaxClients,
	)
}

func init() {
	register("ping", ping)
	register("select", selectDb)
	register("type", execType)
	register("ttlops", clearTTL)
	register("bgrewriteaof", execRewriteAof)
	register("flushdb", flushDb)
	register("quit", execQuit)
	register("memory", execMemory)
	register("info", execInfo)
	register("gc", gc)
}
