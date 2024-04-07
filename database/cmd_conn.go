package database

import (
	"context"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/redis/protocol"
	"strconv"
)

// ping exc ping reply pong
func ping(c context.Context, ctx *CommandContext) redis.Reply {
	args := ctx.GetArgs()
	if len(args) == 0 {
		return protocol.MakePongReply()
	} else if len(args) == 1 {
		return protocol.MakeBulkReply(args[0])
	} else {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
}

// selectDb
func selectDb(c context.Context, ctx *CommandContext) redis.Reply {
	db := ctx.GetDb()
	conn := ctx.GetConn()
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	index, err := strconv.Atoi(string(cmdData[0]))
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	err = db.engineCommand.CheckIndex(index)
	if err != nil {
		return protocol.MakeStandardErrReply(err.Error())
	}
	conn.SetIndex(index)
	return protocol.MakeOkReply()
}

func registerConnCmd() {
	cmdManager.registerCmd("ping", ping, readOnly)
	cmdManager.registerCmd("select", selectDb, writeOnly)
}
