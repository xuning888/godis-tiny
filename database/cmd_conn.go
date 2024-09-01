package database

import (
	"context"
	"github.com/xuning888/godis-tiny/redis"
	"strconv"
)

// ping exc ping reply pong
func ping(c context.Context, ctx *CommandContext) error {
	args := ctx.GetArgs()
	if len(args) == 0 {
		return redis.MakePongReply().WriteTo(ctx.GetConn())
	} else if len(args) == 1 {
		return redis.MakeBulkReply(args[0]).WriteTo(ctx.GetConn())
	} else {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
}

// selectDb
func selectDb(c context.Context, ctx *CommandContext) error {
	db := ctx.GetDb()
	conn := ctx.GetConn()
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(conn)
	}
	cmdData := ctx.GetArgs()
	index, err := strconv.Atoi(string(cmdData[0]))
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	err = db.server.CheckIndex(index)
	if err != nil {
		return redis.MakeStandardErrReply(err.Error()).WriteTo(conn)
	}
	conn.SetDbIndex(index)
	return redis.MakeOkReply().WriteTo(conn)
}

func registerConnCmd() {
	CmdManager.registerCmd("ping", ping, readOnly)
	CmdManager.registerCmd("select", selectDb, writeOnly)
}
