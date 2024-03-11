package database

import (
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"strconv"
)

// ping exc ping reply pong
func ping(ctx *CommandContext, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	if len(args) == 0 {
		return protocol.MakePongReply()
	} else if len(args) == 1 {
		return protocol.MakeBulkReply(args[0])
	} else {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
}

// selectDb
func selectDb(ctx *CommandContext, lint *cmdLint) redis.Reply {
	db := ctx.GetDb()
	conn := ctx.GetConn()
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	index, err := strconv.Atoi(string(cmdData[0]))
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	err = db.indexChecker.CheckIndex(index)
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
