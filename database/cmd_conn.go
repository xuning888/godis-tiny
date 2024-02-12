package database

import (
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
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
		return protocol.MakeNumberOfArgsErrReply(lint.cmdName)
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
	err = db.dbEngin.CheckIndex(index)
	if err != nil {
		return protocol.MakeStandardErrReply(err.Error())
	}
	conn.SetIndex(index)
	return protocol.MakeOkReply()
}

func init() {
	RegisterCmd("ping", ping)
	RegisterCmd("select", selectDb)
}
