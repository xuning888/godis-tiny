package database

import (
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"strconv"
)

func ping(db *DB, conn redis.Connection, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	if len(args) == 0 {
		return protocol.MakePongReply()
	} else if len(args) == 1 {
		return protocol.MakeSimpleReply(args[0])
	} else {
		return protocol.MakeNumberOfArgsErrReply(lint.cmdName)
	}
}

// selectDb
func selectDb(db *DB, conn redis.Connection, lint *cmdLint) redis.Reply {
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
	RegisterCmd("ping", ping, 0)
	RegisterCmd("select", selectDb, 1)
}
