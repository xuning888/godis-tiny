package redis

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"strconv"
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

func init() {
	register("ping", ping)
	register("select", selectDb)
	register("type", execType)
}
