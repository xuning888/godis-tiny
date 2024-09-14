package redis

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/datastruct/dict"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
)

func hset(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum%2 == 0 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	key := string(args[0])
	pairs := args[1:]
	redisObj, exists := conn.GetDb().GetEntity(key)
	if exists {
		if redisObj.ObjType != obj.RedisHash {
			return MakeWrongTypeErrReply().WriteTo(conn)
		}
		var result int64 = 0
		simpleDict := redisObj.Ptr.(*dict.SimpleDict)
		for i := 0; i < len(pairs); i += 2 {
			field, value := string(pairs[i]), pairs[i+1]
			result += int64(simpleDict.Put(field, value))
		}
		return MakeIntReply(result).WriteTo(conn)
	}
	redisObj = obj.NewHashObject()
	var result int64 = 0
	for i := 0; i < len(pairs); i += 2 {
		field, value := string(pairs[i]), pairs[i+1]
		simpleDict := redisObj.Ptr.(*dict.SimpleDict)
		result += int64(simpleDict.Put(field, value))
	}
	conn.GetDb().PutEntity(key, redisObj)
	return MakeIntReply(result).WriteTo(conn)
}

func hget(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum != 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	key := string(args[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeNullBulkReply().WriteTo(conn)
	}
	field := string(args[1])
	simpleDict := redisObj.Ptr.(*dict.SimpleDict)
	if value, exists2 := simpleDict.Get(field); exists2 {
		return MakeBulkReply(value.([]byte)).WriteTo(conn)
	}
	return MakeNullBulkReply().WriteTo(conn)
}

func init() {
	register("hset", hset)
	register("hget", hget)
}
