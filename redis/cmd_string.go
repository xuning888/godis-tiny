package redis

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"github.com/xuning888/godis-tiny/pkg/util"
	"strconv"
	"strings"
	"time"
)

func execGet(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	key := string(args[0])
	db := conn.GetDb()
	redisObj, exists := db.GetEntity(key)
	if !exists {
		return MakeNullBulkReply().WriteTo(conn)
	}
	result, err := obj.StringObjEncoding(redisObj)
	if err != nil {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	if result == nil {
		return MakeNullBulkReply().WriteTo(conn)
	}
	return MakeBulkReply(result).WriteTo(conn)
}

const (
	addOrUpdatePolicy = iota // default
	addPolicy                // set nx
	updatePolicy             // set ex
)

// 过期时间
const (
	unlimitedTTL int64 = -1
	keepTTL      int64 = 0
)

func execSet(ctx context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	key := string(args[0])
	value := args[1]
	// 默认是 update 和 add, 如果 key 已经存在，就覆盖它，如果不在就创建它
	policy := addOrUpdatePolicy
	// 过期时间
	var ttl = unlimitedTTL
	if argNum > 2 {
		for i := 2; i < len(args); i++ {
			upper := strings.ToUpper(string(args[i]))
			if "NX" == upper {
				// set key value nx 仅当key不存在时插入
				if policy == updatePolicy {
					return MakeSyntaxReply().WriteTo(conn)
				}
				policy = addPolicy
			} else if "XX" == upper {
				// set key value xx 仅当key存在时插入
				if policy == addPolicy {
					return MakeSyntaxReply().WriteTo(conn)
				}
				policy = updatePolicy
			} else if "EX" == upper {
				// 秒级过期时间
				if ttl != unlimitedTTL {
					return MakeSyntaxReply().WriteTo(conn)
				}
				if i+1 >= len(args) {
					return MakeSyntaxReply().WriteTo(conn)
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return MakeSyntaxReply().WriteTo(conn)
				}
				if ttlArg <= 0 {
					return MakeStandardErrReply("ERR invalid expire time in set").WriteTo(conn)
				}
				ttl = ttlArg * 1000
				i++
			} else if "PX" == upper {
				// 毫秒级过期时间
				if ttl != unlimitedTTL {
					return MakeSyntaxReply().WriteTo(conn)
				}
				if i+1 >= len(args) {
					return MakeSyntaxReply().WriteTo(conn)
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return MakeSyntaxReply().WriteTo(conn)
				}
				if ttlArg <= 0 {
					return MakeStandardErrReply("ERR invalid expire time in set").WriteTo(conn)
				}
				ttl = ttlArg
				i++
			} else if "KEEPTTL" == upper {
				if ttl != unlimitedTTL {
					return MakeSyntaxReply().WriteTo(conn)
				}
				ttl = keepTTL
			} else {
				return MakeSyntaxReply().WriteTo(conn)
			}
		}
	}
	db := conn.GetDb()
	redisObj, exists := db.GetEntity(key)
	if exists {
		redisObj.ObjType = obj.RedisString
		obj.StringObjSetValue(redisObj, value)
	} else {
		redisObj = obj.NewStringObject(value)
	}
	var result int
	switch policy {
	case addOrUpdatePolicy:
		db.PutEntity(key, redisObj)
		result = 1
	case addPolicy:
		result = db.PutIfAbsent(key, redisObj)
	case updatePolicy:
		result = db.PutIfExists(key, redisObj)
	}
	if result > 0 {
		if ttl != unlimitedTTL {
			if ttl == keepTTL {
				db.AddAof(conn.GetCmdLine())
			} else {
				expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
				db.ExpireV1(key, expireTime)
				db.AddAof(conn.GetCmdLine())
				// convert to expireat
				expireAtCmd := util.MakeExpireCmd(key, expireTime)
				db.AddAof(expireAtCmd)
			}
		} else {
			db.RemoveTTLV1(key)
			db.AddAof(conn.GetCmdLine())
		}
		return MakeOkReply().WriteTo(conn)
	}
	return MakeNullBulkReply().WriteTo(conn)
}

func init() {
	register("set", execSet)
	register("get", execGet)
}
