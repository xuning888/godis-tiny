package database

import (
	"context"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis/protocol"
	"math"
	"path"
	"strconv"
	"time"
)

// execDel del key [key...]
func execDel(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 {
		// 错误参数
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	var deleted = 0
	db := ctx.GetDb()
	for i := 0; i < len(cmdData); i++ {
		result := db.Remove(string(cmdData[i]))
		deleted += result
	}
	if deleted > 0 {
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(int64(deleted))
	}
	return protocol.MakeIntReply(0)
}

// execKeys keys pattern
func execKeys(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	args := ctx.GetArgs()
	pattern := string(args[0])
	_, err := path.Match(pattern, "")
	if err != nil {
		return protocol.MakeStandardErrReply("ERR invalid pattern")
	}
	keys := ctx.GetDb().data.Keys()
	var matchedKeys [][]byte
	if pattern == "*" {
		matchedKeys = make([][]byte, 0, len(keys))
	} else {
		matchedKeys = make([][]byte, 0)
	}
	for _, key := range keys {
		matched, _ := path.Match(pattern, key)
		if matched {
			matchedKeys = append(matchedKeys, []byte(key))
		}
	}
	if len(matchedKeys) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(matchedKeys)
}

// execExists
func execExists(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	keys := make([]string, argNum)
	args := ctx.GetArgs()
	for i := 0; i < argNum; i++ {
		keys[i] = string(args[i])
	}
	result := ctx.GetDb().Exists(keys)
	return protocol.MakeIntReply(result)
}

// execTTL ttl key
func execTTL(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	// key 不存在返回-2
	_, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeIntReply(-2)
	}
	// 没有设置过期时间返回-1
	expired, exists := db.IsExpiredV1(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}

	// 如果过期了，删除key,并且返回-2
	if expired {
		db.Remove(key)
		return protocol.MakeIntReply(-2)
	}
	// 如果没有过期，计算ttl时间
	expireTime := db.ExpiredAt(key)
	// ttl
	remainingTime := expireTime.Sub(time.Now())
	seconds := remainingTime.Seconds()
	return protocol.MakeIntReply(int64(math.Round(seconds)))
}

func execPTTL(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	// key 不存在返回-2
	_, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeIntReply(-2)
	}
	// 没有设置过期时间返回-1
	expired, exists := db.IsExpiredV1(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}

	// 如果过期了，删除key,并且返回-2
	if expired {
		db.Remove(key)
		return protocol.MakeIntReply(-2)
	}

	expireTime := db.ExpiredAt(key)
	remainingTime := expireTime.Sub(time.Now())
	microseconds := remainingTime.Milliseconds()
	return protocol.MakeIntReply(int64(math.Round(float64(microseconds))))
}

// execExpire expire key ttl
func execExpire(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	ttl, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	_, ok := ctx.GetDb().GetEntity(key)
	if !ok {
		// key 不存在返回0
		return protocol.MakeIntReply(0)
	}
	expireTime := time.Now().Add(time.Duration(ttl) * time.Second)
	ctx.GetDb().ExpireV1(key, expireTime)
	ctx.GetDb().addAof(util.MakeExpireCmd(key, expireTime))
	return protocol.MakeIntReply(1)
}

// execPersist persist key 移除key的过期时间
func execPersist(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	expired, exists := ctx.GetDb().IsExpiredV1(key)
	// key 不存在ttl
	if !exists {
		return protocol.MakeIntReply(0)
	}
	// key 已经过期
	if expired {
		return protocol.MakeIntReply(0)
	}
	// key 存在，并且没有过期，就移除他的ttl
	ctx.GetDb().RemoveTTLV1(key)
	// add aof
	ctx.GetDb().addAof(ctx.GetCmdLine())
	return protocol.MakeIntReply(1)
}

// execExpireAt expireat key unix-time-seconds
func execExpireAt(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	timestamp, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	_, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	expireTime := time.Unix(timestamp, 0)
	ctx.GetDb().ExpireV1(key, expireTime)
	// add aof
	ctx.GetDb().addAof(ctx.GetCmdLine())
	return protocol.MakeIntReply(1)
}

func registerKeyCmd() {
	cmdManager.registerCmd("del", execDel, writeOnly)
	cmdManager.registerCmd("keys", execKeys, readOnly)
	cmdManager.registerCmd("exists", execExists, readOnly)
	cmdManager.registerCmd("ttl", execTTL, readWrite)
	cmdManager.registerCmd("expire", execExpire, readWrite)
	cmdManager.registerCmd("persist", execPersist, readWrite)
	cmdManager.registerCmd("expireat", execExpireAt, readWrite)
	cmdManager.registerCmd("pttl", execPTTL, readOnly)
}
