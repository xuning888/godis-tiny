package database

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis"
	"math"
	"path"
	"strconv"
	"time"
)

// execDel del key [key...]
func execDel(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 {
		// 错误参数
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	var deleted = 0
	db := ctx.GetDb()
	for i := 0; i < len(cmdData); i++ {
		result := db.Remove(string(cmdData[i]))
		deleted += result
	}
	if deleted > 0 {
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return redis.MakeIntReply(int64(deleted)).WriteTo(ctx.conn)
	}
	return redis.MakeIntReply(0).WriteTo(ctx.conn)
}

// execKeys keys pattern
func execKeys(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	args := ctx.GetArgs()
	pattern := string(args[0])
	_, err := path.Match(pattern, "")
	if err != nil {
		return redis.MakeStandardErrReply("ERR invalid pattern").WriteTo(ctx.conn)
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
		return redis.MakeEmptyMultiBulkReply().WriteTo(ctx.conn)
	}
	return redis.MakeMultiBulkReply(matchedKeys).WriteTo(ctx.conn)
}

// execExists
func execExists(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	keys := make([]string, argNum)
	args := ctx.GetArgs()
	for i := 0; i < argNum; i++ {
		keys[i] = string(args[i])
	}
	result := ctx.GetDb().Exists(keys)
	return redis.MakeIntReply(result).WriteTo(ctx.GetConn())
}

// execTTL ttl key
func execTTL(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	// key 不存在返回-2
	_, ok := db.GetEntity(key)
	if !ok {
		return redis.MakeIntReply(-2).WriteTo(ctx.GetConn())
	}
	// 没有设置过期时间返回-1
	expired, exists := db.IsExpiredV1(key)
	if !exists {
		return redis.MakeIntReply(-1).WriteTo(ctx.GetConn())
	}

	// 如果过期了，删除key,并且返回-2
	if expired {
		db.Remove(key)
		return redis.MakeIntReply(-2).WriteTo(ctx.GetConn())
	}
	// 如果没有过期，计算ttl时间
	expireTime := db.ExpiredAt(key)
	// ttl
	remainingTime := expireTime.Sub(time.Now())
	seconds := remainingTime.Seconds()
	return redis.MakeIntReply(int64(math.Round(seconds))).WriteTo(ctx.GetConn())
}

func execPTTL(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	// key 不存在返回-2
	_, ok := db.GetEntity(key)
	if !ok {
		return redis.MakeIntReply(-2).WriteTo(ctx.GetConn())
	}
	// 没有设置过期时间返回-1
	expired, exists := db.IsExpiredV1(key)
	if !exists {
		return redis.MakeIntReply(-1).WriteTo(ctx.GetConn())
	}

	// 如果过期了，删除key,并且返回-2
	if expired {
		db.Remove(key)
		return redis.MakeIntReply(-2).WriteTo(ctx.GetConn())
	}

	expireTime := db.ExpiredAt(key)
	remainingTime := expireTime.Sub(time.Now())
	microseconds := remainingTime.Milliseconds()
	return redis.MakeIntReply(int64(math.Round(float64(microseconds)))).WriteTo(ctx.GetConn())
}

// execExpire expire key ttl
func execExpire(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	ttl, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.GetConn())
	}
	_, ok := ctx.GetDb().GetEntity(key)
	if !ok {
		// key 不存在返回0
		return redis.MakeIntReply(0).WriteTo(ctx.GetConn())
	}
	expireTime := time.Now().Add(time.Duration(ttl) * time.Second)
	ctx.GetDb().ExpireV1(key, expireTime)
	ctx.GetDb().AddAof(util.MakeExpireCmd(key, expireTime))
	return redis.MakeIntReply(1).WriteTo(ctx.GetConn())
}

// execPersist persist key 移除key的过期时间
func execPersist(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	expired, exists := ctx.GetDb().IsExpiredV1(key)
	// key 不存在ttl
	if !exists {
		return redis.MakeIntReply(0).WriteTo(ctx.GetConn())
	}
	// key 已经过期
	if expired {
		return redis.MakeIntReply(0).WriteTo(ctx.GetConn())
	}
	// key 存在，并且没有过期，就移除他的ttl
	ctx.GetDb().RemoveTTLV1(key)
	// add aof
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(1).WriteTo(ctx.GetConn())
}

// execExpireAt expireat key unix-time-seconds
func execExpireAt(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	timestamp, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.GetConn())
	}
	_, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeIntReply(0).WriteTo(ctx.GetConn())
	}
	expireTime := time.Unix(timestamp, 0)
	ctx.GetDb().ExpireV1(key, expireTime)
	// add aof
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(1).WriteTo(ctx.GetConn())
}

func registerKeyCmd() {
	CmdManager.registerCmd("del", execDel, writeOnly)
	CmdManager.registerCmd("keys", execKeys, readOnly)
	CmdManager.registerCmd("exists", execExists, readOnly)
	CmdManager.registerCmd("ttl", execTTL, readWrite)
	CmdManager.registerCmd("expire", execExpire, readWrite)
	CmdManager.registerCmd("persist", execPersist, readWrite)
	CmdManager.registerCmd("expireat", execExpireAt, readWrite)
	CmdManager.registerCmd("pttl", execPTTL, readOnly)
}
