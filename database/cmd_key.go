package database

import (
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"path"
	"strconv"
	"time"
)

// execDel del key [key...]
func execDel(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 {
		// 错误参数
		return protocol.MakeNumberOfArgsErrReply(lint.cmdName)
	}
	cmdData := lint.GetCmdData()
	keys := make([]string, len(cmdData))
	for i := 0; i < len(cmdData); i++ {
		keys[i] = string(cmdData[i])
	}
	db := ctx.GetDb()
	deleted := db.Removes(keys...)
	if deleted > 0 {
		return protocol.MakeIntReply(int64(deleted))
	}
	return protocol.MakeIntReply(0)
}

// execKeys keys pattern
func execKeys(ctx *CommandContext, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	if lint.GetArgNum() > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	pattern := string(args[0])
	keys := ctx.GetDb().data.Keys()
	var matchedKeys []string
	for _, key := range keys {
		matched, err := path.Match(pattern, key)
		if err != nil {
			return protocol.MakeStandardErrReply("ERR invalid pattern")
		}
		if matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	if len(matchedKeys) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	replies := make([]redis.Reply, len(matchedKeys))
	for i, key := range matchedKeys {
		replies[i] = protocol.MakeBulkReply([]byte(key))
	}

	return protocol.MakeMultiRowReply(replies)
}

// execExists
func execExists(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	keys := make([]string, argNum)
	for i := 0; i < argNum; i++ {
		keys[i] = string(lint.GetCmdData()[i])
	}
	if len(keys) == 0 {
		return protocol.MakeIntReply(0)
	}
	result := ctx.GetDb().Exists(keys)
	return protocol.MakeIntReply(result)
}

// execTTL ttl key
func execTTL(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
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

	// 如果过期了，删除key,并且返回-1
	if expired {
		db.Remove(key)
		return protocol.MakeIntReply(-1)
	}
	// 如果没有过期，计算ttl时间
	expireTime := db.ExpiredAt(key)
	// ttl
	remainingTime := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(remainingTime / time.Second))
}

// execExpire expire key ttl
func execExpire(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
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
	return protocol.MakeIntReply(1)
}

func registerKeyCmd() {
	RegisterCmd("del", execDel)
	RegisterCmd("keys", execKeys)
	RegisterCmd("exists", execExists)
	RegisterCmd("ttl", execTTL)
	RegisterCmd("expire", execExpire)
}
