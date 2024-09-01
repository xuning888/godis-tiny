package redis

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/util"
	"math"
	"path"
	"strconv"
	"time"
)

func execDel(ctx context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 {
		// 错误参数
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	var deleted = 0
	db := conn.GetDb()
	for i := 0; i < len(cmdData); i++ {
		result := db.Remove(string(cmdData[i]))
		deleted += result
	}
	if deleted > 0 {
		conn.GetDb().AddAof(conn.GetCmdLine())
		return MakeIntReply(int64(deleted)).WriteTo(conn)
	}
	return MakeIntReply(0).WriteTo(conn)
}

func execKeys(ctx context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	pattern := string(args[0])
	_, err := path.Match(pattern, "")
	if err != nil {
		return MakeStandardErrReply("ERR invalid pattern").WriteTo(conn)
	}
	keys := conn.GetDb().Keys()
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
		return MakeEmptyMultiBulkReply().WriteTo(conn)
	}
	return MakeMultiBulkReply(matchedKeys).WriteTo(conn)
}

func execExists(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	keys := make([]string, argNum)
	args := conn.GetArgs()
	for i := 0; i < argNum; i++ {
		keys[i] = string(args[i])
	}
	result := conn.GetDb().Exists(keys)
	return MakeIntReply(result).WriteTo(conn)
}

// execTTL ttl key
func execTTL(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	db := conn.GetDb()
	// key 不存在返回-2
	_, ok := db.GetEntity(key)
	if !ok {
		return MakeIntReply(-2).WriteTo(conn)
	}
	// 没有设置过期时间返回-1
	expired, exists := db.IsExpiredV1(key)
	if !exists {
		return MakeIntReply(-1).WriteTo(conn)
	}

	// 如果过期了，删除key,并且返回-2
	if expired {
		db.Remove(key)
		return MakeIntReply(-2).WriteTo(conn)
	}
	// 如果没有过期，计算ttl时间
	expireTime := db.ExpiredAt(key)
	// ttl
	remainingTime := expireTime.Sub(time.Now())
	seconds := remainingTime.Seconds()
	return MakeIntReply(int64(math.Round(seconds))).WriteTo(conn)
}

func execPTTL(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	db := conn.GetDb()
	// key 不存在返回-2
	_, ok := db.GetEntity(key)
	if !ok {
		return MakeIntReply(-2).WriteTo(conn)
	}
	// 没有设置过期时间返回-1
	expired, exists := db.IsExpiredV1(key)
	if !exists {
		return MakeIntReply(-1).WriteTo(conn)
	}

	// 如果过期了，删除key,并且返回-2
	if expired {
		db.Remove(key)
		return MakeIntReply(-2).WriteTo(conn)
	}

	expireTime := db.ExpiredAt(key)
	remainingTime := expireTime.Sub(time.Now())
	microseconds := remainingTime.Milliseconds()
	return MakeIntReply(int64(math.Round(float64(microseconds)))).WriteTo(conn)
}

// execExpire expire key ttl
func execExpire(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	ttl, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	_, ok := conn.GetDb().GetEntity(key)
	if !ok {
		// key 不存在返回0
		return MakeIntReply(0).WriteTo(conn)
	}
	expireTime := time.Now().Add(time.Duration(ttl) * time.Second)
	conn.GetDb().ExpireV1(key, expireTime)
	conn.GetDb().AddAof(util.MakeExpireCmd(key, expireTime))
	return MakeIntReply(1).WriteTo(conn)
}

// execPersist persist key 移除key的过期时间
func execPersist(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	expired, exists := conn.GetDb().IsExpiredV1(key)
	// key 不存在ttl
	if !exists {
		return MakeIntReply(0).WriteTo(conn)
	}
	// key 已经过期
	if expired {
		return MakeIntReply(0).WriteTo(conn)
	}
	// key 存在，并且没有过期，就移除他的ttl
	conn.GetDb().RemoveTTLV1(key)
	// add aof
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(1).WriteTo(conn)
}

// execExpireAt expireat key unix-time-seconds
func execExpireAt(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	timestamp, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	_, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeIntReply(0).WriteTo(conn)
	}
	expireTime := time.Unix(timestamp, 0)
	conn.GetDb().ExpireV1(key, expireTime)
	// add aof
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(1).WriteTo(conn)
}

func init() {
	register("del", execDel)
	register("keys", execKeys)
	register("exists", execExists)
	register("ttl", execTTL)
	register("pttl", execPTTL)
	register("expire", execExpire)
	register("persist", execPersist)
	register("expireat", execExpireAt)
}
