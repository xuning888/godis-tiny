package redis

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"github.com/xuning888/godis-tiny/pkg/util"
	"math"
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

func execSet(c context.Context, conn *Client) error {
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

// execSetNx setnx key value
func execSetNx(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	value := cmdData[1]
	db := conn.GetDb()
	redisObj := obj.NewStringObject(value)
	res := db.PutEntity(key, redisObj)
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(int64(res)).WriteTo(conn)
}

// execStrLen strlen key
func execStrLen(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	db := conn.GetDb()
	redisObj, exists := db.GetEntity(key)
	if !exists {
		return MakeIntReply(0).WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	result, _ := obj.StringObjEncoding(redisObj)
	str := string(result)
	return MakeIntReply(int64(len(str))).WriteTo(conn)
}

// execGetSet getset key value
func execGetSet(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	value := cmdData[1]
	db := conn.GetDb()
	redisObj, exists := db.GetEntity(key)
	if !exists {
		return MakeIntReply(0).WriteTo(conn)
	}

	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}

	db.PutEntity(key, obj.NewStringObject(value))
	conn.GetDb().AddAof(conn.GetCmdLine())
	if redisObj.Ptr != nil {
		result, _ := obj.StringObjEncoding(redisObj)
		return MakeBulkReply(result).WriteTo(conn)
	}
	return MakeNullBulkReply().WriteTo(conn)
}

// execIncr incr key
// incr 命令存在对内存的读写操作，此处没有使用锁来保证线程安全, 而是在dbEngin中使用队列来保证命令排队执行
func execIncr(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	db := conn.GetDb()
	redisObj, exists := db.GetEntity(key)
	if !exists {
		redisObj = obj.NewStringEmptyObj()
		redisObj.Ptr = int64(1)
		redisObj.Encoding = obj.EncInt
		db.PutEntity(key, redisObj)
		db.AddAof(conn.GetCmdLine())
		return MakeIntReply(1).WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	if redisObj.Encoding != obj.EncInt {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	value := redisObj.Ptr.(int64)
	if math.MaxInt64-1 < value {
		return MakeStandardErrReply("ERR increment or decrement would overflow").WriteTo(conn)
	}
	value++
	redisObj.Ptr = value
	db.AddAof(conn.GetCmdLine())
	return MakeIntReply(value).WriteTo(conn)
}

// execDecr decr key
func execDecr(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		redisObj = obj.NewStringEmptyObj()
		redisObj.Ptr = int64(-1)
		redisObj.Encoding = obj.EncInt
		conn.GetDb().PutEntity(key, redisObj)
		conn.GetDb().AddAof(conn.GetCmdLine())
		return MakeIntReply(-1).WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	if redisObj.Encoding != obj.EncInt {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	value := redisObj.Ptr.(int64)
	if math.MinInt64+1 > value {
		return MakeStandardErrReply("ERR increment or decrement would overflow").WriteTo(conn)
	}
	value--
	redisObj.Ptr = value
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(value).WriteTo(conn)
}

// execGetRange getrange key start end
func execGetRange(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	db := conn.GetDb()
	redisObj, exists := db.GetEntity(key)
	if !exists {
		return MakeBulkReply([]byte("")).WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	bytes, _ := obj.StringObjEncoding(redisObj)
	value := string(bytes)
	// 计算偏移量
	length := int64(len(value))
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
	}
	// 计算边界
	// 如果 start > end 或者 start 超过了数组的范围, 就返回空字符串
	if start > end || start >= length {
		return MakeBulkReply([]byte("")).WriteTo(conn)
	}
	// end 和 length - 1 求一个最小值作为end
	end = util.MinInt64(end, length-1)
	subValue := value[start:(end + 1)]
	return MakeBulkReply([]byte(subValue)).WriteTo(conn)
}

// execMGet mget key[key...]
func execMGet(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	length := len(cmdData)
	db := conn.GetDb()
	if err := MakeMultiBulkHeaderReply(int64(length)).WriteTo(conn); err != nil {
		return err
	}
	for _, keyBytes := range cmdData {
		key := string(keyBytes)
		redisObj, exists := db.GetEntity(key)
		if !exists {
			if err := MakeNullBulkReply().WriteTo(conn); err != nil {
				return err
			}
		} else {
			if redisObj.ObjType != obj.RedisString {
				if err := MakeNullBulkReply().WriteTo(conn); err != nil {
					return err
				}
			} else {
				bytes, _ := obj.StringObjEncoding(redisObj)
				if err := MakeBulkReply(bytes).WriteTo(conn); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// execMSet
func execMSet(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum == 0 || argNum%2 != 0 {
		return MakeUnknownCommand(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	db := conn.GetDb()
	for i := 1; i < argNum; i += 2 {
		key := string(args[i-1])
		value := args[i]
		redisObj, exists := db.GetEntity(key)
		if exists {
			redisObj.ObjType = obj.RedisString
			obj.StringObjSetValue(redisObj, value)
		} else {
			db.PutEntity(key, obj.NewStringObject(value))
		}
	}
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeOkReply().WriteTo(conn)
}

// execGetDel getdel
func execGetDel(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeNullBulkReply().WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	conn.GetDb().Remove(key)
	conn.GetDb().AddAof(conn.GetCmdLine())
	valueBytes, _ := obj.StringObjEncoding(redisObj)
	return MakeBulkReply(valueBytes).WriteTo(conn)
}

// execIncrBy incrby key increment
func execIncrBy(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	increment, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		redisObj = obj.NewStringEmptyObj()
		redisObj.Ptr = increment
		redisObj.Encoding = obj.EncInt
		conn.GetDb().PutEntity(key, redisObj)
		conn.GetDb().AddAof(conn.GetCmdLine())
		return MakeIntReply(increment).WriteTo(conn)
	}

	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply()
	}

	if redisObj.Encoding != obj.EncInt {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}

	value := redisObj.Ptr.(int64)
	if (increment > 0 && math.MaxInt64-increment < value) ||
		(increment < 0 && math.MinInt64-increment > value) {
		return MakeStandardErrReply(
			"ERR increment or decrement would overflow",
		).WriteTo(conn)
	}
	value += increment
	redisObj.Ptr = value
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(value).WriteTo(conn)
}

func execDecrBy(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	decrement, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	if decrement == math.MinInt64 {
		return MakeStandardErrReply("ERR decrement would overflow").WriteTo(conn)
	}
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		value := 0 - decrement
		redisObj = obj.NewStringEmptyObj()
		redisObj.Encoding = obj.EncInt
		redisObj.Ptr = value
		conn.GetDb().PutEntity(key, redisObj)
		conn.GetDb().AddAof(conn.GetCmdLine())
		return MakeIntReply(value).WriteTo(conn)
	}

	if redisObj.ObjType != obj.RedisString {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	if redisObj.Encoding != obj.EncInt {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	value := redisObj.Ptr.(int64)
	if (decrement > 0 && math.MinInt64+decrement > value) ||
		(decrement < 0 && (math.MaxInt64+decrement < value)) {
		return MakeStandardErrReply(
			"ERR increment or decrement would overflow",
		).WriteTo(conn)
	}
	value -= decrement
	redisObj.Ptr = value
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(value).WriteTo(conn)
}

func init() {
	register("set", execSet)
	register("get", execGet)
	register("setnx", execSetNx)
	register("strlen", execStrLen)
	register("incr", execIncr)
	register("decr", execDecr)
	register("getset", execGetSet)
	register("getrange", execGetRange)
	register("mget", execMGet)
	register("mset", execMSet)
	register("getdel", execGetDel)
	register("incrby", execIncrBy)
	register("decrby", execDecrBy)
}
