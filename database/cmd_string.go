package database

import (
	"context"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/protocol"
	"math"
	"strconv"
	"strings"
	"time"
)

func (db *DB) getAsString(key string) ([]byte, redis.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	// 类型转换
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, protocol.MakeWrongTypeErrReply()
	}
	return bytes, nil
}

func execGet(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	args := ctx.GetArgs()
	key := string(args[0])
	db := ctx.GetDb()
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(bytes)
}

const (
	addOrUpdatePolicy = iota // default
	addPolicy                // set nx
	updatePolicy             // set ex
)

// 过期时间
const unlimitedTTL int64 = 0

// execSet set key value [NX|XX] [EX seconds | PS milliseconds]
func execSet(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 4 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	args := ctx.GetArgs()
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
					return protocol.MakeSyntaxReply()
				}
				policy = addPolicy
			} else if "XX" == upper {
				// set key value xx 仅当key存在时插入
				if policy == addPolicy {
					return protocol.MakeSyntaxReply()
				}
				policy = updatePolicy
			} else if "EX" == upper {
				// 秒级过期时间
				if ttl != unlimitedTTL {
					return protocol.MakeSyntaxReply()
				}
				if i+1 > len(args) {
					return protocol.MakeSyntaxReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeSyntaxReply()
				}
				if ttlArg <= 0 {
					return protocol.MakeStandardErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg * 1000
				i++
			} else if "PX" == upper {
				// 毫秒级过期时间
				if ttl != unlimitedTTL {
					return protocol.MakeSyntaxReply()
				}
				if i+1 > len(args) {
					return protocol.MakeSyntaxReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeSyntaxReply()
				}
				if ttlArg <= 0 {
					return protocol.MakeStandardErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++
			} else {
				return protocol.MakeSyntaxReply()
			}
		}
	}
	entity := &database.DataEntity{
		Type: database.String,
		Data: value,
	}
	var result int
	db := ctx.GetDb()
	switch policy {
	case addOrUpdatePolicy:
		db.PutEntity(key, entity)
		result = 1
	case addPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}
	if result > 0 {
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.ExpireV1(key, expireTime)
			db.addAof(ctx.GetCmdLine())
			// convert to expireat
			expireAtCmd := util.MakeExpireCmd(key, expireTime)
			db.addAof(expireAtCmd)
		} else {
			db.RemoveTTLV1(key)
			db.addAof(ctx.GetCmdLine())
		}
		return protocol.MakeOkReply()
	}
	return protocol.MakeNullBulkReply()
}

// execSetNx setnx key value
func execSetNx(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	value := cmdData[1]
	db := ctx.GetDb()
	res := db.PutIfAbsent(key, &database.DataEntity{
		Type: database.String,
		Data: value,
	})
	ctx.GetDb().addAof(ctx.GetCmdLine())
	return protocol.MakeIntReply(int64(res))
}

// execStrLen strlen key
func execStrLen(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	value, reply := db.getAsString(key)
	if reply != nil {
		return reply
	} else if value == nil {
		return protocol.MakeIntReply(0)
	} else {
		str := string(value)
		strLen := len(str)
		return protocol.MakeIntReply(int64(strLen))
	}
}

// execGetSet getset key value
func execGetSet(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	value := cmdData[1]
	db := ctx.GetDb()
	oldValue, reply := db.getAsString(key)
	if reply != nil {
		// TODO error log
		return reply
	}
	db.PutEntity(key, &database.DataEntity{Data: value, Type: database.String})
	ctx.GetDb().addAof(ctx.GetCmdLine())
	if oldValue != nil {
		return protocol.MakeBulkReply(oldValue)
	}
	return protocol.MakeNullBulkReply()
}

// execIncr incr key
// incr 命令存在对内存的读写操作，此处没有使用锁来保证线程安全, 而是在dbEngin中使用队列来保证命令排队执行
func execIncr(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	valueBytes, reply := db.getAsString(key)
	// 如果key不存在，就放一个1进去
	if valueBytes == nil && reply == nil {
		db.PutEntity(key, &database.DataEntity{
			Type: database.String,
			Data: []byte("1"),
		})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(1)
	} else if reply != nil {
		return reply
	} else {
		value, err := strconv.ParseInt(string(valueBytes), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
		if math.MaxInt64-1 < value {
			return protocol.MakeStandardErrReply("ERR increment or decrement would overflow")
		}
		value++
		valueStr := strconv.FormatInt(value, 10)
		db.PutEntity(key, &database.DataEntity{
			Type: database.String,
			Data: []byte(valueStr),
		})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(value)
	}
}

// execDecr decr key
func execDecr(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	valueBytes, reply := ctx.GetDb().getAsString(key)
	if reply != nil {
		return reply
	} else if valueBytes == nil {
		ctx.GetDb().PutEntity(key, &database.DataEntity{Data: []byte("-1"), Type: database.String})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(-1)
	} else {
		value, err := strconv.ParseInt(string(valueBytes), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
		if math.MinInt64+1 > value {
			return protocol.MakeStandardErrReply("ERR increment or decrement would overflow")
		}
		value--
		valueStr := strconv.FormatInt(value, 10)
		ctx.GetDb().PutEntity(key, &database.DataEntity{Data: []byte(valueStr), Type: database.String})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(value)
	}
}

// execGetRange getrange key start end
func execGetRange(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	db := ctx.GetDb()
	bytes, reply := db.getAsString(key)
	if reply != nil {
		return reply
	}
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
		return protocol.MakeBulkReply([]byte(""))
	}
	// end 和 length - 1 求一个最小值作为end
	end = util.MinInt64(end, length-1)
	subValue := value[start:(end + 1)]
	return protocol.MakeBulkReply([]byte(subValue))
}

// execMGet mget key[key...]
func execMGet(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	length := len(cmdData)
	result := make([]redis.Reply, 0, length)
	db := ctx.GetDb()
	for _, keyBytes := range cmdData {
		key := string(keyBytes)
		value, _ := db.getAsString(key)
		if value != nil {
			result = append(result, protocol.MakeBulkReply(value))
		} else {
			result = append(result, protocol.MakeNullBulkReply())
		}
	}
	return protocol.MakeMultiRowReply(result)
}

// execGetDel getdel
// todo if the key dose not exist of if the key's value type is not a string
func execGetDel(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	valueBytes, reply := ctx.GetDb().getAsString(key)
	if reply != nil {
		return reply
	} else if valueBytes == nil {
		return protocol.MakeNullBulkReply()
	} else {
		ctx.GetDb().Remove(key)
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeBulkReply(valueBytes)
	}
}

// execIncrBy incrby key increment
func execIncrBy(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	increment, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	valueBytes, reply := ctx.GetDb().getAsString(key)
	if reply != nil {
		return reply
	} else if valueBytes == nil {
		ctx.GetDb().PutEntity(key, &database.DataEntity{
			Type: database.String,
			Data: []byte(strconv.FormatInt(increment, 10)),
		})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(increment)
	} else {
		value, err := strconv.ParseInt(string(valueBytes), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
		if math.MaxInt64-increment < value {
			return protocol.MakeStandardErrReply("ERR increment or decrement would overflow")
		}
		value += increment
		ctx.GetDb().PutEntity(key, &database.DataEntity{
			Type: database.String,
			Data: []byte(strconv.FormatInt(value, 10)),
		})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(value)
	}
}

func execDecrBy(c context.Context, ctx *CommandContext) redis.Reply {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(ctx.GetCmdName())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	decrement, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	valueBytes, reply := ctx.GetDb().getAsString(key)
	if reply != nil {
		return reply
	} else if valueBytes == nil {
		ctx.GetDb().PutEntity(key, &database.DataEntity{
			Type: database.String,
			Data: []byte(strconv.FormatInt(decrement, 10)),
		})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(decrement)
	} else {
		value, err := strconv.ParseInt(string(valueBytes), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
		if math.MinInt64+decrement > value {
			return protocol.MakeStandardErrReply("ERR increment or decrement would overflow")
		}
		value -= decrement
		ctx.GetDb().PutEntity(key, &database.DataEntity{
			Type: database.String,
			Data: []byte(strconv.FormatInt(value, 10)),
		})
		ctx.GetDb().addAof(ctx.GetCmdLine())
		return protocol.MakeIntReply(value)
	}
}

func registerStringCmd() {
	cmdManager.registerCmd("set", execSet, writeOnly)
	cmdManager.registerCmd("get", execGet, readOnly)
	cmdManager.registerCmd("getset", execGetSet, readWrite)
	cmdManager.registerCmd("incr", execIncr, readWrite)
	cmdManager.registerCmd("decr", execDecr, readWrite)
	cmdManager.registerCmd("setnx", execSetNx, readWrite)
	cmdManager.registerCmd("getrange", execGetRange, readOnly)
	cmdManager.registerCmd("mget", execMGet, readOnly)
	cmdManager.registerCmd("strlen", execStrLen, readOnly)
	cmdManager.registerCmd("getdel", execGetDel, readWrite)
	cmdManager.registerCmd("incrby", execIncrBy, readWrite)
	cmdManager.registerCmd("decrby", execDecrBy, readWrite)
}
