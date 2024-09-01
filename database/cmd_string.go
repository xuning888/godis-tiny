package database

import (
	"context"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis"
	"math"
	"strconv"
	"strings"
	"time"
)

func (db *DB) getAsString(key string) (bytes []byte, errReply redis.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return
	}
	// 类型校验
	if entity.Type == String {
		// 类型转换
		bytes, ok = entity.Data.([]byte)
		if !ok {
			errReply = redis.MakeWrongTypeErrReply()
		}
		return
	}
	errReply = redis.MakeWrongTypeErrReply()
	return
}

func execGet(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	args := ctx.GetArgs()
	key := string(args[0])
	db := ctx.GetDb()
	bytes, reply := db.getAsString(key)
	if reply != nil {
		_, err2 := ctx.conn.Write(reply.ToBytes())
		if err2 == nil {
			return ctx.conn.Flush()
		}
		return nil
	}
	if bytes == nil {
		return redis.MakeNullBulkReply().WriteTo(ctx.conn)
	}
	return redis.MakeBulkReply(bytes).WriteTo(ctx.conn)
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

// execSet set key value [NX|XX] [EX seconds | PS milliseconds | keepttl]
func execSet(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
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
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				policy = addPolicy
			} else if "XX" == upper {
				// set key value xx 仅当key存在时插入
				if policy == addPolicy {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				policy = updatePolicy
			} else if "EX" == upper {
				// 秒级过期时间
				if ttl != unlimitedTTL {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				if i+1 >= len(args) {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				if ttlArg <= 0 {
					return redis.MakeStandardErrReply("ERR invalid expire time in set").WriteTo(ctx.conn)
				}
				ttl = ttlArg * 1000
				i++
			} else if "PX" == upper {
				// 毫秒级过期时间
				if ttl != unlimitedTTL {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				if i+1 >= len(args) {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				if ttlArg <= 0 {
					return redis.MakeStandardErrReply("ERR invalid expire time in set").WriteTo(ctx.conn)
				}
				ttl = ttlArg
				i++
			} else if "KEEPTTL" == upper {
				if ttl != unlimitedTTL {
					return redis.MakeSyntaxReply().WriteTo(ctx.conn)
				}
				ttl = keepTTL
			} else {
				return redis.MakeSyntaxReply().WriteTo(ctx.conn)
			}
		}
	}
	db := ctx.GetDb()
	entity, exists := db.GetEntity(key)
	if exists {
		entity.Type = String
		entity.Data = value
	} else {
		entity = &DataEntity{
			Type: String,
			Data: value,
		}
	}
	var result int
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
			if ttl == keepTTL {
				db.AddAof(ctx.GetCmdLine())
			} else {
				expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
				db.ExpireV1(key, expireTime)
				db.AddAof(ctx.GetCmdLine())
				// convert to expireat
				expireAtCmd := util.MakeExpireCmd(key, expireTime)
				db.AddAof(expireAtCmd)
			}
		} else {
			db.RemoveTTLV1(key)
			db.AddAof(ctx.GetCmdLine())
		}
		return redis.MakeOkReply().WriteTo(ctx.conn)
	}
	return redis.MakeNullBulkReply().WriteTo(ctx.conn)
}

// execSetNx setnx key value
func execSetNx(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	value := cmdData[1]
	db := ctx.GetDb()
	res := db.PutIfAbsent(key, &DataEntity{
		Type: String,
		Data: value,
	})
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(int64(res)).WriteTo(ctx.conn)
}

// execStrLen strlen key
func execStrLen(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	value, reply := db.getAsString(key)
	if reply != nil {
		return reply.WriteTo(ctx.conn)
	} else if value == nil {
		return redis.MakeIntReply(0).WriteTo(ctx.conn)
	} else {
		str := string(value)
		strLen := len(str)
		return redis.MakeIntReply(int64(strLen)).WriteTo(ctx.conn)
	}
}

// execGetSet getset key value
func execGetSet(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	value := cmdData[1]
	db := ctx.GetDb()
	oldValue, reply := db.getAsString(key)
	if reply != nil {
		return reply.WriteTo(ctx.conn)
	}
	db.PutEntity(key, &DataEntity{Data: value, Type: String})
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	if oldValue != nil {
		return redis.MakeBulkReply(oldValue).WriteTo(ctx.conn)
	}
	return redis.MakeNullBulkReply().WriteTo(ctx.conn)
}

// execIncr incr key
// incr 命令存在对内存的读写操作，此处没有使用锁来保证线程安全, 而是在dbEngin中使用队列来保证命令排队执行
func execIncr(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	db := ctx.GetDb()
	entity, exists := db.GetEntity(key)
	if !exists {
		db.PutEntity(key, &DataEntity{
			Type: String,
			Data: []byte("1"),
		})
		db.AddAof(ctx.GetCmdLine())
		return redis.MakeIntReply(1).WriteTo(ctx.conn)
	}
	valueBytes, ok := entity.Data.([]byte)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.conn)
	}
	value, err := strconv.ParseInt(string(valueBytes), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	if math.MaxInt64-1 < value {
		return redis.MakeStandardErrReply("ERR increment or decrement would overflow").WriteTo(ctx.conn)
	}
	value++
	valueStr := strconv.FormatInt(value, 10)
	entity.Data = []byte(valueStr)
	db.AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(value).WriteTo(ctx.conn)
}

// execDecr decr key
func execDecr(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		ctx.GetDb().PutEntity(key, &DataEntity{Data: []byte("-1"), Type: String})
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return redis.MakeIntReply(-1).WriteTo(ctx.conn)
	}
	if entity.Type != String {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.conn)
	}
	valueBytes, ok := entity.Data.([]byte)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.conn)
	}
	value, err := strconv.ParseInt(string(valueBytes), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	if math.MinInt64+1 > value {
		return redis.MakeStandardErrReply("ERR increment or decrement would overflow").WriteTo(ctx.conn)
	}
	value--
	valueStr := strconv.FormatInt(value, 10)
	entity.Data = []byte(valueStr)
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(value).WriteTo(ctx.conn)
}

// execGetRange getrange key start end
func execGetRange(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	db := ctx.GetDb()
	bytes, reply := db.getAsString(key)
	if reply != nil {
		return reply.WriteTo(ctx.conn)
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
		return redis.MakeBulkReply([]byte("")).WriteTo(ctx.conn)
	}
	// end 和 length - 1 求一个最小值作为end
	end = util.MinInt64(end, length-1)
	subValue := value[start:(end + 1)]
	return redis.MakeBulkReply([]byte(subValue)).WriteTo(ctx.conn)
}

// execMGet mget key[key...]
func execMGet(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	length := len(cmdData)
	result := make([]redis.Reply, 0, length)
	db := ctx.GetDb()
	for _, keyBytes := range cmdData {
		key := string(keyBytes)
		value, _ := db.getAsString(key)
		if value != nil {
			result = append(result, redis.MakeBulkReply(value))
		} else {
			result = append(result, redis.MakeNullBulkReply())
		}
	}
	return redis.MakeMultiRowReply(result).WriteTo(ctx.conn)
}

// execMSet
func execMSet(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum == 0 || argNum%2 != 0 {
		return redis.MakeUnknownCommand(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	args := ctx.GetArgs()
	for i := 1; i < argNum; i += 2 {
		key := string(args[i-1])
		value := args[i]
		entity, exists := ctx.GetDb().GetEntity(key)
		if exists {
			entity.Type = String
			entity.Data = value
		} else {
			ctx.GetDb().PutEntity(key, &DataEntity{
				Type: String,
				Data: value,
			})
		}
	}
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeOkReply().WriteTo(ctx.conn)
}

// execGetDel getdel
func execGetDel(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeNullBulkReply().WriteTo(ctx.conn)
	}
	if entity.Type != String {
		return redis.MakeNullBulkReply().WriteTo(ctx.conn)
	}
	ctx.GetDb().Remove(key)
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	valueBytes, ok := entity.Data.([]byte)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.conn)
	}
	return redis.MakeBulkReply(valueBytes).WriteTo(ctx.conn)
}

// execIncrBy incrby key increment
func execIncrBy(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	increment, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		ctx.GetDb().PutEntity(key, &DataEntity{
			Type: String,
			Data: []byte(strconv.FormatInt(increment, 10)),
		})
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return redis.MakeIntReply(increment).WriteTo(ctx.conn)
	}

	if entity.Type != String {
		return redis.MakeWrongTypeErrReply()
	}

	valueBytes, ok := entity.Data.([]byte)
	if !ok {
		return redis.MakeWrongTypeErrReply()
	}
	value, err := strconv.ParseInt(string(valueBytes), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	if (increment > 0 && math.MaxInt64-increment < value) ||
		(increment < 0 && math.MinInt64-increment > value) {
		return redis.MakeStandardErrReply(
			"ERR increment or decrement would overflow",
		).WriteTo(ctx.conn)
	}
	value += increment
	entity.Data = []byte(strconv.FormatInt(value, 10))
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(value).WriteTo(ctx.conn)
}

func execDecrBy(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	decrement, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	if decrement == math.MinInt64 {
		return redis.MakeStandardErrReply("ERR decrement would overflow").WriteTo(ctx.conn)
	}
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		value := 0 - decrement
		ctx.GetDb().PutEntity(key, &DataEntity{
			Type: String,
			Data: []byte(strconv.FormatInt(value, 10)),
		})
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return redis.MakeIntReply(value).WriteTo(ctx.conn)
	}

	if entity.Type != String {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.conn)
	}

	valueBytes, ok := entity.Data.([]byte)
	if !ok {
		return redis.MakeWrongTypeErrReply()
	}

	value, err := strconv.ParseInt(string(valueBytes), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	if (decrement > 0 && math.MinInt64+decrement > value) ||
		(decrement < 0 && (math.MaxInt64+decrement < value)) {
		return redis.MakeStandardErrReply(
			"ERR increment or decrement would overflow",
		).WriteTo(ctx.conn)
	}
	value -= decrement
	entity.Data = []byte(strconv.FormatInt(value, 10))
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(value).WriteTo(ctx.conn)
}

func registerStringCmd() {
	CmdManager.registerCmd("set", execSet, writeOnly)
	CmdManager.registerCmd("get", execGet, readOnly)
	CmdManager.registerCmd("getset", execGetSet, readWrite)
	CmdManager.registerCmd("incr", execIncr, readWrite)
	CmdManager.registerCmd("decr", execDecr, readWrite)
	CmdManager.registerCmd("setnx", execSetNx, readWrite)
	CmdManager.registerCmd("getrange", execGetRange, readOnly)
	CmdManager.registerCmd("mget", execMGet, readOnly)
	CmdManager.registerCmd("strlen", execStrLen, readOnly)
	CmdManager.registerCmd("getdel", execGetDel, readWrite)
	CmdManager.registerCmd("incrby", execIncrBy, readWrite)
	CmdManager.registerCmd("decrby", execDecrBy, readWrite)
	CmdManager.registerCmd("mset", execMSet, writeOnly)
}
