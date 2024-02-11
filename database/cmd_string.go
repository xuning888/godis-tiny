package database

import (
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/pkg/util"
	"g-redis/redis/protocol"
	"strconv"
	"strings"
)

func (db *DB) getAsString(key string) ([]byte, protocol.ErrReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	// 类型转换
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

func execGet(db *DB, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	key := string(args[0])
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
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

func execSet(db *DB, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	key := string(args[0])
	value := args[1]
	// 默认是 update 和 insert, 如果 key 已经存在，就覆盖它，如果不在就创建它
	policy := upsertPolicy
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			upper := strings.ToUpper(string(args[i]))
			if "NX" == upper {
				// set key value nx 仅当key不存在时插入
				if policy == updatePolicy {
					return protocol.MakeSyntaxReply()
				}
				policy = insertPolicy
			} else if "XX" == upper {
				// set key value xx 仅当key存在时插入
				if policy == insertPolicy {
					return protocol.MakeSyntaxReply()
				}
				policy = updatePolicy
			}
			// TODO 过期时间 EX, PX EXAT, PXAT
		}
	}
	entity := &database.DataEntity{
		Data: value,
	}
	var result int
	switch policy {
	case upsertPolicy:
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}
	if result > 0 {
		return protocol.MakeOkReply()
	}
	return protocol.MakeNullBulkReply()
}

// execSetNx setnx key value
func execSetNx(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	value := cmdData[1]
	res := db.PutIfAbsent(key, &database.DataEntity{
		Data: value,
	})
	return protocol.MakeIntReply(int64(res))
}

// execGetSet getset key value
func execGetSet(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	value := cmdData[1]
	oldValue, reply := db.getAsString(key)
	if reply != nil {
		// TODO error log
		return reply
	}
	db.PutEntity(key, &database.DataEntity{Data: value})
	if oldValue != nil {
		return protocol.MakeBulkReply(oldValue)
	}
	return protocol.MakeNullBulkReply()
}

// execIncr incr key
// incr 命令存在对内存的读写操作，此处没有使用锁来保证线程安全, 而是在dbEngin中使用队列来保证命令排队执行
func execIncr(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	valueBytes, reply := db.getAsString(key)
	// 如果key不存在，就放一个1进去
	if valueBytes == nil && reply == nil {
		db.PutEntity(key, &database.DataEntity{
			Data: []byte("1"),
		})
		return protocol.MakeIntReply(1)
	} else if reply != nil {
		return reply
	} else {
		value, err := strconv.ParseInt(string(valueBytes), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
		value++
		valueStr := strconv.FormatInt(value, 10)
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(valueStr),
		})
		return protocol.MakeIntReply(value)
	}
}

// execGetRange getrange key start end
func execGetRange(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 3 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
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
func execMGet(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	length := len(cmdData)
	result := make([]redis.Reply, 0, length)
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

func init() {
	RegisterCmd("set", execSet, -2)
	RegisterCmd("get", execGet, 1)
	RegisterCmd("getset", execGetSet, -2)
	RegisterCmd("incr", execIncr, 1)
	RegisterCmd("setnx", execSetNx, -2)
	RegisterCmd("getrange", execGetRange, -3)
	RegisterCmd("mget", execMGet, -1)
}
