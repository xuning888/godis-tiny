package database

import (
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"strconv"
	"strings"
)

func (db *DB) getAsString(key string) ([]byte, protocol.ErrReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
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
		return reply
	}
	db.PutEntity(key, &database.DataEntity{Data: value})
	if oldValue != nil {
		return protocol.MakeBulkReply(oldValue)
	}
	return protocol.MakeNullBulkReply()
}

// execIncr incr key
// TODO 存在并发问题，待修复
func execIncr(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	valueBytes, reply := db.getAsString(key)
	if valueBytes == nil && reply == nil {
		db.PutEntity(key, &database.DataEntity{
			Data: []byte("1"),
		})
		return protocol.MakeIntReply(1)
	} else if reply != nil {
		return reply
	} else {
		value, err := strconv.Atoi(string(valueBytes))
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
		value++
		valueStr := strconv.Itoa(value)
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(valueStr),
		})
		return protocol.MakeIntReply(int64(value))
	}
}

func init() {
	RegisterCmd("set", execSet, -2)
	RegisterCmd("get", execGet, 1)
	RegisterCmd("getset", execGetSet, -2)
	RegisterCmd("incr", execIncr, 1)
}
