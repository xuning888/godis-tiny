package database

import (
	"fmt"
	"g-redis/datastruct/dict"
	"g-redis/datastruct/lock"
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/pkg/timewheel"
	"g-redis/redis/protocol"
	"log"
	"strings"
	"time"
)

// cmdLint database 内部流转的结构体，包含了客户端发送的命令名称和数据
type cmdLint struct {
	cmdName   string
	cmdData   [][]byte
	cmdString []string
}

func (lint *cmdLint) GetCmdName() string {
	return lint.cmdName
}

func (lint *cmdLint) GetCmdData() [][]byte {
	return lint.cmdData
}

func (lint *cmdLint) GetArgNum() int {
	return len(lint.cmdData)
}

// parseToLint 将resp 协议的字节流转为为 database 内部流转的结构体
func parseToLint(cmdLine database.CmdLine) *cmdLint {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdData := cmdLine[1:]
	cmdString := make([]string, len(cmdData))
	for i := 0; i < len(cmdData); i++ {
		cmdString[i] = string(cmdData[i])
	}
	return &cmdLint{
		cmdName:   cmdName,
		cmdData:   cmdData,
		cmdString: cmdString,
	}
}

type ExeFunc func(db *DB, cmdLint *cmdLint) redis.Reply

// DB 存储
type DB struct {
	index      int
	data       dict.Dict
	ttlMap     dict.Dict
	versionMap dict.Dict
	locker     *lock.Locks
}

func MakeDB(index int) *DB {
	return &DB{
		index:      index,
		data:       dict.MakeConcurrent(16),
		ttlMap:     dict.MakeConcurrent(16),
		versionMap: dict.MakeConcurrent(16),
		locker:     lock.Make(1),
	}
}

func (db *DB) Exec(c redis.Connection, lint *cmdLint) redis.Reply {
	return db.execNormalCmd(c, lint)
}

func (db *DB) execNormalCmd(c redis.Connection, lint *cmdLint) redis.Reply {
	cmdName := lint.GetCmdName()
	cmd := getCommand(cmdName)
	if cmd == nil {
		return protocol.MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			cmdName, strings.Join(lint.cmdString, ",")))
	}
	if !db.validateArray(cmd.arity, lint) {
		return protocol.MakeNumberOfArgsErrReply(cmdName)
	}
	return cmd.exeFunc(db, lint)
}

func (db *DB) validateArray(arity int, lint *cmdLint) bool {
	args := lint.GetCmdData()
	argNum := len(args)
	if arity >= 0 {
		return argNum >= arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity getData
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	row, exists := db.data.Get(key)
	if !exists {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := row.(*database.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, entry *database.DataEntity) int {
	return db.data.Put(key, entry)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove 删除数据
func (db *DB) Remove(key string) {
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Exists(keys []string) int64 {
	var result int64 = 0
	for _, key := range keys {
		_, ok := db.data.Get(key)
		if ok {
			result++
		}
	}
	return result
}

func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}

/* ---- lock function ----- */

func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---- Data TTL ----- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		log.Println("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

/* --- add version --- */

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}
