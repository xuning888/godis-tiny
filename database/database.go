package database

import (
	"fmt"
	"g-redis/datastruct/dict"
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"strings"
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
		cmdString[i] = "'" + string(cmdData[i]) + "'"
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
	index int
	data  dict.Dict
}

func MakeDB(index int) *DB {
	return &DB{
		index: index,
		data:  dict.MakeConcurrent(16),
	}
}

func MakeSimpleDb(index int) *DB {
	return &DB{
		index: index,
		data:  dict.MakeSimpleDict(),
	}
}

func MakeSimpleSync(index int) *DB {
	return &DB{
		index: index,
		data:  dict.MakeSimpleSync(),
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
			cmdName, strings.Join(lint.cmdString, ", ")))
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

// Exists 返回一组key是否存在
// eg: k1 -> v1, k2 -> v2。 input: k1 k2 return 2
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
}
