package database

import (
	"fmt"
	"github.com/bytedance/gopkg/util/logger"
	"godis-tiny/datastruct/dict"
	"godis-tiny/datastruct/ttl"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"math/rand"
	"strings"
	"time"
)

// cmdLint database 内部流转的结构体，包含了客户端发送的命令名称和数据
type cmdLint struct {
	cmdName   string
	cmdData   [][]byte
	cmdString []string
}

type CommandContext struct {
	db   *DB
	conn redis.Conn
}

func (c *CommandContext) GetDb() *DB {
	return c.db
}

func (c *CommandContext) GetConn() redis.Conn {
	return c.conn
}

func MakeCommandContext(db *DB, conn redis.Conn) *CommandContext {
	return &CommandContext{
		db:   db,
		conn: conn,
	}
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

type ExeFunc func(cmdCtx *CommandContext, cmdLint *cmdLint) redis.Reply

// DB 存储数据的DB
type DB struct {
	index        int
	indexChecker database.IndexChecker
	ttlChecker   database.TTLChecker
	data         dict.Dict
	ttlCache     ttl.Cache
}

// MakeSimpleDb 使用map的实现，无锁结构
func MakeSimpleDb(index int, indexChecker database.IndexChecker, ttlChecker database.TTLChecker) *DB {
	return &DB{
		index:        index,
		indexChecker: indexChecker,
		ttlChecker:   ttlChecker,
		data:         dict.MakeSimpleDict(),
		ttlCache:     ttl.MakeSimple(),
	}
}

// MakeSimpleSync 使用sync.Map的实现
func MakeSimpleSync(index int, checker database.IndexChecker, ttlChecker database.TTLChecker) *DB {
	return &DB{
		index:        index,
		indexChecker: checker,
		ttlChecker:   ttlChecker,
		data:         dict.MakeSimpleSync(),
		ttlCache:     ttl.MakeSimple(),
	}
}

func (db *DB) Exec(c redis.Conn, lint *cmdLint) redis.Reply {
	cmdName := lint.GetCmdName()
	cmd := getCommand(cmdName)
	if cmd == nil {
		return protocol.MakeStandardErrReply(fmt.Sprintf("ERR unknown command `%s`, with args beginning with: %s",
			cmdName, strings.Join(lint.cmdString, ", ")))
	}
	ctx := MakeCommandContext(db, c)
	return cmd.exeFunc(ctx, lint)
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
	db.ttlCache.Remove(key)
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
	length := db.data.Len()
	if length > 0 {
		db.data.Clear()
	}
}

/* ---- Data TTL ----- */

// ExpireV1 为key设置过期时间
func (db *DB) ExpireV1(key string, expireTime time.Time) {
	db.ttlCache.Expire(key, expireTime)
}

// IsExpiredV1 返回指定key是否过期，如果key 不存在返回 false
func (db *DB) IsExpiredV1(key string) (expired bool, exists bool) {
	return db.ttlCache.IsExpired(key)
}

// RemoveTTLV1 删除指定 key 的 ttl
func (db *DB) RemoveTTLV1(key string) {
	db.ttlCache.Remove(key)
}

func (db *DB) ExpiredAt(key string) time.Time {
	return db.ttlCache.ExpireAt(key)
}

// RandomCheckTTLAndClear 随机检查一组key的过期时间，如果key已经过期了，那么清理key
func (db *DB) RandomCheckTTLAndClear() {
	if db.data.Len() == 0 {
		return
	}
	randLimit := rand.Intn(db.data.Len() + 1)
	keys := db.data.RandomKeys(randLimit)
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		expired, exists := db.ttlCache.IsExpired(key)
		if !exists {
			logger.Debugf("ttl check, db%d key: %s, 没有设置过期时间", db.index, key)
			continue
		}
		if expired {
			logger.Debugf("ttl check, db%d key: %s, 过期了", db.index, key)
			db.Remove(key)
		}
	}
}

// RandomCheckTTLAndClearV1 随机检查一组key的过期时间，如果key已经过期了，那么清理key。
// ttlCache按照key的过期时间组织了一个小根堆, Peek方法可以查看堆顶元素。随机检查几个堆定元元素,直到遇到没有过期的key
// 优点: 清理的更加及时 缺点: 使用了Peek方法，暴露了底层的实现细节是PQ
func (db *DB) RandomCheckTTLAndClearV1() {
	if db.data.Len() == 0 {
		return
	}
	randLimit := rand.Intn(db.data.Len() + 1)
	for i := 0; i < randLimit; i++ {
		item := db.ttlCache.Peek()
		if item == nil {
			break
		}
		expired, _ := db.ttlCache.IsExpired(item.Key)
		if expired {
			logger.Debugf("ttl check, db%d key: %s, 过期了", db.index, item.Key)
			db.Remove(item.Key)
		} else {
			break
		}
	}
}
