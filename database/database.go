package database

import (
	"context"
	"godis-tiny/datastruct/dict"
	"godis-tiny/datastruct/ttl"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/logger"
	"godis-tiny/redis/protocol"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type CmdLine = [][]byte

// CtxPool 减少重复的内存分配，降低GC压力
var CtxPool = &sync.Pool{
	New: func() interface{} {
		return &CommandContext{}
	},
}

type CommandContext struct {
	db      *DB
	conn    redis.Conn
	cmdName string
	cmdLine database.CmdLine
	args    [][]byte
}

func (c *CommandContext) Reset() {
	c.db = nil
	c.conn = nil
	c.cmdName = ""
	c.cmdLine = nil
	c.args = nil
}

func (c *CommandContext) GetCmdName() string {
	if c.cmdName == "" {
		if len(c.cmdLine) > 0 {
			c.cmdName = strings.ToLower(string(c.cmdLine[0]))
		}
	}
	return c.cmdName
}

func (c *CommandContext) GetArgs() [][]byte {
	if c.args == nil {
		c.args = c.cmdLine[1:]
	}
	return c.args
}

func (c *CommandContext) GetArgNum() int {
	if c.args == nil {
		c.args = c.cmdLine[1:]
	}
	return len(c.args)
}

func (c *CommandContext) GetCmdLine() [][]byte {
	return c.cmdLine
}

func (c *CommandContext) GetDb() *DB {
	return c.db
}

func (c *CommandContext) GetConn() redis.Conn {
	return c.conn
}

type ExeFunc func(c context.Context, ctx *CommandContext) redis.Reply

func nothingTodo(line database.CmdLine) {
}

// DB 存储数据的DB
type DB struct {
	index         int
	engineCommand database.EngineCommand
	ttlChecker    database.TTLChecker
	data          dict.Dict
	ttlCache      ttl.Cache
	addAof        func(line database.CmdLine)
}

// MakeSimpleDb 使用map的实现，无锁结构
func MakeSimpleDb(index int, indexChecker database.EngineCommand, ttlChecker database.TTLChecker) *DB {
	return &DB{
		index:         index,
		engineCommand: indexChecker,
		ttlChecker:    ttlChecker,
		data:          dict.MakeSimpleDict(),
		ttlCache:      ttl.MakeSimple(),
		addAof:        nothingTodo,
	}
}

// MakeSimpleSync 使用sync.Map的实现
func MakeSimpleSync(index int, checker database.EngineCommand, ttlChecker database.TTLChecker) *DB {
	return &DB{
		index:         index,
		engineCommand: checker,
		ttlChecker:    ttlChecker,
		data:          dict.MakeSimpleSync(),
		ttlCache:      ttl.MakeSimple(),
	}
}

func (db *DB) Exec(c context.Context, ctx *CommandContext) redis.Reply {
	cmdName := ctx.GetCmdName()
	cmd := cmdManager.getCmd(cmdName)
	if cmd == nil {
		args := ctx.GetArgs()
		with := make([]string, 0, len(args))
		for _, arg := range args {
			with = append(with, "'"+string(arg)+"'")
		}
		return protocol.MakeUnknownCommand(cmdName, with...)
	}
	return cmd.exeFunc(c, ctx)
}

/* ---- Data Access ----- */

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*database.DataEntity)
		var expiration *time.Time = nil
		expired, exists := db.ttlCache.IsExpired(key)
		if exists && !expired {
			expireTime := db.ttlCache.ExpireAt(key)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}

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
func (db *DB) Remove(key string) int {
	result := db.data.Remove(key)
	if result > 0 {
		db.ttlCache.Remove(key)
	}
	return result
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			remove := db.Remove(key)
			deleted += remove
		}
	}
	return deleted
}

func (db *DB) Len() int {
	return db.data.Len()
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
		db.ttlCache.Clear()
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
