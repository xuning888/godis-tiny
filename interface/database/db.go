package database

import (
	"context"
	"godis-tiny/datastruct/list"
	"godis-tiny/interface/redis"
	"strings"
	"time"
)

type RType string

var (
	String RType = "String"
	List   RType = "List"
)

func (t RType) ToLower() string {
	return strings.ToLower(string(t))
}

type CmdLine = [][]byte

// DBEngine 存储引擎的抽象
type DBEngine interface {
	// Close 关闭
	Close() error
	// Init 做必要的初始化工作
	Init()
	// PushReqEvent 推送一个命令到dbEngine
	PushReqEvent(req *CmdReq)
	// DeliverResEvent 接收res的channel
	DeliverResEvent() <-chan *CmdRes
	// Exec 同步调用
	Exec(req *CmdReq) *CmdRes
	// ForEach 遍历指定的db
	ForEach(dbIndex int, cb func(key string, entity *DataEntity, expiration *time.Time) bool)
	// Cron 定时任务
	Cron()
	// Shutdown 关闭服务器
	Shutdown(ctx context.Context) error
}

type EngineCommand interface {
	// CheckIndex checkIndex
	CheckIndex(index int) error

	Rewrite() error
}

type TTLChecker interface {
	CheckAndClearDb()
}

type DataEntity struct {
	Type RType
	Data interface{}
}

func (d *DataEntity) Memory() int {
	switch d.Type {
	case List:
		dequeue, ok := d.Data.(list.Dequeue)
		if !ok {
			return 0
		}
		mem := 0
		dequeue.ForEach(func(value interface{}, index int) bool {
			bytes := value.([]byte)
			mem += len(bytes)
			return true
		})
		return mem
	case String:
		bytes := d.Data.([]byte)
		return len(bytes)
	default:
		return 0
	}
}

type CmdReq struct {
	conn    redis.Conn
	cmdLine CmdLine
}

func (c *CmdReq) GetConn() redis.Conn {
	return c.conn
}

func (c *CmdReq) GetCmdLine() CmdLine {
	return c.cmdLine
}

type CmdRes struct {
	reply redis.Reply
	conn  redis.Conn
}

func (c *CmdRes) GetReply() redis.Reply {
	return c.reply
}

func (c *CmdRes) GetConn() redis.Conn {
	return c.conn
}

func MakeCmdRes(conn redis.Conn, reply redis.Reply) *CmdRes {
	return &CmdRes{
		conn:  conn,
		reply: reply,
	}
}

func MakeCmdReq(conn redis.Conn, cmdLine CmdLine) *CmdReq {
	return &CmdReq{
		conn:    conn,
		cmdLine: cmdLine,
	}
}
