package database

import (
	"godis-tiny/interface/redis"
)

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
}

type IndexChecker interface {
	// CheckIndex checkIndex
	CheckIndex(index int) error
}

type TTLChecker interface {
	CheckAndClearDb()
}

type DataEntity struct {
	Data interface{}
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
