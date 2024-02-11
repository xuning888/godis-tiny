package database

import (
	"g-redis/interface/redis"
)

type CmdLine = [][]byte

// DBEngine 存储引擎的抽象
type DBEngine interface {
	// Exec client 上的命令
	Exec(client redis.Connection, cmdLine CmdLine) redis.Reply
	// ExecV2 client 上的命令，使用 channel 来保证线程安全，缺点就是无法读并发
	ExecV2(client redis.Connection, cmdLine CmdLine) *CmdRes
	// Close 关闭
	Close() error
	// Init 做必要的初始化工作
	Init()
}

type DataEntity struct {
	Data interface{}
}

type CmdReq struct {
	conn    redis.Connection
	cmdLine CmdLine
}

func (c *CmdReq) GetConn() redis.Connection {
	return c.conn
}

func (c *CmdReq) GetCmdLine() CmdLine {
	return c.cmdLine
}

type CmdRes struct {
	reply redis.Reply
	conn  redis.Connection
}

func (c *CmdRes) GetReply() redis.Reply {
	return c.reply
}

func (c *CmdRes) GetConn() redis.Connection {
	return c.conn
}

func MakeCmdRes(conn redis.Connection, reply redis.Reply) *CmdRes {
	return &CmdRes{
		conn:  conn,
		reply: reply,
	}
}

func MakeCmdReq(conn redis.Connection, cmdLine CmdLine) *CmdReq {
	return &CmdReq{
		conn:    conn,
		cmdLine: cmdLine,
	}
}
