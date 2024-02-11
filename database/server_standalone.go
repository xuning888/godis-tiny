package database

import (
	"errors"
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"sync/atomic"
)

// Standalone 单机存储的存储引擎
type Standalone struct {
	dbSet    []*atomic.Value
	reqQueue chan *database.CmdReq
	resQueue chan *database.CmdRes
}

func MakeStandalone() *Standalone {
	server := &Standalone{
		resQueue: make(chan *database.CmdRes, 1000),
		reqQueue: make(chan *database.CmdReq, 1000),
	}
	dbSet := make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		sdb := MakeSimpleDb(i, server)
		holder := &atomic.Value{}
		holder.Store(sdb)
		dbSet[i] = holder
	}
	server.dbSet = dbSet
	return server
}

func (s *Standalone) CheckIndex(index int) error {
	if index >= len(s.dbSet) || index < 0 {
		return errors.New("ERR DB index is out of range")
	}
	return nil
}

func (s *Standalone) Exec(client redis.Connection, cmdLine database.CmdLine) redis.Reply {
	lint := parseToLint(cmdLine)
	index := client.GetIndex()
	db, reply := s.selectDb(index)
	if reply != nil {
		return reply
	}
	return db.Exec(client, lint)
}

// ExecV2 使用reqChannel 和 resChannel 来保证命令排队执行
func (s *Standalone) ExecV2(client redis.Connection, cmdLine database.CmdLine) *database.CmdRes {
	cmdReq := database.MakeCmdReq(client, cmdLine)
	s.reqQueue <- cmdReq
	cmdRes := <-s.resQueue
	return cmdRes
}

// doExec 调用db执行命令，将结果放到 resQueue 中
func (s *Standalone) doExec(req *database.CmdReq) {
	client := req.GetConn()
	lint := parseToLint(req.GetCmdLine())
	index := client.GetIndex()
	var reply redis.Reply = nil
	db, reply := s.selectDb(index)
	if reply != nil {
		s.resQueue <- database.MakeCmdRes(client, reply)
	} else {
		reply = db.Exec(client, lint)
		s.resQueue <- database.MakeCmdRes(client, reply)
	}
}

// selectDb 检查index是否在可选范围 0 ~ 15, 如果超过了范围返回错误的 reply, 否则返回一个对应index的db
func (s *Standalone) selectDb(index int) (*DB, redis.Reply) {
	if index >= len(s.dbSet) || index < 0 {
		return nil, protocol.MakeStandardErrReply("ERR DB index is out of range")
	}
	return s.dbSet[index].Load().(*DB), nil
}

// Close 关闭资源
func (s *Standalone) Close() error {
	// 关闭接收命令的队列
	close(s.reqQueue)
	// 关闭返回结果的队列
	close(s.resQueue)
	return nil
}

// Init 初始化 standalone，开启一个消费者消费cmdReqQueue中的命令
func (s *Standalone) Init() {
	// 开启一个协程来消费req队列
	go func() {
		for cmdReq := range s.reqQueue {
			s.doExec(cmdReq)
		}
	}()
}
