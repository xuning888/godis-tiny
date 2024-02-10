package database

import (
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"sync/atomic"
)

// Standalone 单机存储的存储引擎
type Standalone struct {
	dbSet    []*atomic.Value
	reqQueue chan *database.CmdReq
	resQueue chan *database.CmdResult
}

func MakeStandalone() *Standalone {
	dbSet := make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		sdb := MakeSimpleSync(i)
		holder := &atomic.Value{}
		holder.Store(sdb)
		dbSet[i] = holder
	}
	return &Standalone{
		dbSet:    dbSet,
		resQueue: make(chan *database.CmdResult, 100),
		reqQueue: make(chan *database.CmdReq, 100),
	}
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
func (s *Standalone) ExecV2(client redis.Connection, cmdLine database.CmdLine) *database.CmdResult {
	index := client.GetIndex()
	_, reply := s.selectDb(index)
	if reply != nil {
		return database.MakeCmdRes(client, reply)
	}
	cmdReq := database.MakeCmdReq(client, cmdLine)
	s.reqQueue <- cmdReq
	cmdRes := <-s.resQueue
	return cmdRes
}

func (s *Standalone) doExec(req *database.CmdReq) {
	client := req.GetConn()
	lint := parseToLint(req.GetCmdLine())
	index := client.GetIndex()
	db, _ := s.selectDb(index)
	reply := db.Exec(client, lint)
	s.resQueue <- database.MakeCmdRes(client, reply)
}

func (s *Standalone) selectDb(index int) (*DB, *protocol.StandardErrReply) {
	if index >= len(s.dbSet) || index < 0 {
		return nil, protocol.MakeStandardErrReply("ERR DB index is out of range")
	}
	return s.dbSet[index].Load().(*DB), nil
}

func (s *Standalone) Close() error {
	return nil
}

func (s *Standalone) Init() {
	go func() {
		for cmdReq := range s.reqQueue {
			s.doExec(cmdReq)
		}
	}()
}
