package database

import (
	"errors"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"sync"
	"sync/atomic"
)

// Standalone 单机存储的存储引擎
type Standalone struct {
	dbSet    []*atomic.Value
	reqQueue chan *database.CmdReq
	resQueue chan *database.CmdRes
	stopChan chan byte
	lock     sync.Mutex
}

func MakeStandalone() *Standalone {
	server := &Standalone{
		resQueue: make(chan *database.CmdRes, 1),
		reqQueue: make(chan *database.CmdReq, 1),
		stopChan: make(chan byte, 1),
		lock:     sync.Mutex{},
	}
	dbSet := make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		sdb := MakeSimpleDb(i, server, server)
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

// Exec 使用reqChannel 和 resChannel 来保证命令排队执行
func (s *Standalone) Exec(client redis.Connection, cmdLine database.CmdLine) *database.CmdRes {
	s.lock.Lock()
	defer s.lock.Unlock()
	cmdReq := database.MakeCmdReq(client, cmdLine)
	s.reqQueue <- cmdReq
	return <-s.resQueue
}

// doExec 调用db执行命令，将结果放到 resQueue 中
func (s *Standalone) doExec(req *database.CmdReq) *database.CmdRes {
	// 执行命令
	client := req.GetConn()
	lint := parseToLint(req.GetCmdLine())
	index := client.GetIndex()
	var reply redis.Reply = nil
	db, reply := s.selectDb(index)
	if reply != nil {
		return database.MakeCmdRes(client, reply)
	} else {
		// 每次执行指令的时候都尝试和检查和清理过期的key
		if lint.GetCmdName() != "ttlops" {
			db.RandomCheckTTLAndClearV1()
		}
		// 执行指令
		reply = db.Exec(client, lint)
		return database.MakeCmdRes(client, reply)
	}
}

func (s *Standalone) CheckAndClearDb() {
	for _, dbHolder := range s.dbSet {
		val := dbHolder.Load()
		if val != nil {
			db := dbHolder.Load().(*DB)
			db.RandomCheckTTLAndClearV1()
		}
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
	s.stopChan <- 1
	close(s.stopChan)
	// 关闭接收命令的队列
	close(s.reqQueue)
	// 关闭返回结果的队列
	close(s.resQueue)
	return nil
}

// Init 初始化 standalone，开启一个消费者消费cmdReqQueue中的命令
func (s *Standalone) Init() {
	// 开启一个协程来消费req队列
	initResister()
	go func() {
		for {
			select {
			case cmdReq := <-s.reqQueue:
				s.resQueue <- s.doExec(cmdReq)
			case <-s.stopChan:
				return
			}
		}
	}()
}
