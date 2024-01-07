package database

import (
	"g-redis/interface/database"
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"sync/atomic"
)

type Standalone struct {
	dbSet []*atomic.Value
}

func MakeStandalone() *Standalone {
	dbSet := make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		sdb := MakeDB(i)
		holder := &atomic.Value{}
		holder.Store(sdb)
		dbSet[i] = holder
	}
	return &Standalone{
		dbSet: dbSet,
	}
}

func (s *Standalone) Exec(client redis.Connection, cmdLine database.CmdLine) redis.Reply {
	lint := parseToLint(cmdLine)
	cmdName := lint.GetCmdName()
	if "ping" == cmdName {
		return Ping(client, lint)
	}
	index := client.GetIndex()
	db, reply := s.selectDb(index)
	if reply != nil {
		return reply
	}
	return db.Exec(client, lint)
}

func (s *Standalone) selectDb(index int) (*DB, *protocol.StandardErrReply) {
	if index >= len(s.dbSet) || index < 0 {
		return nil, protocol.MakeStandardErrReply("ERR DB index is out of range")
	}
	return s.dbSet[index].Load().(*DB), nil
}

func (s *Standalone) Close() error {
	//TODO implement me
	panic("implement me")
}
