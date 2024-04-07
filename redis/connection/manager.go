package connection

import (
	"github.com/xuning888/godis-tiny/interface/redis"
)

type SimpleConnManager struct {
	conns map[string]redis.Conn
}

func (s *SimpleConnManager) CountConnections() int {
	return len(s.conns)
}

func (s *SimpleConnManager) RegisterConn(key string, conn redis.Conn) {
	s.conns[key] = conn
}

func (s *SimpleConnManager) RemoveConnByKey(key string) {
	_, exists := s.conns[key]
	if exists {
		delete(s.conns, key)
		return
	}
}

func (s *SimpleConnManager) Get(key string) redis.Conn {
	c := s.conns[key]
	return c
}

func (s *SimpleConnManager) RemoveConn(conn redis.Conn) {
	key := conn.RemoteAddr().String()
	delete(s.conns, key)
}

func NewConnManager() *SimpleConnManager {
	return &SimpleConnManager{
		conns: make(map[string]redis.Conn),
	}
}
