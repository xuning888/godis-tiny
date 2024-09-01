package redis

var ConnCounter ConnCount = nil

type ConnCount interface {

	// CountConnections connected_clients
	CountConnections() int
}

type Manager struct {
	conns map[int]*Client
}

func (s *Manager) CountConnections() int {
	return len(s.conns)
}

func (s *Manager) RegisterConn(fd int, client *Client) {
	s.conns[fd] = client
}

func (s *Manager) RemoveConnByKey(fd int) {
	_, exists := s.conns[fd]
	if exists {
		delete(s.conns, fd)
		return
	}
}

func (s *Manager) Get(fd int) *Client {
	c := s.conns[fd]
	return c
}

func (s *Manager) RemoveConn(conn *Client) {
	delete(s.conns, conn.Fd)
}

func NewManager() *Manager {
	return &Manager{
		conns: make(map[int]*Client),
	}
}
