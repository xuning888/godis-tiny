package dict

import (
	"sync"
	"sync/atomic"
)

type SimpleSync struct {
	data    sync.Map
	count   int32
	rwMutex sync.RWMutex
}

func (s *SimpleSync) Get(key string) (value interface{}, exists bool) {
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()
	load, ok := s.data.Load(key)
	return load, ok
}

func (s *SimpleSync) Len() int {
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()
	return int(atomic.LoadInt32(&s.count))
}

func (s *SimpleSync) Put(key string, value interface{}) (result int) {
	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
	}()
	_, ok := s.data.Load(key)
	s.data.Store(key, value)
	if ok {
		return 0
	}
	s.addCount()
	return 1
}

func (s *SimpleSync) PutIfAbsent(key string, value interface{}) (result int) {
	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
	}()
	_, ok := s.data.Load(key)
	if ok {
		return 0
	}
	s.data.Store(key, value)
	s.addCount()
	return 1
}

func (s *SimpleSync) PutIfExists(key string, value interface{}) (result int) {
	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
	}()
	_, ok := s.data.Load(key)
	if ok {
		s.data.Store(key, value)
		return 1
	}
	return 0
}

func (s *SimpleSync) Remove(key string) (result int) {
	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
	}()
	_, ok := s.data.Load(key)
	if ok {
		s.data.Delete(key)
		s.desCount()
		return 1
	}
	return 0
}

func (s *SimpleSync) ForEach(consumer Consumer) {
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()
	s.data.Range(func(key, value interface{}) bool {
		entity := key.(string)
		return consumer(entity, value)
	})
}

func (s *SimpleSync) Keys() []string {
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()
	result := make([]string, s.Len())
	i := 0
	s.data.Range(func(key, _ interface{}) bool {
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

func (s *SimpleSync) RandomKeys(limit int) []string {
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()
	if limit >= s.Len() {
		limit = s.Len()
	}
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		s.data.Range(func(key, _ interface{}) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result
}

func (s *SimpleSync) RandomDistinctKeys(limit int) []string {
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()
	if limit >= s.Len() {
		limit = s.Len()
	}
	result := make([]string, limit)
	i := 0
	s.data.Range(func(key, _ interface{}) bool {
		if i == limit {
			return false
		}
		result[i] = key.(string)
		return true
	})
	return result
}

func (s *SimpleSync) Clear() {
	*s = *MakeSimpleSync()
}

func (s *SimpleSync) addCount() int32 {
	return atomic.AddInt32(&s.count, 1)
}

func (s *SimpleSync) desCount() int32 {
	return atomic.AddInt32(&s.count, -1)
}

func MakeSimpleSync() *SimpleSync {
	return &SimpleSync{
		data: sync.Map{},
	}
}
