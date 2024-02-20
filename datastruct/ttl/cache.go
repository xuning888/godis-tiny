package ttl

import (
	"container/heap"
	"time"
)

var _ Cache = &SimpleCache{}

var EmptyTime = time.Unix(0, 0)

type Cache interface {
	// Expire 设置key的过期时间
	Expire(key string, expireTime time.Time)
	// IsExpired IsExpired 检查key是否过期
	IsExpired(key string) (expired bool, exists bool)
	// Remove 删除key的ttl
	Remove(key string)
	// ExpireAt 返回指定key的过期时间
	ExpireAt(key string) time.Time
	// ExpireAtTimestamp 返回指定key的过期时间的时间戳
	ExpireAtTimestamp(key string) int64
	// Len 查看ttlCache 中有多少key
	Len() int
	// Peek 查看过期时间最小的key, 这个方法会返回nil
	Peek() *Item
}

type SimpleCache struct {
	// ttlMap 存储key的过期时间
	ttlMap map[string]*Item
	// ttlHeap, 使用小根堆，按照expireTime排队
	heap ttlHeap
}

func (s *SimpleCache) Peek() *Item {
	peek := s.heap.Peek()
	if peek == nil {
		return nil
	}
	return peek.(*Item)
}

func (s *SimpleCache) Expire(key string, expireTime time.Time) {
	item, ok := s.ttlMap[key]
	if ok {
		item.ExpireTime = expireTime
		item.ExpireTimestamp = expireTime.UnixMilli()
		s.ttlMap[key] = item
		s.heap.update(item, key, expireTime)
		return
	}
	item = makeItem(key, expireTime)
	s.ttlMap[key] = item
	heap.Push(&s.heap, item)
}

func (s *SimpleCache) IsExpired(key string) (expired bool, exists bool) {
	item, ok := s.ttlMap[key]
	if ok {
		return time.Now().UnixMilli() > item.ExpireTimestamp, true
	}
	return false, false
}

func (s *SimpleCache) Remove(key string) {
	item, ok := s.ttlMap[key]
	if ok {
		heap.Remove(&s.heap, item.index)
		delete(s.ttlMap, key)
	}
}

func (s *SimpleCache) ExpireAt(key string) time.Time {
	item, ok := s.ttlMap[key]
	if ok {
		return item.ExpireTime
	}
	return EmptyTime
}

func (s *SimpleCache) ExpireAtTimestamp(key string) int64 {
	item, ok := s.ttlMap[key]
	if ok {
		return item.ExpireTimestamp
	}
	return EmptyTime.UnixMilli()
}

func (s *SimpleCache) Len() int {
	ttlMapLen := len(s.ttlMap)
	return ttlMapLen
}

func MakeSimple() *SimpleCache {
	h := make(ttlHeap, 0)
	heap.Init(&h)
	return &SimpleCache{
		ttlMap: make(map[string]*Item),
		heap:   h,
	}
}
