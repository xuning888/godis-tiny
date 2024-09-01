package ttl

import (
	"container/heap"
	"time"
)

var _ ttlPq = &ttlHeap{}

type ttlPq interface {
	heap.Interface
	// Peek 查看对顶元素
	Peek() interface{}
}

type Item struct {
	Key             string
	ExpireTime      time.Time
	ExpireTimestamp int64
	index           int
}

func makeItem(key string, expiryTime time.Time) *Item {
	return &Item{
		Key:             key,
		ExpireTime:      expiryTime,
		ExpireTimestamp: expiryTime.UnixMilli(),
	}
}

type ttlHeap []*Item

func (t ttlHeap) Len() int {
	return len(t)
}

func (t ttlHeap) Less(i, j int) bool {
	return t[i].ExpireTimestamp < t[j].ExpireTimestamp
}

func (t ttlHeap) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
	t[i].index = i
	t[j].index = j
}

func (t *ttlHeap) Push(x interface{}) {
	n := len(*t)
	ele := x.(*Item)
	ele.index = n
	*t = append(*t, ele)
}

func (t *ttlHeap) Pop() interface{} {
	old := *t
	n := len(old)
	ele := old[n-1]
	old[n-1] = nil // avoid memory leak
	ele.index = -1 // for safety
	*t = old[0 : n-1]
	return ele
}

// Peek 查看堆顶元素
func (t *ttlHeap) Peek() interface{} {
	temp := *t
	n := len(temp)
	var ele interface{} = nil
	if n > 0 {
		ele = temp[n-1]
	}
	return ele
}

func (t *ttlHeap) update(item *Item, key string, expiryTime time.Time) {
	item.Key = key
	item.ExpireTime = expiryTime
	item.ExpireTimestamp = expiryTime.UnixMilli()
	heap.Fix(t, item.index)
}
