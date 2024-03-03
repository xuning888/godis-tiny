package list

import (
	"container/list"
)

const segmentCapacity = 1 << 11

const maxSize = (1 << 32) - 1

type QuickDequeue struct {
	size int
	l    *list.List
}

func (q *QuickDequeue) AddFirst(ele interface{}) error {
	var segment Dequeue
	if q.Len() == 0 {
		segment = NewArrayDequeWithCap(segmentCapacity, false)
		q.l.PushFront(segment)
	} else {
		front := q.l.Front()
		dequeue := front.Value.(Dequeue)
		if dequeue.Len() == segmentCapacity {
			segment = NewArrayDequeWithCap(segmentCapacity, false)
			q.l.PushFront(segment)
		} else {
			segment = dequeue
		}
	}
	_ = segment.AddFirst(ele)
	q.size++
	return nil
}

func (q *QuickDequeue) AddLast(ele interface{}) error {
	var segment Dequeue
	if q.Len() == 0 {
		segment = NewArrayDequeWithCap(segmentCapacity, false)
		q.l.PushFront(segment)
	} else {
		front := q.l.Front()
		dequeue := front.Value.(Dequeue)
		if dequeue.Len() == segmentCapacity {
			segment = NewArrayDequeWithCap(segmentCapacity, false)
			q.l.PushFront(segment)
		} else {
			segment = dequeue
		}
	}
	_ = segment.AddLast(ele)
	q.size++
	return nil
}

func (q *QuickDequeue) RemoveFirst() (interface{}, error) {
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	var segment Dequeue
	front := q.l.Front()
	segment = front.Value.(Dequeue)
	first, err := segment.RemoveFirst()
	if segment.Len() == 0 {
		q.l.Remove(front)
	}
	q.size--
	return first, err
}

func (q *QuickDequeue) RemoveLast() (interface{}, error) {
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	var segment Dequeue
	back := q.l.Back()
	segment = back.Value.(Dequeue)
	first, err := segment.RemoveLast()
	if segment.Len() == 0 {
		q.l.Remove(back)
	}
	q.size--
	return first, err
}

func (q *QuickDequeue) GetFirst() (interface{}, error) {
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	var segment Dequeue
	front := q.l.Front()
	segment = front.Value.(Dequeue)
	return segment.GetFirst()
}

func (q *QuickDequeue) GetLast() (interface{}, error) {
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	var segment Dequeue
	back := q.l.Back()
	segment = back.Value.(Dequeue)
	return segment.GetFirst()
}

func (q *QuickDequeue) Get(index int) (interface{}, error) {
	if index < 0 || index >= q.size {
		return nil, ErrorOutIndex
	}
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	nodeIndex := index / segmentCapacity
	segIndex := index % segmentCapacity
	node := q.l.Front()
	for i := 0; i < nodeIndex; i++ {
		node = node.Next()
	}
	segment := node.Value.(Dequeue)
	return segment.Get(segIndex)
}

func (q *QuickDequeue) Len() int {
	return q.size
}

func (q *QuickDequeue) ForEach(f func(value interface{}, index int) bool) {
	if q.Len() == 0 {
		return
	}
	idx := 0
	for node := q.l.Front(); node != nil; node = node.Next() {
		segment := node.Value.(Dequeue)
		segment.ForEach(func(value interface{}, index int) bool {
			run := f(value, idx)
			idx++
			return run
		})
	}
}

var _ Dequeue = &QuickDequeue{}

func NewQuickDequeue() *QuickDequeue {
	l := list.New()
	return &QuickDequeue{
		size: 0,
		l:    l,
	}
}
