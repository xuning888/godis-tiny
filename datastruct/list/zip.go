package list

import (
	"container/list"
	"godis-tiny/interface/redis"
)

const segmentCapacity = 1 << 9

const maxSize = (1 << 32) - 1

type ZipQueue struct {
	size int
	l    *list.List
}

func (q *ZipQueue) Range(begin, end int, convert func(ele interface{}) redis.Reply) ([]redis.Reply, error) {
	if begin < 0 || end >= q.Len() || begin > end {
		return nil, ErrorOutIndex
	}

	n := end - begin + 1
	res := make([]redis.Reply, 0, n)

	for i := begin; i <= end; i++ {
		value, _ := q.Get(i)
		res = append(res, convert(value))
	}
	return res, nil
}

func (q *ZipQueue) AddFirst(ele interface{}) error {
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

func (q *ZipQueue) AddLast(ele interface{}) error {
	var segment Dequeue
	if q.Len() == 0 {
		segment = NewArrayDequeWithCap(segmentCapacity, false)
		q.l.PushBack(segment)
	} else {
		back := q.l.Back()
		dequeue := back.Value.(Dequeue)
		if dequeue.Len() == segmentCapacity {
			segment = NewArrayDequeWithCap(segmentCapacity, false)
			q.l.PushBack(segment)
		} else {
			segment = dequeue
		}
	}
	_ = segment.AddLast(ele)
	q.size++
	return nil
}

func (q *ZipQueue) RemoveFirst() (interface{}, error) {
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

func (q *ZipQueue) RemoveLast() (interface{}, error) {
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

func (q *ZipQueue) GetFirst() (interface{}, error) {
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	var segment Dequeue
	front := q.l.Front()
	segment = front.Value.(Dequeue)
	return segment.GetFirst()
}

func (q *ZipQueue) GetLast() (interface{}, error) {
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}
	var segment Dequeue
	back := q.l.Back()
	segment = back.Value.(Dequeue)
	return segment.GetFirst()
}

func (q *ZipQueue) Get(index int) (interface{}, error) {
	if index < 0 || index >= q.size {
		return nil, ErrorOutIndex
	}
	if q.Len() == 0 {
		return nil, ErrorEmpty
	}

	node := q.l.Front()
	end := 0
	if index <= q.size/2 {
		// 访问索引在前半部分， 从头部开始查找
		for node != nil {
			seg := node.Value.(Dequeue)
			end += seg.Len()
			if index < end {
				// 找到了对应的段
				segIndex := seg.Len() - 1 - (end - 1 - index)
				return seg.Get(segIndex)
			}
			node = node.Next()
		}
	} else {
		// 访问索引在后半部分， 从尾部开始查找
		node = q.l.Back()
		index = q.size - 1 - index // 转换为从尾部开始的索引
		for node != nil {
			seg := node.Value.(Dequeue)
			end += seg.Len()
			if index < end {
				// 找到了对应的段
				segIndex := seg.Len() - 1 - (end - 1 - index)
				return seg.Get(segIndex)
			}
			node = node.Prev()
		}
	}

	return nil, ErrorOutIndex
}

func (q *ZipQueue) Len() int {
	return q.size
}

func (q *ZipQueue) ForEach(f func(value interface{}, index int) bool) {
	if q.Len() == 0 {
		return
	}
	idx := 0
	var run bool
	for node := q.l.Front(); node != nil; node = node.Next() {
		segment := node.Value.(Dequeue)
		segment.ForEach(func(value interface{}, index int) bool {
			run = f(value, idx)
			idx++
			return run
		})
		if !run {
			break
		}
	}
}

var _ Dequeue = &ZipQueue{}

func NewZipQueue() *ZipQueue {
	l := list.New()
	return &ZipQueue{
		size: 0,
		l:    l,
	}
}
