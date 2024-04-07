package list

import (
	"errors"
	"github.com/xuning888/godis-tiny/interface/redis"
)

var _ Dequeue = &ArrayDeque{}

const (
	minCapacity int = 1 << 4
	maxCapacity int = 1 << 30
)

var (
	ErrorOutOfCapacity = errors.New("out of max capacity")
	ErrorEmpty         = errors.New("array deque is empty")
	ErrorOutIndex      = errors.New("out of index")
	ErrorNil           = errors.New("not allowed null point")
)

// ArrayDeque 使用切片实现的循环队列，极限容量时 2 ^ 30
// 这玩意由于数组的扩容, 导致性能太差了
type ArrayDeque struct {
	head     int
	tail     int
	size     int
	growth   bool
	elements []interface{}
}

func NewArrayDeque(growth bool) *ArrayDeque {
	return NewArrayDequeWithCap(minCapacity, growth)
}

func NewArrayDequeWithCap(c int, growth bool) *ArrayDeque {
	capacity := calculateCapacity(c - 1)
	return &ArrayDeque{
		head:     0,
		tail:     0,
		size:     0,
		growth:   growth,
		elements: make([]interface{}, capacity),
	}
}

func (a *ArrayDeque) Front() (interface{}, error) {
	return a.GetFirst()
}

func (a *ArrayDeque) Enqueue(ele interface{}) error {
	return a.AddLast(ele)
}

func (a *ArrayDeque) Dequeue() (interface{}, error) {
	return a.RemoveFirst()
}

func (a *ArrayDeque) Push(ele interface{}) error {
	return a.AddLast(ele)
}

func (a *ArrayDeque) Pop() (interface{}, error) {
	return a.RemoveLast()
}

func (a *ArrayDeque) Peek() (interface{}, error) {
	return a.GetFirst()
}

func (a *ArrayDeque) Get(index int) (interface{}, error) {
	if index < 0 || index >= a.Len() {
		return nil, ErrorOutIndex
	}
	t := (a.head + index) & (a.Cap() - 1)
	result := a.elements[t]
	return result, nil
}

func (a *ArrayDeque) Len() int {
	return a.size
}

func (a *ArrayDeque) AddFirst(ele interface{}) error {
	if ele == nil {
		return ErrorNil
	}
	a.head = (a.head - 1) & (a.Cap() - 1)
	a.elements[a.head] = ele
	a.size++
	if a.head == a.tail {
		if !a.growth {
			return ErrorOutOfCapacity
		}
		err := a.doubleCapacity()
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *ArrayDeque) AddLast(ele interface{}) error {
	if ele == nil {
		return ErrorNil
	}
	a.elements[a.tail] = ele
	a.tail = (a.tail + 1) & (a.Cap() - 1)
	a.size++
	if a.head == a.tail {
		if !a.growth {
			return ErrorOutOfCapacity
		}
		err := a.doubleCapacity()
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *ArrayDeque) RemoveLast() (ele interface{}, err error) {
	if a.Len() == 0 {
		return nil, ErrorEmpty
	}
	a.tail = (a.tail - 1) & (a.Cap() - 1)
	result := a.elements[a.tail]
	a.elements[a.tail] = nil
	a.size--
	return result, nil
}

func (a *ArrayDeque) RemoveFirst() (interface{}, error) {
	if a.Len() == 0 {
		return nil, ErrorEmpty
	}
	result := a.elements[a.head]
	a.elements[a.head] = nil
	a.head = (a.head + 1) & (a.Cap() - 1)
	a.size--
	return result, nil
}

func (a *ArrayDeque) GetFirst() (interface{}, error) {
	if a.Len() == 0 {
		return nil, ErrorEmpty
	}
	result := a.elements[a.head]
	return result, nil
}

func (a *ArrayDeque) GetLast() (interface{}, error) {
	if a.Len() == 0 {
		return nil, ErrorEmpty
	}
	t := (a.tail - 1) & (a.Cap() - 1)
	result := a.elements[t]
	return result, nil
}

func (a *ArrayDeque) Cap() int {
	return cap(a.elements)
}

func (a *ArrayDeque) Trim() error {
	n := a.Len()
	newCap := calculateCapacity(n + 1)
	if a.Cap() == newCap {
		return nil
	}
	newElements := make([]interface{}, newCap)
	if a.head <= a.tail {
		copy(newElements, a.elements[a.head:a.tail])
	} else {
		copy(newElements, a.elements[a.head:a.Cap()])
		copy(newElements[a.Cap()-a.head:], a.elements[:a.tail])
	}
	a.elements = newElements
	a.head = 0
	a.tail = n
	return nil
}

func (a *ArrayDeque) ForEach(f func(value interface{}, index int) bool) {
	if a.Len() == 0 {
		return
	}
	for i := 0; i < a.Len(); i++ {
		eleIdx := (a.head + i) & (a.Cap() - 1)
		value := a.elements[eleIdx]
		if !f(value, i) {
			break
		}
	}
}

func (a *ArrayDeque) Range(begin, end int, convert func(ele interface{}) redis.Reply) ([]redis.Reply, error) {
	if begin < 0 || end >= a.Len() || begin > end {
		return nil, ErrorOutIndex
	}
	n := end - begin + 1
	res := make([]redis.Reply, 0, n)
	for i := begin; i <= end; i++ {
		value, _ := a.Get(i)
		res = append(res, convert(value))
	}
	return res, nil
}

func (a *ArrayDeque) doubleCapacity() error {
	n := a.Cap()
	newCapacity := n << 1
	if newCapacity < 0 || newCapacity > maxCapacity {
		// Double allowed capacity
		return ErrorOutOfCapacity
	}
	newElements := make([]interface{}, newCapacity)
	r := n - a.head
	copy(newElements[0:r], a.elements[a.head:r+a.head])
	copy(newElements[r:], a.elements[:a.tail])
	a.elements = newElements
	a.head = 0
	a.tail = n
	return nil
}

func calculateCapacity(expected int) int {
	initialCapacity := minCapacity
	if expected > initialCapacity {
		initialCapacity = expected
		initialCapacity |= initialCapacity >> 1
		initialCapacity |= initialCapacity >> 2
		initialCapacity |= initialCapacity >> 4
		initialCapacity |= initialCapacity >> 8
		initialCapacity |= initialCapacity >> 16
		initialCapacity++
		if initialCapacity < 0 || initialCapacity >= maxCapacity {
			initialCapacity = maxCapacity
		}
	}
	return initialCapacity
}
