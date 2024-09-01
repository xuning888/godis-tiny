package list

import "container/list"

type Linked struct {
	list *list.List
}

func NewLinked() *Linked {
	return &Linked{
		list: list.New(),
	}
}

func (l *Linked) AddFirst(ele interface{}) error {
	l.list.PushFront(ele)
	return nil
}

func (l *Linked) AddLast(ele interface{}) error {
	l.list.PushBack(ele)
	return nil
}

func (l *Linked) RemoveFirst() (ele interface{}, err error) {
	if l.Len() > 0 {
		front := l.list.Front()
		l.list.Remove(front)
		ele = front.Value
		return
	}
	return nil, ErrorEmpty
}

func (l *Linked) RemoveLast() (ele interface{}, err error) {
	if l.Len() > 0 {
		back := l.list.Back()
		l.list.Remove(back)
		ele = back.Value
		return
	}
	return nil, ErrorEmpty
}

func (l *Linked) GetFirst() (ele interface{}, err error) {
	if l.Len() > 0 {
		ele = l.list.Front().Value
		return
	}
	return nil, ErrorEmpty
}

func (l *Linked) GetLast() (ele interface{}, err error) {
	if l.Len() > 0 {
		ele = l.list.Back().Value
		return
	}
	return nil, ErrorEmpty
}

func (l *Linked) Get(index int) (ele interface{}, err error) {
	if index < 0 || index >= l.list.Len() {
		return nil, ErrorOutIndex
	}
	i := 0
	for e := l.list.Front(); e.Next() != nil; e = e.Next() {
		if i == index {
			ele = e.Value
			break
		}
	}
	return
}

func (l *Linked) Len() int {
	return l.list.Len()
}

func (l *Linked) ForEach(fun func(value interface{}, index int) bool) {
	if l.Len() == 0 {
		return
	}
	i := 0
	for e := l.list.Front(); e != nil; e = e.Next() {
		if !fun(e.Value, i) {
			break
		}
		i++
	}
}
