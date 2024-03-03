package list

type Dequeue interface {
	AddFirst(ele interface{}) error
	AddLast(ele interface{}) error
	RemoveFirst() (interface{}, error)
	RemoveLast() (interface{}, error)
	GetFirst() (interface{}, error)
	GetLast() (interface{}, error)
	// Get 获取index位置的数据
	Get(index int) (interface{}, error)
	// Len 获取长度
	Len() int
	// ForEach 遍历双端队列
	ForEach(func(value interface{}, index int) bool)
}
