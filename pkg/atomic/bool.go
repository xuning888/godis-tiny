package atomic

import "sync/atomic"

// Boolean 原子Bool, 0表示false, 1表示true
type Boolean uint32

func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

func (b *Boolean) Set(value bool) {
	if value {
		atomic.AddUint32((*uint32)(b), 1)
	} else {
		atomic.AddUint32((*uint32)(b), 0)
	}
}
