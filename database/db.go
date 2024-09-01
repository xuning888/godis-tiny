package database

import (
	"github.com/xuning888/godis-tiny/pkg/datastruct/list"
	"strings"
)

type RType string

var (
	stringValue RType = "String"
	listValue   RType = "List"

	String = &stringValue
	List   = &listValue
)

func (t RType) ToLower() string {
	return strings.ToLower(string(t))
}

type DataEntity struct {
	Type *RType
	Data interface{}
}

func (d *DataEntity) Memory() int {
	switch d.Type {
	case List:
		dequeue, ok := d.Data.(list.Dequeue)
		if !ok {
			return 0
		}
		mem := 0
		dequeue.ForEach(func(value interface{}, index int) bool {
			bytes := value.([]byte)
			mem += len(bytes)
			return true
		})
		return mem
	case String:
		bytes := d.Data.([]byte)
		return len(bytes)
	default:
		return 0
	}
}
