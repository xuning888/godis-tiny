package obj

import (
	"errors"
	"fmt"
	"github.com/xuning888/godis-tiny/pkg/datastruct/dict"
	"github.com/xuning888/godis-tiny/pkg/datastruct/intset"
	"github.com/xuning888/godis-tiny/pkg/datastruct/list"
	"github.com/xuning888/godis-tiny/pkg/datastruct/sds"
	"strconv"
	"unsafe"
)

type ObjectType int

const (
	RedisString ObjectType = iota // StringObject, EncRaw, EncInt
	RedisList                     // ListObject, EncLinkedList
	RedisSet                      // SetObject, EncHT, EncIntSet
	RedisZSet                     // ZSetObject, EncSkipList
	RedisHash                     // HashObject, EncHT
)

type EncodingType int

const (
	EncRaw        EncodingType = iota // Raw representation
	EncInt                            // Encoded as integer
	EncEmbStr                         // Encoding str
	EncHT                             // Encoded as hash table
	EncLinkedList                     // Encoded as regular linked list
	EncZipList                        // Encoded as ziplist
	EncIntSet                         // Encoded as intset
	EncSkipList                       // Encoded as skiplist
)

var (
	ErrorObjectType   = errors.New("error object type")
	ErrorEncodingType = errors.New("error encoding type")
)

func ObjectTypeName(objectType ObjectType) string {
	switch objectType {
	case RedisString:
		return "string"
	case RedisList:
		return "list"
	case RedisSet:
		return "set"
	case RedisZSet:
		return "zset"
	case RedisHash:
		return "hash"
	default:
		return "unknown"
	}
}

func EncodingTypeName(encodingType EncodingType) string {
	switch encodingType {
	case EncRaw:
		return "raw"
	case EncInt:
		return "int"
	case EncHT:
		return "hashtable"
	case EncSkipList:
		return "skiplist"
	case EncIntSet:
		return "intset"
	case EncZipList:
		return "ziplist"
	case EncLinkedList:
		return "linkedlist"
	default:
		return "unknown"
	}
}

type RedisObject struct {
	ObjType  ObjectType
	Encoding EncodingType
	Ptr      interface{}
}

func NewObject(objType ObjectType, ptr interface{}) *RedisObject {
	redisObj := &RedisObject{}
	redisObj.ObjType = objType
	redisObj.Encoding = EncRaw
	redisObj.Ptr = ptr
	return redisObj
}

func NewStringObject(p []byte) *RedisObject {
	redisObject := NewObject(RedisString, sds.NewWithBytes(p))
	if len(p) <= 32 {
		// 小于32个字节, 编码改为EncEmbStr, 内部还是使用 []byte表示
		redisObject.Encoding = EncEmbStr
		if len(p) <= 20 {
			// 尝试转换为64位整数
			value, err := strconv.ParseInt(string(p), 10, 64)
			if err == nil {
				redisObject.Ptr = value
				redisObject.Encoding = EncInt
			}
		}
	}
	return redisObject
}

func NewStringEmptyObj() *RedisObject {
	redisObject := NewObject(RedisString, nil)
	return redisObject
}

func NewIntSetObject() *RedisObject {
	intSet := intset.NewIntSet(intset.EncInt16)
	object := NewObject(RedisSet, intSet)
	object.Encoding = EncIntSet
	return object
}

func NewHashObject() *RedisObject {
	redisObj := NewObject(RedisHash, dict.MakeSimpleDict())
	redisObj.Encoding = EncHT
	return redisObj
}

func NewSetObject(members [][]byte) (*RedisObject, int64) {
	var encoding = EncIntSet
	var distinct int64 = 0
	intSet := intset.NewIntSet(intset.EncInt16)
	simpleDict := dict.MakeSimpleDict()
	for idx, member := range members {
		number, err := strconv.ParseInt(string(member), 10, 64)
		if err == nil {
			distinct += intSet.Add(number)
		} else {
			distinct = 0
			encoding = EncHT
			intSet.Range(func(index int, value int64) bool {
				distinct += int64(simpleDict.Put(fmt.Sprintf("%d", value), struct{}{}))
				return true
			})
			for _, mem := range members[idx:] {
				distinct += int64(simpleDict.Put(string(mem), struct{}{}))
			}
			break
		}
	}
	redisObject := NewObject(RedisSet, nil)
	redisObject.Encoding = encoding
	if encoding == EncIntSet {
		redisObject.Ptr = intSet
	} else {
		redisObject.Ptr = simpleDict
	}
	return redisObject, distinct
}

func NewListObject() *RedisObject {
	redisObj := NewObject(RedisList, list.NewLinked())
	redisObj.Encoding = EncLinkedList
	return redisObj
}

// StringObjIntConvertRaw 如果对象是StringObject, 并且编码格式是int, 那么就将其转换为raw用sds保存数据
func StringObjIntConvertRaw(obj *RedisObject, apped []byte) {
	if obj.ObjType != RedisString || obj.Encoding != EncInt {
		return
	}
	intValue, ok := obj.Ptr.(int64)
	if !ok {
		return
	}
	obj.Encoding = EncRaw
	var bytes []byte = nil
	if apped != nil && len(apped) > 0 {
		bytes = make([]byte, 0, 8+len(apped))
		bytes = strconv.AppendInt(bytes, intValue, 10)
		sdsValue := sds.NewWithBytes(bytes)
		sdsValue.SdsCat(apped)
		obj.Ptr = sdsValue
	} else {
		bytes = make([]byte, 0, 8)
		bytes = strconv.AppendInt(bytes, intValue, 10)
		obj.Ptr = sds.NewWithBytes(bytes)
	}
}

func StringObjSetValue(obj *RedisObject, p []byte) {
	if obj.ObjType != RedisString {
		return
	}
	if len(p) <= 32 {
		// 小于32个字节, 编码改为EncEmbStr, 内部还是使用 []byte
		obj.Encoding = EncEmbStr
		obj.Ptr = sds.NewWithBytes(p)
		if len(p) <= 20 {
			// 尝试转换为64位整数
			value, err := strconv.ParseInt(string(p), 10, 64)
			if err == nil {
				obj.Ptr = value
				obj.Encoding = EncInt
			}
		}
	} else {
		obj.Encoding = EncRaw
		obj.Ptr = sds.NewWithBytes(p)
	}
}

func StringObjEncoding(obj *RedisObject) (result []byte, err error) {
	if obj.ObjType != RedisString {
		return nil, ErrorObjectType
	}
	switch obj.Encoding {
	case EncEmbStr | EncRaw:
		sdss := obj.Ptr.(*sds.Sds)
		result = *sdss
		break
	case EncInt:
		result = strconv.AppendInt([]byte{}, obj.Ptr.(int64), 10)
		break
	default:
		err = ErrorEncodingType
	}
	return
}

func StringObjMem(obj *RedisObject) (int64, error) {
	if obj.ObjType != RedisString {
		return 0, ErrorObjectType
	}
	sizeof := int64(unsafe.Sizeof(*obj)) + 8
	switch obj.Encoding {
	case EncRaw | EncEmbStr:
		sdss := obj.Ptr.(*sds.Sds)
		return sizeof + int64(sdss.Memory()) + int64(8), nil
	case EncInt:
		return sizeof + 8, nil
	default:
		return 0, ErrorEncodingType
	}
}

func ListObjMem(obj *RedisObject) (int64, error) {
	if obj.ObjType != RedisList {
		return 0, ErrorObjectType
	}
	sizeof := int64(unsafe.Sizeof(*obj)) + 8
	switch obj.Encoding {
	case EncLinkedList:
		dequeue := obj.Ptr.(list.Dequeue)
		var sum int64
		dequeue.ForEach(func(value interface{}, index int) bool {
			bytes := value.([]byte)
			sum += int64(cap(bytes))
			return true
		})
		return sizeof + sum, nil
	default:
		return 0, ErrorEncodingType
	}
}
