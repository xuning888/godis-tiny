package obj

import (
	"errors"
	"github.com/xuning888/godis-tiny/pkg/datastruct/intset"
	"github.com/xuning888/godis-tiny/pkg/datastruct/list"
	"github.com/xuning888/godis-tiny/pkg/datastruct/sds"
	"strconv"
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

func NewIntSetObject() *RedisObject {
	intSet := intset.NewIntSet(intset.EncInt16)
	object := NewObject(RedisSet, intSet)
	object.Encoding = EncIntSet
	return object
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
	case EncRaw:
	case EncEmbStr:
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
