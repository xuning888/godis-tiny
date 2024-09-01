package obj

import (
	"github.com/stretchr/testify/assert"
	"github.com/xuning888/godis-tiny/pkg/datastruct/sds"
	"testing"
)

func TestNewStringObj(t *testing.T) {
	testCases := []struct {
		Name      string
		Input     []byte
		wantEnc   EncodingType
		wantValue interface{}
	}{
		{
			Name:      "embedStr",
			Input:     []byte("hello"),
			wantEnc:   EncEmbStr,
			wantValue: sds.NewWithBytes([]byte("hello")),
		},
		{
			Name:      "raw",
			Input:     []byte("1234567890abcdefghijklmnopqrstuvwxyz0123456789"),
			wantEnc:   EncRaw,
			wantValue: sds.NewWithBytes([]byte("1234567890abcdefghijklmnopqrstuvwxyz0123456789")),
		},
		{
			Name:      "number",
			Input:     []byte("10086"),
			wantEnc:   EncInt,
			wantValue: int64(10086),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			redisObj := NewStringObject(tc.Input)
			assert.Equal(t, RedisString, redisObj.ObjType)
			assert.Equal(t, tc.wantEnc, redisObj.encoding)
			assert.Equal(t, tc.wantValue, redisObj.ptr)
		})
	}
}

func TestStringObjIntConvertRaw(t *testing.T) {
	redisObj := NewStringObject([]byte("10086"))
	assert.Equal(t, RedisString, redisObj.ObjType)
	assert.Equal(t, EncInt, redisObj.encoding)
	assert.Equal(t, int64(10086), redisObj.ptr)
	StringObjIntConvertRaw(redisObj, nil)
	assert.Equal(t, RedisString, redisObj.ObjType)
	assert.Equal(t, EncRaw, redisObj.encoding)
	assert.Equal(t, sds.NewWithBytes([]byte("10086")), redisObj.ptr)

	redisObj = NewStringObject([]byte("10086"))
	StringObjIntConvertRaw(redisObj, []byte("hello"))
	assert.Equal(t, RedisString, redisObj.ObjType)
	assert.Equal(t, EncRaw, redisObj.encoding)
	assert.Equal(t, sds.NewWithBytes([]byte("10086hello")), redisObj.ptr)
}
