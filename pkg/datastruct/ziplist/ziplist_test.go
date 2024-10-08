package ziplist

import (
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestNewZipList(t *testing.T) {
	values := []string{"-2", "name", "tielei", "age", "20"}
	zllist := NewZipList()
	size := 0
	for _, value := range values {
		b := []byte(value)
		size += len(b)
		zllist.PushBack(b)
	}
	t.Logf("size: %v", size)
	t.Logf("zlBytes: %v\n", zllist.zlBytes())
	t.Logf("zlTail: %v\n", zllist.zlTail())
	t.Logf("zlLen: %v\n", zllist.zlLen())
	t.Logf("bytes: %v\n", zllist.Show())
	for i := 0; i < zllist.Len(); i++ {
		value, _ := zllist.Index(i)
		assert.Equal(t, values[i], string(value))
	}
	values = append([]string{"1", "2"}, values...)
	zllist.PushFront([]byte("2"))
	zllist.PushFront([]byte("1"))
	for i := 0; i < zllist.Len(); i++ {
		value, _ := zllist.Index(i)
		assert.Equal(t, values[i], string(value))
	}
}

func TestPushBack_Int(t *testing.T) {
	values := []string{
		"1", "12", "20", "name", "tielei", "age", "20",
		strconv.Itoa(math.MinInt16),
		strconv.Itoa(math.MinInt16 + 1),
		strconv.Itoa(math.MaxInt16),
		strconv.Itoa(math.MaxInt16 - 1),
		strconv.Itoa(minInt24),
		strconv.Itoa(minInt24 + 1),
		strconv.Itoa(maxInt24),
		strconv.Itoa(maxInt24 - 1),
		strconv.Itoa(math.MinInt32),
		strconv.Itoa(math.MinInt32 + 1),
		strconv.Itoa(math.MaxInt32),
		strconv.Itoa(math.MaxInt32 - 1),
		strconv.Itoa(math.MinInt64),
		strconv.Itoa(math.MinInt64 + 1),
		strconv.Itoa(math.MaxInt64),
		strconv.Itoa(math.MaxInt64 - 1),
	}
	zllist := NewZipList()
	size := 0
	for _, value := range values {
		b := []byte(value)
		size += len(b)
		zllist.PushBack(b)
	}
	t.Logf("size: %v", size)
	t.Logf("zlBytes: %v\n", zllist.zlBytes())
	t.Logf("zlTail: %v\n", zllist.zlTail())
	t.Logf("zlLen: %v\n", zllist.zlLen())
	t.Logf("bytes: %v\n", zllist.Show())
	for i, expected := range values {
		actual, _ := zllist.Index(i)
		assert.Equal(t, expected, string(actual))
	}
}

func TestPushBack_Raw(t *testing.T) {
	values := []string{
		"unsigned char *ziplistNew(void);\nunsigned char *ziplistMerge(unsigned char **first, unsigned char **second);\nunsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where);",
		buildStr((1 << 14) - 1),
		buildStr(1 << 14),
	}
	size := 0
	zllist := NewZipList()
	for _, value := range values {
		b := []byte(value)
		size += len(b)
		zllist.PushBack(b)
	}
	t.Logf("size: %v", size)
	t.Logf("zlBytes: %v\n", zllist.zlBytes())
	t.Logf("zlTail: %v\n", zllist.zlTail())
	t.Logf("zlLen: %v\n", zllist.zlLen())
	for i, expected := range values {
		actual, _ := zllist.Index(i)
		assert.Equal(t, expected, string(actual))
	}
}

func buildStr(n int) string {
	sbd := strings.Builder{}
	for i := 0; i < n; i++ {
		sbd.WriteByte('a')
	}
	return sbd.String()
}
