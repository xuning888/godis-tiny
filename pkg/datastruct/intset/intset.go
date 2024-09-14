package intset

import (
	"encoding/binary"
	"errors"
	"math"
)

type EncodingType int

const (
	EncInt16 EncodingType = iota
	EncInt32
	EncInt64
)

type RangeFunc func(index int, value int64) bool

var (
	ErrOutOfBounds = errors.New("index out of bounds")
)

type IntSet struct {
	encoding EncodingType
	length   int
	contents []byte
}

func NewIntSet(encoding EncodingType) *IntSet {
	return &IntSet{
		encoding: encoding,
		length:   0,
		contents: make([]byte, 0),
	}
}

func (is *IntSet) Add(value int64) int64 {
	// 查看value适合的编码
	valEnc := is.valueEncoding(value)
	if valEnc > is.encoding {
		is.upgradeAndAdd(value)
		return 1
	}
	insterPos, exists := is.Search(value)
	if exists {
		return 0
	}
	// 每个ele占用的字节数
	bytePerEle := is.byteSize()
	// 计算value在物理空间中占用的长度
	offset := bytePerEle * insterPos
	newContents := make([]byte, (is.length+1)*bytePerEle)
	copy(newContents, is.contents[:offset])
	copy(newContents[offset+bytePerEle:], is.contents[offset:])
	buf := encodingValue(value, is.encoding)
	copy(newContents[offset:], buf)
	is.contents = newContents
	is.length += 1
	return 1
}

func (is *IntSet) Remove(value int64) (success bool) {
	pos, exists := is.Search(value)
	if !exists {
		return true
	}
	bytePerEle := is.byteSize()
	offset := pos * bytePerEle
	limit := offset + bytePerEle
	newContents := make([]byte, len(is.contents)-bytePerEle)
	copy(newContents, is.contents[:offset])
	copy(newContents[offset:], is.contents[limit:])
	is.contents = newContents
	is.length -= 1
	return true
}

func (is *IntSet) Contains(value int64) (exists bool) {
	_, exists = is.Search(value)
	return
}

func (is *IntSet) Search(value int64) (pos int, exists bool) {
	// the value can never be found when the set is empty
	if is.length == 0 {
		return 0, false
	} else {
		las, _ := is.getAt(is.length - 1)
		if value > las {
			pos, exists = is.length, false
			return
		}
		fis, _ := is.getAt(0)
		if value < fis {
			pos, exists = 0, false
			return
		}
	}

	l, r, m := 0, is.length-1, -1
	var cur int64 = -1
	for l <= r {
		m = l + (r-l)>>1
		cur, _ = is.getAt(m)
		if value > cur {
			l = m + 1
		} else if value < cur {
			r = m - 1
		} else {
			break
		}
	}

	if value == cur {
		return m, true
	} else {
		return l, false
	}
}

func (is *IntSet) upgradeAndAdd(value int64) {
	valenc := is.valueEncoding(value)
	prepend := false
	if value < 0 {
		prepend = true
	}
	is.changeEncoding(valenc)
	if prepend {
		is.contents = append(encodingValue(value, valenc), is.contents...)
	} else {
		is.contents = append(is.contents, encodingValue(value, valenc)...)
	}
	is.length++
}

func (is *IntSet) changeEncoding(newEncoding EncodingType) {
	newContents := make([]byte, 0)
	for i := 0; i < is.length; i++ {
		value, _ := is.getAt(i)
		buf := encodingValue(value, newEncoding)
		newContents = append(newContents, buf...)
	}
	is.contents = newContents
	is.encoding = newEncoding
}

func encodingValue(value int64, encoding EncodingType) []byte {
	var buf []byte
	switch encoding {
	case EncInt16:
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(int16(value)))
	case EncInt32:
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(int32(value)))
	case EncInt64:
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(value))
	}
	return buf
}

func (is *IntSet) Elements() []int64 {
	if is.length == 0 {
		return make([]int64, 0)
	}
	result := make([]int64, is.length)
	for i := 0; i < is.length; i++ {
		value, _ := is.getAt(i)
		result[i] = value
	}
	return result
}

func (is *IntSet) getAt(index int) (int64, error) {
	if index < 0 || index >= is.length {
		return 0, ErrOutOfBounds
	}
	bytePerEle := is.byteSize()
	offset := index * bytePerEle
	var value int64
	switch is.encoding {
	case EncInt16:
		value = int64(int16(binary.LittleEndian.Uint16(is.contents[offset : offset+2])))
	case EncInt32:
		value = int64(int32(binary.LittleEndian.Uint32(is.contents[offset : offset+4])))
	case EncInt64:
		value = int64(binary.LittleEndian.Uint64(is.contents[offset : offset+8]))
	}
	return value, nil
}

func (is *IntSet) byteSize() int {
	switch is.encoding {
	case EncInt16:
		return 2
	case EncInt32:
		return 4
	case EncInt64:
		return 8
	default:
		return 0
	}
}

func (is *IntSet) valueEncoding(value int64) EncodingType {
	var result = EncInt16
	if value > math.MaxInt16 || value < math.MinInt16 {
		result = EncInt32
	}
	if value > math.MaxInt32 || value < math.MinInt32 {
		result = EncInt64
	}
	return result
}

func (is *IntSet) Len() int {
	return is.length
}

func (is *IntSet) Range(fun RangeFunc) {
	if is.length == 0 {
		return
	}
	for i := 0; i < is.length; i++ {
		val, _ := is.getAt(i)
		if !fun(i, val) {
			break
		}
	}
}
