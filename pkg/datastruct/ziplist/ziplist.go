package ziplist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

const (
	maxInt24 = 8388607
	minInt24 = -8388608
	zlEnd    = 255
)

const (
	encInt8Embed = 0xF0
	encInt8      = 0xFE
	encInt16     = 0xC0
	encInt24     = 0xF0
	encInt32     = 0xD0
	encInt64     = 0xE0
)

var (
	ErrorTooLarge   = errors.New("too large")
	ErrorOutOfRange = errors.New("out of range")
)

// ZipList
// 参考这篇文章做的实现: http://zhangtielei.com/posts/blog-redis-ziplist.html
type ZipList struct {
	buff *bytes.Buffer
}

func NewZipList() *ZipList {
	zl := &ZipList{
		buff: bytes.NewBuffer(make([]byte, 11)),
	}
	zl.setZlBytes(11)
	zl.setZlTail(10)
	zl.setZlLen(0)
	zl.buff.Bytes()[10] = zlEnd
	return zl
}

func (zl *ZipList) PushBack(data []byte) error {
	oldZlBytes, oldZlTail := zl.zlBytes(), zl.zlTail()
	prevLen := oldZlBytes - oldZlTail - 1
	entry, err := encode(prevLen, data)
	if err != nil {
		return err
	}

	zl.buff.Truncate(zl.buff.Len() - 1)
	zl.buff.Write(entry)
	zl.buff.WriteByte(zlEnd)

	zl.setZlBytes(uint32(zl.buff.Len()))
	zl.setZlTail(uint32(oldZlBytes - 1))
	zl.setZlLen(uint16(zl.zlLen() + 1))
	return nil
}

func (zl *ZipList) Index(index int) ([]byte, error) {
	zlLen := zl.zlLen()
	if index < 0 || index >= zlLen {
		return nil, ErrorOutOfRange
	}
	b := zl.buff.Bytes()
	datas := b[10:]
	pos := 0
	for i := 0; i <= index; i++ {
		prevLen := int(datas[pos])
		if prevLen == 0 || prevLen < 254 {
			pos++
		} else {
			pos++
			if pos+4 < len(datas) {
				prevLen = int(binary.LittleEndian.Uint32(datas[pos : pos+4]))
				pos += 4
			}
		}
		reqLen := datas[pos]
		if (reqLen >> 4) == 0x00 {
			reqLen = reqLen & 0x3f
			pos++
			if i == index {
				return append([]byte{}, datas[pos:pos+int(reqLen)]...), nil
			}
			pos += int(reqLen)
		} else if reqLen == encInt8 {
			pos++
			if i == index {
				byteValue := datas[pos]
				return []byte(fmt.Sprintf("%d", int8(byteValue))), nil
			}
			pos++
		} else if reqLen>>4 == 0x0F && reqLen<<4 != 0 {
			if i == index {
				reqLen = reqLen & 0x0F
				return []byte(fmt.Sprintf("%d", reqLen)), nil
			}
			pos++
		} else if reqLen == encInt16 {
			pos++
			if i == index {
				dataInt16 := int16(datas[pos])<<8 | int16(datas[pos+1])
				return []byte(fmt.Sprintf("%d", dataInt16)), nil
			}
			pos += 2
		} else if reqLen == encInt24 {
			pos++
			if i == index {
				dataInt24 := int32(datas[pos])<<16 | int32(datas[pos+1])<<8 | int32(datas[pos+2])
				// 检查符号位（第 23 位）
				if dataInt24&0x800000 != 0 {
					// 如果符号位为 1，则进行符号扩展
					dataInt24 |= ^0xFFFFFF
				}
				return []byte(fmt.Sprintf("%d", dataInt24)), nil
			}
			pos += 3
		} else if reqLen == encInt32 {
			pos++
			if i == index {
				dataInt32 := int32(datas[pos])<<24 | int32(datas[pos+1])<<16 | int32(datas[pos+2])<<8 | int32(datas[pos+3])
				return []byte(fmt.Sprintf("%d", dataInt32)), nil
			}
			pos += 4
		} else if reqLen == encInt64 {
			pos++
			if i == index {
				dataInt64 := int64(datas[pos])<<56 | int64(datas[pos+1])<<48 |
					int64(datas[pos+2])<<40 | int64(datas[pos+3])<<32 |
					int64(datas[pos+4])<<24 | int64(datas[pos+5])<<16 |
					int64(datas[pos+6])<<8 | int64(datas[pos+7])
				return []byte(fmt.Sprintf("%d", dataInt64)), nil
			}
			pos += 8
		} else if reqLen>>6 == 0x01 {
			high, low := datas[pos]&0x3F, datas[pos+1]
			length := int16(high)<<8 | int16(low)
			pos += 2
			if i == index {
				byteb := datas[pos : pos+int(length)]
				return append([]byte{}, byteb...), nil
			}
			pos += int(length)
		} else if reqLen>>7 == 0x01 {
			firstByte := datas[pos] & 0x7F
			secondByte := datas[pos+1]
			thirdByte := datas[pos+2]
			fourthByte := datas[pos+3]
			fifthByte := datas[pos+4]
			pos += 5
			length := int(firstByte)<<32 |
				int(secondByte)<<24 |
				int(thirdByte)<<16 |
				int(fourthByte)<<8 |
				int(fifthByte)
			if i == index {
				byteb := datas[pos : pos+int(length)]
				return append([]byte{}, byteb...), nil
			}
			pos += length
		}
	}
	return nil, nil
}

func (zl *ZipList) Len() int {
	return zl.zlLen()
}

func encode(prevLen int, data []byte) ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0, 6))
	if err := encodePrevLen(prevLen, buff); err != nil {
		return nil, err
	}
	if value, yes := maybeInt(data); yes {
		encodeInt(value, buff)
	} else {
		if err := encodeRaw(data, buff); err != nil {
			return nil, err
		}
	}
	return buff.Bytes(), nil
}

func encodePrevLen(prevLen int, buff *bytes.Buffer) error {
	if prevLen < 254 {
		if err := binary.Write(buff, binary.LittleEndian, byte(prevLen)); err != nil {
			return err
		}
	} else {
		buff.WriteByte(254)
		if err := binary.Write(buff, binary.LittleEndian, uint32(prevLen)); err != nil {
			return err
		}
	}
	return nil
}

func encodeInt(value int, buff *bytes.Buffer) {
	if value >= math.MinInt8 && value <= math.MaxInt8 {
		if value >= 0 && value <= 12 {
			buff.WriteByte(encInt8Embed | byte(value))
		} else {
			buff.WriteByte(encInt8)
			buff.WriteByte(byte(value))
		}
	} else if value >= math.MinInt16 && value <= math.MaxInt16 {
		buff.WriteByte(encInt16)
		buff.WriteByte(byte(value >> 8))
		buff.WriteByte(byte(value & 0xFF))
	} else if value >= minInt24 && value <= maxInt24 {
		buff.WriteByte(encInt24)
		buff.WriteByte(byte(value >> 16))
		buff.WriteByte(byte(value >> 8))
		buff.WriteByte(byte(value))
	} else if value >= math.MinInt32 && value <= math.MaxInt32 {
		buff.WriteByte(encInt32)
		buff.WriteByte(byte(value >> 24))
		buff.WriteByte(byte(value >> 16))
		buff.WriteByte(byte(value >> 8))
		buff.WriteByte(byte(value))
	} else if value >= math.MinInt64 && value <= math.MaxInt64 {
		buff.WriteByte(encInt64)
		buff.WriteByte(byte(value >> 56))
		buff.WriteByte(byte(value >> 48))
		buff.WriteByte(byte(value >> 40))
		buff.WriteByte(byte(value >> 32))
		buff.WriteByte(byte(value >> 24))
		buff.WriteByte(byte(value >> 16))
		buff.WriteByte(byte(value >> 8))
		buff.WriteByte(byte(value))
	}
}

func encodeRaw(data []byte, buff *bytes.Buffer) (err error) {
	if len(data) < 63 {
		buff.WriteByte(byte(0x00 | len(data)))
		buff.Write(data)
	} else if len(data) <= ((1 << 14) - 1) {
		length := int16(len(data))
		high, low := byte(length>>8)|0x40, byte(length&0xFF)
		buff.Write([]byte{high, low})
		buff.Write(data)
	} else if len(data) <= 4294967295 {
		// 长度在16384到4294967295字节之间的情况，用五个字节表示长度。
		length := len(data)
		buff.WriteByte(byte(0x80 | (length>>32)&0xFF))
		buff.WriteByte(byte((length >> 24) & 0xFF))
		buff.WriteByte(byte((length >> 16) & 0xFF))
		buff.WriteByte(byte((length >> 8) & 0xFF))
		buff.WriteByte(byte(length & 0xFF))
		buff.Write(data)
	} else {
		err = ErrorTooLarge
	}
	return nil
}

func maybeInt(data []byte) (value int, yes bool) {
	if number, err := strconv.Atoi(string(data)); err == nil {
		return number, true
	}
	return 0, false
}

func (zl *ZipList) setZlBytes(size uint32) {
	b := zl.buff.Bytes()
	binary.LittleEndian.PutUint32(b[:4], size)
}

func (zl *ZipList) setZlTail(tail uint32) {
	b := zl.buff.Bytes()
	binary.LittleEndian.PutUint32(b[4:8], tail)
}

func (zl *ZipList) setZlLen(len uint16) {
	b := zl.buff.Bytes()
	binary.LittleEndian.PutUint16(b[8:10], len)
}

func (zl *ZipList) zlBytes() int {
	return int(binary.LittleEndian.Uint32(zl.buff.Bytes()[:4]))
}

func (zl *ZipList) zlTail() int {
	return int(binary.LittleEndian.Uint32(zl.buff.Bytes()[4:8]))
}

func (zl *ZipList) zlLen() int {
	return int(binary.LittleEndian.Uint16(zl.buff.Bytes()[8:10]))
}

func (zl *ZipList) Show() []byte {
	return zl.buff.Bytes()
}
