package protocol

import (
	"bytes"
	"fmt"
)

var (
	CRCF = "\r\n"
)

// ErrReply 错误信息
type ErrReply interface {
	ToBytes() []byte
	Error() string
}

// SimpleReply 简单字符串
type SimpleReply struct {
	Arg []byte
}

func (s *SimpleReply) ToBytes() []byte {
	return s.Arg
}

func MakeSimpleReply(arg []byte) *SimpleReply {
	return &SimpleReply{
		Arg: arg,
	}
}

// IntReply 整数
type IntReply struct {
	num int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(fmt.Sprintf(":%d%s", r.num, CRCF))
}

func MakeIntReply(num int64) *IntReply {
	return &IntReply{
		num: num,
	}
}

// BulkReply 命令的返回值
type BulkReply struct {
	Arg []byte
}

func (b *BulkReply) ToBytes() []byte {
	if b.Arg == nil {
		return NullBulkReply
	}
	return []byte(fmt.Sprintf("$%d%s%s%s", len(b.Arg), CRCF, string(b.Arg), CRCF))
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// MultiBulkReply 数组返回
type MultiBulkReply struct {
	Rows [][]byte
}

func MakeMultiBulkReply(rows [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Rows: rows,
	}
}

func (m *MultiBulkReply) ToBytes() []byte {
	if m.Rows == nil {
		return NullBulkReply
	}
	numRows := len(m.Rows)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d%s", numRows, CRCF))
	for _, row := range m.Rows {
		buf.Write(MakeBulkReply(row).ToBytes())
	}
	return buf.Bytes()
}
