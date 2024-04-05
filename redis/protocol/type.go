package protocol

import (
	"bytes"
	"fmt"
	"godis-tiny/interface/redis"
	"strconv"
	"strings"
)

var (
	CRLF      = "\r\n"
	CRLFBytes = []byte(CRLF)
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
	return []byte(fmt.Sprintf("+%s%s", string(s.Arg), CRLF))
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
	return []byte(fmt.Sprintf(":%d%s", r.num, CRLF))
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
		return NullBulkReplyBytes
	}
	argLenStr := strconv.Itoa(len(b.Arg))
	bufLen := 1 + len(argLenStr) + 2 + len(b.Arg) + 2
	buffer := make([]byte, 0, bufLen)
	buffer = append(buffer, '$')
	buffer = append(buffer, []byte(argLenStr)...)
	buffer = append(buffer, CRLFBytes...)
	buffer = append(buffer, b.Arg...)
	buffer = append(buffer, CRLFBytes...)
	return buffer
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// MultiBulkReply 数组返回
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (m *MultiBulkReply) ToBytes() []byte {
	if m.Args == nil {
		return NullBulkReplyBytes
	}
	argLen := len(m.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2
	for _, arg := range m.Args {
		if arg == nil {
			bufLen += 3 + 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
		}
	}
	buffer := make([]byte, 0, bufLen)
	buffer = append(buffer, '*')
	buffer = append(buffer, []byte(strconv.Itoa(argLen))...)
	buffer = append(buffer, CRLFBytes...)
	for _, arg := range m.Args {
		if arg == nil {
			buffer = append(buffer, []byte{'$', '-', '1'}...)
			buffer = append(buffer, CRLFBytes...)
		} else {
			buffer = append(buffer, '$')
			buffer = append(buffer, []byte(strconv.Itoa(len(arg)))...)
			buffer = append(buffer, CRLFBytes...)
			buffer = append(buffer, arg...)
			buffer = append(buffer, CRLFBytes...)
		}
	}
	return buffer
}

type MultiRowReply struct {
	replies []redis.Reply
}

func MakeMultiRowReply(replies []redis.Reply) *MultiRowReply {
	return &MultiRowReply{
		replies: replies,
	}
}

func (m *MultiRowReply) ToBytes() []byte {
	if m.replies == nil {
		return NullBulkReplyBytes
	}
	argLen := len(m.replies)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d%s", argLen, CRLF))
	for _, reply := range m.replies {
		buf.Write(reply.ToBytes())
	}
	return buf.Bytes()
}

type StandardErrReply struct {
	Status string
}

func (s *StandardErrReply) ToBytes() []byte {
	bufLen := 3 + len(s.Status)
	buff := make([]byte, 0, bufLen)
	buff = append(buff, []byte{'-'}...)
	buff = append(buff, []byte(s.Status)...)
	buff = append(buff, CRLFBytes...)
	return buff
}

func MakeStandardErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

type NumberOfArgsErrReply struct {
	cmdName string
}

func MakeNumberOfArgsErrReply(cmdName string) *NumberOfArgsErrReply {
	return &NumberOfArgsErrReply{cmdName: cmdName}
}

func (n *NumberOfArgsErrReply) ToBytes() []byte {
	errMsg := fmt.Sprintf("ERR wrong number of arguments for '%s' command", n.cmdName)
	return MakeStandardErrReply(errMsg).ToBytes()
}

func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

type UnknownCommand struct {
	cmdName string
	args    []string
}

func MakeUnknownCommand(cmdName string, args ...string) *UnknownCommand {
	return &UnknownCommand{
		cmdName: cmdName,
		args:    args,
	}
}

func (u *UnknownCommand) ToBytes() []byte {
	var errMsg string
	if u.args != nil {
		argJoin := strings.Join(u.args, ", ")
		errMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: %v", u.cmdName, argJoin)
	} else {
		errMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: ", u.cmdName)
	}
	return MakeStandardErrReply(errMsg).ToBytes()
}
