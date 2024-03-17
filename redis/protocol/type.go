package protocol

import (
	"bytes"
	"fmt"
	"godis-tiny/interface/redis"
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
	//return []byte(fmt.Sprintf("$%d%s%s%s", len(b.Arg), CRLF, string(b.Arg), CRLF))
	argg := make([]byte, 0, len(b.Arg)+20)
	argg = append(argg, []byte(fmt.Sprintf("$%d", len(b.Arg)))...)
	argg = append(argg, CRLFBytes...)
	argg = append(argg, b.Arg...)
	argg = append(argg, CRLFBytes...)
	return argg
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
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d%s", argLen, CRLF))
	for _, row := range m.Args {
		buf.Write(MakeBulkReply(row).ToBytes())
	}
	return buf.Bytes()
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
	return []byte("-" + s.Status + CRLF)
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
