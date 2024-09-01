package redis

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Reply interface {
	WriteTo(client *Client) error
	ToBytes() []byte
}

// ErrReply 错误信息
type ErrReply interface {
	Reply
	Error() string
}

func IsErrorReply(reply Reply) bool {
	_, ok := reply.(ErrReply)
	return ok
}

type SimpleReply struct {
	Arg []byte
}

func (s *SimpleReply) WriteTo(client *Client) error {
	if _, err := client.Write(s.ToBytes()); err != nil {
		return err
	}
	return client.Flush()
}

func (s *SimpleReply) ToBytes() []byte {
	return []byte(fmt.Sprintf("+%s%s", string(s.Arg), CRLF))
}

func MakeSimpleReply(arg []byte) *SimpleReply {
	return &SimpleReply{Arg: arg}
}

type IntReply struct {
	Num int64
}

func (s *IntReply) WriteTo(client *Client) error {

	if _, err := client.Write(smallTypeLineWithNum(':', int(s.Num))); err != nil {
		return err
	}

	return client.Flush()
}

func (s *IntReply) ToBytes() []byte {
	return smallTypeLineWithNum(':', int(s.Num))
}

func MakeIntReply(num int64) *IntReply {
	return &IntReply{
		Num: num,
	}
}

// BulkReply 命令的返回值
type BulkReply struct {
	Arg []byte
}

func smallTypeLineWithNum(ttype byte, num int) []byte {
	numStr := strconv.Itoa(num)
	result := make([]byte, 0, 1+len(numStr)+2)
	result = append(result, ttype)
	result = append(result, numStr...)
	result = append(result, '\r', '\n')
	return result
}

func (b *BulkReply) WriteTo(client *Client) error {
	if b.Arg == nil {
		return MakeNullBulkReply().WriteTo(client)
	}
	if _, err := client.Write(b.ToBytes()); err != nil {
		return err
	}
	return client.Flush()
}

func (b *BulkReply) ToBytes() []byte {
	if b.Arg == nil {
		return nullBulkReplyBytes
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

type MultiBulkHeaderReply struct {
	Num int64
}

func (m *MultiBulkHeaderReply) WriteTo(client *Client) error {
	if _, err := client.Write(smallTypeLineWithNum('*', int(m.Num))); err != nil {
		return err
	}
	return nil
}

func MakeMultiBulkHeaderReply(num int64) *MultiBulkHeaderReply {
	return &MultiBulkHeaderReply{num}
}

func (m *MultiBulkHeaderReply) ToBytes() []byte {
	return smallTypeLineWithNum('*', int(m.Num))
}

type MultiBulkReply struct {
	Args [][]byte
}

func (m *MultiBulkReply) WriteTo(client *Client) error {
	if m.Args == nil {
		return MakeNullBulkReply().WriteTo(client)
	}

	if _, err := client.Write(smallTypeLineWithNum('*', len(m.Args))); err != nil {
		return err
	}

	for _, arg := range m.Args {
		if _, err := client.Write(MakeBulkReply(arg).ToBytes()); err != nil {
			return err
		}
	}
	return client.Flush()
}

func (m *MultiBulkReply) ToBytes() []byte {
	if m.Args == nil {
		return nullBulkReplyBytes
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

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

type MultiRowReply struct {
	replies []Reply
}

func (ml *MultiRowReply) WriteTo(client *Client) error {
	if ml.replies == nil {
		return MakeNullBulkReply().WriteTo(client)
	}
	argLen := len(ml.replies)
	if _, err := client.Write(smallTypeLineWithNum('*', argLen)); err != nil {
		return err
	}
	for _, reply := range ml.replies {
		err := reply.WriteTo(client)
		if err != nil {
			return err
		}
	}
	return client.Flush()
}

func (ml *MultiRowReply) ToBytes() []byte {
	if ml.replies == nil {
		return nullBulkReplyBytes
	}
	argLen := len(ml.replies)
	var buf bytes.Buffer
	buf.Write(smallTypeLineWithNum('*', argLen))
	for _, reply := range ml.replies {
		buf.Write(reply.ToBytes())
	}
	return buf.Bytes()
}

func MakeMultiRowReply(replies []Reply) *MultiRowReply {
	return &MultiRowReply{
		replies: replies,
	}
}

type StandardErrReply struct {
	Status string
}

func (s *StandardErrReply) WriteTo(client *Client) error {
	if _, err := client.Write(s.ToBytes()); err != nil {
		return err
	}
	return client.Flush()
}

func (s *StandardErrReply) ToBytes() []byte {
	bufLen := 3 + len(s.Status)
	buff := make([]byte, 0, bufLen)
	buff = append(buff, []byte{'-'}...)
	buff = append(buff, []byte(s.Status)...)
	buff = append(buff, CRLFBytes...)
	return buff
}

func (s *StandardErrReply) Error() string {
	return s.Status
}

func MakeStandardErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

type NumberOfArgsErrReply struct {
	cmdName string
}

func (n *NumberOfArgsErrReply) WriteTo(client *Client) error {
	errMsg := fmt.Sprintf("ERR wrong number of arguments for '%s' command", n.cmdName)
	return MakeStandardErrReply(errMsg).WriteTo(client)
}

func (n *NumberOfArgsErrReply) ToBytes() []byte {
	errMsg := fmt.Sprintf("ERR wrong number of arguments for '%s' command", n.cmdName)
	return MakeStandardErrReply(errMsg).ToBytes()
}

func MakeNumberOfArgsErrReply(cmdName string) *NumberOfArgsErrReply {
	return &NumberOfArgsErrReply{cmdName: cmdName}
}

type UnknownCommandReply struct {
	cmdName string
	args    []string
}

func (u *UnknownCommandReply) WriteTo(client *Client) error {
	var errMsg string
	if u.args != nil {
		argJoin := strings.Join(u.args, ", ")
		errMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: %v", u.cmdName, argJoin)
	} else {
		errMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: ", u.cmdName)
	}
	return MakeStandardErrReply(errMsg).WriteTo(client)
}

func (u *UnknownCommandReply) ToBytes() []byte {
	var errMsg string
	if u.args != nil {
		argJoin := strings.Join(u.args, ", ")
		errMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: %v", u.cmdName, argJoin)
	} else {
		errMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: ", u.cmdName)
	}
	return MakeStandardErrReply(errMsg).ToBytes()
}

func MakeUnknownCommand(cmdName string, args ...string) *UnknownCommandReply {
	return &UnknownCommandReply{
		cmdName: cmdName,
		args:    args,
	}
}
