package redis

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/panjf2000/gnet/v2"
	"strconv"
)

type DecodeFunc func(conn gnet.Conn) ([]byte, error)

// Codec RESP2协议的codec
// 参考了 netty RedisDecoder类的实现。
// 对于redis服务端来说, 只需关注客户端发送来的 *(Array)和$(Bulk)。
// 省去了对 +(simpleString) -(ERR) :(Integer)的支持。
// + - : 都被认为是inline，然后返回 -ERR Protocol Unknown command <inline> with: <args> 给客户端，随后关闭连接。
type Codec struct {
	// state codec 当前需要处理的状态
	state State
	// messageType 当前需要处理的消息类型
	messageType *MessageType
	// remainingBulkLength 如果当前处理的消息类型是bulkString, 用于记录bulkString 所占的字节数。
	remainingBulkLength int
	// remainingBulkCount 如果当前处理的是数组，用于记录数组的长度
	remainingBulkCount int
	// decodeForArray 标记当前是否正在处理数组
	decodeForArray bool
	// argsBuf 缓存已经解码的数据
	argsBuf [][]byte
}

func (c *Codec) Decode(conn gnet.Conn, commands *list.List) error {
	for conn.InboundBuffered() > 0 {
		decode, err2 := c.getDecode()
		if err2 != nil {
			return handleDecodeError(err2, c)
		}

		line, err3 := decode(conn)
		if err3 != nil {
			return handleDecodeError(err3, c)
		}
		if line != nil {
			appendReply(line, commands, c)
		}
	}
	return nil
}

func appendReply(reply []byte, commands *list.List, c *Codec) {
	if c.decodeForArray {
		c.argsBuf = append(c.argsBuf, reply)

		if c.remainingBulkCount > 0 {
			c.remainingBulkCount--
		}

		if c.remainingBulkCount <= 0 {
			c.decodeForArray = false
			commands.PushBack(c.argsBuf)
			c.argsBuf = make([][]byte, 0)
		}
	} else {
		commands.PushBack([][]byte{reply})
	}
}
func handleDecodeError(err error, c *Codec) error {
	// 如果err是 黏包/半包 就直接返回，否则就把接收到的包丢弃
	if errors.Is(err, ErrIncompletePacket) {
		return err
	}
	c.Reset()
	return err
}

func (c *Codec) getDecode() (DecodeFunc, error) {
	switch c.state {
	case DecodeType:
		return c.decodeType, nil
	case DecodeInline:
		return c.decodeInline, nil
	case DecodeBulkStringContent:
		return c.decodeBulkString, nil
	case DecodeLength:
		return c.decodeLength, nil
	default:
		return nil, errors.New("unhandled state")
	}
}

func (c *Codec) decodeType(conn gnet.Conn) ([]byte, error) {
	n := conn.InboundBuffered()
	if n == 0 {
		return nil, ErrIncompletePacket
	}
	buf, err := conn.Peek(1)
	if err != nil {
		return nil, err
	}
	// 查看第一个字节
	b := buf[0]
	c.messageType = valueOf(b)
	if c.messageType.isInline() {
		c.state = DecodeInline
	} else {
		c.state = DecodeLength
	}
	if c.messageType == ArrayHeader {
		c.decodeForArray = true
	}
	return nil, nil
}

func (c *Codec) decodeInline(conn gnet.Conn) ([]byte, error) {
	line, err := c.readLine(conn)
	if err != nil {
		return nil, err
	}
	c.resetDecoder()
	return line, nil
}

func (c *Codec) decodeLength(conn gnet.Conn) ([]byte, error) {
	line, err := c.readLine(conn)
	if err != nil {
		return nil, err
	}
	length, err := c.parserNumber(line)
	if err != nil {
		return nil, err
	}
	switch c.messageType {
	case ArrayHeader:
		// 记录下这个array需要解码的bulk
		c.remainingBulkCount = int(length)
		c.resetDecoder()
		return nil, nil
	case BulkString:
		if length > RedisMessageMaxLength {
			return nil, NewErrProtocol("invalid bulk length")
		}
		c.remainingBulkLength = int(length)
		bulkLine, err := c.decodeBulkString(conn)
		return bulkLine, err
	default:
		return nil, nil
	}
}

func (c *Codec) decodeBulkString(conn gnet.Conn) ([]byte, error) {
	n := conn.InboundBuffered()
	if n == 0 {
		c.state = DecodeBulkStringContent
		return nil, ErrIncompletePacket
	}

	readableBytes := n
	if readableBytes < c.remainingBulkLength+2 {
		c.state = DecodeBulkStringContent
		return nil, ErrIncompletePacket
	}

	var line []byte
	if c.remainingBulkLength == 0 {
		line = make([]byte, 0)
	} else {
		buf, err := conn.Peek(c.remainingBulkLength)
		if err != nil {
			return nil, err
		}
		line = make([]byte, c.remainingBulkLength)
		copy(line, buf[:c.remainingBulkLength])
		if _, err2 := conn.Discard(c.remainingBulkLength); err2 != nil {
			return nil, err2
		}
	}
	if err2 := c.readEndOfLine(conn); err2 != nil {
		return nil, err2
	}
	c.resetDecoder()
	return line, nil
}

func (c *Codec) readLine(conn gnet.Conn) ([]byte, error) {
	// $0\r\n\r\n
	buf, err := conn.Peek(2)
	if err != nil || len(buf) < 2 {
		return nil, ErrIncompletePacket
	}

	buff, index, err := peekBytes(conn, '\n')
	if err != nil {
		// 没有读取到有效的line
		return nil, ErrIncompletePacket
	}

	crIndex := index - 1
	data := make([]byte, crIndex)
	copy(data, buff[:crIndex])
	if _, err3 := conn.Discard(len(data)); err3 != nil {
		return nil, err3
	}
	if err2 := c.readEndOfLine(conn); err2 != nil {
		return nil, err2
	}
	return data, nil
}

func peekBytes(conn gnet.Conn, b byte) ([]byte, int, error) {
	offset := 0
	for {
		buf, err := conn.Peek(offset + 1) // 从偏移位置读取切片并增加长度
		if err != nil {
			return nil, 0, err
		}

		// 检查我们刚刚读取的最后一个字节
		if buf[offset] == b {
			return buf, offset, nil
		}

		offset++ // 如果不是要找的字节，则继续增加偏移量
	}
}

func (c *Codec) readEndOfLine(conn gnet.Conn) error {
	buf, err := conn.Peek(2)
	if err == nil && len(buf) == 2 && buf[0] == '\r' && buf[1] == '\n' {
		if _, err2 := conn.Discard(2); err2 != nil {
			return err2
		}
		return nil
	}
	return NewErrProtocol(fmt.Sprintf("expected: '\\r\\n',got'%v, %v'", buf[0], buf[1]))
}

func (c *Codec) parserNumber(buf []byte) (int64, error) {
	numberBytes := buf[1:]
	number, err := strconv.ParseInt(string(numberBytes), 10, 64)
	if err != nil {
		return 0, NewErrProtocol("illegal number " + string(buf))
	}
	return number, nil
}

func (c *Codec) resetDecoder() {
	c.state = DecodeType
	c.remainingBulkLength = 0
}

func (c *Codec) Reset() {
	c.resetDecoder()
	c.argsBuf = make([][]byte, 0)
}

func NewCodec() *Codec {
	return &Codec{
		argsBuf: make([][]byte, 0),
	}
}
