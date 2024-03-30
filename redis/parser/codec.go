package parser

import (
	"bytes"
	"errors"
	"fmt"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"strconv"
)

type buffer []byte

func (b buffer) isReadable() bool {
	return len(b) > 0
}

func (b buffer) isReadableWithN(n int) bool {
	return len(b) >= n
}

func (b buffer) readByte() byte {
	return b[0]
}

func (b buffer) readableBytes() int {
	return len(b)
}

type DecodeFunc func() ([]byte, error)

// Codec RESP2协议的codec
// 参考了 netty RedisDecoder类的实现。
// 对于redis服务端来说, 只需关注客户端发送来的 *(Array)和$(Bulk)。
// 省去了对 +(simpleString) -(ERR) :(Integer)的支持。
// + - : 都被认为是inline，然后返回 -ERR Protocol Unknown command <inline> with: <args> 给客户端，随后关闭连接。
// todo buf的初始长度是64KB, 当bulk的长度是512M时, 会因为半包导致buf出现大量的扩容. 例如: set key value(512M) 可能占用2G内存。
type Codec struct {
	// buf 缓存从客户端接收到的数据
	buf buffer
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

func (c *Codec) Decode(data []byte) ([]redis.Reply, error) {
	c.buf = append(c.buf, data...)
	replies := make([]redis.Reply, 0)

	for c.buf.isReadable() {
		decode, err := c.getDecode()
		if err != nil {
			return nil, handleDecodeError(err, c)
		}

		line, err := decode()
		if err != nil {
			return nil, handleDecodeError(err, c)
		}

		if line != nil {
			replies = appendReply(line, replies, c)
		}
	}
	return replies, nil
}

func appendReply(reply []byte, replies []redis.Reply, c *Codec) []redis.Reply {
	if c.decodeForArray {
		c.argsBuf = append(c.argsBuf, reply)

		if c.remainingBulkCount > 0 {
			c.remainingBulkCount--
		}

		if c.remainingBulkCount <= 0 {
			c.decodeForArray = false
			replies = append(replies, protocol.MakeMultiBulkReply(c.argsBuf))
			c.argsBuf = make([][]byte, 0)
		}
	} else {
		message := protocol.MakeSimpleReply(reply)
		replies = append(replies, message)
	}
	return replies
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

func (c *Codec) decodeType() ([]byte, error) {
	if !c.buf.isReadable() {
		return nil, ErrIncompletePacket
	}
	// 查看第一个字节
	b := c.buf.readByte()
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

func (c *Codec) decodeInline() ([]byte, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	c.resetDecoder()
	return line, nil
}

func (c *Codec) decodeLength() ([]byte, error) {
	line, err := c.readLine()
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
		bulkLine, err := c.decodeBulkString()
		return bulkLine, err
	default:
		return nil, nil
	}
}

func (c *Codec) decodeBulkString() ([]byte, error) {
	readableBytes := c.buf.readableBytes()
	if readableBytes == 0 {
		c.state = DecodeBulkStringContent
		return nil, ErrIncompletePacket
	}

	if readableBytes < c.remainingBulkLength+2 {
		c.state = DecodeBulkStringContent
		return nil, ErrIncompletePacket
	}

	var line []byte
	if c.remainingBulkLength == 0 {
		line = make([]byte, 0)
	} else {
		line = make([]byte, c.remainingBulkLength)
		copy(line, c.buf[:c.remainingBulkLength])
		c.buf = c.buf[c.remainingBulkLength:]
	}
	if err := c.readEndOfLine(); err != nil {
		return nil, err
	}
	c.resetDecoder()
	return line, nil
}

func (c *Codec) readLine() ([]byte, error) {
	// $0\r\n\r\n
	if !c.buf.isReadableWithN(2) {
		return nil, ErrIncompletePacket
	}
	index := bytes.Index(c.buf, []byte{'\n'})
	if index < 0 {
		// 没有读取到有效的line
		return nil, ErrIncompletePacket
	}
	crIndex := index - 1
	data := make([]byte, crIndex)
	copy(data, c.buf[:crIndex])
	c.buf = c.buf[crIndex:]
	// 再次检查，然后扔掉已经读取的部分
	if err := c.readEndOfLine(); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Codec) readEndOfLine() error {
	if c.buf.isReadableWithN(2) && c.buf[0] == '\r' && c.buf[1] == '\n' {
		c.buf = c.buf[2:]
		return nil
	}
	return NewErrProtocol(fmt.Sprintf("expected: '\\r\\n',got'%v, %v'", c.buf[0], c.buf[1]))
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
	c.buf = make(buffer, 0, 1<<16)
	c.argsBuf = make([][]byte, 0)
}

func NewCodec() *Codec {
	return &Codec{
		buf:     make(buffer, 0, 1<<16),
		argsBuf: make([][]byte, 0),
	}
}
