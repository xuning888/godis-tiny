package mnetpoll

import (
	"bytes"
	"context"
	"errors"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/cloudwego/netpoll"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"strconv"
)

type Payload struct {
	Data  redis.Reply
	Error error
}

func ParseFromStream(ctx context.Context, conn netpoll.Connection) chan *Payload {
	ch := make(chan *Payload)
	go parse0(ctx, conn, ch)
	return ch
}

func parse0(ctx context.Context, conn netpoll.Connection, ch chan<- *Payload) {
	for {
		if !conn.IsActive() {
			return
		}
		reader := conn.Reader()
		line, err := reader.Until('\n')
		if err != nil {
			ch <- &Payload{Error: err}
			close(ch)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		copyLine := make([]byte, length)
		copy(copyLine, line)
		// 去除末尾的 \r\n
		copyLine = bytes.TrimSuffix(copyLine, []byte{'\r', '\n'})
		cmd := copyLine[0]
		switch cmd {
		case '+':
			ch <- &Payload{
				Data: protocol.MakeSimpleReply(copyLine[1:]),
			}
		case '-':
			ch <- &Payload{
				Data: protocol.MakeStandardErrReply(string(copyLine[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(copyLine[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(copyLine[1:]))
				continue
			}
			ch <- &Payload{Data: protocol.MakeIntReply(value)}
		case '$':
			err = parseBulkString(copyLine, reader, ch)
			if err != nil {
				ch <- &Payload{Error: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(copyLine, reader, ch)
			if err != nil {
				ch <- &Payload{Error: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

func parseBulkString(header []byte, reader netpoll.Reader, ch chan<- *Payload) error {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	} else {
		var body []byte = nil
		// strLen + 2 是为了把 \r\n读走
		body, err = reader.Next(int(strLen + 2))
		if err != nil {
			return err
		}
		// \r\n 不属于数据的部分, 所以要把 \r\n 干掉， body[:len(body)-2]
		copyBody := make([]byte, len(body)-2)
		copy(copyBody, body[:len(body)-2])
		ch <- &Payload{
			Data: protocol.MakeBulkReply(copyBody),
		}
		return nil
	}
}

func parseArray(header []byte, reader netpoll.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.Until('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		copyLine := make([]byte, length)
		copy(copyLine, line)
		var strLen int64
		strLen, err = strconv.ParseInt(string(copyLine[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(copyLine))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body, err := reader.Next(int(strLen + 2))
			if err != nil {
				return err
			}
			copyBody := make([]byte, len(body)-2)
			copy(copyBody, body[:len(body)-2])
			lines = append(lines, copyBody)
		}
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Error: err}
}

func stopOnError(ctx context.Context, reader netpoll.Reader, ch chan<- *Payload, err error) {
	defer func() {
		releaseErr := reader.Release()
		if releaseErr != nil {
			logger.Errorf("parser stop on error, reader release has error: %v", err)
		}
	}()
	ch <- &Payload{Error: err}
	close(ch)
}
