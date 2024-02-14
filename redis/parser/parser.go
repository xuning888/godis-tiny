package parser

import (
	"bufio"
	"bytes"
	"errors"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"io"
	"strconv"
)

type Payload struct {
	Data  redis.Reply
	Error error
}

func ParseFromStream(reader io.Reader) chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	bufReader := bufio.NewReader(reader)
	for {
		line, err := bufReader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Error: err}
			close(ch)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		// 去除末尾的 \r\n
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		cmd := line[0]
		switch cmd {
		case '+':
			ch <- &Payload{
				Data: protocol.MakeSimpleReply(line[1:]),
			}
		case '-':
			ch <- &Payload{
				Data: protocol.MakeStandardErrReply(string(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{Data: protocol.MakeIntReply(value)}
		case '$':
			err = parseBulkString(line, bufReader, ch)
			if err != nil {
				ch <- &Payload{Error: err}
				close(ch)
				return
			}
		case '*':
			err := parseArray(line, bufReader, ch)
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

func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
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
		body := make([]byte, strLen+2)
		_, err = io.ReadFull(reader, body)
		if err != nil {
			return err
		}
		ch <- &Payload{
			Data: protocol.MakeBulkReply(body[:len(body)-2]),
		}
		return nil
	}
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
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
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
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
