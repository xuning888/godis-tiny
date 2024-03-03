package parser

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"io"
	"strconv"
)

type Payload struct {
	Data  redis.Reply
	Error error
}

func makePayload(data redis.Reply, err error) *Payload {
	return &Payload{
		Data:  data,
		Error: err,
	}
}

func Decode(conn gnet.Conn) *Payload {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return makePayload(nil, err)
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		copyLine := make([]byte, length)
		copy(copyLine, line)
		copyLine = bytes.TrimSuffix(copyLine, []byte{'\r', '\n'})
		cmd := copyLine[0]
		switch cmd {
		case '+':
			return makePayload(protocol.MakeSimpleReply(copyLine[1:]), nil)
		case '-':
			return makePayload(protocol.MakeStandardErrReply(string(copyLine[1:])), nil)
		case ':':
			value, err := strconv.ParseInt(string(copyLine[1:]), 10, 64)
			if err != nil {
				return protocolErrPayload("illegal number " + string(copyLine[1:]))
			}
			return makePayload(protocol.MakeIntReply(value), nil)
		case '$':
			strLen, err := strconv.ParseInt(string(copyLine[1:]), 10, 64)
			if err != nil || strLen < -1 {
				return protocolErrPayload("illegal bulk string header: " + string(copyLine))
			} else if strLen == -1 {
				return makePayload(protocol.MakeNullBulkReply(), nil)
			} else {
				body := make([]byte, strLen+2)
				_, err = io.ReadFull(reader, body)
				if err != nil {
					return makePayload(nil, err)
				}
				return makePayload(protocol.MakeBulkReply(body[:len(body)-2]), nil)
			}
		case '*':
			payload, err := parseArray(copyLine, reader)
			if err != nil {
				return makePayload(nil, err)
			}
			return payload
		default:
			args := bytes.Split(copyLine, []byte{' '})
			return makePayload(protocol.MakeMultiBulkReply(args), nil)
		}
	}
}

func parseArray(header []byte, reader *bufio.Reader) (*Payload, error) {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		return protocolErrPayload("illegal number " + string(header[1:])), nil
	} else if nStrs == 0 {
		return makePayload(protocol.MakeEmptyMultiBulkReply(), nil), nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			return protocolErrPayload("illegal bulk string header " + string(line)), nil
		}
		copyLine := make([]byte, length)
		copy(copyLine, line)
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			return protocolErrPayload("illegal number " + string(line)), nil
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return nil, err
			}
			lines = append(lines, body[:len(body)-2:len(body)-2])
		}
	}
	return makePayload(protocol.MakeMultiBulkReply(lines), nil), nil
}

func protocolErrPayload(errMsg string) *Payload {
	return makePayload(nil, errors.New("protocol error: "+errMsg))
}
