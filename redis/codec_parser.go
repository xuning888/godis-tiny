package redis

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"io"
	"log"
	"runtime/debug"
	"strconv"
)

type Payload struct {
	Data  Reply
	Error error
}

func makePayload(data Reply, err error) *Payload {
	return &Payload{
		Data:  data,
		Error: err,
	}
}

func DecodeInStream(r io.Reader) chan *Payload {
	ch := make(chan *Payload)
	go func() {
		decodeStream(r, ch)
	}()
	return ch
}

func decodeStream(r io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- makePayload(nil, err)
			close(ch)
			return
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
			ch <- makePayload(MakeSimpleReply(copyLine[1:]), nil)
		case '-':
			ch <- makePayload(MakeStandardErrReply(string(copyLine[1:])), nil)
		case ':':
			value, err := strconv.ParseInt(string(copyLine[1:]), 10, 64)
			if err != nil {
				ch <- protocolErrPayload("illegal number " + string(copyLine[1:]))
			} else {
				ch <- makePayload(MakeIntReply(value), nil)
			}
		case '$':
			strLen, err := strconv.ParseInt(string(copyLine[1:]), 10, 64)
			if err != nil || strLen < -1 {
				ch <- protocolErrPayload("illegal bulk string header: " + string(copyLine))
			} else if strLen == -1 {
				ch <- makePayload(MakeNullBulkReply(), nil)
			} else {
				body := make([]byte, strLen+2)
				_, err = io.ReadFull(reader, body)
				if err != nil {
					ch <- makePayload(nil, err)
				} else {
					ch <- makePayload(MakeBulkReply(body[:len(body)-2]), nil)
				}
			}
		case '*':
			decodeInStreamArray(copyLine[1:], reader, ch)
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- makePayload(MakeMultiBulkReply(args), nil)
		}
	}
}

func decodeInStreamArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) {
	nStrs, err := strconv.ParseInt(string(header), 10, 64)
	if err != nil || nStrs < 0 {
		ch <- protocolErrPayload("illegal number " + string(header[1:]))
		return
	} else if nStrs == 0 {
		ch <- makePayload(MakeEmptyMultiBulkReply(), nil)
		return
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			ch <- makePayload(nil, err)
			return
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			ch <- protocolErrPayload("illegal bulk string header " + string(line))
			return
		}
		copyLine := make([]byte, length)
		copy(copyLine, line)
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			ch <- protocolErrPayload("illegal number " + string(line))
			return
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				ch <- makePayload(nil, err)
				return
			}
			lines = append(lines, body[:len(body)-2:len(body)-2])
		}
	}
	ch <- makePayload(MakeMultiBulkReply(lines), nil)
}

func parseArray(header []byte, reader *bufio.Reader) (*Payload, error) {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		return protocolErrPayload("illegal number " + string(header[1:])), nil
	} else if nStrs == 0 {
		return makePayload(MakeEmptyMultiBulkReply(), nil), nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			log.Printf("read n failed with error %v\n", err)
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
			totalRead := 0
			for totalRead < len(body) {
				n, err := reader.Read(body[totalRead:])
				if err != nil && err != io.EOF {
					log.Printf("read n failed with error %v\n", err)
					return nil, err
				}
				totalRead += n
			}
			lines = append(lines, body[:len(body)-2:len(body)-2])
		}
	}
	return makePayload(MakeMultiBulkReply(lines), nil), nil
}

func protocolErrPayload(errMsg string) *Payload {
	return makePayload(nil, errors.New("protocol error: "+errMsg))
}
