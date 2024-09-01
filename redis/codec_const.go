package redis

import (
	"errors"
	"fmt"
)

var (
	ErrIncompletePacket         = errors.New("incomplete packet")
	RedisMessageMaxLength int64 = 512 * 1024 * 1024
)

type State int

const (
	DecodeType State = iota
	DecodeInline
	DecodeLength
	DecodeBulkStringContent
)

type ErrProtocol struct {
	msg string
}

func (e *ErrProtocol) Error() string {
	return fmt.Sprintf("ERR Protocol error: %s", e.msg)
}

func NewErrProtocol(msg string) *ErrProtocol {
	return &ErrProtocol{
		msg: msg,
	}
}

type MessageType struct {
	value  byte
	inline bool
}

var (
	UnknownCommand = &MessageType{value: '@', inline: true}
	BulkString     = &MessageType{value: '$', inline: false}
	ArrayHeader    = &MessageType{value: '*', inline: false}
)

func (m *MessageType) isInline() bool {
	return m.inline
}

func valueOf(b byte) *MessageType {
	switch b {
	case '$':
		return BulkString
	case '*':
		return ArrayHeader
	default:
		return UnknownCommand
	}
}
