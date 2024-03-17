package parser

import (
	"errors"
	"fmt"
)

var (
	ErrIncompletePacket             = errors.New("incomplete packet")
	ErrDisableDecodeInlineCmd       = errors.New("decoding of inline commands is disabled")
	RedisMessageMaxLength     int64 = 512 * 1024 * 1024
)

type State int

const (
	DecodeType State = iota
	DecodeInline
	DecodeLength
	DecodeBulkStringEol
	DecodeBulkStringContent
)

type ErrUnKnowState struct {
	state State
}

func (e *ErrUnKnowState) Error() string {
	return fmt.Sprintf("unknown state: %v", e.state)
}

func NewErrUnKnowState(state State) error {
	return &ErrUnKnowState{
		state: state,
	}
}

type ErrProtocol struct {
	msg string
}

func (e *ErrProtocol) Error() string {
	return fmt.Sprintf("protocol error: %s", e.msg)
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

func (m *MessageType) length() int {
	if m.value == '@' {
		return 1
	}
	return 0
}

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
