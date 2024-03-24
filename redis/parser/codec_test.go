package parser

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/protocol"
	"testing"
)

func TestDecode(t *testing.T) {

	testCases := []struct {
		Name            string
		InputBytes      [][]byte
		expectedReplies []redis.Reply
		expectedError   error
	}{
		{
			Name: "multi bulk string",
			// set a b --> *3\r\n$3\r\nset\r\n$1\r\n$1\r\na\r\n$1\r\nb\r\n
			InputBytes: [][]byte{
				protocol.MakeMultiBulkReply(util.ToCmdLine("set", "a", "b")).ToBytes(),
			},
			expectedReplies: []redis.Reply{
				protocol.MakeMultiBulkReply(util.ToCmdLine("set", "a", "b")),
			},
			expectedError: nil,
		},
		{
			Name: "半包",
			// set a b
			InputBytes: [][]byte{
				{'*', '3', '\r'}, {'\n'},
				{'$', '3'}, {'\r', '\n'},
				{'s'}, {'e', 't', '\r', '\n'},
				{'$', '1', '\r', '\n'},
				{'a'}, {'\r'}, {'\n'},
				{'$', '1'}, {'\r', '\n'},
				{'b', '\r'}, {'\n'},
			},
			expectedReplies: []redis.Reply{
				protocol.MakeMultiBulkReply(util.ToCmdLine("set", "a", "b")),
			},
			expectedError: nil,
		},
		{
			Name: "粘包",
			// set a b
			InputBytes: [][]byte{
				{'*', '3', '\r', '\n', '$', '3'},
				{'\r', '\n', 's'},
				{'e', 't', '\r', '\n'},
				{'$', '1', '\r', '\n'},
				{'a'}, {'\r'}, {'\n'},
				{'$', '1'}, {'\r', '\n'},
				{'b', '\r'}, {'\n'},
			},
			expectedReplies: []redis.Reply{
				protocol.MakeMultiBulkReply(util.ToCmdLine("set", "a", "b")),
			},
			expectedError: nil,
		},
		{
			Name: "decode inline",
			InputBytes: [][]byte{
				{'+', 'p', 'i', 'n', 'g'}, {'\r', '\n'},
			},
			expectedReplies: []redis.Reply{
				protocol.MakeSimpleReply(util.ToCmdLine("+ping")[0]),
			},
			expectedError: nil,
		},
		{
			Name: "protocol error",
			InputBytes: [][]byte{
				{'+', 'p', 'i', 'n', 'g'}, {'\n'},
			},
			expectedReplies: []redis.Reply{
				protocol.MakeSimpleReply(util.ToCmdLine("+ping")[0]),
			},
			expectedError: NewErrProtocol(fmt.Sprintf("expected: '\\r\\n',got'%v, %v'", 'g', '\n')),
		},
		{
			Name: "error array header",
			InputBytes: [][]byte{
				{'*', 'a', '\r', '\n'},
			},
			expectedReplies: []redis.Reply{},
			expectedError:   NewErrProtocol("illegal number *a"),
		},
		{
			Name: "empty bulk",
			InputBytes: [][]byte{
				{'*', '1', '\r', '\n'},
				{'$', '0', '\r', '\n'},
				{'\r', '\n'},
			},
			expectedReplies: []redis.Reply{
				protocol.MakeMultiBulkReply([][]byte{make([]byte, 0)}),
			},
			expectedError: nil,
		},
		{
			Name: "512M bulk length",
			InputBytes: [][]byte{
				{'*', '1', '\r', '\n'},
				{'$', '5', '3', '6', '8', '7', '0', '9', '1', '3', '\r', '\n'},
			},
			expectedError: NewErrProtocol("invalid bulk length"),
		},
	}

	codec := NewCodec()
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			for _, bytes := range tc.InputBytes {
				replies, err := codec.Decode(bytes)
				if err != nil {
					if errors.Is(err, ErrIncompletePacket) {
						continue
					}
					assert.Equal(t, tc.expectedError, err)
					break
				}
				for idx, reply := range replies {
					expected := tc.expectedReplies[idx]
					assert.True(t, arrayEqual(expected.ToBytes(), reply.ToBytes()))
				}
			}
			assert.Equal(t, 0, len(codec.buf))
			assert.Equal(t, 0, len(codec.argsBuf))
			assert.Equal(t, codec.state, DecodeType)
		})
	}
}

func arrayEqual(bb1, bb2 []byte) bool {
	for i, value := range bb1 {
		if bb2[i] != value {
			return false
		}
	}
	return true
}
