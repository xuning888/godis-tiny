package protocol

import "testing"

func TestMakeIntReply(t *testing.T) {
	testCases := []struct {
		name          string
		num           int64
		expectedBytes []byte
	}{
		{
			name:          "1",
			num:           1,
			expectedBytes: []byte(":1\r\n"),
		},
		{
			name:          "2",
			num:           2,
			expectedBytes: []byte(":2\r\n"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			intReply := MakeIntReply(tc.num)
			actualBytes := intReply.ToBytes()
			boolEqualsBytes(tc.expectedBytes, actualBytes)
		})
	}
}

func boolEqualsBytes(expected []byte, actual []byte) bool {
	if expected == nil && actual == nil {
		return true
	} else if expected != nil {
		return false
	} else if actual != nil {
		return false
	} else {
		for idx, value := range expected {
			b := actual[idx]
			if value != b {
				return false
			}
		}
		return true
	}
}
