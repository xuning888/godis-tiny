package list

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Quick_AddFirst(t *testing.T) {
	testCassses := []struct {
		name        string
		deque       *ZipQueue
		loopNum     int
		expectedErr error
		expectedLen int
	}{
		{
			name:        "向尾部添加，并对比元素数量",
			deque:       NewZipQueue(),
			loopNum:     1 << 16,
			expectedLen: 1 << 16,
		},
		{
			name:        "向末尾添加，直到达到队列的最大容量",
			deque:       NewZipQueue(),
			loopNum:     (1 << 30) + 1,
			expectedErr: ErrorOutOfCapacity,
			expectedLen: (1 << 30) - 1,
		},
	}

	for _, tc := range testCassses {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < tc.loopNum; i++ {
				err := tc.deque.AddFirst(i)
				if err != nil {
					t.Logf("error: %v", err)
					assert.Equal(t, tc.expectedErr, err)
					break
				}
			}
			assert.Equal(t, tc.expectedLen, tc.deque.Len())
		})
	}
}
