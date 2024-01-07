package wait

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWaitWithTimeout(t *testing.T) {
	testCases := []struct {
		name       string
		timeout    time.Duration
		execTime   time.Duration
		wantResult bool
	}{
		{
			name:       "timeout",
			timeout:    time.Second * 1,
			execTime:   time.Second * 2,
			wantResult: true,
		},
		{
			name:       "not timeout",
			timeout:    time.Second * 2,
			execTime:   time.Second * 1,
			wantResult: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var wait Wait
			wait.Add(1)
			go func() {
				defer func() {
					wait.Done()
				}()
				time.Sleep(tc.execTime)
			}()
			timeout := wait.WaitWithTimeout(tc.timeout)
			assert.Equal(t, timeout, tc.wantResult)
		})
	}
}
