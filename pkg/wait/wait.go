package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout 等待指定的超时时间， 如果超时了返回 true, 否则返回  false
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer func() {
			close(c)
		}()
		w.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}
