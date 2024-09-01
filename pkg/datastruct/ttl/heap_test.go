package ttl

import (
	"container/heap"
	"testing"
	"time"
)

func TestRandomOps(t *testing.T) {
	now := time.Now()
	items := map[string]time.Time{
		"5": now.Add(5 * time.Second),
		"3": now.Add(3 * time.Second),
		"1": now.Add(1 * time.Second),
	}

	pq := make(ttlHeap, len(items))
	i := 0
	for key, expiryTime := range items {
		item := makeItem(key, expiryTime)
		item.index = i
		pq[i] = item
		i++
	}
	heap.Init(&pq)

	ele := makeItem("2", now.Add(2*time.Second))
	heap.Push(&pq, ele)
	pq.update(ele, ele.Key, time.Now().Add(1*time.Second))

	for pq.Len() > 0 {
		ele, _ := pq.Peek().(*Item)
		if ele.ExpireTime.UnixMilli() <= time.Now().UnixMilli() {
			pop, _ := heap.Pop(&pq).(*Item)
			t.Logf("Key: %s | Expiry Time: %d\n", pop.Key, pop.ExpireTime.UnixMilli())
		}
	}
}
