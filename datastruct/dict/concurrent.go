package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ConcurrentDict struct {
	table      []*Shard
	count      int32
	shardCount int
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.RLock()
	defer func() {
		shard.mutex.RUnlock()
	}()
	val, exists = shard.m[key]
	return
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		return 0
	}
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, value interface{}) (result int) {
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer func() {
		shard.mutex.Unlock()
	}()
	_, exists := shard.m[key]
	if exists {
		shard.m[key] = value
		return 0
	}
	shard.m[key] = value
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfAbsent(key string, value interface{}) (result int) {
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer func() {
		shard.mutex.Unlock()
	}()
	_, exists := shard.m[key]
	if exists {
		return 0
	}
	shard.m[key] = value
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfExists(key string, value interface{}) (result int) {
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer func() {
		shard.mutex.Unlock()
	}()
	_, exists := shard.m[key]
	if !exists {
		return 0
	}
	shard.m[key] = value
	return 1
}

func (dict *ConcurrentDict) Remove(key string) (result int) {
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer func() {
		shard.mutex.Unlock()
	}()
	_, exists := shard.m[key]
	if exists {
		delete(shard.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	for _, shard := range dict.table {
		f := func() bool {
			shard.mutex.RLock()
			defer func() {
				shard.mutex.RUnlock()
			}()
			for key, value := range shard.m {
				consum := consumer(key, value)
				if !consum {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey returns a key randomly
func (shard *Shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & uint32(hashCode)
}

func (dict *ConcurrentDict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
