package intset

import (
	"math"
	"math/rand"
	"testing"
)

func TestAll(t *testing.T) {
	n := 100000
	intSet := NewIntSet(EncInt16)
	intSet.Add(math.MinInt16)
	for i := 0; i < n; i++ {
		randVal := rand.Intn(n)
		intSet.Add(int64(randVal))
	}
	intSet.Range(func(index int, value int64) bool {
		t.Logf("index: %d, value: %d\n", index, value)
		return true
	})
	elements := intSet.Elements()
	for _, value := range elements {
		intSet.Remove(value)
	}

	for _, value := range elements {
		if intSet.Contains(value) {
			t.Logf("value: %d\n", value)
			break
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	intSet := NewIntSet(EncInt16)
	for i := 0; i < b.N; i++ {
		randVal := rand.Int()
		intSet.Add(int64(randVal))
	}
}

func BenchmarkAdd2(b *testing.B) {
	set := make(map[int64]struct{})
	for i := 0; i < b.N; i++ {
		randVal := rand.Int()
		set[int64(randVal)] = struct{}{}
	}
}
