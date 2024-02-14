package ttl

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTTLCache(t *testing.T) {
	location, err := time.LoadLocation("UTC")
	assert.Nil(t, err)
	t.Logf("loc: %v", location)
	baseTime := time.Now()
	ttlCache := MakeSimple()
	ttlCache.Expire("1", baseTime.Add(time.Second*time.Duration(1)))
	ttlCache.Expire("2", baseTime.Add(time.Second*time.Duration(2)))
	ttlCache.Expire("3", baseTime.Add(time.Second*time.Duration(3)))
	assert.Equal(t, baseTime.Add(time.Second*time.Duration(1)), ttlCache.ExpireAt("1"))
	assert.Equal(t, baseTime.Add(time.Second*time.Duration(2)), ttlCache.ExpireAt("2"))
	assert.Equal(t, baseTime.Add(time.Second*time.Duration(3)), ttlCache.ExpireAt("3"))
	assert.Equal(t, EmptyTime, ttlCache.ExpireAt("4"))

	assert.Equal(t, baseTime.Add(time.Second*time.Duration(1)).UnixMilli(), ttlCache.ExpireAtTimestamp("1"))
	assert.Equal(t, baseTime.Add(time.Second*time.Duration(2)).UnixMilli(), ttlCache.ExpireAtTimestamp("2"))
	assert.Equal(t, baseTime.Add(time.Second*time.Duration(3)).UnixMilli(), ttlCache.ExpireAtTimestamp("3"))
	assert.Equal(t, EmptyTime.UnixMilli(), ttlCache.ExpireAtTimestamp("4"))

	baseTime2 := time.Now()
	ttlCache.Expire("1", baseTime2.Add(time.Second*time.Duration(1)))
	assert.Equal(t, baseTime2.Add(time.Second*time.Duration(1)), ttlCache.ExpireAt("1"))
	assert.Equal(t, baseTime2.Add(time.Second*time.Duration(1)).UnixMilli(), ttlCache.ExpireAtTimestamp("1"))

	assert.Equal(t, 3, ttlCache.Len())

	time.Sleep(time.Second * time.Duration(1))
	expired, exists := ttlCache.IsExpired("1")
	if exists && expired {
		ttlCache.Remove("1")
	}
	assert.True(t, expired)
	expired, exists = ttlCache.IsExpired("2")
	if exists && expired {
		ttlCache.Remove("2")
	}
	assert.False(t, expired)
	expired, exists = ttlCache.IsExpired("3")
	if exists && expired {
		ttlCache.Remove("3")
	}
	assert.False(t, expired)

	// --------------------------
	time.Sleep(time.Second * time.Duration(1))
	expired, exists = ttlCache.IsExpired("1")
	if exists && expired {
		ttlCache.Remove("1")
	}
	assert.False(t, expired)
	expired, exists = ttlCache.IsExpired("2")
	if exists && expired {
		ttlCache.Remove("2")
	}
	assert.True(t, expired)
	expired, exists = ttlCache.IsExpired("3")
	if exists && expired {
		ttlCache.Remove("3")
	}
	assert.False(t, expired)

	// --------------------------
	time.Sleep(time.Second * time.Duration(1))
	expired, exists = ttlCache.IsExpired("1")
	if exists && expired {
		ttlCache.Remove("1")
	}
	assert.False(t, expired)
	expired, exists = ttlCache.IsExpired("2")
	if exists && expired {
		ttlCache.Remove("2")
	}
	assert.False(t, expired)
	expired, exists = ttlCache.IsExpired("3")
	if exists && expired {
		ttlCache.Remove("3")
	}
	assert.True(t, expired)

	assert.Equal(t, 0, ttlCache.Len())
}
