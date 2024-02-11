package util

// MinInt64 输入两个数，返回他们较小的那个
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
