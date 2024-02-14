package util

import "godis-tiny/interface/database"

// MinInt64 输入两个数，返回他们较小的那个
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func ToCmdLine(key string, args ...string) database.CmdLine {
	cmdLine := make([][]byte, 0)
	cmdLine = append(cmdLine, []byte(key))
	if args != nil && len(args) > 0 {
		for _, arg := range args {
			cmdLine = append(cmdLine, []byte(arg))
		}
	}
	return cmdLine
}
