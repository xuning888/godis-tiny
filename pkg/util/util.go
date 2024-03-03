package util

import (
	"godis-tiny/interface/database"
	"godis-tiny/pkg/logger"
	"io"
	"math/rand"
	"time"
)

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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandStr(length int) string {
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[nR.Intn(len(letters))]
	}
	return string(b)
}

func Close(closer io.Closer) {
	err := closer.Close()
	if err != nil {
		logger.Errorf("close faild with error: %v", err)
	}
}
