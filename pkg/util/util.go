package util

import (
	"github.com/xuning888/godis-tiny/interface/database"
	"io"
	"log"
	"math/rand"
	"strconv"
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

func ToCmdLine2(key string, args [][]byte) database.CmdLine {
	cmdLine := make([][]byte, 0)
	cmdLine = append(cmdLine, []byte(key))
	if args != nil && len(args) > 0 {
		for _, arg := range args {
			cmdLine = append(cmdLine, arg)
		}
	}
	return cmdLine
}

var expireat = []byte("expireat")

func MakeExpireCmd(key string, expireAt time.Time) database.CmdLine {
	args := make([][]byte, 3)
	args[0] = expireat
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.Unix(), 10))
	return args
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
		log.Printf("close faild with error: %v\n", err)
	}
}
