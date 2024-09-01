package redis

import (
	"context"
	"path"
)

func execDel(ctx context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 {
		// 错误参数
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	var deleted = 0
	db := conn.GetDb()
	for i := 0; i < len(cmdData); i++ {
		result := db.Remove(string(cmdData[i]))
		deleted += result
	}
	if deleted > 0 {
		conn.GetDb().AddAof(conn.GetCmdLine())
		return MakeIntReply(int64(deleted)).WriteTo(conn)
	}
	return MakeIntReply(0).WriteTo(conn)
}

func execKeys(ctx context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	pattern := string(args[0])
	_, err := path.Match(pattern, "")
	if err != nil {
		return MakeStandardErrReply("ERR invalid pattern").WriteTo(conn)
	}
	keys := conn.GetDb().Keys()
	var matchedKeys [][]byte
	if pattern == "*" {
		matchedKeys = make([][]byte, 0, len(keys))
	} else {
		matchedKeys = make([][]byte, 0)
	}
	for _, key := range keys {
		matched, _ := path.Match(pattern, key)
		if matched {
			matchedKeys = append(matchedKeys, []byte(key))
		}
	}
	if len(matchedKeys) == 0 {
		return MakeEmptyMultiBulkReply().WriteTo(conn)
	}
	return MakeMultiBulkReply(matchedKeys).WriteTo(conn)
}

func init() {
	register("del", execDel)
	register("keys", execKeys)
}
