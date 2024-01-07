package database

import (
	"g-redis/interface/redis"
	"g-redis/redis/protocol"
	"path"
)

func execDel(db *DB, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	if len(args) == 0 {
		// 错误参数
		return protocol.MakeNumberOfArgsErrReply(lint.cmdName)
	}
	keys := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		keys[i] = string(args[i])
	}
	deleted := db.Removes(keys...)
	if deleted > 0 {
		return protocol.MakeIntReply(int64(deleted))
	}
	return protocol.MakeIntReply(0)
}

func execKeys(db *DB, lint *cmdLint) redis.Reply {
	args := lint.GetCmdData()
	if lint.GetArgNum() > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	pattern := string(args[0])
	keys := db.data.Keys()
	var matchedKeys []string
	for _, key := range keys {
		matched, err := path.Match(pattern, key)
		if err != nil {
			return protocol.MakeStandardErrReply("ERR invalid pattern")
		}
		if matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	if len(matchedKeys) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	replies := make([]redis.Reply, len(matchedKeys))
	for i, key := range matchedKeys {
		replies[i] = protocol.MakeBulkReply([]byte(key))
	}

	return protocol.MakeMultiRowReply(replies)
}

func exists(db *DB, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum == 0 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	keys := make([]string, argNum)
	for i := 0; i < argNum; i++ {
		keys[i] = string(lint.GetCmdData()[i])
	}
	if len(keys) == 0 {
		return protocol.MakeIntReply(0)
	}
	result := db.Exists(keys)
	return protocol.MakeIntReply(result)
}

func init() {
	RegisterCmd("Del", execDel, -1)
	RegisterCmd("keys", execKeys, 1)
	RegisterCmd("exists", exists, -1)
}
