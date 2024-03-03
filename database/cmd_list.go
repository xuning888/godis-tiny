package database

import (
	"errors"
	"godis-tiny/datastruct/list"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/redis/protocol"
	"strconv"
)

// execLLen llen mylist
func execLLen(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.cmdData
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return protocol.MakeWrongTypeErrReply()
	}
	return protocol.MakeIntReply(int64(dequeue.Len()))
}

// execLIndex lindex key index.
// O(N) where N is number of elements to traverse to get to the element at index.
// This makes asking the first of the last element of the list O(1).
// Returns the element at index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
func execLIndex(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.cmdData
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return protocol.MakeWrongTypeErrReply()
	}
	index, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	if index < 0 {
		index = int64(dequeue.Len()) + index
	}
	value, err := dequeue.Get(int(index))
	if err != nil && errors.Is(err, list.ErrorOutIndex) {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(value.([]byte))
}

// execLInsert LINSERT key <BEFORE | AFTER> pivot element
func execLInsert(ctx *CommandContext, lint *cmdLint) redis.Reply {
	return nil
}

// execLPush lpush key element [elements...]
func execLPush(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.cmdData
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if exists {
		dequeue, ok := entity.Data.(list.Dequeue)
		if !ok {
			return protocol.MakeWrongTypeErrReply()
		}
		var err error
		for _, value := range cmdData[1:] {
			err = dequeue.AddFirst(value)
			if err != nil {
				break
			}
		}
		if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
			return protocol.MakeStandardErrReply("ERR list is full")
		}
		length := dequeue.Len()
		return protocol.MakeIntReply(int64(length))
	}
	var dequeue list.Dequeue = list.NewQuickDequeue()
	var err error
	for _, value := range cmdData[1:] {
		err = dequeue.AddFirst(value)
		if err != nil {
			break
		}
	}
	ctx.GetDb().PutEntity(key, &database.DataEntity{
		Data: dequeue,
	})
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		return protocol.MakeStandardErrReply("ERR list is full")
	}
	length := dequeue.Len()
	return protocol.MakeIntReply(int64(length))
}

// execLPop lpop key [count]
func execLPop(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.cmdData
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	var count int64 = 0
	var err error = nil
	if argNum > 1 {
		count, err = strconv.ParseInt(string(cmdData[1]), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return protocol.MakeWrongTypeErrReply()
	}
	if count > 0 {
		result := make([]redis.Reply, 0, count)
		for i := 0; i < int(count); i++ {
			pop, err := dequeue.RemoveFirst()
			if err != nil && errors.Is(err, list.ErrorEmpty) {
				ctx.GetDb().Remove(key)
				break
			}
			result = append(result, protocol.MakeBulkReply(pop.([]byte)))
		}
		if dequeue.Len() == 0 {
			ctx.GetDb().Remove(key)
		}
		return protocol.MakeMultiRowReply(result)
	}

	pop, err := dequeue.RemoveFirst()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		ctx.GetDb().Remove(key)
		return protocol.MakeNullBulkReply()
	}
	if dequeue.Len() == 0 {
		ctx.GetDb().Remove(key)
	}
	return protocol.MakeBulkReply(pop.([]byte))
}

// execLRange lrange key start stop
func execLRange(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	return nil
}

// execRPush RPUSH key element [element...]
func execRPush(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.cmdName)
	}
	cmdData := lint.cmdData
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if exists {
		dequeue, ok := entity.Data.(list.Dequeue)
		if !ok {
			return protocol.MakeWrongTypeErrReply()
		}
		var err error
		for _, value := range cmdData[1:] {
			err = dequeue.AddLast(value)
			if err != nil {
				break
			}
		}
		if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
			return protocol.MakeStandardErrReply("ERR list is full")
		}
		length := dequeue.Len()
		return protocol.MakeIntReply(int64(length))
	}

	var dequeue list.Dequeue = list.NewQuickDequeue()
	var err error
	for _, value := range cmdData[1:] {
		err = dequeue.AddLast(value)
		if err != nil {
			break
		}
	}
	ctx.GetDb().PutEntity(key, &database.DataEntity{
		Data: dequeue,
	})
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		return protocol.MakeStandardErrReply("ERR list is full")
	}
	length := dequeue.Len()
	return protocol.MakeIntReply(int64(length))
}

// execRPop rpop key [count]
func execRPop(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.cmdData
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	var count int64 = 0
	var err error = nil
	if argNum > 1 {
		count, err = strconv.ParseInt(string(cmdData[1]), 10, 64)
		if err != nil {
			return protocol.MakeOutOfRangeOrNotInt()
		}
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return protocol.MakeWrongTypeErrReply()
	}
	if count > 0 {
		result := make([]redis.Reply, 0, count)
		for i := 0; i < int(count); i++ {
			pop, err := dequeue.RemoveLast()
			if err != nil && errors.Is(err, list.ErrorEmpty) {
				ctx.GetDb().Remove(key)
				break
			}
			result = append(result, protocol.MakeBulkReply(pop.([]byte)))
		}
		if dequeue.Len() == 0 {
			ctx.GetDb().Remove(key)
		}
		return protocol.MakeMultiRowReply(result)
	}

	pop, err := dequeue.RemoveLast()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		ctx.GetDb().Remove(key)
		return protocol.MakeNullBulkReply()
	}
	if dequeue.Len() == 0 {
		ctx.GetDb().Remove(key)
	}
	return protocol.MakeBulkReply(pop.([]byte))
}

func registerListCmd() {
	cmdManager.registerCmd("lpush", execLPush)
	cmdManager.registerCmd("lpop", execLPop)
	cmdManager.registerCmd("llen", execLLen)
	cmdManager.registerCmd("lindex", execLIndex)
	cmdManager.registerCmd("lrange", execLRange)
	cmdManager.registerCmd("rpush", execRPush)
	cmdManager.registerCmd("rpop", execRPop)
}
