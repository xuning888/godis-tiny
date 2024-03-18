package database

import (
	"errors"
	"godis-tiny/datastruct/list"
	"godis-tiny/interface/database"
	"godis-tiny/interface/redis"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/protocol"
	"strconv"
)

// execLLen llen mylist
func execLLen(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
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
	cmdData := lint.GetCmdData()
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
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if exists {
		dequeue, ok := entity.Data.(list.Dequeue)
		if !ok {
			return protocol.MakeWrongTypeErrReply()
		}
		var err error
		var curIdx = 0
		for idx, value := range cmdData[1:] {
			err = dequeue.AddFirst(value)
			if err != nil {
				break
			}
			curIdx = idx
		}
		if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
			// aof
			ctx.GetDb().addAof(lint.GetCmdLine()[:curIdx+1])
			return protocol.MakeStandardErrReply("ERR list is full")
		}
		// aof
		ctx.GetDb().addAof(lint.GetCmdLine())
		length := dequeue.Len()
		return protocol.MakeIntReply(int64(length))
	}
	var dequeue list.Dequeue = list.NewArrayDeque(true)
	var err error
	var curIdx = 0
	for idx, value := range cmdData[1:] {
		err = dequeue.AddFirst(value)
		if err != nil {
			break
		}
		curIdx = idx
	}
	ctx.GetDb().PutEntity(key, &database.DataEntity{
		Type: database.List,
		Data: dequeue,
	})
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		ctx.GetDb().addAof(lint.GetCmdLine()[:curIdx+1])
		return protocol.MakeStandardErrReply("ERR list is full")
	}
	// aof
	ctx.GetDb().addAof(lint.GetCmdLine())
	length := dequeue.Len()
	return protocol.MakeIntReply(int64(length))
}

// execLPop lpop key [count]
func execLPop(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
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
		ccap := util.MinInt64(int64(dequeue.Len()), count)
		result := make([][]byte, 0, ccap)
		for i := 0; i < int(count); i++ {
			pop, err := dequeue.RemoveFirst()
			if err != nil && errors.Is(err, list.ErrorEmpty) {
				ctx.GetDb().Remove(key)
				break
			}
			result = append(result, pop.([]byte))
		}
		if dequeue.Len() == 0 {
			ctx.GetDb().Remove(key)
		}
		// aof
		ctx.GetDb().addAof(lint.GetCmdLine())
		return protocol.MakeMultiBulkReply(result)
	}

	pop, err := dequeue.RemoveFirst()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		ctx.GetDb().Remove(key)
		return protocol.MakeNullBulkReply()
	}
	if dequeue.Len() == 0 {
		ctx.GetDb().Remove(key)
	}
	ctx.GetDb().addAof(lint.GetCmdLine())
	return protocol.MakeBulkReply(pop.([]byte))
}

// execLRange lrange key start stop
func execLRange(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return protocol.MakeOutOfRangeOrNotInt()
	}
	dataEntity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}
	dequeue, ok := dataEntity.Data.(list.Dequeue)
	if !ok {
		return protocol.MakeWrongTypeErrReply()
	}
	length := int64(dequeue.Len())
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return protocol.MakeEmptyMultiBulkReply()
	}

	res := make([][]byte, 0, end-start+1)
	for i := start; i <= end; i++ {
		ele, _ := dequeue.Get(int(i))
		res = append(res, ele.([]byte))
	}
	if err != nil {
		return protocol.MakeStandardErrReply(err.Error())
	}
	return protocol.MakeMultiBulkReply(res)
}

// execRPush RPUSH key element [element...]
func execRPush(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
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
		// aof
		ctx.GetDb().addAof(lint.GetCmdLine())
		return protocol.MakeIntReply(int64(length))
	}

	var dequeue list.Dequeue = list.NewArrayDeque(true)
	var err error
	for _, value := range cmdData[1:] {
		err = dequeue.AddLast(value)
		if err != nil {
			break
		}
	}
	ctx.GetDb().PutEntity(key, &database.DataEntity{
		Type: database.List,
		Data: dequeue,
	})
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		return protocol.MakeStandardErrReply("ERR list is full")
	}
	length := dequeue.Len()
	// aof
	ctx.GetDb().addAof(lint.GetCmdLine())
	return protocol.MakeIntReply(int64(length))
}

// execRPop rpop key [count]
func execRPop(ctx *CommandContext, lint *cmdLint) redis.Reply {
	argNum := lint.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return protocol.MakeNumberOfArgsErrReply(lint.GetCmdName())
	}
	cmdData := lint.GetCmdData()
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
		ccap := util.MinInt64(count, int64(dequeue.Len()))
		result := make([][]byte, 0, ccap)
		for i := 0; i < int(count); i++ {
			pop, err := dequeue.RemoveLast()
			if err != nil && errors.Is(err, list.ErrorEmpty) {
				ctx.GetDb().Remove(key)
				break
			}
			result = append(result, pop.([]byte))
		}
		if dequeue.Len() == 0 {
			ctx.GetDb().Remove(key)
		}
		ctx.GetDb().addAof(lint.GetCmdLine())
		return protocol.MakeMultiBulkReply(result)
	}

	pop, err := dequeue.RemoveLast()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		ctx.GetDb().Remove(key)
		return protocol.MakeNullBulkReply()
	}
	if dequeue.Len() == 0 {
		ctx.GetDb().Remove(key)
	}
	ctx.GetDb().addAof(lint.GetCmdLine())
	return protocol.MakeBulkReply(pop.([]byte))
}

func registerListCmd() {
	cmdManager.registerCmd("lpush", execLPush, readWrite)
	cmdManager.registerCmd("lpop", execLPop, readWrite)
	cmdManager.registerCmd("llen", execLLen, readOnly)
	cmdManager.registerCmd("lindex", execLIndex, readOnly)
	cmdManager.registerCmd("lrange", execLRange, readOnly)
	cmdManager.registerCmd("rpush", execRPush, writeOnly)
	cmdManager.registerCmd("rpop", execRPop, writeOnly)
}
