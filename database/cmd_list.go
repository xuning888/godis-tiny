package database

import (
	"context"
	"errors"
	"github.com/xuning888/godis-tiny/pkg/datastruct/list"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis"
	"strconv"
)

// execLLen llen mylist
func execLLen(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 1 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeIntReply(0).WriteTo(ctx.GetConn())
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.GetConn())
	}
	return redis.MakeIntReply(int64(dequeue.Len())).WriteTo(ctx.GetConn())
}

// execLIndex lindex key index.
// O(N) where N is number of elements to traverse to get to the element at index.
// This makes asking the first of the last element of the list O(1).
// Returns the element at index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
func execLIndex(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeNullBulkReply().WriteTo(ctx.GetConn())
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.GetConn())
	}
	index, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.GetConn())
	}
	if index < 0 {
		index = int64(dequeue.Len()) + index
	}
	value, err := dequeue.Get(int(index))
	if err != nil && errors.Is(err, list.ErrorOutIndex) {
		return redis.MakeNullBulkReply().WriteTo(ctx.GetConn())
	}
	return redis.MakeBulkReply(value.([]byte)).WriteTo(ctx.GetConn())
}

// execLPush lpush key element [elements...]
func execLPush(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if exists {
		dequeue, ok := entity.Data.(list.Dequeue)
		if !ok {
			return redis.MakeWrongTypeErrReply().WriteTo(ctx.GetConn())
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
			ctx.GetDb().AddAof(util.ToCmdLine2(key, cmdData[:curIdx+1]))
			return redis.MakeStandardErrReply("ERR list is full").WriteTo(ctx.GetConn())
		}
		// aof
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		length := dequeue.Len()
		return redis.MakeIntReply(int64(length)).WriteTo(ctx.GetConn())
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
	ctx.GetDb().PutEntity(key, &DataEntity{
		Type: List,
		Data: dequeue,
	})
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		ctx.GetDb().AddAof(util.ToCmdLine2(key, cmdData[:curIdx+1]))
		return redis.MakeStandardErrReply("ERR list is full").WriteTo(ctx.GetConn())
	}
	// aof
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	length := dequeue.Len()
	return redis.MakeIntReply(int64(length)).WriteTo(ctx.GetConn())
}

// execLPop lpop key [count]
func execLPop(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.GetConn())
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeNullBulkReply().WriteTo(ctx.GetConn())
	}
	var count int64 = 0
	var err error = nil
	if argNum > 1 {
		count, err = strconv.ParseInt(string(cmdData[1]), 10, 64)
		if err != nil {
			return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.GetConn())
		}
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.GetConn())
	}
	if count > 0 {
		ccap := util.MinInt64(int64(dequeue.Len()), count)
		if _, err2 := ctx.conn.Write(redis.MakeMultiBulkHeaderReply(ccap).ToBytes()); err2 != nil {
			return err2
		}
		for i := 0; i < int(count); i++ {
			pop, err3 := dequeue.RemoveFirst()
			if err3 != nil && errors.Is(err3, list.ErrorEmpty) {
				ctx.GetDb().Remove(key)
				break
			}
			if _, err4 := ctx.conn.Write(redis.MakeBulkReply(pop.([]byte)).ToBytes()); err4 != nil {
				return err4
			}
		}
		if dequeue.Len() == 0 {
			ctx.GetDb().Remove(key)
		}
		// aof
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return ctx.conn.Flush()
	}

	pop, err := dequeue.RemoveFirst()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		ctx.GetDb().Remove(key)
		return redis.MakeNullBulkReply().WriteTo(ctx.GetConn())
	}
	if dequeue.Len() == 0 {
		ctx.GetDb().Remove(key)
	}
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeBulkReply(pop.([]byte)).WriteTo(ctx.GetConn())
}

// execLRange lrange key start stop
func execLRange(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
	}
	dataEntity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeEmptyMultiBulkReply().WriteTo(ctx.conn)
	}
	dequeue, ok := dataEntity.Data.(list.Dequeue)
	if !ok {
		return redis.MakeWrongTypeErrReply().WriteTo(ctx.conn)
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
		return redis.MakeEmptyMultiBulkReply().WriteTo(ctx.conn)
	}

	if err2 := redis.MakeMultiBulkHeaderReply(end - start + 1).WriteTo(ctx.conn); err2 != nil {
		return err2
	}

	for i := start; i <= end; i++ {
		ele, _ := dequeue.Get(int(i))
		if _, err3 := ctx.conn.Write(redis.MakeBulkReply(ele.([]byte)).ToBytes()); err3 != nil {
			return err3
		}
	}
	return ctx.conn.Flush()
}

// execRPush RPUSH key element [element...]
func execRPush(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if exists {
		dequeue, ok := entity.Data.(list.Dequeue)
		if !ok {
			return redis.MakeWrongTypeErrReply().WriteTo(ctx.GetConn())
		}
		var err error
		for _, value := range cmdData[1:] {
			err = dequeue.AddLast(value)
			if err != nil {
				break
			}
		}
		if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
			return redis.MakeStandardErrReply("ERR list is full").WriteTo(ctx.conn)
		}
		length := dequeue.Len()
		// aof
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return redis.MakeIntReply(int64(length)).WriteTo(ctx.conn)
	}

	var dequeue list.Dequeue = list.NewArrayDeque(true)
	var err error
	for _, value := range cmdData[1:] {
		err = dequeue.AddLast(value)
		if err != nil {
			break
		}
	}
	ctx.GetDb().PutEntity(key, &DataEntity{
		Type: List,
		Data: dequeue,
	})
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		return redis.MakeStandardErrReply("ERR list is full").WriteTo(ctx.conn)
	}
	length := dequeue.Len()
	// aof
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeIntReply(int64(length)).WriteTo(ctx.conn)
}

// execRPop rpop key [count]
func execRPop(c context.Context, ctx *CommandContext) error {
	argNum := ctx.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return redis.MakeNumberOfArgsErrReply(ctx.GetCmdName()).WriteTo(ctx.conn)
	}
	cmdData := ctx.GetArgs()
	key := string(cmdData[0])
	entity, exists := ctx.GetDb().GetEntity(key)
	if !exists {
		return redis.MakeNullBulkReply().WriteTo(ctx.conn)
	}
	var count int64 = 0
	var err error = nil
	if argNum > 1 {
		count, err = strconv.ParseInt(string(cmdData[1]), 10, 64)
		if err != nil {
			return redis.MakeOutOfRangeOrNotInt().WriteTo(ctx.conn)
		}
	}
	dequeue, ok := entity.Data.(list.Dequeue)
	if !ok {
		return redis.MakeWrongTypeErrReply()
	}
	if count > 0 {
		ccap := util.MinInt64(count, int64(dequeue.Len()))
		if _, err2 := ctx.conn.Write(redis.MakeMultiBulkHeaderReply(ccap).ToBytes()); err2 != nil {
			return err2
		}
		for i := 0; i < int(count); i++ {
			pop, err := dequeue.RemoveLast()
			if err != nil && errors.Is(err, list.ErrorEmpty) {
				ctx.GetDb().Remove(key)
				break
			}
			if _, err4 := ctx.conn.Write(redis.MakeBulkReply(pop.([]byte)).ToBytes()); err4 != nil {
				return err4
			}
		}
		if dequeue.Len() == 0 {
			ctx.GetDb().Remove(key)
		}
		ctx.GetDb().AddAof(ctx.GetCmdLine())
		return ctx.conn.Flush()
	}

	pop, err := dequeue.RemoveLast()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		ctx.GetDb().Remove(key)
		return redis.MakeNullBulkReply().WriteTo(ctx.conn)
	}
	if dequeue.Len() == 0 {
		ctx.GetDb().Remove(key)
	}
	ctx.GetDb().AddAof(ctx.GetCmdLine())
	return redis.MakeBulkReply(pop.([]byte)).WriteTo(ctx.conn)
}

func registerListCmd() {
	CmdManager.registerCmd("lpush", execLPush, readWrite)
	CmdManager.registerCmd("lpop", execLPop, readWrite)
	CmdManager.registerCmd("llen", execLLen, readOnly)
	CmdManager.registerCmd("lindex", execLIndex, readOnly)
	CmdManager.registerCmd("lrange", execLRange, readOnly)
	CmdManager.registerCmd("rpush", execRPush, writeOnly)
	CmdManager.registerCmd("rpop", execRPop, writeOnly)
}
