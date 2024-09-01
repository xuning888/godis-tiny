package redis

import (
	"context"
	"errors"
	"github.com/xuning888/godis-tiny/pkg/datastruct/list"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"github.com/xuning888/godis-tiny/pkg/util"
	"strconv"
)

func execLLen(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum != 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	args := conn.GetArgs()
	key := string(args[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeIntReply(0).WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisList {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	dequeue := redisObj.Ptr.(list.Dequeue)
	return MakeIntReply(int64(dequeue.Len())).WriteTo(conn)
}

func execLIndex(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeNullBulkReply().WriteTo(conn)
	}

	dequeue, ok := redisObj.Ptr.(list.Dequeue)
	if !ok {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	index, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	if index < 0 {
		index = int64(dequeue.Len()) + index
	}
	value, err := dequeue.Get(int(index))
	if err != nil && errors.Is(err, list.ErrorOutIndex) {
		return MakeNullBulkReply().WriteTo(conn)
	}
	return MakeBulkReply(value.([]byte)).WriteTo(conn)
}

func execLPush(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if exists {
		if redisObj.ObjType != obj.RedisList {
			return MakeWrongTypeErrReply().WriteTo(conn)
		}
		dequeue := redisObj.Ptr.(list.Dequeue)
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
			conn.GetDb().AddAof(util.ToCmdLine2(key, cmdData[:curIdx+2]))
			return MakeStandardErrReply("ERR list is full").WriteTo(conn)
		}
		conn.GetDb().AddAof(conn.GetCmdLine())
		length := dequeue.Len()
		return MakeIntReply(int64(length)).WriteTo(conn)
	}
	redisObj = obj.NewListObject()
	dequeue := redisObj.Ptr.(list.Dequeue)
	var err error
	var curIdx = 0
	for idx, value := range cmdData[1:] {
		err = dequeue.AddFirst(value)
		if err != nil {
			break
		}
		curIdx = idx
	}
	conn.GetDb().PutEntity(key, redisObj)
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		conn.GetDb().AddAof(util.ToCmdLine2(key, cmdData[:curIdx+2]))
		return MakeStandardErrReply("ERR list is full").WriteTo(conn)
	}
	conn.GetDb().AddAof(conn.GetCmdLine())
	length := dequeue.Len()
	return MakeIntReply(int64(length)).WriteTo(conn)
}

// execLPop lpop key [count]
func execLPop(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeNullBulkReply().WriteTo(conn)
	}
	var count int64 = -1
	var err error = nil
	if argNum > 1 {
		count, err = strconv.ParseInt(string(cmdData[1]), 10, 64)
		if err != nil {
			return MakeOutOfRangeOrNotInt().WriteTo(conn)
		}
	}
	dequeue, ok := redisObj.Ptr.(list.Dequeue)
	if !ok {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	if count > 0 {
		ccap := util.MinInt64(int64(dequeue.Len()), count)
		if _, err2 := conn.Write(MakeMultiBulkHeaderReply(ccap).ToBytes()); err2 != nil {
			return err2
		}
		for i := 0; i < int(count); i++ {
			pop, err3 := dequeue.RemoveFirst()
			if err3 != nil && errors.Is(err3, list.ErrorEmpty) {
				conn.GetDb().Remove(key)
				break
			}
			if _, err4 := conn.Write(MakeBulkReply(pop.([]byte)).ToBytes()); err4 != nil {
				return err4
			}
		}
		if dequeue.Len() == 0 {
			conn.GetDb().Remove(key)
		}
		// aof
		conn.GetDb().AddAof(conn.GetCmdLine())
		return conn.Flush()
	} else if count == 0 {
		return MakeEmptyMultiBulkReply().WriteTo(conn)
	}
	pop, err := dequeue.RemoveFirst()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		conn.GetDb().Remove(key)
		return MakeNullBulkReply().WriteTo(conn)
	}
	if dequeue.Len() == 0 {
		conn.GetDb().Remove(key)
	}
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeBulkReply(pop.([]byte)).WriteTo(conn)
}

// execLRange lrange key start stop
func execLRange(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 3 || argNum > 3 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	start, err := strconv.ParseInt(string(cmdData[1]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	end, err := strconv.ParseInt(string(cmdData[2]), 10, 64)
	if err != nil {
		return MakeOutOfRangeOrNotInt().WriteTo(conn)
	}
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeEmptyMultiBulkReply().WriteTo(conn)
	}

	dequeue, ok := redisObj.Ptr.(list.Dequeue)
	if !ok {
		return MakeWrongTypeErrReply().WriteTo(conn)
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
		return MakeEmptyMultiBulkReply().WriteTo(conn)
	}

	if err2 := MakeMultiBulkHeaderReply(end - start + 1).WriteTo(conn); err2 != nil {
		return err2
	}

	var err3 error = nil
	dequeue.ForEach(func(value interface{}, index int) bool {
		if int64(index) < start {
			return true
		} else if int64(index) > end {
			return false
		}
		if _, err3 = conn.Write(MakeBulkReply(value.([]byte)).ToBytes()); err3 != nil {
			return false
		}
		return true
	})

	if err3 != nil {
		_ = conn.Flush()
		return err3
	}
	return conn.Flush()
}

func execRPush(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if exists {
		dequeue, ok := redisObj.Ptr.(list.Dequeue)
		if !ok {
			return MakeWrongTypeErrReply().WriteTo(conn)
		}
		var err error
		var curIdx = 0
		for idx, value := range cmdData[1:] {
			err = dequeue.AddLast(value)
			if err != nil {
				break
			}
			curIdx = idx
		}
		if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
			conn.GetDb().AddAof(util.ToCmdLine2(key, cmdData[:curIdx+2]))
			return MakeStandardErrReply("ERR list is full").WriteTo(conn)
		}
		length := dequeue.Len()
		// aof
		conn.GetDb().AddAof(conn.GetCmdLine())
		return MakeIntReply(int64(length)).WriteTo(conn)
	}

	redisObj = obj.NewListObject()
	dequeue := redisObj.Ptr.(list.Dequeue)
	var err error
	var curIdx = 0
	for idx, value := range cmdData[1:] {
		err = dequeue.AddLast(value)
		if err != nil {
			break
		}
		curIdx = idx
	}
	conn.GetDb().PutEntity(key, redisObj)
	if err != nil && errors.Is(err, list.ErrorOutOfCapacity) {
		conn.GetDb().AddAof(util.ToCmdLine2(key, cmdData[:curIdx+2]))
		return MakeStandardErrReply("ERR list is full").WriteTo(conn)
	}
	length := dequeue.Len()
	// aof
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeIntReply(int64(length)).WriteTo(conn)
}

// execRPop rpop key [count]
func execRPop(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 || argNum > 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	cmdData := conn.GetArgs()
	key := string(cmdData[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeNullBulkReply().WriteTo(conn)
	}
	var count int64 = 0
	var err error = nil
	if argNum > 1 {
		count, err = strconv.ParseInt(string(cmdData[1]), 10, 64)
		if err != nil {
			return MakeOutOfRangeOrNotInt().WriteTo(conn)
		}
	}
	dequeue, ok := redisObj.Ptr.(list.Dequeue)
	if !ok {
		return MakeWrongTypeErrReply()
	}
	if count > 0 {
		ccap := util.MinInt64(count, int64(dequeue.Len()))
		if _, err2 := conn.Write(MakeMultiBulkHeaderReply(ccap).ToBytes()); err2 != nil {
			return err2
		}
		for i := 0; i < int(count); i++ {
			pop, err := dequeue.RemoveLast()
			if err != nil && errors.Is(err, list.ErrorEmpty) {
				conn.GetDb().Remove(key)
				break
			}
			if _, err4 := conn.Write(MakeBulkReply(pop.([]byte)).ToBytes()); err4 != nil {
				return err4
			}
		}
		if dequeue.Len() == 0 {
			conn.GetDb().Remove(key)
		}
		conn.GetDb().AddAof(conn.GetCmdLine())
		return conn.Flush()
	}

	pop, err := dequeue.RemoveLast()
	if err != nil && errors.Is(err, list.ErrorEmpty) {
		conn.GetDb().Remove(key)
		return MakeNullBulkReply().WriteTo(conn)
	}
	if dequeue.Len() == 0 {
		conn.GetDb().Remove(key)
	}
	conn.GetDb().AddAof(conn.GetCmdLine())
	return MakeBulkReply(pop.([]byte)).WriteTo(conn)
}

func init() {
	register("lpush", execLPush)
	register("lpop", execLPop)
	register("lrange", execLRange)
	register("rpush", execRPush)
	register("llen", execLLen)
	register("lindex", execLIndex)
	register("rpop", execRPop)
}
