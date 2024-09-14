package redis

import (
	"context"
	"fmt"
	"github.com/xuning888/godis-tiny/pkg/datastruct/dict"
	"github.com/xuning888/godis-tiny/pkg/datastruct/intset"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"strconv"
)

func sadd(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 2 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	key := string(conn.GetArgs()[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if exists {
		var result int64 = 0
		if redisObj.ObjType != obj.RedisSet {
			return MakeWrongTypeErrReply().WriteTo(conn)
		}
		members := conn.GetArgs()[1:]
		for idx, member := range members {
			if redisObj.Encoding == obj.EncIntSet {
				intSet := redisObj.Ptr.(*intset.IntSet)
				number, err := strconv.ParseInt(string(member), 10, 64)
				if err == nil {
					result += intSet.Add(number)
				} else {
					// 转换编码格式
					simpleDict := dict.MakeSimpleDict()
					intSet.Range(func(index int, value int64) bool {
						simpleDict.Put(fmt.Sprintf("%d", value), struct{}{})
						return true
					})
					for _, mem := range members[idx:] {
						result += int64(simpleDict.Put(string(mem), struct{}{}))
					}
					redisObj.Encoding = obj.EncHT
					redisObj.Ptr = simpleDict
					break
				}
			} else {
				simpleDict := redisObj.Ptr.(*dict.SimpleDict)
				result += int64(simpleDict.Put(string(member), struct{}{}))
			}
		}
		return MakeIntReply(result).WriteTo(conn)
	}
	var result int64
	redisObj, result = obj.NewSetObject(conn.GetArgs()[1:])
	conn.GetDb().PutEntity(key, redisObj)
	return MakeIntReply(result).WriteTo(conn)
}

func smembers(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum < 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	key := string(conn.GetArgs()[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeEmptyMultiBulkReply().WriteTo(conn)
	}
	if redisObj.ObjType != obj.RedisSet {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}
	if redisObj.Encoding == obj.EncIntSet {
		intSet := redisObj.Ptr.(*intset.IntSet)
		if err := MakeMultiBulkHeaderReply(int64(intSet.Len())).WriteTo(conn); err != nil {
			return err
		}
		var err error
		intSet.Range(func(index int, value int64) bool {
			err = MakeBulkReply([]byte(fmt.Sprintf("%d", value))).WriteTo(conn)
			if err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
	} else {
		simpleDic := redisObj.Ptr.(*dict.SimpleDict)
		if err := MakeMultiBulkHeaderReply(int64(simpleDic.Len())).WriteTo(conn); err != nil {
			return err
		}
		var err error
		simpleDic.ForEach(func(key string, val interface{}) bool {
			err = MakeBulkReply([]byte(key)).WriteTo(conn)
			if err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func scard(c context.Context, conn *Client) error {
	argNum := conn.GetArgNum()
	if argNum != 1 {
		return MakeNumberOfArgsErrReply(conn.GetCmdName()).WriteTo(conn)
	}
	key := string(conn.GetArgs()[0])
	redisObj, exists := conn.GetDb().GetEntity(key)
	if !exists {
		return MakeIntReply(0).WriteTo(conn)
	}

	if redisObj.ObjType != obj.RedisSet {
		return MakeWrongTypeErrReply().WriteTo(conn)
	}

	if redisObj.Encoding == obj.EncIntSet {
		intSet := redisObj.Ptr.(*intset.IntSet)
		return MakeIntReply(int64(intSet.Len())).WriteTo(conn)
	}
	simpleDict := redisObj.Ptr.(*dict.SimpleDict)
	return MakeIntReply(int64(simpleDict.Len())).WriteTo(conn)
}

func init() {
	register("sadd", sadd)
	register("smembers", smembers)
	register("scard", scard)
}
