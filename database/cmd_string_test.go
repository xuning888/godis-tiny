package database

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xuning888/godis-tiny/interface/database"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis/connection"
	"github.com/xuning888/godis-tiny/redis/protocol"
	"math"
	"strconv"
	"testing"
)

func TestExeSet(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value nx",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "nx"),
				util.ToCmdLine("set", "key", "value", "nx"),
				util.ToCmdLine("set", "key1", "value"),
				util.ToCmdLine("set", "key1", "value", "nx"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeNullBulkReply(),
				protocol.MakeOkReply(),
				protocol.MakeNullBulkReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value xx",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "xx"),
				util.ToCmdLine("set", "key", "value"),
				util.ToCmdLine("set", "key", "value1", "xx"),
				util.ToCmdLine("get", "key"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeNullBulkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value1")),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value ex seconds",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "ex", "1"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value", "ex", "2"),
				util.ToCmdLine("ttl", "key"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(2),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value px milliseconds",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "px", "1000"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value", "px", "2000"),
				util.ToCmdLine("ttl", "key"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(2),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value nx ex 10",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "nx", "ex", "10"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value", "nx"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value")),
				protocol.MakeIntReply(10),
				protocol.MakeNullBulkReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value nx px 1",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "nx", "px", "10000"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value", "nx"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value")),
				protocol.MakeIntReply(10),
				protocol.MakeNullBulkReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value xx ex 1",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "xx", "ex", "1"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value1", "xx", "ex", "1"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeNullBulkReply(),
				protocol.MakeNullBulkReply(),
				protocol.MakeIntReply(-2),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value")),
				protocol.MakeIntReply(-1),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value1")),
				protocol.MakeIntReply(1),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value xx px 10000",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "xx", "px", "10000"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value1", "xx", "px", "10000"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("ttl", "key"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeNullBulkReply(),
				protocol.MakeNullBulkReply(),
				protocol.MakeIntReply(-2),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value")),
				protocol.MakeIntReply(-1),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value1")),
				protocol.MakeIntReply(10),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value error",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "xx", "nx"),
				util.ToCmdLine("set", "key", "value", "nx", "xx"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value error",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "xx", "nx", "ex", "10"),
				util.ToCmdLine("set", "key", "value", "nx", "xx", "ex", "10"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value error",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "ex", "10", "px", "1000"),
				util.ToCmdLine("set", "key", "value", "px", "10000", "ex", "10"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value error",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "ex"),
				util.ToCmdLine("set", "key", "value", "ex", "q"),
				util.ToCmdLine("set", "key", "value", "px"),
				util.ToCmdLine("set", "key", "value", "px", "a"),
				util.ToCmdLine("set", "key", "value", "ex", "-1"),
				util.ToCmdLine("set", "key", "value", "px", "-1"),
				util.ToCmdLine("set", "key", "value", "hh", "-1"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
				protocol.MakeStandardErrReply("ERR invalid expire time in set"),
				protocol.MakeStandardErrReply("ERR invalid expire time in set"),
				protocol.MakeSyntaxReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "set key value keepttl",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value", "ex", "1000"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("set", "key", "value1", "keepttl"),
				util.ToCmdLine("ttl", "key"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("del", "key"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("set", "key", "value", "ex", "10", "keepttl"),
				util.ToCmdLine("set", "key", "value", "px", "10000", "keepttl"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1000),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1000),
				protocol.MakeBulkReply([]byte("value1")),
				protocol.MakeIntReply(1),
				protocol.MakeNullBulkReply(),
				protocol.MakeSyntaxReply(),
				protocol.MakeSyntaxReply(),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_SetNx(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "setnx key value",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value"),
				util.ToCmdLine("setnx", "key", "value1"),
				util.ToCmdLine("setnx", "key"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(0),
				protocol.MakeNumberOfArgsErrReply("setnx"),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_Strlen(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "strlen",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "key", "value"),
				util.ToCmdLine("strlen", "key"),
				util.ToCmdLine("strlen", "key1"),
				util.ToCmdLine("lpush", "mylist", "a"),
				util.ToCmdLine("strlen", "mylist"),
				util.ToCmdLine("strlen"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(5),
				protocol.MakeIntReply(0),
				protocol.MakeIntReply(1),
				protocol.MakeWrongTypeErrReply(),
				protocol.MakeNumberOfArgsErrReply("strlen"),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_GetSet(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("getset", "a", "c"),
				util.ToCmdLine("get", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply(util.ToCmdLine("b")[0]),
				protocol.MakeBulkReply(util.ToCmdLine("c")[0]),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "number of args error",
			Lines: []database.CmdLine{
				util.ToCmdLine("getset", "a"),
				util.ToCmdLine("getset", "a", "b", "c"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeNumberOfArgsErrReply("getset"),
				protocol.MakeNumberOfArgsErrReply("getset"),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "type failed",
			Lines: []database.CmdLine{
				util.ToCmdLine("lpush", "mylist", "a"),
				util.ToCmdLine("getset", "mylist", "b"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeIntReply(1),
				protocol.MakeWrongTypeErrReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "value not exists",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("getset", "b", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeNullBulkReply(),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_Incr(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("incr", "a"),
				util.ToCmdLine("set", "a", "1"),
				util.ToCmdLine("incr", "a"),
				util.ToCmdLine("get", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeIntReply(1),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(2),
				protocol.MakeBulkReply([]byte("2")),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "incr overflow int64",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "9223372036854775807"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("incr", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte(strconv.Itoa(math.MaxInt64))),
				protocol.MakeStandardErrReply("ERR increment or decrement would overflow"),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "simple str",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "aaa"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("incr", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("aaa")),
				protocol.MakeOutOfRangeOrNotInt(),
			},
		},
	}
	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_GetDel(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("getdel", "a"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("getdel", "a"),
				util.ToCmdLine("lpush", "mylist", "a", "b"),
				util.ToCmdLine("getdel", "mylist"),
				util.ToCmdLine("getdel"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("b")),
				protocol.MakeNullBulkReply(),
				protocol.MakeNullBulkReply(),
				protocol.MakeIntReply(2),
				protocol.MakeNullBulkReply(),
				protocol.MakeNumberOfArgsErrReply("getdel"),
			},
		},
	}
	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_Decr(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("decr", "a"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("decr", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeIntReply(-1),
				protocol.MakeBulkReply([]byte("-1")),
				protocol.MakeIntReply(-2),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "overflow",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "-9223372036854775808"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("decr", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("-9223372036854775808")),
				protocol.MakeStandardErrReply("ERR increment or decrement would overflow"),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "overflow",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "aaa"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("decr", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("aaa")),
				protocol.MakeOutOfRangeOrNotInt(),
			},
		},
	}
	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_IncrBy(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("incrby", "a", "1"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("incrby", "a", "1"),
				util.ToCmdLine("get", "a"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeIntReply(1),
				protocol.MakeBulkReply([]byte("1")),
				protocol.MakeIntReply(2),
				protocol.MakeBulkReply([]byte("2")),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "error",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "aaa"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("incrby", "a", "1"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("aaa")),
				protocol.MakeOutOfRangeOrNotInt(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "overflow",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "-9223372036854775808"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("incrby", "a", "-1"),
				util.ToCmdLine("incrby", "a", "9223372036854775807"),
				util.ToCmdLine("incrby", "a", "9223372036854775807"),
				util.ToCmdLine("incrby", "a", "1"),
				util.ToCmdLine("incrby", "a", "1"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("-9223372036854775808")),
				protocol.MakeStandardErrReply("ERR increment or decrement would overflow"),
				protocol.MakeIntReply(-1),
				protocol.MakeIntReply(math.MaxInt64 - 1),
				protocol.MakeIntReply(math.MaxInt64),
				protocol.MakeStandardErrReply("ERR increment or decrement would overflow"),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "error key type",
			Lines: []database.CmdLine{
				util.ToCmdLine("lpush", "mylist", "aaa"),
				util.ToCmdLine("get", "a"),
				util.ToCmdLine("incrby", "mylist", "1"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeIntReply(1),
				protocol.MakeNullBulkReply(),
				protocol.MakeWrongTypeErrReply(),
			},
		},
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "error increment",
			Lines: []database.CmdLine{
				util.ToCmdLine("incrby", "value", "aaa"),
				util.ToCmdLine("get", "value"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOutOfRangeOrNotInt(),
				protocol.MakeNullBulkReply(),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_DecrBy(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "test",
			Lines: []database.CmdLine{
				util.ToCmdLine("decrby", "a", "1"),
				util.ToCmdLine("del", "a"),
				util.ToCmdLine("decrby", "a", "-1"),
				util.ToCmdLine("del", "a"),
				util.ToCmdLine("set", "a", func() string {
					return strconv.Itoa(math.MaxInt64)
				}()),
				util.ToCmdLine("decrby", "a", "-1"),
				util.ToCmdLine("decrby", "a", func() string {
					return strconv.Itoa(math.MaxInt64)
				}()),
				util.ToCmdLine("decrby", "a", func() string {
					return strconv.Itoa(math.MinInt64)
				}()),
				util.ToCmdLine("decrby", "a", "a"),
				util.ToCmdLine("lpush", "mylist", "1"),
				util.ToCmdLine("decrby", "mylist", "1"),
				util.ToCmdLine("flushdb"),
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("decrby", "a", "1"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeIntReply(-1),
				protocol.MakeIntReply(1),
				protocol.MakeIntReply(1),
				protocol.MakeIntReply(1),
				protocol.MakeOkReply(),
				protocol.MakeStandardErrReply("ERR increment or decrement would overflow"),
				protocol.MakeIntReply(0),
				protocol.MakeStandardErrReply("ERR decrement would overflow"),
				protocol.MakeOutOfRangeOrNotInt(),
				protocol.MakeIntReply(1),
				protocol.MakeWrongTypeErrReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOutOfRangeOrNotInt(),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_MGet(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("mset", "k1", "v1", "k2", "v2", "k3", "v3"),
				util.ToCmdLine("mget", "k1", "k2"),
				util.ToCmdLine("mget", "k3", "k4"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeMultiRowReply([]redis.Reply{
					protocol.MakeBulkReply([]byte("v1")),
					protocol.MakeBulkReply([]byte("v2")),
				}),
				protocol.MakeMultiRowReply([]redis.Reply{
					protocol.MakeBulkReply([]byte("v3")),
					protocol.MakeNullBulkReply(),
				}),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}

func Test_GetRange(t *testing.T) {
	_, _ = logger.SetUpLoggerv2(logger.DefaultLevel)
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		wantReplies  []redis.Reply
	}{
		{
			serverSupply: func() database.DBEngine {
				server := MakeStandalone()
				server.Init()
				return server
			},
			Name: "success",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "mykey", "This is a string"),
				util.ToCmdLine("getrange", "mykey", "0", "3"),
				util.ToCmdLine("getrange", "mykey", "-3", "-1"),
				util.ToCmdLine("getrange", "mykey", "0", "-1"),
				util.ToCmdLine("getrange", "mykey", "10", "100"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("This")),
				protocol.MakeBulkReply([]byte("ing")),
				protocol.MakeBulkReply([]byte("This is a string")),
				protocol.MakeBulkReply([]byte("string")),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				req := database.MakeCmdReq(client, line)
				_ = server.PushReqEvent(req)
				deliverResEvent := server.DeliverResEvent()
				cmdRes := <-deliverResEvent
				conn := cmdRes.GetConn()
				assert.Equal(t, client, conn)
				actualReply := cmdRes.GetReply()
				assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
			}
		})
	}
}
