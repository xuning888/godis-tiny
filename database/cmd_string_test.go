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
