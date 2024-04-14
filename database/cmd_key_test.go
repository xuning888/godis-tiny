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
	"strings"
	"testing"
	"time"
)

func Test_KEYS(t *testing.T) {
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
			Name: "keys",
			Lines: []database.CmdLine{
				util.ToCmdLine("mset", strings.Split("firstname Jack lastname Stuntman age 35", " ")...),
				util.ToCmdLine("set", "hello", "1"),
				util.ToCmdLine("set", "hallo", "1"),
				util.ToCmdLine("set", "hxllo", "1"),
				util.ToCmdLine("set", "hllo", "1"),
				util.ToCmdLine("set", "heeeello", "1"),
				util.ToCmdLine("set", "hillo", "1"),
				util.ToCmdLine("set", "hbllo", "1"),
				util.ToCmdLine("keys", "*name*"),
				util.ToCmdLine("keys", "h?llo"),
				util.ToCmdLine("keys", "h*llo"),
				util.ToCmdLine("keys", "h[ae]llo"),
				util.ToCmdLine("keys", "h[^e]llo"),
				util.ToCmdLine("keys", "h[a-b]llo"),
				util.ToCmdLine("keys"),
				util.ToCmdLine("keys", "*"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeMultiBulkReply([][]byte{[]byte("firstname"), []byte("lastname")}),
				protocol.MakeMultiBulkReply([][]byte{[]byte("hello"), []byte("hallo"), []byte("hxllo"), []byte("hillo"), []byte("hbllo")}),
				protocol.MakeMultiBulkReply([][]byte{[]byte("hello"), []byte("hallo"), []byte("hxllo"), []byte("hllo"), []byte("heeeello"), []byte("hillo"), []byte("hbllo")}),
				protocol.MakeMultiBulkReply([][]byte{[]byte("hello"), []byte("hallo")}),
				protocol.MakeMultiBulkReply([][]byte{[]byte("hallo"), []byte("hxllo"), []byte("hillo"), []byte("hbllo")}),
				protocol.MakeMultiBulkReply([][]byte{[]byte("hallo"), []byte("hbllo")}),
				protocol.MakeNumberOfArgsErrReply("keys"),
				protocol.MakeMultiBulkReply([][]byte{[]byte("firstname"), []byte("lastname"), []byte("age"), []byte("hello"), []byte("hallo"), []byte("hxllo"), []byte("hllo"), []byte("heeeello"), []byte("hillo"), []byte("hbllo")}),
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
				expectedReply := tc.wantReplies[idx]
				ac, ok := actualReply.(*protocol.MultiBulkReply)
				ex, ok2 := expectedReply.(*protocol.MultiBulkReply)
				if ok && ok2 {
					assert.ElementsMatch(t, ac.Args, ex.Args)
				} else {
					assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
				}
			}
		})
	}
}

func Test_Exists(t *testing.T) {
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
			Name: "exists",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("set", "c", "d"),
				util.ToCmdLine("exists", "a"),
				util.ToCmdLine("exists", "b"),
				util.ToCmdLine("exists"),
				util.ToCmdLine("exists", "a", "c"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1),
				protocol.MakeIntReply(0),
				protocol.MakeNumberOfArgsErrReply("exists"),
				protocol.MakeIntReply(2),
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

func Test_TTL(t *testing.T) {
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
			Name: "ttl",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b", "ex", "10"),
				util.ToCmdLine("ttl", "a"),
				util.ToCmdLine("ttl"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(10),
				protocol.MakeNumberOfArgsErrReply("ttl"),
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

func Test_PTTL(t *testing.T) {
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
			Name: "pttl",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b", "ex", "10"),
				util.ToCmdLine("pttl", "a"),
				util.ToCmdLine("pttl"),
				util.ToCmdLine("set", "c", "d"),
				util.ToCmdLine("pttl", "c"),
				util.ToCmdLine("pttl", "b"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(10000),
				protocol.MakeNumberOfArgsErrReply("pttl"),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(-1),
				protocol.MakeIntReply(-2),
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
				intReply, ok := actualReply.(*protocol.IntReply)
				if ok {
					exIntReply, _ := tc.wantReplies[idx].(*protocol.IntReply)
					if intReply.Num < 0 {
						assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
					} else {
						// 小于10ms, 就认为是可接受的结果
						assert.True(t, exIntReply.Num-intReply.Num < 10, fmt.Sprintf("%dms", exIntReply.Num-intReply.Num))
					}
				} else {
					assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
				}
			}
		})
	}
}

func Test_Expire(t *testing.T) {
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
			Name: "expire",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("expire", "a", "10"),
				util.ToCmdLine("expire", "a", "a"),
				util.ToCmdLine("expire", "c", "10"),
				util.ToCmdLine("expire"),
				util.ToCmdLine("expire", "d"),
				util.ToCmdLine("pttl", "a"),
				util.ToCmdLine("pttl"),
				util.ToCmdLine("set", "c", "d"),
				util.ToCmdLine("pttl", "c"),
				util.ToCmdLine("pttl", "b"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1),
				protocol.MakeOutOfRangeOrNotInt(),
				protocol.MakeIntReply(0),
				protocol.MakeNumberOfArgsErrReply("expire"),
				protocol.MakeNumberOfArgsErrReply("expire"),
				protocol.MakeIntReply(10000),
				protocol.MakeNumberOfArgsErrReply("pttl"),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(-1),
				protocol.MakeIntReply(-2),
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
				intReply, ok := actualReply.(*protocol.IntReply)
				if ok {
					exIntReply, _ := tc.wantReplies[idx].(*protocol.IntReply)
					if intReply.Num < 0 {
						assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
					} else {
						// 小于10ms, 就认为是可接受的结果
						assert.True(t, exIntReply.Num-intReply.Num < 10, fmt.Sprintf("%dms", exIntReply.Num-intReply.Num))
					}
				} else {
					assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
				}
			}
		})
	}
}

func Test_Persist(t *testing.T) {
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
			Name: "persist",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("expire", "a", "10"),
				util.ToCmdLine("pttl", "a"),
				util.ToCmdLine("persist", "a"),
				util.ToCmdLine("persist"),
				util.ToCmdLine("persist", "b"),
				util.ToCmdLine("pttl"),
				util.ToCmdLine("set", "c", "d"),
				util.ToCmdLine("pttl", "c"),
				util.ToCmdLine("pttl", "b"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1),
				protocol.MakeIntReply(10000),
				protocol.MakeIntReply(1),
				protocol.MakeNumberOfArgsErrReply("persist"),
				protocol.MakeIntReply(0),
				protocol.MakeNumberOfArgsErrReply("pttl"),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(-1),
				protocol.MakeIntReply(-2),
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
				intReply, ok := actualReply.(*protocol.IntReply)
				if ok {
					exIntReply, _ := tc.wantReplies[idx].(*protocol.IntReply)
					if intReply.Num < 0 {
						assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
					} else {
						// 小于10ms, 就认为是可接受的结果
						assert.True(t, exIntReply.Num-intReply.Num < 10, fmt.Sprintf("%dms", exIntReply.Num-intReply.Num))
					}
				} else {
					assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
				}
			}
		})
	}
}

func Test_ExpireAt(t *testing.T) {
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
			Name: "persist",
			Lines: []database.CmdLine{
				util.ToCmdLine("set", "a", "b"),
				util.ToCmdLine("expireat", "a", fmt.Sprintf("%d", time.Now().Add(time.Second*10).UnixMilli())),
				util.ToCmdLine("expireat", "a", "a"),
				util.ToCmdLine("expireat", "a"),
				util.ToCmdLine("expireat", "a", "1000", "a"),
				util.ToCmdLine("expireat"),
				util.ToCmdLine("expireat", "b", fmt.Sprintf("%d", time.Now().Add(time.Second*10).UnixMilli())),
				util.ToCmdLine("pttl", "a"),
				util.ToCmdLine("pttl"),
				util.ToCmdLine("set", "c", "d"),
				util.ToCmdLine("pttl", "c"),
				util.ToCmdLine("pttl", "b"),
			},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeIntReply(1),
				protocol.MakeOutOfRangeOrNotInt(),
				protocol.MakeNumberOfArgsErrReply("expireat"),
				protocol.MakeNumberOfArgsErrReply("expireat"),
				protocol.MakeNumberOfArgsErrReply("expireat"),
				protocol.MakeIntReply(0),
				protocol.MakeIntReply(10000),
				protocol.MakeNumberOfArgsErrReply("pttl"),
				protocol.MakeOkReply(),
				protocol.MakeIntReply(-1),
				protocol.MakeIntReply(-2),
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
				intReply, ok := actualReply.(*protocol.IntReply)
				if ok {
					exIntReply, _ := tc.wantReplies[idx].(*protocol.IntReply)
					if intReply.Num < 0 {
						assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
					} else {
						// 小于10ms, 就认为是可接受的结果
						assert.True(t, exIntReply.Num-intReply.Num < 10, fmt.Sprintf("%dms", exIntReply.Num-intReply.Num))
					}
				} else {
					assert.Equal(t, tc.wantReplies[idx], actualReply, fmt.Sprintf("idx: %d", idx))
				}
			}
		})
	}
}
