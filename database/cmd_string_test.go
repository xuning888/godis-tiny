package database

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xuning888/godis-tiny/interface/database"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis/connection"
	"github.com/xuning888/godis-tiny/redis/protocol"
	"testing"
	"time"
)

func TestExeSet(t *testing.T) {

	// TODO 这个单元测试写的不好，有时间重写一下
	testCases := []struct {
		serverSupply func() database.DBEngine
		Name         string
		Lines        []database.CmdLine
		waitTimes    []int
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
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("set", "key", "value", "ex", "2"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("get", "key"),
			},
			waitTimes: []int{0, 1000, 0, 1000, 1000},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeNullBulkReply(),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value")),
				protocol.MakeNullBulkReply(),
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
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("set", "key", "value", "ex", "2000"),
				util.ToCmdLine("get", "key"),
				util.ToCmdLine("get", "key"),
			},
			waitTimes: []int{0, 1000, 0, 1000, 1000},
			wantReplies: []redis.Reply{
				protocol.MakeOkReply(),
				protocol.MakeNullBulkReply(),
				protocol.MakeOkReply(),
				protocol.MakeBulkReply([]byte("value")),
				protocol.MakeNullBulkReply(),
			},
		},
	}

	client := connection.NewConn(nil, false)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server := tc.serverSupply()
			for idx, line := range tc.Lines {
				if tc.waitTimes != nil {
					waitTime := tc.waitTimes[idx]
					if waitTime != 0 {
						time.Sleep(time.Millisecond * time.Duration(waitTime))
					}
				}
				req := database.MakeCmdReq(client, line)
				server.PushReqEvent(req)
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
