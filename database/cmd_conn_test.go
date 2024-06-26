package database

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/xuning888/godis-tiny/interface/database"
	"github.com/xuning888/godis-tiny/interface/redis"
	"github.com/xuning888/godis-tiny/pkg/util"
	"github.com/xuning888/godis-tiny/redis/connection"
	"github.com/xuning888/godis-tiny/redis/protocol"
	"testing"
)

func TestPing(t *testing.T) {
	testCases := []struct {
		Name      string
		Line      database.CmdLine
		wantReply redis.Reply
	}{
		{
			Name:      "简单的ping命令",
			Line:      util.ToCmdLine("ping"),
			wantReply: protocol.MakePongReply(),
		},
		{
			Name:      "ping simple str",
			Line:      util.ToCmdLine("ping", "a"),
			wantReply: protocol.MakeBulkReply([]byte("a")),
		},
		{
			Name:      "超过ping的可选测参数",
			Line:      util.ToCmdLine("ping", "a", "b"),
			wantReply: protocol.MakeNumberOfArgsErrReply("ping"),
		},
	}

	server := MakeStandalone()
	server.Init()
	client := connection.NewConn(nil, false)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server.PushReqEvent(database.MakeCmdReq(client, tc.Line))
			event := server.DeliverResEvent()
			cmdRes := <-event
			conn := cmdRes.GetConn()
			assert.Equal(t, client, conn)
			actualReply := cmdRes.GetReply()
			assert.Equal(t, tc.wantReply, actualReply)
		})
	}
	_ = server.Shutdown(context.Background())
}

func TestSelectDb(t *testing.T) {
	testCases := []struct {
		Name      string
		Line      database.CmdLine
		wantReply redis.Reply
	}{
		{
			Name:      "success",
			Line:      util.ToCmdLine("select", "1"),
			wantReply: protocol.MakeOkReply(),
		},
		{
			Name:      "number of args error",
			Line:      util.ToCmdLine("select", "1", "2"),
			wantReply: protocol.MakeNumberOfArgsErrReply("select"),
		},
		{
			Name:      "error arg",
			Line:      util.ToCmdLine("select", "a"),
			wantReply: protocol.MakeOutOfRangeOrNotInt(),
		},
		{
			Name:      "out of index eg1",
			Line:      util.ToCmdLine("select", "-1"),
			wantReply: protocol.MakeStandardErrReply("ERR DB index is out of range"),
		},
		{
			Name:      "out of index eg2",
			Line:      util.ToCmdLine("select", "16"),
			wantReply: protocol.MakeStandardErrReply("ERR DB index is out of range"),
		},
	}

	server := MakeStandalone()
	server.Init()
	client := connection.NewConn(nil, false)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			server.PushReqEvent(database.MakeCmdReq(client, tc.Line))
			event := server.DeliverResEvent()
			cmdRes := <-event
			conn := cmdRes.GetConn()
			assert.Equal(t, client, conn)
			actualReply := cmdRes.GetReply()
			assert.Equal(t, tc.wantReply, actualReply)
		})
	}

	err := server.Shutdown(context.Background())
	assert.Nil(t, err)
}
