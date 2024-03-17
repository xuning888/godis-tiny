package persistence

import (
	"context"
	"go.uber.org/zap"
	"godis-tiny/interface/database"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/redis/connection"
	"godis-tiny/redis/parser"
	"godis-tiny/redis/protocol"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	aofQueueSize = 1 << 16
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type CmdLine [][]byte

type payload struct {
	dbIndex int
	cmdLine CmdLine
}

type Listener interface {
	CallBack([]CmdLine)
}

var _ Aof = &aof{}

// Aof aof的功能定义
type Aof interface {
	// AppendAof 添加到Aof中
	AppendAof(dbIndex int, cmdLine CmdLine)

	LoadAof(maxBytes int)
}

// Aof persistence
type aof struct {
	db database.DBEngine
	// aofFilename aof文件名称
	aofFilename string
	// aofFsync
	aofFsync string
	// aofFile
	aofFile *os.File
	// aofChan
	aofChan chan *payload
	// aofFinished
	aofFinished chan struct{}
	// currentDb 后续命令操作的db
	currentDb int
	// listeners
	listeners map[Listener]struct{}
	// ctx
	ctx context.Context
	// cancel
	cancel context.CancelFunc
	lg     *zap.Logger
	mux    sync.Mutex
}

func (a *aof) AppendAof(dbIndex int, cmdLine CmdLine) {
	if a.aofChan == nil {
		return
	}

	p := &payload{
		dbIndex: dbIndex,
		cmdLine: cmdLine,
	}

	// fsync == always
	if a.aofFsync == FsyncAlways {
		a.writeAof(p)
		return
	}

	// 写入缓冲区
	a.aofChan <- p
}

func (a *aof) writeAof(p *payload) {
	if p.cmdLine == nil || len(p.cmdLine) == 0 {
		return
	}
	a.mux.Lock()
	defer a.mux.Unlock()
	if p.dbIndex != a.currentDb {
		selectCmd := util.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		selectData := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := a.aofFile.Write(selectData)
		if err != nil {
			// 切换db失败
			a.lg.Sugar().Errorf("write aof file buffer failed with error: %v", err)
			return
		}
		a.currentDb = p.dbIndex
	}
	cmdLineData := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	// 写入文件缓冲区
	_, err := a.aofFile.Write(cmdLineData)
	if err != nil {
		a.lg.Sugar().Errorf("write aof file buffer failed with error: %v", err)
	}

	// 如果模式是always,就将内存中的数据拷贝到磁盘
	if a.aofFsync == FsyncAlways {
		err = a.aofFile.Sync()
		if err != nil {
			a.lg.Sugar().Errorf("wirte aof file sync fialed with error: %v", err)
		}
	}
}

func (a *aof) LoadAof(maxBytes int) {
	// 在加载aof文件时，先把chan拿走，加载完事后再放回去
	aofChan := a.aofChan
	a.aofChan = nil
	defer func(aofChan chan *payload) {
		a.aofChan = aofChan
	}(aofChan)

	defer a.lg.Sync()

	file, err := os.Open(a.aofFilename)
	defer file.Close()
	if err != nil {
		a.lg.Sugar().Errorf("load aof failed with error: %v", err)
		return
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	cnt := 0
	ch := parser.DecodeInStream(reader)
	conn := connection.NewConn(nil, true)
	for p := range ch {
		if p.Error != nil {
			if p.Error == io.EOF {
				break
			}
			a.lg.Sugar().Error("parse error: " + p.Error.Error())
			continue
		}
		if p.Data == nil {
			a.lg.Sugar().Error("empty payload")
			continue
		}
		reply, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			a.lg.Sugar().Error("require multi bulk protocol")
			continue
		}
		cnt++
		cmdRes := a.db.Exec(database.MakeCmdReq(conn, reply.Args))
		resReply := cmdRes.GetReply()
		if protocol.IsErrorReply(resReply) {
			a.lg.Sugar().Warnf("load aof falied with error: %s", resReply.ToBytes())
		}
		if strings.ToLower(string(reply.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(reply.Args[1]))
			if err != nil {
				logger.Errorf("")
			}
			a.currentDb = dbIndex
		}
	}
	a.lg.Sugar().Infof("load aof complete, cnt: %v", cnt)
}

func (a *aof) listenCmd() {
	ch := a.aofChan
	for p := range ch {
		a.writeAof(p)
	}
	a.aofFinished <- struct{}{}
}

func (a *aof) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	fsyncEverySec := func() {
		a.mux.Lock()
		defer a.mux.Unlock()
		if err := a.aofFile.Sync(); err != nil {
			a.lg.Sugar().Errorf("fsync everysec failed: %v", err)
		}
	}
	go func() {
		for {
			select {
			case <-ticker.C:
				fsyncEverySec()
			case <-a.ctx.Done():
				return
			}
		}
	}()
}

func (a *aof) Close() {
	if a.aofFile != nil {
		close(a.aofChan)
		<-a.aofFinished
		err := a.aofFile.Close()
		if err != nil {
			a.lg.Sugar().Warnf("close aof file failed with error: %v", err)
		}
	}
	a.cancel()
}

func NewAof(db database.DBEngine, filename string, fsync string) (Aof, error) {
	persister := &aof{}
	persister.db = db
	// aof 的文件名称
	persister.aofFilename = filename
	// aof 的模式
	persister.aofFsync = strings.ToLower(fsync)

	// 创建aof文件
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile

	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})

	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel

	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		return nil, err
	}
	persister.lg = lg.Named("aof-persister")

	go func() {
		persister.listenCmd()
	}()

	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}
