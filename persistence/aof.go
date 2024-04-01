package persistence

import (
	"context"
	"go.uber.org/zap"
	"godis-tiny/interface/database"
	"godis-tiny/pkg/logger"
	"godis-tiny/pkg/util"
	"godis-tiny/pkg/wait"
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
	aofQueueSize = 1
)

const (
	_ = iota
	none
	rewrite
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

// Aof persistence
type Aof struct {
	status      uint32
	db          database.DBEngine
	tempDbMaker func() database.DBEngine
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
	cancel             context.CancelFunc
	lg                 *zap.Logger
	mux                sync.Mutex
	wait               wait.Wait
	lastRewriteAofSize int64
}

// CurrentAofSize aof file size
func (a *Aof) CurrentAofSize() (int64, error) {
	stat, err := os.Stat(a.aofFilename)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// LasAofRewriteSize last aof file size
func (a *Aof) LasAofRewriteSize() int64 {
	return a.lastRewriteAofSize
}

func (a *Aof) AppendAof(dbIndex int, cmdLine CmdLine) {
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
	a.addToAofBuffer(p)
}

// addToAofBuffer add command to aof channel
func (a *Aof) addToAofBuffer(p *payload) {
	go func() {
		defer a.wait.Done()
		a.wait.Add(1)
		select {
		case <-a.ctx.Done():
			return
		case a.aofChan <- p:
		}
	}()
}

func (a *Aof) writeAof(p *payload) {
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

func (a *Aof) LoadAof(maxBytes int) {
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
	stat, _ := os.Stat(a.aofFilename)
	a.lastRewriteAofSize = stat.Size()
	a.lg.Sugar().Infof("load aof complete, cnt: %v", cnt)
}

func (a *Aof) listenCmd() {
	ch := a.aofChan
	for p := range ch {
		a.writeAof(p)
	}
	a.aofFinished <- struct{}{}
}

func (a *Aof) fsyncEverySecond() {
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

func (a *Aof) Close() {
	a.wait.Wait()
	if a.aofFile != nil {
		for chanLen := len(a.aofChan); chanLen > 0; {
			a.lg.Sugar().Warnf("aof chan size: %v", chanLen)
			time.Sleep(time.Millisecond * 100)
		}
		close(a.aofChan)
		<-a.aofFinished
		err := a.aofFile.Close()
		if err != nil {
			a.lg.Sugar().Warnf("close aof file failed with error: %v", err)
		}
	}
	a.cancel()
}

func (a *Aof) Shutdown(ctx context.Context) error {
	a.lg.Sugar().Info("shutdown aof...")
	a.wait.Wait()
	a.cancel()
	close(a.aofChan)
	for lenAofChan := len(a.aofChan); lenAofChan > 0; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 10):
			a.lg.Sugar().Warnf("aof chan len: %v", lenAofChan)
		}
	}
	<-a.aofFinished
	var err error
	if err = a.aofFile.Sync(); err != nil {
		a.lg.Sugar().Errorf("shutdown Aof sync aofFile failed with error: %v", err)
	}
	if err = a.aofFile.Close(); err != nil {
		a.lg.Sugar().Errorf("close aof file failed with error: %v", err)
	}
	return err
}

func NewAof(db database.DBEngine, filename string, fsync string, tempDbMaker func() database.DBEngine) (*Aof, error) {
	persister := &Aof{}
	persister.status = none
	persister.db = db
	// aof 的文件名称
	persister.aofFilename = filename
	// aof 的模式
	persister.aofFsync = strings.ToLower(fsync)

	persister.tempDbMaker = tempDbMaker
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
