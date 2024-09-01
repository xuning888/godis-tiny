package redis

import (
	"bufio"
	"context"
	"errors"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"github.com/xuning888/godis-tiny/pkg/util"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// aofBufferSize aof缓冲区大小16MB
	aofBufferSize = 1 << 24
	_             = iota
	none
	rewrite
)

type FileBuffer struct {
	file   *os.File
	writer *bufio.Writer
}

// Write
// 写入缓冲区，如果p的大小大于了缓冲区的可用大小
func (f *FileBuffer) Write(p []byte) (int, error) {
	return f.writer.Write(p)
}

func (f *FileBuffer) Sync() error {
	err := f.writer.Flush()
	if err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *FileBuffer) Close() error {
	return f.file.Close()
}

func (f *FileBuffer) Buffered() int {
	return f.writer.Buffered()
}

func NewFileBuffer(f *os.File, bufSize int) *FileBuffer {
	return &FileBuffer{
		file:   f,
		writer: bufio.NewWriterSize(f, bufSize),
	}
}

const (
	// FsyncAlways do fsync for every command
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type Exec func(ctx context.Context, conn *Client) error

type ForEach func(i int, fun func(key string, object *obj.RedisObject, expiration *time.Time) bool)

// Aof persistence
type Aof struct {
	status      uint32
	exec        Exec
	tempDbMaker func() (Exec, ForEach)
	each        ForEach
	// aofFilename aof文件名称
	aofFilename string
	// aofFsync
	aofFsync string
	// aofFile
	fileBuffer *FileBuffer
	// currentDb 后续命令操作的db
	currentDb int
	// ctx
	ctx context.Context
	// cancel
	cancel context.CancelFunc
	// logger
	lg *zap.Logger
	// mux
	mux sync.Mutex
	// lastRewriteAofSize
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

func (a *Aof) AppendAof(dbIndex int, cmdLine [][]byte) {
	if a.fileBuffer == nil {
		return
	}

	if cmdLine == nil || len(cmdLine) == 0 {
		return
	}

	a.writeAof(dbIndex, cmdLine)
}

func (a *Aof) writeAof(dbIndex int, cmdLine [][]byte) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if dbIndex != a.currentDb {
		selectCmd := util.ToCmdLine("SELECT", strconv.Itoa(dbIndex))
		selectData := MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := a.fileBuffer.Write(selectData)
		if err != nil {
			// 切换db失败
			a.lg.Sugar().Errorf("write aof file buffer failed with error: %v", err)
			return
		}
		a.currentDb = dbIndex
	}
	cmdLineData := MakeMultiBulkReply(cmdLine).ToBytes()
	// 写入文件缓冲区
	_, err := a.fileBuffer.Write(cmdLineData)
	if err != nil {
		a.lg.Sugar().Errorf("write aof file buffer failed with error: %v", err)
	}

	// 如果模式是always,就将内存中的数据拷贝到磁盘
	if a.aofFsync == FsyncAlways {
		err = a.fileBuffer.Sync()
		if err != nil {
			a.lg.Sugar().Errorf("wirte aof file sync fialed with error: %v", err)
		}
	}
}

func (a *Aof) LoadAof(maxBytes int) {

	fileBuffer := a.fileBuffer
	a.fileBuffer = nil
	defer func(fb *FileBuffer) {
		a.fileBuffer = fb
	}(fileBuffer)

	defer a.lg.Sync()

	file, err := os.Open(a.aofFilename)
	if err != nil {
		a.lg.Sugar().Errorf("load aof failed with error: %v", err)
		return
	}
	defer file.Close()
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := DecodeInStream(reader)
	conn := NewClient(0, nil, true)
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
		reply, ok := p.Data.(*MultiBulkReply)
		if !ok {
			a.lg.Sugar().Error("require multi bulk protocol")
			continue
		}
		conn.PushCmd(reply.Args)
		err2 := a.exec(context.Background(), conn)
		if err2 != nil {
			if !errors.Is(err2, ErrorsShutdown) {
				a.lg.Sugar().Warnf("load aof falied with error: %s", err2)
			}
			continue
		}
		if strings.ToLower(string(reply.Args[0])) == "select" {
			dbIndex, err2 := strconv.Atoi(string(reply.Args[1]))
			if err2 != nil {
				a.lg.Sugar().Error(err2)
			}
			a.currentDb = dbIndex
		}
	}
	stat, _ := os.Stat(a.aofFilename)
	a.lastRewriteAofSize = stat.Size()
}

func (a *Aof) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	fsyncEverySec := func() {
		a.mux.Lock()
		defer a.mux.Unlock()
		if a.fileBuffer == nil {
			return
		}
		// 尽量减少sync的次数
		if a.fileBuffer.Buffered() == 0 {
			return
		}
		if err := a.fileBuffer.Sync(); err != nil {
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

func (a *Aof) Shutdown(ctx context.Context) (err error) {
	defer a.lg.Sync()
	// 调用cancel, 关闭其他的goroutine
	defer func() {
		a.cancel()
		// 关闭aof文件
		if err = a.fileBuffer.Close(); err != nil {
			a.lg.Sugar().Errorf("close aof file failed with error: %v", err)
		}
		a.lg.Sugar().Info("shutdown aof complete...")
	}()
	a.lg.Sugar().Infof("shutdown aof begin...")
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		// 尝试把文件数据都落盘
		a.lg.Sugar().Info("Calling fsync() on the Aof file.")
		err = a.fileBuffer.Sync()
		if err != nil {
			return
		}
		var buffered int
		if buffered = a.fileBuffer.Buffered(); buffered == 0 {
			break
		}
		a.lg.Sugar().Infof("Shutdown aof buffered: %v", buffered)
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ticker.C:
			continue
		}
	}
	return
}

func NewAof(exec Exec, filename string, fsync string, tempDbMaker func() (Exec, ForEach)) (*Aof, error) {
	persister := &Aof{}
	persister.status = none
	persister.exec = exec
	// aof 的文件名称
	persister.aofFilename = filename
	// aof 的模式
	persister.aofFsync = strings.ToLower(fsync)

	persister.tempDbMaker = tempDbMaker
	// 创建aof文件
	aofFile, err := initFile(persister.aofFilename)
	if err != nil {
		return nil, err
	}
	persister.fileBuffer = NewFileBuffer(aofFile, aofBufferSize)

	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel

	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		return nil, err
	}
	persister.lg = lg.Named("aof-persister")

	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

func initFile(path string) (file *os.File, err error) {
	dir := filepath.Dir(path)
	err = os.Mkdir(dir, 0755)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	return
}
