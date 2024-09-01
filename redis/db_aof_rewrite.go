package redis

import (
	"bufio"
	"errors"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/pkg/datastruct/list"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"github.com/xuning888/godis-tiny/pkg/logger"
	"github.com/xuning888/godis-tiny/pkg/util"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type RewriteCtx struct {
	tmpFile     *os.File
	fileSize    int64
	dbIdx       int
	writtenSize int64
}

var (
	ErrAofRewriteIsRunning = errors.New("aof rewrite is running")
)

func (a *Aof) Rewrite() error {

	if atomic.LoadUint32(&a.status) != none {
		return ErrAofRewriteIsRunning
	}

	if !atomic.CompareAndSwapUint32(&a.status, none, rewrite) {
		return ErrAofRewriteIsRunning
	}

	// 准备重写时需要的信息, 这个时候会暂停aof的写入
	ctx, err := a.StartRewrite()
	if err != nil {
		return err
	}

	// 把aof重写前的数据拷贝到内存,然后使用命令替换的方式重写到tmpFile, 这个时候是允许 aof 继续写入的
	err = a.DoRewrite(ctx)
	if err != nil {
		return err
	}

	// 加锁禁止aof写入，直到aof数据整合完毕
	a.FinishRewrite(ctx)
	a.lg.Sugar().Info("rewrite aof completed")
	atomic.CompareAndSwapUint32(&a.status, rewrite, none)
	return nil
}

func (a *Aof) DoRewrite(ctx *RewriteCtx) (err error) {

	// 临时文件
	tmpFile := ctx.tmpFile
	buffer := bufio.NewWriterSize(tmpFile, 1<<16)
	defer func() {
		err = buffer.Flush()
		if err != nil {
			a.lg.Sugar().Errorf("DoRewrite flush aof file failed with error: %v", err)
		}
	}()

	// 将重写开始前的数据加载到内存
	tmpAof := a.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))

	// 将内存中的数据写到临时文件
	// 遍历DB, 获取其中的每一个数据，根据其数据类型将其转换为命令写入tmpFile
	// string类型: incr a 会被重写为  set a 1 命令
	// list 类型: 遍历list中的所有数据, 重写为 rpush ele1 ele2 ele3
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := MakeMultiBulkReply(util.ToCmdLine("select", strconv.Itoa(i))).ToBytes()
		written1, err := buffer.Write(data)
		if err != nil {
			return err
		}
		ctx.writtenSize += int64(written1)
		// 将内存中的数据写入临时文件
		tmpAof.each(i, func(key string, redisObj *obj.RedisObject, expiration *time.Time) bool {
			cmd := EntityToCmd(key, redisObj)
			if cmd != nil {
				written2, _ := buffer.Write(cmd.ToBytes())
				ctx.writtenSize += int64(written2)
			}
			if expiration != nil {
				expireCmd := ExpireCmd(key, expiration)
				if expireCmd != nil {
					written3, _ := buffer.Write(expireCmd.ToBytes())
					ctx.writtenSize += int64(written3)
				}
			}
			return true
		})
	}
	return nil
}

func (a *Aof) FinishRewrite(ctx *RewriteCtx) {
	// 暂停aof写入
	a.mux.Lock()
	defer a.mux.Unlock()

	tmpFile := ctx.tmpFile
	errOccurs := func() bool {

		// 尝试打开重写前的 aof文件
		src, err := os.Open(a.aofFilename)
		if err != nil {
			a.lg.Sugar().Errorf("open aofFilname %v fialed with error: %v", a.aofFilename, err)
			return true
		}

		buffer := bufio.NewWriter(tmpFile)
		defer func() {
			_ = buffer.Flush()
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		// 跳转到重写前的末尾
		_, err = src.Seek(ctx.fileSize, io.SeekStart)
		if err != nil {
			a.lg.Sugar().Warnf("seek failed with error: %v", err)
			return true
		}

		// 插入一个aof重写前aof写入时使用的db
		selectDbBytes := MakeMultiBulkReply(util.ToCmdLine("select", strconv.Itoa(ctx.dbIdx))).ToBytes()

		written1, err := buffer.Write(selectDbBytes)
		if err != nil {
			a.lg.Sugar().Errorf("tmp file rewrite failed with error: %v", err)
			return true
		}
		ctx.writtenSize += int64(written1)

		// 把重写时可能写入到原来aof文件中命令拷贝到tmp文件中
		written2, err := io.Copy(buffer, src)
		if err != nil {
			a.lg.Sugar().Errorf("copy aof file failed with error: %v", err)
			return true
		}
		ctx.writtenSize += written2
		return false
	}

	// 把DoRewrite期间的产生的数据落盘
	err2 := a.fileBuffer.Sync()
	if err2 != nil {
		a.lg.Sugar().Errorf("last sync aoffile fialed error %v", err2)
		return
	}

	if errOccurs() {
		return
	}

	// 关闭原来的aofFile
	err := a.fileBuffer.Close()
	if err != nil {
		a.lg.Sugar().Errorf("close aofFile failed with error: %v", err)
	}
	// 使用 mv 命令把 原来的 aofFile 替换为 重写后的 tmpFile
	if err = os.Rename(tmpFile.Name(), a.aofFilename); err != nil {
		a.lg.Sugar().Errorf("rename aof file failed with error: %v", err)
	}

	// 记录aof重写完成后的文件大小
	a.lastRewriteAofSize = ctx.writtenSize

	// 重新打开 aofFile
	aofFile, err := os.OpenFile(a.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	// 替换aofFile
	a.fileBuffer = NewFileBuffer(aofFile, aofBufferSize)
	// 插入一个aof当前记录的db的命令 select curDbIndex
	selectBytes := MakeMultiBulkReply(util.ToCmdLine("select", strconv.Itoa(a.currentDb))).ToBytes()
	_, err = a.fileBuffer.Write(selectBytes)
	if err != nil {
		panic(err)
	}
}

func (a *Aof) StartRewrite() (*RewriteCtx, error) {
	// 加锁暂停主流程的aof写入
	a.mux.Lock()
	defer a.mux.Unlock()

	// fsync 将缓冲区中的数据落盘，防止aof文件不完整造成数据错误
	err := a.fileBuffer.Sync()
	if err != nil {
		a.lg.Sugar().Warnf("fsync failed, err: %v", err)
		return nil, err
	}

	// 获取当前aof文件大小, 用于判断哪些数据是 aof 重写过程中产生的
	// 因为前边已经暂停了aof的落盘, 可以确定当前aof文件的大小
	fileInfo, _ := os.Stat(a.aofFilename)
	filesize := fileInfo.Size()

	// 创建临时文件目录
	if _, err = os.Stat(config.TmpDir()); os.IsNotExist(err) {
		err = os.MkdirAll(config.TmpDir(), 0755)
		if err != nil {
			a.lg.Sugar().Errorf("tmp file create failed, err: %v", err)
			return nil, err
		}
	}

	// 创建临时文件供重写使用
	file, err := os.CreateTemp(config.TmpDir(), "*.aof")
	if err != nil {
		a.lg.Sugar().Warnf("tmp file create failed, err: %v", err)
		return nil, err
	}

	ctx := &RewriteCtx{
		// tmpFile 临时文件
		tmpFile: file,
		// fileSize aof文件的大小
		fileSize: filesize,
		// aof记录的dbIndex
		dbIdx:       a.currentDb,
		writtenSize: 0,
	}
	return ctx, nil
}

func EntityToCmd(key string, redisObj *obj.RedisObject) *MultiBulkReply {
	if redisObj == nil {
		return nil
	}
	switch redisObj.ObjType {
	case obj.RedisString:
		result, _ := obj.StringObjEncoding(redisObj)
		return stringToCmd(key, result)
	case obj.RedisList:
		dequeue := redisObj.Ptr.(list.Dequeue)
		return listToCmd(key, dequeue)
	default:
		return nil
	}
}

func ExpireCmd(key string, expiration *time.Time) *MultiBulkReply {
	cmdLine := util.MakeExpireCmd(key, *expiration)
	return MakeMultiBulkReply(cmdLine)
}

var setCmd = []byte("set")

func stringToCmd(key string, bytes []byte) *MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return MakeMultiBulkReply(args)
}

var pushCmd = []byte("rpush")

func listToCmd(key string, deque list.Dequeue) *MultiBulkReply {
	args := make([][]byte, 2+deque.Len())
	args[0] = pushCmd
	args[1] = []byte(key)
	i := 0
	deque.ForEach(func(value interface{}, index int) bool {
		bytes, _ := value.([]byte)
		args[i+2] = bytes
		i++
		return true
	})
	return MakeMultiBulkReply(args)
}

func (a *Aof) newRewriteHandler() *Aof {
	h := &Aof{}
	h.aofFilename = a.aofFilename
	h.exec, h.each = a.tempDbMaker()
	lg, err := logger.CreateLogger(logger.DefaultLevel)
	if err != nil {
		panic(err)
	}
	h.lg = lg
	return h
}
