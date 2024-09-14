package redis

import (
	"context"
	"errors"
	"github.com/xuning888/godis-tiny/config"
	"github.com/xuning888/godis-tiny/pkg/datastruct/obj"
	"github.com/xuning888/godis-tiny/pkg/util"
	"sync"
	"time"
)

var (
	lock           = sync.Mutex{}
	processWait    = sync.WaitGroup{}
	systemClient   = NewClient(0, nil, true)
	ttlOpsCmdLine  = util.ToCmdLine("ttlops")
	ErrorsShutdown = errors.New("shutdown")
)

func (r *RedisServer) Init() {
	begin := time.Now()
	r.loadAof()
	r.lg.Infof("DB loaded from append only file: %.3f seconds", time.Now().Sub(begin).Seconds())
}

func (r *RedisServer) loadAof() {
	processWait.Add(1)
	defer processWait.Done()
	if config.Properties.AppendOnly {
		r.aof.LoadAof(0)
	}
}

func (r *RedisServer) cron() {
	systemClient.PushCmd(ttlOpsCmdLine)
	if err := r.process(context.Background(), systemClient); err != nil {
		return
	}
	// 触发aof重写
	//r.doAofRewrite()
}

func (r *RedisServer) process(ctx context.Context, conn *Client) error {
	lock.Lock()
	processWait.Add(1)
	defer func() {
		lock.Unlock()
		processWait.Done()
	}()

	if r.shutdown.Load() {
		return ErrorsShutdown
	}

	conn.Rewrite = r.rewrite
	conn.RangeCheck = r.RangeCheck
	conn.ClearDatabase = r.clear

	for conn.HasRemaining() {
		dbIndex := conn.GetDbIndex()
		mdb, err := r.SelectDb(dbIndex)
		if err != nil {
			if err2 := MakeStandardErrReply(err.Error()).WriteTo(conn); err2 != nil {
				return err2
			}
			conn.ResetQueryBuffer()
			return nil
		}
		conn.SetDb(mdb)
		if err = r.processCmd(ctx, conn); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisServer) processCmd(ctx context.Context, conn *Client) error {
	defer func() {
		conn.curCommand = nil
	}()
	_ = conn.PollCmd()
	cmdName := conn.GetCmdName()
	cmd, err := router(cmdName)
	if err != nil {
		args := conn.GetArgs()
		with := make([]string, 0, len(args))
		for _, arg := range args {
			with = append(with, "'"+string(arg)+"'")
		}
		return MakeUnknownCommand(cmdName, with...).WriteTo(conn)
	}
	if cmdName != "ttlops" {
		conn.GetDb().RandomCheckTTLAndClearV1()
	}
	return cmd.process(ctx, conn)
}

func (r *RedisServer) SelectDb(index int) (*DB, error) {
	if err := r.RangeCheck(index); err != nil {
		return nil, err
	}
	return r.dbs[index], nil
}

func (r *RedisServer) RangeCheck(index int) error {
	if index < 0 || index >= len(r.dbs) {
		return errors.New("ERR DB index is out of range")
	}
	return nil
}

func (r *RedisServer) ForEach(dbIndex int, cb func(key string, entity *obj.RedisObject, expiration *time.Time) bool) {
	mdb, _ := r.SelectDb(dbIndex)
	if mdb.Len() > 0 {
		mdb.ForEach(cb)
	}
}

func (r *RedisServer) clear() {
	if r.dbs != nil && len(r.dbs) > 0 {
		for _, mdb := range r.dbs {
			mdb.RandomCheckTTLAndClearV1()
		}
	}
}

func (r *RedisServer) rewrite() error {
	return r.aof.Rewrite()
}

func (r *RedisServer) doAofRewrite() {
	if !config.Properties.AppendOnly {
		return
	}
	defer r.lg.Sync()
	// 当前aof文件的大小
	currentAofFileSize, err := r.aof.CurrentAofSize()
	if err != nil {
		r.lg.Errorf("check aof filesize failed with error: %v", err)
		return
	}
	// 上一次aof重写后的大小
	lastAofRewriteSize := r.aof.LasAofRewriteSize()
	// 计算aof文件的增长量
	aofSizeIncrease := currentAofFileSize - lastAofRewriteSize

	if aofSizeIncrease <= 0 {
		return
	}
	// 计算当前增长的百分比是否超过设置的阈值
	rewriteNeeded := (aofSizeIncrease >= int64(float64(lastAofRewriteSize)*float64(config.Properties.AofRewritePercentage)/100.0)) &&
		(currentAofFileSize >= int64(config.Properties.AofRewriteMinSize))

	if rewriteNeeded {
		go func() {
			err2 := r.aof.Rewrite()
			if err2 != nil {
				if !errors.Is(err2, ErrAofRewriteIsRunning) {
					r.lg.Errorf("aof rewrite failed with error: %v", err2)
				}
			} else {
				r.lg.Info("aof rewrite successfully")
			}
		}()
	}
}
