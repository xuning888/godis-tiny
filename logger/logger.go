package logger

import (
	"fmt"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"time"
)

var logger = logrus.New()

type Configuration struct {
	Level         logrus.Level
	TimeFormat    string
	LogPath       string
	EnableFileLog bool
}

func Configure(config *Configuration) {
	logger.Level = config.Level

	// 用于控制台输出的格式
	consoleFormatter := &logrus.TextFormatter{
		TimestampFormat: config.TimeFormat,
		FullTimestamp:   true, // 必须设置为 true 以打印时间戳
	}

	// 控制台输出设置格式
	logger.SetFormatter(consoleFormatter)

	if config.EnableFileLog {
		writerMap := lfshook.WriterMap{
			logrus.InfoLevel:  setupWriter(config.LogPath, "info"),
			logrus.WarnLevel:  setupWriter(config.LogPath, "warn"),
			logrus.ErrorLevel: setupWriter(config.LogPath, "error"),
			logrus.DebugLevel: setupWriter(config.LogPath, "debug"),
		}
		// 用于文件输出的格式
		fileFormatter := &logrus.TextFormatter{
			TimestampFormat: config.TimeFormat,
			FullTimestamp:   true,
			DisableColors:   true, // 文件中需要禁用颜色代码
		}
		hook := lfshook.NewHook(writerMap, fileFormatter)
		logger.AddHook(hook)
	}

	// 输出到 stderr 而不是默认的 stdout
	logger.SetOutput(os.Stderr)
}

func setupWriter(logPath string, level string) *rotatelogs.RotateLogs {
	logFullPath := path.Join(logPath, level)
	writer, _ := rotatelogs.New(
		logFullPath+".%Y%m%d.log",
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)

	return writer
}

func appendGoroutineID(msg string) string {
	return fmt.Sprintf("[g: %v] %s", runtime.NumGoroutine(), msg)
}

func InfoF(format string, args ...interface{}) {
	logger.Infof(appendGoroutineID(format), args...)
}

func DebugF(format string, args ...interface{}) {
	logger.Debugf(appendGoroutineID(format), args...)
}

func ErrorF(format string, args ...interface{}) {
	logger.Errorf(appendGoroutineID(format), args...)
}

func IsEnabledDebug() bool {
	return logger.IsLevelEnabled(logrus.DebugLevel)
}
