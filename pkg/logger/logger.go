package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

var DefaultLevel = zapcore.DebugLevel

var logger *zap.Logger = nil

func SetUpLogger(level zapcore.Level) (*zap.Logger, error) {
	lgcfg := DefaultZapLoggerConfig
	lgcfg.Level = zap.NewAtomicLevelAt(level)
	var err error = nil
	logger, err = lgcfg.Build()
	if err != nil {
		return nil, err
	}
	return logger, err
}

func SetUpLoggerv2(level zapcore.Level) (*zap.Logger, error) {
	lgcfg := DefaultZapLoggerConfig
	lgcfg.Level = zap.NewAtomicLevelAt(level)
	lg, err := lgcfg.Build()
	logger = lg
	return lg, err
}

func CreateLogger(level zapcore.Level) (*zap.Logger, error) {
	lgcfg := DefaultZapLoggerConfig
	lgcfg.Level = zap.NewAtomicLevelAt(level)
	lg, err := lgcfg.Build()
	if err != nil {
		return nil, err
	}
	return lg, err
}

var DefaultZapLoggerConfig = zap.Config{
	Level:       zap.NewAtomicLevelAt(DefaultLevel),
	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},

	Encoding: "console",

	// copied from "zap.NewProductionEncoderConfig" with some updates
	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:       "ts",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder,

		// Custom EncodeTime function to ensure we match format and precision of historic capnslog timestamps
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
		},

		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},

	OutputPaths:      []string{"stderr"},
	ErrorOutputPaths: []string{"stderr"},
}

func Infof(format string, args ...interface{}) {
	sugar := logger.Sugar()
	sugar.Infof(format, args...)
}

func Info(args ...interface{}) {
	logger.Sugar().Info(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Sugar().Debugf(format, args...)
}

func Debug(args ...interface{}) {
	logger.Sugar().Debug(args...)
}

func Warn(args ...interface{}) {
	logger.Sugar().Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Sugar().Warnf(format, args...)
}

func Error(args ...interface{}) {
	logger.Sugar().Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Sugar().Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	logger.Sugar().Fatal(args...)
}
