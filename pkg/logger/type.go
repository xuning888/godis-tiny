package logger

type Logger interface {
	// Debugf logs messages at DEBUG level.
	Debugf(format string, args ...interface{})
	// Infof logs messages at INFO level.
	Infof(format string, args ...interface{})
	// Info logs messages at INFO level.
	Info(args ...interface{})
	// Warnf logs messages at WARN level.
	Warnf(format string, args ...interface{})
	// Errorf logs messages at ERROR level.
	Errorf(format string, args ...interface{})
	// Error logs messages at ERROR level.
	Error(args ...interface{})
	// Fatalf logs messages at FATAL level.
	Fatalf(format string, args ...interface{})
	// Sync sync
	Sync() error
}

// Debugf logs messages at DEBUG level.
func Debugf(format string, args ...interface{}) {
	globalLogger.Sugar().Debugf(format, args)
}

// Infof logs messages at INFO level.
func Infof(format string, args ...interface{}) {
	globalLogger.Sugar().Infof(format, args)
}

func Info(args ...interface{}) {
	globalLogger.Sugar().Info(args)
}

// Warnf logs messages at WARN level.
func Warnf(format string, args ...interface{}) {
	globalLogger.Sugar().Warnf(format, args)
}

// Errorf logs messages at ERROR level.
func Errorf(format string, args ...interface{}) {
	globalLogger.Sugar().Errorf(format, args)
}

func Error(args ...interface{}) {
	globalLogger.Sugar().Error(args)
}

// Fatalf logs messages at FATAL level.
func Fatalf(format string, args ...interface{}) {
	globalLogger.Sugar().Fatalf(format, args)
}

// Sync sync
func Sync() error {
	return globalLogger.Sugar().Sync()
}
