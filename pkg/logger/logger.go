package logger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// LogLevel определяет уровень логирования
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// String возвращает строковое представление уровня логирования
func (l LogLevel) String() string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	default:
		return "unknown"
	}
}

// Logger интерфейс для логирования
type Logger interface {
	Debug(format string, v ...interface{})
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(format string, v ...interface{})

	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

// StructuredLogger реализация структурированного логгера
type StructuredLogger struct {
	level     LogLevel
	output    io.Writer
	debugMode bool
	mu        sync.Mutex
}

// NewLogger создает новый логгер с указанным уровнем и выходным потоком
func NewLogger(level LogLevel, output io.Writer) Logger {
	return &StructuredLogger{
		level:     level,
		output:    output,
		debugMode: level == DebugLevel,
	}
}

// ParseLogLevel парсит строку в уровень логирования
func ParseLogLevel(s string) (LogLevel, error) {
	switch strings.ToLower(s) {
	case "debug":
		return DebugLevel, nil
	case "info":
		return InfoLevel, nil
	case "warn":
		return WarnLevel, nil
	case "error":
		return ErrorLevel, nil
	default:
		return InfoLevel, fmt.Errorf("unknown log level: %s", s)
	}
}

// Debug логирует сообщение уровня debug
func (l *StructuredLogger) Debug(format string, v ...interface{}) {
	if l.level > DebugLevel {
		return
	}
	l.log(DebugLevel, format, v...)
}

// Info логирует сообщение уровня info
func (l *StructuredLogger) Info(format string, v ...interface{}) {
	if l.level > InfoLevel {
		return
	}
	l.log(InfoLevel, format, v...)
}

// Warn логирует сообщение уровня warn
func (l *StructuredLogger) Warn(format string, v ...interface{}) {
	if l.level > WarnLevel {
		return
	}
	l.log(WarnLevel, format, v...)
}

// Error логирует сообщение уровня error
func (l *StructuredLogger) Error(format string, v ...interface{}) {
	if l.level > ErrorLevel {
		return
	}
	l.log(ErrorLevel, format, v...)
}

// Debugf логирует сообщение уровня debug с форматированием
func (l *StructuredLogger) Debugf(format string, v ...interface{}) {
	l.Debug(format, v...)
}

// Infof логирует сообщение уровня info с форматированием
func (l *StructuredLogger) Infof(format string, v ...interface{}) {
	l.Info(format, v...)
}

// Warnf логирует сообщение уровня warn с форматированием
func (l *StructuredLogger) Warnf(format string, v ...interface{}) {
	l.Warn(format, v...)
}

// Errorf логирует сообщение уровня error с форматированием
func (l *StructuredLogger) Errorf(format string, v ...interface{}) {
	l.Error(format, v...)
}

// log внутренняя функция для записи сообщения
func (l *StructuredLogger) log(level LogLevel, format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(time.RFC3339)
	levelStr := strings.ToUpper(level.String())
	message := fmt.Sprintf(format, v...)

	logMsg := fmt.Sprintf("[%s] [%s] %s\n", timestamp, levelStr, message)
	
	_, err := fmt.Fprintln(l.output, logMsg)
	if err != nil {
		// Если не можем записать в output, пишем в stderr
		fmt.Fprintf(os.Stderr, "failed to write log: %v\n", err)
	}
}

// DefaultLogger создает логгер с настройками по умолчанию
func DefaultLogger() Logger {
	return NewLogger(InfoLevel, os.Stdout)
}

// DebugLogger создает логгер в режиме отладки
func DebugLogger() Logger {
	return NewLogger(DebugLevel, os.Stdout)
}

// ErrorLogger создает логгер, который выводит только ошибки
func ErrorLogger() Logger {
	return NewLogger(ErrorLevel, os.Stderr)
}
