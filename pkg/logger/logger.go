package logger

import (
	"fmt"
	"os"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// Logger struct
type Logger struct {
	logPath string
	print bool
}

// New init Logger struct
func New(logPath string) (*Logger, error) {
	logger := Logger{
		logPath: logPath,
		print: logPath == "",
	}
	if logger.print {
		return &logger, nil
	}
	if err := logger.init(); err != nil {
		return nil, errors.Trace(err)
	}

	return &logger, nil
}

func (l *Logger)init() error {
	if err := l.writeLine("start file_logger log"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (l *Logger)writeLine(line string) error {
	f, err := os.OpenFile(l.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return errors.Trace(err)
	}
	defer func () {
		if err := f.Close(); err != nil {
			log.Error(err)
		}
	}()

	if _, err = f.WriteString(fmt.Sprintf("%s\n", line)); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Info log line to log file
func (l *Logger) Info(line string) error {
	if l.print {
		log.Info(line)
		return nil
	}
	return errors.Trace(l.writeLine(line))
}

// Infof log line with format to log file
func (l *Logger) Infof(line string, args ...interface{}) error {
	if l.print {
		log.Infof(line, args...)
		return nil
	}
	return errors.Trace(l.writeLine(fmt.Sprintf(line, args...)))
}

// Fatal log line to log file
func (l *Logger) Fatal(line string) error {
	if l.print {
		log.Fatal(line)
		return nil
	}
	return errors.Trace(l.writeLine(line))
}

// Fatalf log line with format to log file
func (l *Logger) Fatalf(line string, args ...interface{}) error {
	if l.print {
		log.Fatalf(line, args...)
		return nil
	}
	return errors.Trace(l.writeLine(fmt.Sprintf(line, args...)))
}
