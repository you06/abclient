package logger

import (
	"fmt"
	"os"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/you06/doppelganger/util"
)

// Logger struct
type Logger struct {
	logPath string
	print bool
	mute bool
}

// New init Logger struct
func New(logPath string, mute bool) (*Logger, error) {
	logger := Logger{
		logPath: logPath,
		print: logPath == "",
		mute: mute,
	}

	return logger.init()
}

func (l *Logger)init() (*Logger, error) {
	if l.print || l.mute {
		return l, nil
	}
	if err := l.writeLine("start file_logger log"); err != nil {
		return nil, errors.Trace(err)
	}
	return l, nil
}

func (l *Logger)writeLine(line string) error {
	line = fmt.Sprintf("%s %s", util.CurrentTimeStrAsLog(), line)
	if l.mute {
		return nil
	} else if l.print {
		log.Info(line)
		return nil
	}
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
	return errors.Trace(l.writeLine(line))
}

// Infof log line with format to log file
func (l *Logger) Infof(line string, args ...interface{}) error {
	return errors.Trace(l.writeLine(fmt.Sprintf(line, args...)))
}

// Fatal log line to log file
func (l *Logger) Fatal(line string) error {
	return errors.Trace(l.writeLine(line))
}

// Fatalf log line with format to log file
func (l *Logger) Fatalf(line string, args ...interface{}) error {
	return errors.Trace(l.writeLine(fmt.Sprintf(line, args...)))
}
