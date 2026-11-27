package runtime

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// loggerService implements Skynet's built-in "logger" service (service_logger.c).
type loggerService struct {
	mu        sync.Mutex
	w         io.Writer
	file      *os.File
	filename  string
	startTime uint64
}

func newLoggerService() ServiceHandler {
	return &loggerService{}
}

func (l *loggerService) Init(actor *Actor, param string) error {
	l.startTime = actor.Node().Timer().StartTime()
	if param != "" {
		f, err := os.OpenFile(param, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		l.file = f
		l.w = f
		l.filename = param
	} else {
		l.w = actor.Node().LogOutput()
	}
	return nil
}

func (l *loggerService) Dispatch(actor *Actor, msg *Message) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	switch msg.Type {
	case PTypeSystem:
		if l.filename != "" {
			if l.file != nil {
				_ = l.file.Close()
			}
			f, err := os.OpenFile(l.filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err == nil {
				l.file = f
				l.w = f
			}
		}
	case PTypeText:
		if l.w == nil {
			return nil
		}
		if l.filename != "" {
			now := actor.Node().Timer().Now()
			sec := int64(now/100 + l.startTime)
			csec := now % 100
			t := time.Unix(sec, 0)
			_, _ = fmt.Fprintf(l.w, "%s.%02d [:%08x] %s\n", t.Format("02/01/06 15:04:05"), csec, msg.Source, string(msg.Data))
		} else {
			_, _ = fmt.Fprintf(l.w, "[:%08x] %s\n", msg.Source, string(msg.Data))
		}
	}
	return nil
}

func (l *loggerService) Signal(_ *Actor, _ int) {}

func (l *loggerService) Close(_ *Actor) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
		l.w = nil
	}
}
