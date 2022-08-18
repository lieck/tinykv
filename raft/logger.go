package raft

import (
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"os"
	"strconv"
)

var (
	calldepth = 3

	// 日志等价
	// 0 ：不开启日志
	// 1 ： info 等必要信息
	// 2 ： debug 信息
	// 3 ： debugX 信息
	debugLv = 0

	headDetails = true
	writeFile   = false
	colorIdx    = 30
)

func setHeadDetails(is bool) {
	headDetails = is
}

type Logger struct {
	*log.Logger
	col       int
	id        uint64
	term      uint64
	lead      uint64
	stateType StateType
}

func (l *Logger) EnableTimestamps() {
	l.SetFlags(l.Flags() | log.Ldate | log.Ltime)
}

func (l *Logger) Debug(v ...interface{}) {
	if debugLv >= 2 {
		l.Output(calldepth, l.header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	if debugLv >= 2 {
		l.Output(calldepth, l.header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

func (l *Logger) DebugfX(format string, v ...interface{}) {
	if debugLv >= 3 {
		l.Output(calldepth, l.header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

func (l *Logger) DebugX(v ...interface{}) {
	if debugLv >= 3 {
		l.Output(calldepth, l.header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *Logger) Info(v ...interface{}) {
	if debugLv >= 1 {
		l.Output(calldepth, l.header("INFO", fmt.Sprint(v...)))
	}
}

func (l *Logger) Infof(format string, v ...interface{}) {
	if debugLv >= 1 {
		l.Output(calldepth, l.header("INFO", fmt.Sprintf(format, v...)))
	}
}

func (l *Logger) Error(v ...interface{}) {
	if debugLv >= 1 {
		l.Output(calldepth, l.header("ERROR", fmt.Sprint(v...)))
	}
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	if debugLv >= 1 {
		l.Output(calldepth, l.header("ERROR", fmt.Sprintf(format, v...)))
	}
}

func (l *Logger) Warning(v ...interface{}) {
	if debugLv >= 2 {
		l.Output(calldepth, l.header("WARN", fmt.Sprint(v...)))
	}
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	if debugLv >= 2 {
		l.Output(calldepth, l.header("WARN", fmt.Sprintf(format, v...)))
	}
}

func (l *Logger) Fatal(v ...interface{}) {
	l.Output(calldepth, l.header("FATAL", fmt.Sprint(v...)))
	os.Exit(1)
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.Output(calldepth, l.header("FATAL", fmt.Sprintf(format, v...)))
	os.Exit(1)
}

func (l *Logger) Panic(v ...interface{}) {
	l.Logger.Panic(v...)
}

func (l *Logger) Panicf(format string, v ...interface{}) {
	l.Logger.Panicf(format, v...)
}

func (l *Logger) header(lvl, msg string) string {
	var hdStr string
	var hStr string = fmt.Sprintf("\u001B[1;31;40m %s \u001B[0m", lvl)
	if headDetails {
		var state = [3]string{"F", "C", "L"}
		hdStr = fmt.Sprintf("\u001B[1;%v;40m[ID:%v, T:%v, L:%v, S:%v]\u001B[0m", l.col, l.id, l.term, l.lead, state[l.stateType])
	} else {
		hdStr = fmt.Sprintf("\u001B[1;%v;40m[ID:%v]\u001B[0m", l.col, l.id)
	}
	msg = fmt.Sprintf("\u001B[1;32;40m%s\u001B[0m", msg)
	return fmt.Sprintf("%s %s : %s", hStr, hdStr, msg)
}

func (l *Logger) setStatus(id, lead, term uint64, stateType StateType) {
	l.lead = lead
	l.term = term
	l.id = id
	l.stateType = stateType
}

func (l *Logger) msgInfo(m pb.Message) {
	if debugLv >= 3 {
		str := fmt.Sprintf("SendMsg type:%v  (form:%v, to:%v, Term:%v)  ", m.MsgType.String(), m.From, m.To, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend {
			str += fmt.Sprintf("(len:%v, preIndex:%v, preTerm:%v)", len(m.Entries), m.Index, m.LogTerm)
		}
		if m.Reject {
			str += "Reject"
		}
		l.DebugX(str)
	}
}

func NewLogger(id uint64) *Logger {
	colorIdx++
	if colorIdx == 31 || colorIdx == 32 {
		colorIdx = 33
	}
	if colorIdx > 37 {
		colorIdx = 30
	}
	l := &Logger{
		id:     id,
		col:    colorIdx,
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
	if writeFile {
		file, _ := os.Create(strconv.FormatUint(id, 10) + ".log")
		log.SetOutput(file)
	}
	return l
}
