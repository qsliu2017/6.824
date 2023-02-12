package raft

import (
	"fmt"
	"log"
	"time"
)

var dStart time.Time

func init() {
	dStart = time.Now()

	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime))
}

// Debugging
const Debug = true

func DPrintf(topic logTopic, who int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		time := time.Since(dStart).Microseconds() / 100
		prefix := fmt.Sprintf("%06d %v S%d ", time, topic, who)
		log.Printf(prefix+format, a...)
	}
	return
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)
