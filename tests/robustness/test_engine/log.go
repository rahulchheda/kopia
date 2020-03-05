package engine

import (
	"fmt"
	"strings"
	"time"
)

// Log keeps track of the actions taken by the engine
type Log struct {
	runOffset int
	Log       []*LogEntry
}

// LogEntry is an entry for the engine log
type LogEntry struct {
	StartTime       time.Time
	EndTime         time.Time
	EngineTimestamp int64
	Action          ActionKey
	Error           string
	Idx             int64
	ActionOpts      map[string]string
	CmdOpts         map[string]string
}

func (l *LogEntry) String() string {
	b := &strings.Builder{}

	fmt.Fprintf(b, "%4v t=%ds %s (%s): %v -> error=%s\n",
		l.Idx,
		l.EngineTimestamp,
		formatTime(l.StartTime),
		l.EndTime.Sub(l.StartTime).Round(100*time.Millisecond),
		l.Action,
		l.Error,
	)

	return b.String()
}

func formatTime(tm time.Time) string {
	return tm.Format("2006/01/02 15:04:05 MST")
}

// StringThisRun returns a string of only the log entries generated
// by actions in this run of the engine
func (elog *Log) StringThisRun() string {
	b := &strings.Builder{}

	for i, l := range elog.Log {
		if i > elog.runOffset {
			fmt.Fprintf(b, l.String())
		}
	}

	return b.String()
}

func (elog *Log) String() string {
	b := &strings.Builder{}

	fmt.Fprintf(b, "Log size:    %10v\n", len(elog.Log))
	fmt.Fprintf(b, "========\n")
	for _, l := range elog.Log {
		fmt.Fprintf(b, l.String())
	}

	return b.String()
}

// AddEntry adds a LogEntry to the Log
func (elog *Log) AddEntry(l *LogEntry) {
	elog.Log = append(elog.Log, l)
	l.Idx = int64(len(elog.Log))
}

// AddCompleted finalizes a log entry at the time it is called
// and with the provided error, before adding it to the Log.
func (elog *Log) AddCompleted(logEntry *LogEntry, err error) {
	logEntry.EndTime = time.Now()
	if err != nil {
		logEntry.Error = err.Error()
	}
	elog.AddEntry(logEntry)

	if len(elog.Log) == 0 {
		panic("Did not get added")
	}
}

func setLogEntryCmdOpts(l *LogEntry, opts map[string]string) {
	if l == nil {
		return
	}

	l.CmdOpts = opts
}
