package raft

import (
	"fmt"
	"log"
	"os"
)

// Debugging
const Debug = false

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(os.Stdout)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) printf(format string, a ...interface{}) {
	format = fmt.Sprintf("[%v]", rf.me) + format
	log.Printf(format, a...)
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
func min32(a, b int32) int32 {
	if a > b {
		return b
	}
	return a
}

func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func consumeAll(ch chan struct{}) {
L:
	for {
		select {
		case <-ch:
		default:
			break L
		}
	}
}

func consumeAllReasons(ch chan string) []string {
	var reasons []string
L:
	for {
		select {
		case reason := <-ch:
			reasons = append(reasons, reason)
		default:
			break L
		}
	}
	return reasons
}

func cloneEntries(entries []LogData) []LogData {
	x := make([]LogData, len(entries))
	copy(x, entries)
	return x
}

func (rf *Raft) triggerAppendEntriesLocked(reason string) {
	rf.appendEntriesReasons = append(rf.appendEntriesReasons, reason)
	rf.appendEntriesCond.Signal()
}
