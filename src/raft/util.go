package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

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
