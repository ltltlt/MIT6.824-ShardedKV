package raft

import "time"

const (
	electionTimeOut       = time.Millisecond * 700
	heartbeatTimeOutMs    = 700
	appendEntriesTimeOut  = time.Millisecond * 500
	appendEntriesInterval = time.Millisecond * 150
)
