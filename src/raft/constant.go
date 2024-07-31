package raft

import "time"

const (
	ElectionTimeOut       = time.Millisecond * 700
	HeartbeatTimeOutMinMs = 600
	HeartbeatTimeoutMaxMs = 800
	AppendEntriesTimeOut  = time.Millisecond * 500
	AppendEntriesInterval = time.Millisecond * 150
)

const (
	RejectAppendEntries_OldId int16 = 1
)

const (
	StateFollower int32 = iota
	StateCandidate
	StateLeader int32 = iota
)
