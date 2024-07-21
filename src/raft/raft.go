package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu is only used to protect data structure in 3B (nextIndex, matchIndex, logEntries)
	// because they are not easy to use atomic to replace
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      atomic.Int32        // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// log index starts from 1
	// here we internally still start from 0, but when returing, we add 1

	// volatile
	commitIdx      atomic.Int32 // commit index at leader
	lastAppliedIdx int          // index apply on this server, might not commit

	// volatile leader state
	nextIndex  []int // next log entry send to followers
	matchIndex []int // what have been already sent

	// stable storage
	currentTerm atomic.Int32
	votedFor    int // vote for which candidate in this term
	logEntries  []LogData

	// custom
	isLeader          atomic.Bool
	heartbeatTime     atomic.Int64
	electionStartTime atomic.Int64

	works                  chan work
	reply                  chan work
	workId                 atomic.Int32
	receiveAppendEntriesId int32
	lastAppendEntriesId    atomic.Int32

	// stable storage
	snapshotEndIndex int
	snapshotEndTerm  int32
	snapshot         []byte
}

const (
	WorkType_SetLeader = iota
	WorkType_AppendEntries
	WorkType_RunElection
	WorkType_ToFollower
	WorkType_CallAppendEntries
	WorkType_SendHeartBeatFail
	WorkType_SendHeartBeatSuccess
	WorkType_RequestVote
	WorkType_StartCommand
)

const (
	electionTimeOut      = time.Millisecond * 700
	heartbeatTimeOutMs   = 700
	appendEntriesTimeOut = time.Millisecond * 500
)

type work struct {
	workId   int32
	workType int
	data     interface{}
}

type LogData struct {
	Term    int32
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm.Load()), rf.isLeader.Load()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persistLocked()
}

func (rf *Raft) persistLocked() {
	buffer := &bytes.Buffer{}
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm.Load() + 1)
	encoder.Encode(rf.votedFor + 1)
	encoder.Encode(rf.logEntries)
	encoder.Encode(rf.snapshotEndIndex + 1)
	encoder.Encode(rf.snapshotEndTerm + 1)
	state := buffer.Bytes()
	rf.persister.Save(state, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int32
	var votedFor int
	var logEntries []LogData
	var snapshotEndIndex int
	var snapshotEndTerm int32
	err := decoder.Decode(&currentTerm)
	if err != nil {
		panic(fmt.Sprintf("Decode invalid currentTerm with err %v", err))
	}
	err = decoder.Decode(&votedFor)
	if err != nil {
		panic(fmt.Sprintf("Decode invalid votedFor with err %v", err))
	}
	err = decoder.Decode(&logEntries)
	if err != nil {
		panic(fmt.Sprintf("Decode invalid logEntries with err %v", err))
	}
	err = decoder.Decode(&snapshotEndIndex)
	if err != nil {
		panic(fmt.Sprintf("Decode invalid snapshotEndIndex with err %v", err))
	}
	err = decoder.Decode(&snapshotEndTerm)
	if err != nil {
		panic(fmt.Sprintf("Decode invalid snapshotEndTerm with err %v", err))
	}
	rf.currentTerm.Store(currentTerm - 1)
	rf.votedFor = votedFor - 1
	rf.logEntries = logEntries
	rf.snapshotEndIndex = snapshotEndIndex - 1
	rf.snapshotEndTerm = snapshotEndTerm - 1
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int32
	CandidateId int

	// used to compare log freshness between this and candidate
	LastLogIndex int
	LastLogTerm  int32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int32
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	workId := rf.workId.Add(1)
	rf.works <- work{
		workId:   workId,
		workType: WorkType_RequestVote,
		data:     args,
	}
	for {
		result := <-rf.reply
		if result.workId != workId {
			rf.reply <- result
		} else {
			*reply = *(result.data.(*RequestVoteReply))
			break
		}
	}
}

func (rf *Raft) runRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	currTerm := rf.currentTerm.Load()
	rf.printf("Run RequestVote in term %v, from %v in term %v", currTerm, args.CandidateId, args.Term)
	if args.Term < currTerm {
		rf.printf("RequestVote from node %v, term %v is older than my term %v, reject",
			args.CandidateId, args.Term, currTerm)
		return &RequestVoteReply{
			VoteGranted: false,
			Term:        currTerm,
		}
	}
	runPersist := false
	defer func() {
		if runPersist {
			rf.persist()
		}
	}()
	if args.Term > currTerm {
		// reset votedFor for new term (might caused by vote split), to ensure new term leader election can elect a leader
		rf.votedFor = -1
		// also need to transfer to follower if currently candidate
		rf.electionStartTime.Store(0)

		rf.currentTerm.Store(int32(args.Term))
		rf.isLeader.Store(false)
		runPersist = true
	}
	// if already vote for someone else, reject
	// compare log freshness, reject if mine is fresher than candidate
	rf.mu.Lock()
	votedFor := rf.votedFor
	if votedFor == -1 || votedFor == args.CandidateId {
		if rf.isUpToDateLocked(args) {
			rf.votedFor = args.CandidateId
			runPersist = true
			rf.printf("grant vote for %v in term %v", args.CandidateId, args.Term)

			// reset timer, although this is not mentioned in the paper, but this helps
			// to make sure me won't start a new round quickly and causes the voted for target
			// to give up the election
			// see https://github.com/nats-io/nats-server/discussions/5023
			rf.heartbeatTime.Store(time.Now().UnixMilli())

			rf.mu.Unlock()

			return &RequestVoteReply{
				VoteGranted: true,
				Term:        currTerm,
			}
		}
		rf.printf("won't grant vote for %v in term %v, candidate is out of date", args.CandidateId, args.Term)
	} else {
		rf.printf("won't grant vote for %v in term %v, already vote for %v",
			args.CandidateId, args.Term, votedFor)
	}
	rf.mu.Unlock()
	return &RequestVoteReply{
		VoteGranted: false,
		Term:        currTerm,
	}
}

func (rf *Raft) isUpToDateLocked(args *RequestVoteArgs) bool {
	myLastLogIdx := len(rf.logEntries) - 1
	myLastLogTerm := int32(-1)
	snapshotEndIdx := rf.snapshotEndIndex
	if myLastLogIdx < 0 {
		if snapshotEndIdx < 0 {
			return true
		}
		myLastLogIdx = int(snapshotEndIdx)
		myLastLogTerm = rf.snapshotEndTerm
	} else {
		myLastLogTerm = rf.logEntries[myLastLogIdx].Term
		myLastLogIdx += 1 + int(snapshotEndIdx)
	}
	if myLastLogTerm != args.LastLogTerm {
		return myLastLogTerm <= args.LastLogTerm
	}
	return myLastLogIdx <= args.LastLogIndex
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).

	isLeader := rf.isLeader.Load()
	if isLeader {
		workId := rf.workId.Add(1)
		rf.works <- work{
			workId:   workId,
			workType: WorkType_StartCommand,
			data:     command,
		}
		for {
			reply := <-rf.reply
			if reply.workId == workId {
				data := reply.data.([]int)
				rf.printf("StartCommand result: %v (cmdIdx, term, isLeader), %v", data, command)
				return data[0] + 1, data[1], data[2] == 1
			} else {
				rf.reply <- reply
			}
		}
	}

	return -1, 0, false
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.dead.Store(1)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load() == 1
}

func (rf *Raft) isTimeOut() bool {
	return time.Now().UnixMilli()-rf.heartbeatTime.Load() > heartbeatTimeOutMs
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// move it before everything to avoid all servers start election at same time
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.

		isLeader := rf.isLeader.Load()
		if isLeader {
			rf.works <- work{
				workType: WorkType_CallAppendEntries,
			}
		} else if rf.isTimeOut() {
			rf.works <- work{
				workType: WorkType_RunElection,
			}
		}
	}
}

func (rf *Raft) workSendAppendEntries(currTerm int32, appendEntriesId int32) {
	rf.printf("leader send AppendEntries in term %v, id %v", currTerm, appendEntriesId)
	heartbeatStatus := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		if !rf.isLeader.Load() {
			break
		}

		go rf.appendEntriesToFollower(i, appendEntriesId, heartbeatStatus, currTerm)
	}
	timeout := time.After(appendEntriesTimeOut)
	receiveFrom := []int{rf.me}
L:
	for len(receiveFrom) <= len(rf.peers)/2 {
		select {
		case <-timeout:
			break L
		case peerNo := <-heartbeatStatus:
			receiveFrom = append(receiveFrom, peerNo)
		}
	}
	if len(receiveFrom) <= len(rf.peers)/2 {
		rf.printf("too few connected node %v in term %v, id %v",
			receiveFrom, currTerm, appendEntriesId)
		rf.works <- work{
			workId:   appendEntriesId,
			workType: WorkType_SendHeartBeatFail,
		}
	} else {
		rf.works <- work{
			workId:   appendEntriesId,
			workType: WorkType_SendHeartBeatSuccess,
		}
	}
}

func (rf *Raft) setLeader() {
	rf.electionStartTime.Store(0)
	rf.isLeader.Store(true)
	rf.mu.Lock()
	rf.lastAppliedIdx = len(rf.logEntries) - 1 + int(rf.snapshotEndIndex) + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastAppliedIdx + 1
		rf.matchIndex[i] = -1
	}
	rf.persistLocked()
	rf.mu.Unlock()

	rf.printf("become leader since term %v, lastAppliedIdx: %v", rf.currentTerm.Load(), rf.lastAppliedIdx)

	rf.works <- work{
		workType: WorkType_CallAppendEntries,
	}
}

type AppendEntriesArgs struct {
	Term            int32
	Leader          int
	LogEntries      []LogData
	PrevLogIndex    int
	PrevLogTerm     int32
	LeaderCommitIdx int32

	// additional rule, if followers receive old AppendEntriesId, ignore it
	AppendEntriesId int32
}

type AppendEntriesReply struct {
	IsSuccess bool
	Term      int32

	// conflicting term
	XTerm int32
	// first index of XTerm
	XIndex int
	// log len
	XLogLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	workId := rf.workId.Add(1)
	rf.works <- work{
		workId:   workId,
		workType: WorkType_AppendEntries,
		data:     args,
	}
	for {
		result := <-rf.reply
		if result.workId != workId {
			rf.reply <- result
		} else {
			*reply = *result.data.(*AppendEntriesReply)
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.works = make(chan work, 10)
	rf.reply = make(chan work, 10)
	rf.commitIdx.Store(-1)
	rf.lastAppliedIdx = -1
	rf.snapshotEndIndex = -1

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.work()
	go rf.sendCommittedToChan(applyCh)

	return rf
}

func (rf *Raft) work() {
	var lastSendHeartBeatSuccessId int32
	var lastAppendEntriesId int32
	for !rf.killed() {
		work := <-rf.works
		switch work.workType {
		case WorkType_SetLeader:
			rf.setLeader()
		case WorkType_CallAppendEntries:
			rf.heartbeatTime.Store(time.Now().UnixMilli())
			rf.updateCommitIdx()
			lastAppendEntriesId = rf.workId.Add(1)
			rf.lastAppendEntriesId.Store(lastAppendEntriesId)
			go rf.workSendAppendEntries(rf.currentTerm.Load(), lastAppendEntriesId)
		case WorkType_AppendEntries:
			reply := rf.followerAppendEntries(work.data.(*AppendEntriesArgs))
			work.data = reply
			rf.reply <- work
		case WorkType_SendHeartBeatSuccess:
			if work.workId > lastSendHeartBeatSuccessId {
				lastSendHeartBeatSuccessId = work.workId
			}
		case WorkType_SendHeartBeatFail:
			//if work.workId == lastAppendEntriesId {
			//	// last append entries failed
			//	rf.printf("AppendEntries failed in id %v, switch to follower state", work.workId)
			//	rf.isLeader.Store(false)
			//}
			// for old heartbeat, ignore it
		case WorkType_RequestVote:
			reply := rf.runRequestVote(work.data.(*RequestVoteArgs))
			work.data = reply
			rf.reply <- work
		case WorkType_ToFollower:
			rf.printf("become follower")
			rf.isLeader.Store(false)
		case WorkType_StartCommand:
			work.data = rf.leaderStartCommand(work.data)
			rf.reply <- work
		case WorkType_RunElection:
			electionStartTime := rf.electionStartTime.Load()
			if electionStartTime > 0 {
				rf.printf("detect timeout %v, while election is ongoing, startTime %v...",
					time.UnixMilli(rf.heartbeatTime.Load()).Format(time.RFC3339),
					time.UnixMilli(electionStartTime).Format(time.RFC3339))
			} else {
				// start new election

				// reset timeout to avoid run election in every ticker
				rf.heartbeatTime.Store(time.Now().UnixMilli())
				// we should set votefor to me
				// if not, we might still vote for someone else in old term and causes me cannot win (only 3 of 5 nodes are alive case)
				rf.votedFor = rf.me
				newTerm := rf.currentTerm.Add(1)
				rf.persist()
				rf.printf("detect timeout %v, start election in new term %v...",
					time.UnixMilli(rf.heartbeatTime.Load()).Format(time.RFC3339), newTerm)
				rf.electionStartTime.Store(time.Now().UnixMilli())
				lastLogTerm := int32(-1)
				rf.mu.Lock()
				lastLogIdx := len(rf.logEntries) - 1
				snapshotEndIdx := rf.snapshotEndIndex
				if lastLogIdx >= 0 {
					lastLogTerm = rf.logEntries[lastLogIdx].Term
					lastLogIdx += int(snapshotEndIdx) + 1
				} else if snapshotEndIdx >= 0 {
					lastLogTerm = rf.snapshotEndTerm
					lastLogIdx = int(snapshotEndIdx)
				}
				rf.mu.Unlock()
				go rf.workRunElection(lastLogIdx, lastLogTerm, newTerm)
			}
		}
	}
}

func (rf *Raft) updateCommitIdx() {
	// update commit index
	rf.mu.Lock()
	snapshotEndIdx := int(rf.snapshotEndIndex)
	commitIdx := int(rf.commitIdx.Load())
	commitLogIdx := max(commitIdx-snapshotEndIdx-1, 0)
	defer rf.mu.Unlock()
	for i := len(rf.logEntries) - 1; i > commitLogIdx; i-- {
		matchCount := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i+snapshotEndIdx+1 {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			// only update when log entry is in same term, see section 5.4.2
			if rf.logEntries[i].Term == rf.currentTerm.Load() {
				i += snapshotEndIdx + 1
				rf.printf("Leader update commitIdx to %v", i)
				rf.commitIdx.Store(int32(i))
				return
			}
		}
	}
}

func (rf *Raft) sendCommittedToChan(applyCh chan ApplyMsg) {
	// send lastSendToChan ~ commitIdx to channel (lab requirement) and avoid blocking
	lastSendToChanIdx := -1
	snapshotEndIndex := -1
	for {
		commitIdx := int(rf.commitIdx.Load())
		var messages []ApplyMsg

		rf.mu.Lock()
		newSnapshotEndIndex := rf.snapshotEndIndex
		if snapshotEndIndex != newSnapshotEndIndex && commitIdx >= newSnapshotEndIndex {
			snapshot := rf.snapshot
			snapshotEndIndex = newSnapshotEndIndex
			rf.printf("Send Snapshot EndIdx %v to applyCh", newSnapshotEndIndex)
			messages = append(messages, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotIndex: int(newSnapshotEndIndex) + 1,
				SnapshotTerm:  int(rf.snapshotEndTerm),
			})
			// need to reset this
			lastSendToChanIdx = int(newSnapshotEndIndex)
		}

		if lastSendToChanIdx < commitIdx {
			// make a copy to avoid send to channel took too long
			start := lastSendToChanIdx + 1
			for lastSendToChanIdx < commitIdx {
				lastSendToChanIdx++
				cmdIdx := lastSendToChanIdx
				cmdLogIdx := cmdIdx - 1 - snapshotEndIndex
				command := rf.logEntries[cmdLogIdx]
				messages = append(messages, ApplyMsg{
					Command:      command.Command,
					CommandValid: true,
					CommandIndex: cmdIdx + 1,
				})
			}
			rf.printf("Send [%v, %v] to applyCh", start, commitIdx)
		}
		rf.mu.Unlock()
		for _, msg := range messages {
			applyCh <- msg
		}

		time.Sleep(time.Millisecond * 200)
	}
}

func (rf *Raft) followerAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	var reply *AppendEntriesReply
	rf.mu.Lock()
	currTerm := rf.currentTerm.Load()
	lastAppliedIdx := rf.lastAppliedIdx
	rf.mu.Unlock()
	if args.Term == currTerm && rf.receiveAppendEntriesId > args.AppendEntriesId {
		rf.printf("receive old AppendEntries from %v for appendEntriesId %v, ignore",
			args.Leader, args.AppendEntriesId)
		reply = &AppendEntriesReply{
			IsSuccess: true,
			Term:      currTerm,
		}
	} else if args.Term >= currTerm {
		rf.printf("receive AppendEntries (entries: %v) from %v for term %v, id %v, leaderCommitIdx: %v, lastAppliedIdx %v",
			len(args.LogEntries), args.Leader, args.Term, args.AppendEntriesId, args.LeaderCommitIdx, lastAppliedIdx)

		rf.receiveHeartbeat(args.Term, currTerm, args.Leader)

		// we cannot directly update commitIdx, because me might store stale data
		// consider I StartCommand on 123 but it's not committed (no enough follower)
		// node 2 and 3 StartCommand on 124 and committed, and when node 2 or 3 update my commitIdx
		// because our records might not updated to 124 in time
		// me will report 123 as commited while it's not
		if args.LeaderCommitIdx > rf.commitIdx.Load() {
			rf.mu.Lock()
			if len(rf.logEntries) > 0 && rf.logEntries[len(rf.logEntries)-1].Term == args.Term {
				// we don't commit on previous term, only when it's the latest term
				// TODO: does this check necessary?
				commitIdx := min32(args.LeaderCommitIdx, int32(rf.lastAppliedIdx))
				rf.printf("Follower update commitIdx to %v", commitIdx)
				rf.commitIdx.Store(commitIdx)
			}
			rf.mu.Unlock()
		}

		reply = rf.doAppendEntries(args.LogEntries, currTerm, args.PrevLogIndex, args.PrevLogTerm)
	} else {
		rf.printf("receive old AppendEntries from %v for term %v, ignore", args.Leader, args.Term)
		reply = &AppendEntriesReply{
			IsSuccess: false,
			Term:      currTerm,
		}
	}
	return reply
}

func (rf *Raft) receiveHeartbeat(term, myTerm int32, leader int) {
	rf.heartbeatTime.Store(time.Now().UnixMilli())

	if rf.electionStartTime.Swap(0) > 0 {
		rf.printf("receive AppendEntries from %v when election, stop election", leader)
	}
	if term > myTerm {
		rf.currentTerm.Store(term)
		// we can reset votedFor only when term change
		// because in this term we shouldn't change voted for, or we might vote for different candidates
		// consider a case: node 0, 1 are connected and 1 is leader in term 50 (0, 1 voted for 1 in term 50)
		// node 2 is in term 49 and rejoin the cluster, it detect timeout (node 1 not send appendentries to it yet)
		// node 2 increase term to 50 and run election, if we reset 0's votedFor, 0 will vote for 2 in term 50
		// and in term 50, we will have 2 leader
		rf.votedFor = -1

		rf.persist()
	}
	rf.isLeader.Store(false)
}

func (rf *Raft) doAppendEntries(entries []LogData, currTerm int32,
	prevLogIndex int, prevLogTerm int32) *AppendEntriesReply {
	reply := AppendEntriesReply{
		Term: currTerm,
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotEndIndex := rf.snapshotEndIndex
	if prevLogIndex == -1 {
		rf.printf("Append all entries in term %v", currTerm)
		rf.lastAppliedIdx = len(entries) - 1
		// in case we already take snapshot
		// append 60 entries, [0] already in snapshot, we only need to append [1, 59]
		count := rf.lastAppliedIdx - snapshotEndIndex
		rf.logEntries = entries[len(entries)-count:]
		reply.IsSuccess = true
		rf.persistLocked()
	} else if logLen := len(rf.logEntries) + snapshotEndIndex + 1; logLen <= prevLogIndex {
		rf.printf("prevLogIndex doesn't match")
		reply.IsSuccess = false
		reply.XLogLen = logLen
	} else if len(entries)+prevLogIndex <= snapshotEndIndex {
		rf.printf("receive %v entries, prevLogIndex %v that is already in snapshot (snapshotEndIdx %v), ignore",
			len(entries), prevLogIndex, snapshotEndIndex)
		reply.IsSuccess = true
	} else {
		realPrevLogIndex := prevLogIndex - snapshotEndIndex - 1
		myPrevLogTerm := int32(-1)
		if realPrevLogIndex < 0 {
			// prev log is in snapshot
			// since snapshot only work for committed entries, we can safely conclude they match
			myPrevLogTerm = prevLogTerm
		} else {
			myPrevLogTerm = rf.logEntries[realPrevLogIndex].Term
		}
		if myPrevLogTerm != prevLogTerm {
			rf.printf("prevLogTerm %v doesn't match with mine %v in index %v",
				prevLogTerm, myPrevLogTerm, prevLogIndex)
			reply.IsSuccess = false
			reply.XTerm = myPrevLogTerm
			reply.XLogLen = logLen
			var xIndex int
			for xIndex = realPrevLogIndex; xIndex >= 0 && rf.logEntries[xIndex].Term == myPrevLogTerm; xIndex-- {
			}
			reply.XIndex = xIndex + 1 + snapshotEndIndex + 1
		} else if len(entries) == 0 {
			reply.IsSuccess = true
		} else {
			if realPrevLogIndex >= 0 {
				// prevLogIndex 100, entries [0~59]([101~160]), snapshotEndIndex 99
				// need to keep current 100 (0) + entries
				rf.logEntries = append(rf.logEntries[:realPrevLogIndex+1], entries...)
				rf.lastAppliedIdx = len(rf.logEntries) - 1 + snapshotEndIndex + 1
			} else {
				// prevLogIndex 100, entries [0~59]([101~160]), snapshotEndIndex 150
				// only need to append (50~59)[151~160]
				rf.lastAppliedIdx = len(entries) + prevLogIndex
				count := rf.lastAppliedIdx - snapshotEndIndex
				rf.logEntries = entries[len(entries)-count:]
			}
			reply.IsSuccess = true
			rf.persistLocked()
		}
	}
	return &reply
}

func (rf *Raft) workRunElection(lastLogIndex int, lastLogTerm int32, term int32) {
	currTerm := term

	args := &RequestVoteArgs{
		Term:         currTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// request for votes asyncally with timeout
	replyChan := make(chan int, len(rf.peers))
	totalVoteCount := 0
	for i := range rf.peers {
		if rf.electionStartTime.Load() == 0 {
			break
		}
		if i == rf.me {
			continue
		}
		totalVoteCount++
		go func(peer int) {
			rf.printf("request %v for vote in term %v", peer, currTerm)
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, &reply)
			if ok {
				if reply.VoteGranted {
					rf.printf("receive vote from %v in term %v", peer, currTerm)
					replyChan <- peer
				} else {
					rf.printf("receive reject vote from %v in term %v", peer, currTerm)
					replyChan <- -1
				}
			} else {
				rf.printf("failed rpc to RequestVote to %v in term %v", peer, currTerm)
			}
		}(i)
	}

	targetVoteCount := len(rf.peers)/2 + 1
	voters := []int{rf.me}
	voteCount := 1 // me already vote for me
	ctx, cancel := context.WithTimeout(context.Background(), electionTimeOut)

L:
	for i := 0; i < totalVoteCount && voteCount < targetVoteCount && rf.electionStartTime.Load() > 0; {
		select {
		case <-ctx.Done():
			break L
		case reply := <-replyChan:
			if reply >= 0 {
				voters = append(voters, reply)
				voteCount++
			}
			i++
		}
	}
	cancel()
	if rf.electionStartTime.Load() == 0 {
		// ignore results
		rf.printf("receive vote in term %v ended while election stop, ignore results", term)
		return
	}
	if voteCount >= targetVoteCount {
		rf.printf("receive vote from %v, will become leader", voters)
		rf.works <- work{
			workType: WorkType_SetLeader,
		}
		return
	}

	rf.printf("didn't get enough vote (%v, %v) in time for term %v", voters, targetVoteCount, currTerm)
	rf.electionStartTime.Store(0)
}

func (rf *Raft) leaderStartCommand(command interface{}) []int {
	// append log to local, won't call AppendEntries to followers
	// followers will be synced during heartbeat
	term := rf.currentTerm.Load()
	if rf.isLeader.Load() {
		rf.mu.Lock()
		cmdIdx := len(rf.logEntries) + rf.snapshotEndIndex + 1
		rf.lastAppliedIdx = cmdIdx
		rf.logEntries = append(rf.logEntries, LogData{
			Command: command,
			Term:    term,
		})
		rf.matchIndex[rf.me] = cmdIdx
		rf.persistLocked()
		rf.mu.Unlock()
		return []int{cmdIdx, int(term), 1}
	}
	return []int{-1, int(term), 0}
}

func (rf *Raft) appendEntriesToFollower(peer int, appendEntriesId int32,
	heartbeatChan chan int, currTerm int32) {
	if appendEntriesId < rf.lastAppendEntriesId.Load() || currTerm != rf.currentTerm.Load() {
		// there's more recent run
		return
	}

	args := AppendEntriesArgs{
		Term:            currTerm,
		Leader:          rf.me,
		LeaderCommitIdx: rf.commitIdx.Load(),
		AppendEntriesId: appendEntriesId,
	}
	rf.mu.Lock()
	lastLogIndex := len(rf.logEntries) - 1
	nextIndex := rf.nextIndex[peer]
	snapshotEndIndex := rf.snapshotEndIndex
	nextLogIndex := nextIndex - (snapshotEndIndex + 1)
	if nextLogIndex < 0 {
		rf.mu.Unlock()
		rf.printf("nextIndex %v is in snapshot %v, InstallSnapshot to peer %v", nextIndex, snapshotEndIndex, peer)
		rf.sendInstallSnapshotToFollower(peer, currTerm, appendEntriesId, heartbeatChan)
		return
	}
	prevIndex := nextIndex - 1
	prevLogIndex := nextLogIndex - 1
	prevLogTerm := int32(-1)
	if prevLogIndex >= 0 && prevLogIndex <= lastLogIndex {
		prevLogTerm = rf.logEntries[prevLogIndex].Term
	} else if prevLogIndex == -1 && nextIndex > 0 {
		// there's an edge case, snapshot include 200, next index is 201
		// in this case, we cannot get prevIndex from logEntries
		prevLogTerm = rf.snapshotEndTerm
	}
	if nextLogIndex <= lastLogIndex {
		args.LogEntries = rf.logEntries[nextLogIndex:]
	}
	rf.mu.Unlock()
	args.PrevLogIndex = prevIndex
	args.PrevLogTerm = prevLogTerm
	if lastLogIndex >= nextLogIndex {
		rf.printf("AppendEntries to %v in term %v, id %v, commitIndex: %v, nextLogIdx: %v, nextIdx: %v, lastLogIndex: %v",
			peer, currTerm, appendEntriesId, rf.commitIdx.Load(), nextLogIndex, nextIndex, lastLogIndex)
	} else {
		rf.printf("AppendEntries to %v in term %v, id %v, heartbeat only", peer, currTerm, appendEntriesId)
	}

	reply := AppendEntriesReply{}

	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		if heartbeatChan != nil {
			heartbeatChan <- peer
		}
		if reply.IsSuccess {
			appendEntries := args.LogEntries
			if len(appendEntries) > 0 {
				rf.printf("Success AppendEntries to %v in term %v, id %v, match index: %v",
					peer, currTerm, appendEntriesId, len(appendEntries)+nextIndex-1)
				rf.mu.Lock()
				rf.nextIndex[peer] = nextIndex + len(appendEntries)
				rf.matchIndex[peer] = nextIndex + len(appendEntries) - 1
				rf.mu.Unlock()
			}
		} else {
			if currTerm < reply.Term {
				rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to new term in peer",
					peer, currTerm, appendEntriesId)
				rf.works <- work{
					workType: WorkType_ToFollower,
				}
			} else {
				rf.handleAppendEntriesConflict(&reply, peer, nextIndex, currTerm, appendEntriesId)
				rf.appendEntriesToFollower(peer, appendEntriesId, nil, currTerm)
			}
		}
	} else {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to timeout",
			peer, currTerm, appendEntriesId)
	}
}

func (rf *Raft) handleAppendEntriesConflict(reply *AppendEntriesReply, peer, currNextIndex int, currTerm, appendEntriesId int32) {
	if reply.XLogLen < currNextIndex {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to follower log entries too short %v",
			peer, currTerm, appendEntriesId, reply.XLogLen)
		rf.mu.Lock()
		rf.nextIndex[peer] = reply.XLogLen
		rf.mu.Unlock()
	} else {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to conflict, xterm: %v, xindex: %v",
			peer, currTerm, appendEntriesId, reply.XTerm, reply.XIndex)
		rf.mu.Lock()
		isUpdated := false
		for i := len(rf.logEntries) - 1; i >= 0; i-- {
			// we have this term, so parts of this term should have replicate
			// to peer but not us, so we don't have to reset all of that term
			// instead, we reset to what we don't have
			if rf.logEntries[i].Term <= reply.XTerm {
				if rf.logEntries[i].Term == reply.XTerm {
					rf.printf("for peer %v and xterm %v, we already have this term in %v",
						peer, reply.XTerm, i)
					isUpdated = true
					rf.nextIndex[peer] = i + 1
				}
				break
			}
		}
		if !isUpdated {
			rf.printf("for peer %v and xterm %v, we don't have this term", peer, reply.XTerm)
			rf.nextIndex[peer] = reply.XIndex
		}
		rf.mu.Unlock()
	}
}
