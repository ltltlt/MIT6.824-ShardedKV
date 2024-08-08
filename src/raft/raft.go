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
	state         atomic.Int32
	heartbeatTime atomic.Int64

	receiveAppendEntriesId int32
	lastAppendEntriesId    atomic.Int32

	// stable storage
	snapshotEndIndex int
	snapshotEndTerm  int32
	snapshot         []byte

	updateApplyChCond   *sync.Cond
	updateCommitIdxCond *sync.Cond
	appendEntriesCond   *sync.Cond

	updateApplyChSignals   int
	updateCommitIdxSignals int
	appendEntriesReasons   []string
}

type LogData struct {
	Term    int32
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// need lock here to make sure we didn't update isLeader while already load currentTerm
	// an optimization would be use 1 struct to store and update both of them
	return int(rf.currentTerm.Load()), rf.state.Load() == StateLeader
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
	rf.lastAppliedIdx = rf.snapshotEndIndex + len(logEntries)

	if rf.snapshotEndIndex >= 0 || len(logEntries) > 0 {
		rf.updateApplyChSignals++
	}
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
	runPersist := false
	defer func() {
		if runPersist {
			rf.persist()
		}
	}()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	currTerm := rf.currentTerm.Load()
	reply.Term = currTerm
	rf.printf("Run RequestVote in term %v, from %v in term %v", currTerm, args.CandidateId, args.Term)
	if args.Term < currTerm {
		rf.printf("RequestVote from node %v, term %v is older than my term %v, reject",
			args.CandidateId, args.Term, currTerm)
		reply.VoteGranted = false
		return
	}

	if args.Term > currTerm {
		// reset votedFor for new term (might caused by vote split), to ensure new term leader election can elect a leader
		rf.votedFor = -1
		// also need to transfer to follower if currently candidate
		rf.setFollower()

		rf.currentTerm.Store(int32(args.Term))
		rf.receiveAppendEntriesId = 0
		runPersist = true
	}
	// if already vote for someone else, reject
	// compare log freshness, reject if mine is fresher than candidate
	votedFor := rf.votedFor
	if votedFor == -1 || votedFor == args.CandidateId {
		ok, reason := rf.isUpToDateLocked(args)
		if ok {
			rf.votedFor = args.CandidateId
			runPersist = true
			rf.printf("grant vote for %v in term %v, %v", args.CandidateId, args.Term, reason)

			// reset timer, although this is not mentioned in the paper, but this helps
			// to make sure me won't start a new round quickly and causes the voted for target
			// to give up the election
			// see https://github.com/nats-io/nats-server/discussions/5023
			rf.heartbeatTime.Store(time.Now().UnixMilli())

			rf.receiveAppendEntriesId = -1

			reply.VoteGranted = true
			return
		}
		rf.printf("won't grant vote for %v in term %v, candidate is out of date, %v", args.CandidateId, args.Term, reason)
	} else {
		rf.printf("won't grant vote for %v in term %v, already vote for %v",
			args.CandidateId, args.Term, votedFor)
	}
	reply.VoteGranted = false
	return
}

func (rf *Raft) isUpToDateLocked(args *RequestVoteArgs) (bool, string) {
	myLastLogIdx := len(rf.logEntries) - 1
	myLastLogTerm := int32(-1)
	snapshotEndIdx := rf.snapshotEndIndex
	if myLastLogIdx < 0 {
		if snapshotEndIdx < 0 {
			return true, "me is empty"
		}
		myLastLogIdx = snapshotEndIdx
		myLastLogTerm = rf.snapshotEndTerm
	} else {
		myLastLogTerm = rf.logEntries[myLastLogIdx].Term
		myLastLogIdx += 1 + snapshotEndIdx
	}
	reason := fmt.Sprintf("lastLogTerm %v vs. %v, lastLogLen %v vs. %v (me, peer)",
		myLastLogTerm, args.LastLogTerm, myLastLogIdx, args.LastLogIndex)
	if myLastLogTerm != args.LastLogTerm {
		return myLastLogTerm <= args.LastLogTerm, reason
	}
	return myLastLogIdx <= args.LastLogIndex, reason
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
	if rf.state.Load() == StateLeader {
		rf.mu.Lock()
		term := rf.currentTerm.Load()
		cmdIdx := len(rf.logEntries) + rf.snapshotEndIndex + 1
		rf.lastAppliedIdx = cmdIdx
		rf.logEntries = append(rf.logEntries, LogData{
			Command: command,
			Term:    term,
		})
		rf.matchIndex[rf.me] = cmdIdx
		rf.persistLocked()

		rf.triggerAppendEntriesLocked("StartCommand")
		rf.mu.Unlock()

		rf.printf("StartCommand result, cmdIdx %v, term %v, cmd %v", cmdIdx, term, command)
		return cmdIdx + 1, int(term), true
	}
	return -1, int(rf.currentTerm.Load()), false
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateCommitIdxCond.Signal()
	rf.updateApplyChCond.Signal()
	rf.appendEntriesCond.Signal()
}

func (rf *Raft) killed() bool {
	return rf.dead.Load() == 1
}

func (rf *Raft) isTimeout(timeoutMs int64) bool {
	return time.Now().UnixMilli()-rf.heartbeatTime.Load() > timeoutMs
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// move it before everything to avoid all servers start election at same time
		rnd := rand.Int63()
		ms := 50 + (rnd % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		state := rf.state.Load()
		// use random timeout to avoid all nodes timeout at the same time
		// prev sleep is only to check, so it should be lower than the real timeout
		isTimeout := rf.isTimeout(HeartbeatTimeOutMinMs + (rnd % (HeartbeatTimeoutMaxMs - HeartbeatTimeOutMinMs)))
		rf.printf("ticker state %v, timeout %v", state, isTimeout)
		if state == StateFollower && isTimeout {
			go rf.runElection()
		}
	}
}

func (rf *Raft) goTriggerAppendEntries() {
	for !rf.killed() {
		time.Sleep(AppendEntriesInterval)
		if rf.state.Load() == StateLeader {
			rf.mu.Lock()
			if rf.state.Load() == StateLeader {
				rf.triggerAppendEntriesLocked("Heartbeat")
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) workSendAppendEntries(currTerm int32, appendEntriesId int32) {
	heartbeatStatus := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		if rf.state.Load() != StateLeader {
			return
		}

		go rf.appendEntriesToFollower(i, appendEntriesId, heartbeatStatus, currTerm)
	}
}

func (rf *Raft) setLeader(leaderTerm int32) {
	rf.mu.Lock()
	currTerm := rf.currentTerm.Load()

	// we might receive higher term before setLeader but after election
	// so we need to double check to make sure the term matches before we actually setLeader
	if leaderTerm != currTerm {
		rf.mu.Unlock()
		rf.printf("receive higher term %v while setLeader for term %v, won't set leader", currTerm, leaderTerm)
		return
	}
	if !rf.state.CompareAndSwap(StateCandidate, StateLeader) {
		rf.mu.Unlock()
		rf.printf("Try to setLeader in non candidate state, ignore")
		return
	}
	lastAppliedIdx := len(rf.logEntries) - 1 + rf.snapshotEndIndex + 1
	rf.lastAppliedIdx = lastAppliedIdx
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastAppliedIdx + 1
		rf.matchIndex[i] = -1
	}
	rf.triggerAppendEntriesLocked("BecomeLeader")
	rf.mu.Unlock()

	rf.printf("become leader since term %v, lastAppliedIdx: %v", leaderTerm, lastAppliedIdx)
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

	Reason int16
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	*reply = *rf.followerAppendEntries(args)
	return
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
	rf.commitIdx.Store(-1)
	rf.lastAppliedIdx = -1
	rf.snapshotEndIndex = -1

	rf.state.Store(StateFollower)

	rf.updateApplyChCond = sync.NewCond(&rf.mu)
	rf.updateCommitIdxCond = sync.NewCond(&rf.mu)
	rf.appendEntriesCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.printf("Starting...")

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.goSendCommittedToApplyCh(applyCh)
	go rf.goUpdateCommitIdx()
	go rf.goAppendEntries()
	go rf.goTriggerAppendEntries()

	return rf
}

func (rf *Raft) runElection() {
	now := time.Now()
	if !rf.state.CompareAndSwap(StateFollower, StateCandidate) {
		rf.printf("detect timeout %v, while election is ongoing...",
			time.UnixMilli(rf.heartbeatTime.Load()).Format(time.StampMilli))
		return
	}
	// start new election

	// reset timeout to avoid run election in every ticker
	rf.heartbeatTime.Store(now.UnixMilli())
	// we should set votefor to me
	// if not, we might still vote for someone else in old term and causes me cannot win (only 3 of 5 nodes are alive case)
	rf.mu.Lock()
	rf.votedFor = rf.me
	newTerm := rf.currentTerm.Add(1)
	rf.persistLocked()
	rf.printf("detect timeout %v, start election in new term %v...",
		now.Format(time.StampMilli), newTerm)
	lastLogTerm := int32(-1)
	lastLogIdx := len(rf.logEntries) - 1
	snapshotEndIdx := rf.snapshotEndIndex
	if lastLogIdx >= 0 {
		lastLogTerm = rf.logEntries[lastLogIdx].Term
		lastLogIdx += snapshotEndIdx + 1
	} else if snapshotEndIdx >= 0 {
		lastLogTerm = rf.snapshotEndTerm
		lastLogIdx = snapshotEndIdx
	}
	rf.mu.Unlock()

	rf.leaderRequestVotes(lastLogIdx, lastLogTerm, newTerm)
}

func (rf *Raft) goAppendEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		for len(rf.appendEntriesReasons) == 0 && !rf.killed() {
			rf.appendEntriesCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state.Load() == StateLeader {
			id := rf.lastAppendEntriesId.Add(1)
			rf.printf("Trigger AppendEntries id %v, reasons: %v", id, rf.appendEntriesReasons)
			rf.heartbeatTime.Store(time.Now().UnixMilli())

			term := rf.currentTerm.Load()
			// separate goroutine to avoid one node timeout block other nodes
			go rf.workSendAppendEntries(term, id)
		}
		rf.appendEntriesReasons = nil
		rf.mu.Unlock()
	}
}

func (rf *Raft) goUpdateCommitIdx() {
	for {
		rf.mu.Lock()
		for rf.updateCommitIdxSignals == 0 && !rf.killed() {
			rf.updateCommitIdxCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.updateCommitIdxSignals = 0
		if rf.state.Load() == StateLeader {
			rf.updateCommitIdxLocked()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIdxLocked() {
	snapshotEndIdx := rf.snapshotEndIndex
	commitIdx := int(rf.commitIdx.Load())
	commitLogIdx := commitIdx - snapshotEndIdx - 1
	for i := len(rf.logEntries) - 1; i > commitLogIdx && i >= 0; i-- {
		matchCount := 1
		for j, matchIndex := range rf.matchIndex {
			if j != rf.me && matchIndex >= i+snapshotEndIdx+1 {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			// only update when log entry is in same term, see section 5.4.2
			if rf.logEntries[i].Term == rf.currentTerm.Load() {
				i += snapshotEndIdx + 1
				rf.printf("Leader update commitIdx to %v", i)
				rf.commitIdx.Store(int32(i))

				rf.updateApplyChSignals++
				rf.updateApplyChCond.Signal()
				if rf.state.Load() == StateLeader {
					// update follower's commit index
					rf.triggerAppendEntriesLocked("UpdateCommitIdx")
				}
				return
			}
		}
	}
}

func (rf *Raft) goSendCommittedToApplyCh(applyCh chan ApplyMsg) {
	// send lastSendToChan ~ commitIdx to channel (lab requirement) and avoid blocking
	lastSendToChanIdx := -1
	snapshotEndIndex := -1

	for !rf.killed() {
		var messages []ApplyMsg

		rf.mu.Lock()

		for rf.updateApplyChSignals == 0 && !rf.killed() {
			rf.updateApplyChCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		rf.updateApplyChSignals = 0

		commitIdx := int(rf.commitIdx.Load())

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
			lastSendToChanIdx = newSnapshotEndIndex
		}

		if lastSendToChanIdx < commitIdx {
			// make a copy to avoid send to channel took too long
			start := lastSendToChanIdx + 1
			for lastSendToChanIdx < commitIdx {
				lastSendToChanIdx++
				cmdIdx := lastSendToChanIdx
				cmdLogIdx := cmdIdx - 1 - newSnapshotEndIndex
				if cmdLogIdx < 0 {
					// tricky cases: commitIdx (65) < newSnapshotEndIdx (68), while oldSnapshotEndIdx (58) < commitIdx
					// happens when we just have a snapshot installed from leader but since leaderCommitIdx=65 (might caused by leader restart so it's commit idx < it's snapshot),
					// so we cannot change myCommitIdx to 68
					// in this case, we cannot find entries not commited since they already snapshoted,
					// so we need to wait for commitIdx >= newSnapshotEndIdx
					rf.printf("commitIdx (%v) < snapshotEndIdx (%v), lastAppliedIdx %v, we don't have entries to send to applyCh, need to wait for commitIdx update",
						commitIdx, newSnapshotEndIndex, rf.lastAppliedIdx)
					break
				}
				if cmdLogIdx >= len(rf.logEntries) {
					panic(fmt.Sprintf("[%v] discrepancy in commitIdx %v, logEntries %v, lastApplied %v, snapshotEndIdx %v",
						rf.me, commitIdx, len(rf.logEntries), rf.lastAppliedIdx, rf.snapshotEndIndex))
				}
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
	}
}

func (rf *Raft) followerAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	var reply *AppendEntriesReply
	rf.printf("followerAppendEntries from %v, term %v, id %v", args.Leader, args.Term, args.AppendEntriesId)
	rf.mu.Lock()
	currTerm := rf.currentTerm.Load()

	if args.Term < currTerm {
		rf.printf("receive old AppendEntries from %v for term %v, ignore", args.Leader, args.Term)
		rf.mu.Unlock()
		return &AppendEntriesReply{
			IsSuccess: false,
			Term:      currTerm,
		}
	}

	lastAppliedIdx := rf.lastAppliedIdx

	// tricky part: need to make sure check receiveAppendEntriesId, appendEntries and updateCommitIdx happened in a tx
	// or we might appendEntries using old data (leader issued long times ago) while updateCommitIdx (leader issued just now)
	// using new data and causes inconsistent
	if args.Term == currTerm && rf.receiveAppendEntriesId > args.AppendEntriesId {
		rf.printf("receive old AppendEntries from %v for appendEntriesId %v, latest id %v, ignore...",
			args.Leader, args.AppendEntriesId, rf.receiveAppendEntriesId)
		rf.mu.Unlock()
		return &AppendEntriesReply{
			IsSuccess: false,
			Term:      currTerm,
			Reason:    RejectAppendEntries_OldId,
		}
	}

	rf.receiveAppendEntriesId = args.AppendEntriesId

	rf.printf("receive AppendEntries (entries: %v) from %v for term %v, id %v, leaderCommitIdx: %v, lastAppliedIdx %v, prevLogIdx %v",
		len(args.LogEntries), args.Leader, args.Term, args.AppendEntriesId, args.LeaderCommitIdx, lastAppliedIdx, args.PrevLogIndex)

	needPersist := rf.receiveHeartbeatLocked(args.Term, currTerm, args.Leader)

	var appendSuccess bool
	appendSuccess, reply = rf.doAppendEntriesLocked(args.LogEntries, currTerm, args.PrevLogIndex, args.PrevLogTerm)
	if needPersist || appendSuccess {
		rf.persistLocked()
	}

	// update commitIdx after appendEntries, just in case our log is shorten
	if reply.IsSuccess {
		// if not success, that must be caused by conflict, which means our log is diverted from leader, and since
		// we might have long log and leader might update entries to other followers and update commitIdx
		// we might falsely commit some logs that we are not aligned with leader
		// in this case, we shouldn't update commit index
		rf.checkAndUpdateCommitIdxLocked(args.LeaderCommitIdx, args.Term)
	}

	rf.mu.Unlock()

	return reply
}

func (rf *Raft) checkAndUpdateCommitIdxLocked(leaderCommitIdx int32, leaderTerm int32) {
	// we cannot directly update commitIdx, because me might store stale data
	// consider I StartCommand on 123 but it's not committed (no enough follower)
	// node 2 and 3 StartCommand on 124 and committed, and when node 2 or 3 update my commitIdx
	// because our records might not updated to 124 in time
	// me will report 123 as commited while it's not
	oldCommitIdx := rf.commitIdx.Load()
	newCommitIdx := oldCommitIdx
	if leaderCommitIdx > oldCommitIdx {
		if len(rf.logEntries) > 0 && rf.logEntries[len(rf.logEntries)-1].Term == leaderTerm {
			// we don't commit on previous term, only when it's the latest term
			// TODO: does this check necessary?
			newCommitIdx = min32(leaderCommitIdx, int32(rf.lastAppliedIdx))
			rf.printf("Follower update commitIdx to %v, lastAppliedIdx %v, leaderCommitIdx %v",
				newCommitIdx, rf.lastAppliedIdx, leaderCommitIdx)
			rf.commitIdx.Store(newCommitIdx)
		}
	}
	if newCommitIdx != oldCommitIdx {
		rf.updateApplyChSignals++
		rf.updateApplyChCond.Signal()
	}
}

func (rf *Raft) receiveHeartbeatLocked(term, myTerm int32, leader int) (needPersist bool) {
	rf.heartbeatTime.Store(time.Now().UnixMilli())

	prevState := rf.state.Swap(StateFollower)
	if prevState == StateCandidate {
		rf.printf("receive AppendEntries from %v in candidate state, stop election", leader)
	} else if prevState == StateLeader {
		rf.printf("receive AppendEntries from %v, in leader state, change to follower", leader)
	}
	needPersist = false
	if term > myTerm {

		rf.currentTerm.Store(term)
		// we can reset votedFor only when term change
		// because in this term we shouldn't change voted for, or we might vote for different candidates
		// consider a case: node 0, 1 are connected and 1 is leader in term 50 (0, 1 voted for 1 in term 50)
		// node 2 is in term 49 and rejoin the cluster, it detect timeout (node 1 not send appendentries to it yet)
		// node 2 increase term to 50 and run election, if we reset 0's votedFor, 0 will vote for 2 in term 50
		// and in term 50, we will have 2 leader
		rf.votedFor = -1

		needPersist = true
	}
	return needPersist
}

func (rf *Raft) doAppendEntriesLocked(entries []LogData, currTerm int32,
	prevLogIndex int, prevLogTerm int32) (appendSuccess bool, reply *AppendEntriesReply) {
	reply = &AppendEntriesReply{
		Term: currTerm,
	}
	snapshotEndIndex := rf.snapshotEndIndex
	if prevLogIndex == -1 {
		rf.printf("Append all entries in term %v", currTerm)
		rf.lastAppliedIdx = len(entries) - 1
		// in case we already take snapshot
		// append 60 entries, [0] already in snapshot, we only need to append [1, 59]
		count := rf.lastAppliedIdx - snapshotEndIndex
		rf.logEntries = entries[len(entries)-count:]
		reply.IsSuccess = true
		return true, reply
	}
	appendSuccess = false
	if logLen := len(rf.logEntries) + snapshotEndIndex + 1; logLen <= prevLogIndex {
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
				rf.printf("receive %v entries, term %v, prevLogIdx %v, after appended entries %v",
					len(entries), currTerm, realPrevLogIndex, len(rf.logEntries))
			} else {
				// prevLogIndex 100, entries [0~59]([101~160]), snapshotEndIndex 150
				// only need to append (50~59)[151~160]
				rf.lastAppliedIdx = len(entries) + prevLogIndex
				count := rf.lastAppliedIdx - snapshotEndIndex
				rf.logEntries = entries[len(entries)-count:]
			}
			reply.IsSuccess = true
			appendSuccess = true
		}
	}
	return appendSuccess, reply
}

func (rf *Raft) leaderRequestVotes(lastLogIndex int, lastLogTerm int32, currTerm int32) {
	args := &RequestVoteArgs{
		Term:         currTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// request for votes asyncally with timeout
	replyChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if rf.state.Load() != StateCandidate {
			break
		}
		if i == rf.me {
			continue
		}
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
				replyChan <- -1
				rf.printf("failed rpc to RequestVote to %v in term %v", peer, currTerm)
			}
		}(i)
	}

	targetVoteCount := len(rf.peers)/2 + 1
	voters := []int{rf.me}
	voteCount := 1 // me already vote for me
	ctx, cancel := context.WithTimeout(context.Background(), ElectionTimeOut)

L:
	for i := 0; i < len(rf.peers)-1 && voteCount < targetVoteCount && rf.state.Load() == StateCandidate; {
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
	if rf.state.Load() != StateCandidate {
		// ignore results
		rf.printf("receive vote in term %v ended while election stop, ignore results", currTerm)
		return
	}
	if voteCount >= targetVoteCount {
		rf.printf("receive vote from %v in term %v, will become leader", voters, currTerm)
		rf.setLeader(currTerm)
		return
	}

	rf.printf("didn't get enough vote (%v, %v) in time for term %v", voters, targetVoteCount, currTerm)
	rf.state.Store(StateFollower)
}

func (rf *Raft) appendEntriesToFollower(peer int, appendEntriesId int32,
	heartbeatChan chan int, currTerm int32) {
	if rf.killed() {
		return
	}
	// fix: make sure the check and set args in same lock statement
	// if not, 95 perform check first, then 96 perform checks, then 96 might set args prior to 95
	// and cause 96 have shorter log and lower commitIdx and finally causes follower commit index not increment monotonically
	rf.mu.Lock()
	latestAppendEntriesId := rf.lastAppendEntriesId.Load()
	if appendEntriesId < latestAppendEntriesId {
		rf.mu.Unlock()
		rf.printf("skip appendEntriesToFollower to %v, old id %v (latest id %v)",
			peer, appendEntriesId, latestAppendEntriesId)
		return
	}
	latestTerm := rf.currentTerm.Load()
	if currTerm != latestTerm {
		rf.mu.Unlock()
		rf.printf("skip appendEntriesToFollower to %v, old term %v (latest term %v)",
			peer, currTerm, latestTerm)
		return
	}

	args := AppendEntriesArgs{
		Term:            currTerm,
		Leader:          rf.me,
		LeaderCommitIdx: rf.commitIdx.Load(),
		AppendEntriesId: appendEntriesId,
	}
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
		args.LogEntries = cloneEntries(rf.logEntries[nextLogIndex:])
	}
	rf.mu.Unlock()
	args.PrevLogIndex = prevIndex
	args.PrevLogTerm = prevLogTerm
	rf.printf("AppendEntries to %v in term %v, id %v, entries: %v, commitIndex: %v, nextLogIdx: %v, nextIdx: %v, lastLogIndex: %v",
		peer, currTerm, appendEntriesId, len(args.LogEntries), rf.commitIdx.Load(), nextLogIndex, nextIndex, lastLogIndex)

	reply := AppendEntriesReply{}

	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to timeout",
			peer, currTerm, appendEntriesId)
		return
	}
	if heartbeatChan != nil {
		heartbeatChan <- peer
	}
	if reply.IsSuccess {
		appendEntries := args.LogEntries
		if len(appendEntries) > 0 {
			newNextIdx := nextIndex + len(appendEntries)
			newMatchIdx := newNextIdx - 1
			rf.printf("Success AppendEntries to %v in term %v, id %v, match index: %v",
				peer, currTerm, appendEntriesId, newMatchIdx)
			rf.mu.Lock()
			rf.nextIndex[peer] = newNextIdx
			rf.matchIndex[peer] = newMatchIdx
			if len(appendEntries) > 0 {
				rf.updateCommitIdxSignals++
				rf.updateCommitIdxCond.Signal()
			}
			rf.mu.Unlock()
		}
		return
	}
	if reply.Reason == RejectAppendEntries_OldId {
		// the follower have received new AppendEntries Request, so it reject me, ignore this
		return
	}
	if currTerm < reply.Term {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to new term in peer, switch to follower state",
			peer, currTerm, appendEntriesId)
		rf.state.Store(StateFollower)
		return
	}
	rf.handleAppendEntriesConflict(&reply, peer, currTerm, appendEntriesId)
	rf.appendEntriesToFollower(peer, appendEntriesId, nil, currTerm)
}

func (rf *Raft) handleAppendEntriesConflict(reply *AppendEntriesReply, peer int, currTerm, appendEntriesId int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currNextIndex := rf.nextIndex[peer]
	if reply.XLogLen < currNextIndex {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to follower log entries too short %v",
			peer, currTerm, appendEntriesId, reply.XLogLen)
		rf.nextIndex[peer] = reply.XLogLen
	} else {
		rf.printf("failed to AppendEntries to follower %v in term %v, id %v due to conflict, xterm: %v, xindex: %v",
			peer, currTerm, appendEntriesId, reply.XTerm, reply.XIndex)
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
					// start by the end of this term, as logs added in one term must be identical
					rf.nextIndex[peer] = i + 1
				}
				break
			}
		}
		if !isUpdated {
			rf.printf("for peer %v and xterm %v, we don't have this term", peer, reply.XTerm)
			rf.nextIndex[peer] = reply.XIndex
		}
	}
}

func (rf *Raft) setFollower() {
	rf.printf("become follower")
	rf.state.Store(StateFollower)
}
