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
	"context"
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
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
	rf.printf("Run RequestVote in term %v, from %v", currTerm, args.CandidateId)
	if args.Term < currTerm {
		rf.printf("RequestVote from node %v, term %v is older than my term %v, reject",
			args.CandidateId, args.Term, currTerm)
		return &RequestVoteReply{
			VoteGranted: false,
			Term:        currTerm,
		}
	}
	if args.Term > currTerm {
		// reset votedFor for new term (might caused by vote split), to ensure new term leader election can elect a leader
		rf.votedFor = -1
		// also need to transfer to follower if currently candidate
		rf.electionStartTime.Store(0)

		rf.currentTerm.Store(int32(args.Term))
		rf.isLeader.Store(false)
	}
	// if already vote for someone else, reject
	// compare log freshness, reject if mine is fresher than candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isUpToDate(args) {
			rf.votedFor = args.CandidateId
			rf.printf("grant vote for %v in term %v", args.CandidateId, args.Term)
			return &RequestVoteReply{
				VoteGranted: true,
				Term:        currTerm,
			}
		}
		rf.printf("won't grant vote for %v in term %v, candidate is out of date", args.CandidateId, args.Term)
	} else {
		rf.printf("won't grant vote for %v in term %v, already vote for %v",
			args.CandidateId, args.Term, rf.votedFor)
	}
	return &RequestVoteReply{
		VoteGranted: false,
		Term:        currTerm,
	}
}

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	if len(rf.logEntries) == 0 {
		return true
	}
	myLastLogTerm := rf.logEntries[len(rf.logEntries)-1].Term
	if myLastLogTerm != args.LastLogTerm {
		return myLastLogTerm <= args.LastLogTerm
	}
	return len(rf.logEntries)-1 <= args.LastLogIndex
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
	rf.printf("StartCommand, isLeader %v", isLeader)
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
				rf.printf("StartCommand result: %v (cmdIdx, term, isLeader)", data)
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
	return time.Now().UnixMilli()-rf.heartbeatTime.Load() > 500
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

		if rf.isLeader.Load() {
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
	rf.printf("leader send heartbeats in term %v", currTerm)
	heartbeatStatus := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		if !rf.isLeader.Load() {
			break
		}

		rf.mu.Lock()
		entries := rf.logEntries
		rf.mu.Unlock()
		go rf.appendEntriesToFollower(i, entries, appendEntriesId, heartbeatStatus)
	}
	timeout := time.After(time.Millisecond * 500)
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
		rf.printf("too few connected node %v, change to follower state", receiveFrom)
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
	rf.lastAppliedIdx = len(rf.logEntries) - 1
	rf.printf("become leader since term %v, lastAppliedIdx: %v", rf.currentTerm.Load(), rf.lastAppliedIdx)
	rf.isLeader.Store(true)
	rf.matchIndex = nil
	rf.nextIndex = nil
	rf.electionStartTime.Store(0)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, rf.lastAppliedIdx+1)
		rf.matchIndex = append(rf.matchIndex, -1)
	}
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

	rf.works = make(chan work, 10)
	rf.reply = make(chan work, 10)
	rf.commitIdx.Store(-1)
	rf.lastAppliedIdx = -1

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.work()
	go rf.sendCommittedToChan(applyCh)

	return rf
}

func (rf *Raft) work() {
	var lastSendHeartBeatSuccessId int32
	for !rf.killed() {
		work := <-rf.works
		switch work.workType {
		case WorkType_SetLeader:
			rf.setLeader()
		case WorkType_CallAppendEntries:
			rf.heartbeatTime.Store(time.Now().UnixMilli())
			rf.updateCommitIdx()
			go rf.workSendAppendEntries(rf.currentTerm.Load(), rf.workId.Add(1))
		case WorkType_AppendEntries:
			reply := rf.followerAppendEntries(work.data.(*AppendEntriesArgs))
			work.data = reply
			rf.reply <- work
		case WorkType_SendHeartBeatSuccess:
			if work.workId > lastSendHeartBeatSuccessId {
				lastSendHeartBeatSuccessId = work.workId
			}
		case WorkType_SendHeartBeatFail:
			if work.workId > lastSendHeartBeatSuccessId {
				rf.isLeader.Store(false)
			}
			// for old heartbeat, ignore it
		case WorkType_RequestVote:
			reply := rf.runRequestVote(work.data.(*RequestVoteArgs))
			work.data = reply
			rf.reply <- work
		case WorkType_ToFollower:
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
				rf.printf("detect timeout %v, start election...",
					time.UnixMilli(rf.heartbeatTime.Load()).Format(time.RFC3339))
				rf.electionStartTime.Store(time.Now().UnixMilli())
				lastLogTerm := int32(-1)
				if len(rf.logEntries) > 0 {
					lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
				}
				lastLogIndex := len(rf.logEntries) - 1
				// we should set votefor to me
				// if not, we might still vote for someone else in old term and causes me cannot win (only 3 of 5 nodes are alive case)
				rf.votedFor = rf.me
				go rf.workRunElection(lastLogIndex, lastLogTerm, rf.currentTerm.Add(1))
			}
		}
	}
}

func (rf *Raft) updateCommitIdx() {
	// update commit index
	commitIdx := int(rf.commitIdx.Load())
	for i := len(rf.logEntries) - 1; i > commitIdx; i-- {
		matchCount := 1
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			// only update when log entry is in same term, see section 5.4.2
			if rf.logEntries[i].Term == rf.currentTerm.Load() {
				rf.printf("Update commit index to %v", i)
				rf.commitIdx.Store(int32(i))
				return
			}
		}
	}
}

func (rf *Raft) sendCommittedToChan(applyCh chan ApplyMsg) {
	// send lastSendToChan ~ commitIdx to channel (lab requirement) and avoid blocking
	lastSendToChanIdx := -1
	for {
		commitIdx := int(rf.commitIdx.Load())
		if len(rf.logEntries)-1 < commitIdx {
			// when log is commited on leader, but not replicated to me
			commitIdx = len(rf.logEntries) - 1
		}
		for lastSendToChanIdx < commitIdx {
			lastSendToChanIdx++
			cmdIdx := lastSendToChanIdx
			command := rf.logEntries[cmdIdx]
			rf.printf("send %v to applyCh", cmdIdx)
			applyCh <- ApplyMsg{
				Command:      command.Command,
				CommandValid: true,
				CommandIndex: cmdIdx + 1,
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (rf *Raft) followerAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := AppendEntriesReply{}
	currTerm := rf.currentTerm.Load()
	if args.Term == currTerm && rf.receiveAppendEntriesId > args.AppendEntriesId {
		rf.printf("receive old AppendEntries from %v for appendEntriesId %v, ignore", args.Leader, args.AppendEntriesId)
		reply.IsSuccess = true
		reply.Term = currTerm
	} else if args.Term >= currTerm {
		rf.printf("receive AppendEntries (heartbeat: %v) from %v for term %v, leaderCommitIdx: %v",
			len(args.LogEntries) == 0, args.Leader, args.Term, args.LeaderCommitIdx)
		rf.heartbeatTime.Store(time.Now().UnixMilli())
		rf.currentTerm.Store(args.Term)

		if rf.electionStartTime.Swap(0) > 0 {
			rf.printf("receive AppendEntries from %v when election, stop election", args.Leader)
		}
		rf.votedFor = -1
		rf.isLeader.Store(false)

		// we cannot directly update commitIdx, because me might store stale data
		// consider I StartCommand on 123 but it's not committed (no enough follower)
		// node 2 and 3 StartCommand on 124 and committed, and when node 2 or 3 update my commitIdx
		// because our records might not updated to 124 in time
		// me will report 123 as commited while it's not
		if args.LeaderCommitIdx > rf.commitIdx.Load() {
			if rf.logEntries[len(rf.logEntries)-1].Term == args.Term {
				// we don't commit on previous term, only when it's the latest term
				// TODO: doesn't this check necessary?
				rf.commitIdx.Store(min32(args.LeaderCommitIdx, int32(rf.lastAppliedIdx)))
			}
		}

		if len(args.LogEntries) == 0 {
			return &AppendEntriesReply{
				IsSuccess: true,
				Term:      currTerm,
			}
		} else {
			return rf.doAppendEntries(args.LogEntries, currTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommitIdx)
		}
	} else {
		rf.printf("receive old AppendEntries from %v for term %v, ignore", args.Leader, args.Term)
		reply.IsSuccess = false
		reply.Term = currTerm
	}
	return &reply
}

func (rf *Raft) doAppendEntries(entries []LogData, currTerm int32, prevLogIndex int, prevLogTerm int32, leaderCommitIndex int32) *AppendEntriesReply {
	reply := AppendEntriesReply{
		Term: currTerm,
	}
	if prevLogIndex == -1 {
		rf.printf("Append all entries in term %v", currTerm)
		rf.logEntries = entries
		reply.IsSuccess = true
		rf.lastAppliedIdx = len(rf.logEntries) - 1
	} else if prevLogIndex >= len(rf.logEntries) {
		rf.printf("prevLogIndex doesn't match")
		reply.IsSuccess = false
	} else {
		myPrevLogTerm := rf.logEntries[prevLogIndex].Term
		if myPrevLogTerm != prevLogTerm {
			rf.printf("prevLogTerm doesn't match")
			reply.IsSuccess = false
		} else {
			rf.logEntries = append(rf.logEntries[:prevLogIndex+1], entries...)
			rf.lastAppliedIdx = len(rf.logEntries) - 1
			reply.IsSuccess = true
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
	voters := []int{rf.me}
	voteCount := 1 // me already vote for me
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
				rf.printf("failed to RequestVote to %v in term %v", peer, currTerm)
			}
		}(i)
	}
	targetVoteCount := len(rf.peers)/2 + 1
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)

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
		rf.lastAppliedIdx = len(rf.logEntries)
		rf.logEntries = append(rf.logEntries, LogData{
			Command: command,
			Term:    term,
		})
		cmdIdx := len(rf.logEntries) - 1
		return []int{cmdIdx, int(term), 1}
	}
	return []int{-1, int(term), 0}
}

func (rf *Raft) appendEntriesToFollower(peer int, entries []LogData, appendEntriesId int32, heartbeatChan chan int) {
	lastLogIndex := len(entries) - 1
	nextIndex := rf.nextIndex[peer]

	currTerm := rf.currentTerm.Load()
	args := AppendEntriesArgs{
		Term:            currTerm,
		Leader:          rf.me,
		LeaderCommitIdx: rf.commitIdx.Load(),
		AppendEntriesId: appendEntriesId,
	}
	if lastLogIndex >= nextIndex {
		rf.printf("AppendEntries to %v in term %v, commitIndex: %v, nextIndex: %v, lastLogIndex: %v",
			peer, currTerm, rf.commitIdx.Load(), nextIndex, lastLogIndex)
		args.PrevLogIndex = nextIndex - 1
		args.PrevLogTerm = -1
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = entries[args.PrevLogIndex].Term
		}
		args.LogEntries = entries[nextIndex:]
	} else {
		rf.printf("AppendEntries to %v in term %v, heartbeat only", peer, currTerm)
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
				rf.nextIndex[peer] = nextIndex + len(appendEntries)
				rf.matchIndex[peer] = nextIndex + len(appendEntries) - 1
			}
		} else {
			if currTerm < reply.Term {
				rf.printf("failed to append entries to follower %v due to new term in peer", peer)
				rf.works <- work{
					workType: WorkType_ToFollower,
				}
			} else {
				rf.printf("failed to append entries to follower %v due to log inconsistent, decrement nextIndex and retry", peer)
				rf.nextIndex[peer]--
				rf.appendEntriesToFollower(peer, entries, appendEntriesId, nil)
			}
		}
	} else {
		rf.printf("failed to append entries to follower %v due to timeout", peer)
	}
}
