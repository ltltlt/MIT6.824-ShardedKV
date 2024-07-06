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

const (
	ElectionComplete = iota
	ElectionOnGoing
)

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

	// volatile
	commitIdx   int // commit index at leader
	lastApplied int // index apply on this server, might not commit

	// volatile leader state
	nextIndex  []int // next log entry send to followers
	matchIndex []int // what have been already sent

	// stable storage
	currentTerm atomic.Int32
	votedFor    int // vote for which candidate in this term
	logEntries  []LogData

	// custom
	isLeader              atomic.Bool
	heartbeatTime         atomic.Int64
	electionStartTime     atomic.Int64
	latestHeartBeatStatus []atomic.Int32
	heartbeatId           atomic.Int32

	works  chan work
	reply  chan work
	workId atomic.Int32
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
)

type work struct {
	workId   int32
	workType int
	data     interface{}
}

type LogData struct {
	Term int
	Data []byte
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
	LastLogTerm  int
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
	log.Printf("%v Run RequestVote in term %v, from %v", rf.me, rf.currentTerm, args.CandidateId)
	if args.Term < currTerm {
		log.Printf("RequestVote from node (%v and %v), term (%v vs. %v), which is old, reject",
			rf.me, args.CandidateId, currTerm, args.Term)
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
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args) {
		rf.votedFor = args.CandidateId
		log.Printf("%v grant vote for %v in term %v", rf.me, args.CandidateId, args.Term)
		return &RequestVoteReply{
			VoteGranted: true,
			Term:        currTerm,
		}
	}
	log.Printf("%v won't grant vote for %v in term %v, already vote for %v",
		rf.me, args.CandidateId, args.Term, rf.votedFor)
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
	index := -1
	term := 0
	isLeader := false

	// Your code here (3B).

	return index, term, isLeader
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

func (rf *Raft) workSendHeartBeats(currTerm int32, heartbeatId int32) {
	log.Printf("%v leader send heartbeats in term %v", rf.me, currTerm)
	args := &AppendEntriesArgs{
		Term:   currTerm,
		Leader: rf.me,
	}
	heartbeatStatus := make(chan int, len(rf.peers))
	for i, peer := range rf.peers {
		if rf.me == i {
			continue
		}
		if !rf.isLeader.Load() {
			break
		}
		// notify async to avoid block send heartbeat to others
		// also avoid to send heartbeat too many times which causes goroutine leakage
		log.Printf("%v AppendEntries to %v in term %v", rf.me, i, currTerm)

		go func(peerNo int, peer1 *labrpc.ClientEnd, heartbeatId int32) {
			reply := &AppendEntriesReply{}
			if !peer1.Call("Raft.AppendEntries", args, reply) {
				log.Printf("%v failed to AppendEntries for %v in term %v", rf.me, peerNo, currTerm)
			} else {
				if reply.IsSuccess == false {
					rf.works <- work{
						workType: WorkType_ToFollower,
					}
				}
				heartbeatStatus <- peerNo
			}
		}(i, peer, heartbeatId)
	}
	timeout := time.After(time.Millisecond * 500)
	var receiveFrom []int
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
		log.Printf("%v too few connected node %v, change to follower state", rf.me, receiveFrom)
		rf.works <- work{
			workId:   heartbeatId,
			workType: WorkType_SendHeartBeatFail,
		}
	} else {
		rf.works <- work{
			workId:   heartbeatId,
			workType: WorkType_SendHeartBeatSuccess,
		}
	}
}

func (rf *Raft) setLeader() {
	log.Printf("%v is now leader since term %v, update data and notify followers", rf.me, rf.currentTerm)
	rf.isLeader.Store(true)
	rf.matchIndex = nil
	rf.nextIndex = nil
	rf.electionStartTime.Store(0)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, rf.lastApplied+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.works <- work{
		workType: WorkType_CallAppendEntries,
	}
}

type AppendEntriesArgs struct {
	Term       int32
	Leader     int
	LogEntries []LogData
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
	rf.latestHeartBeatStatus = make([]atomic.Int32, len(rf.peers))
	rf.votedFor = -1

	rf.works = make(chan work, 10)
	rf.reply = make(chan work, 10)

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.work()

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
			go rf.workSendHeartBeats(rf.currentTerm.Load(), rf.workId.Add(1))
		case WorkType_AppendEntries:
			reply := rf.workAppendEntries(work.data.(*AppendEntriesArgs))
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
		case WorkType_RunElection:
			electionStartTime := rf.electionStartTime.Load()
			if electionStartTime > 0 {
				log.Printf("%v detect timeout %v, while election is ongoing, startTime %v...",
					rf.me, time.UnixMilli(rf.heartbeatTime.Load()).Format(time.RFC3339),
					time.UnixMilli(electionStartTime).Format(time.RFC3339))
			} else {
				// start new election
				log.Printf("%v detect timeout %v, start election...",
					rf.me, time.UnixMilli(rf.heartbeatTime.Load()).Format(time.RFC3339))
				rf.electionStartTime.Store(time.Now().UnixMilli())
				lastLogTerm := -1
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

func (rf *Raft) workAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := AppendEntriesReply{}
	currTerm := rf.currentTerm.Load()
	if args.Term >= currTerm {
		log.Printf("%v receive AppendEntries from %v for term %v", rf.me, args.Leader, args.Term)
		rf.heartbeatTime.Store(time.Now().UnixMilli())
		rf.currentTerm.Store(args.Term)
		if rf.electionStartTime.Swap(0) > 0 {
			log.Printf("%v receive AppendEntries from %v when election, stop election", rf.me, args.Leader)
		}
		rf.votedFor = -1
		reply.IsSuccess = true
		rf.isLeader.Store(false)
	} else {
		log.Printf("%v receive old AppendEntries from %v for term %v, ignore", rf.me, args.Leader, args.Term)
		reply.IsSuccess = false
		reply.Term = currTerm
	}
	return &reply
}
func (rf *Raft) workRunElection(lastLogIndex int, lastLogTerm int, term int32) {
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
			log.Printf("%v request %v for vote in term %v", rf.me, peer, currTerm)
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, &reply)
			if ok {
				if reply.VoteGranted {
					log.Printf("%v receive vote from %v in term %v", rf.me, peer, currTerm)
					replyChan <- peer
				} else {
					log.Printf("%v receive reject vote from %v in term %v", rf.me, peer, currTerm)
					replyChan <- -1
				}
			} else {
				log.Printf("%v failed to RequestVote to %v in term %v", rf.me, peer, currTerm)
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
	if voteCount >= targetVoteCount {
		log.Printf("%v receive vote from %v, will become leader", rf.me, voters)
		rf.works <- work{
			workType: WorkType_SetLeader,
		}
		return
	}

	log.Printf("%v didn't get enough vote (%v, %v) in time for term %v", rf.me, voters, targetVoteCount, currTerm)
	rf.electionStartTime.Store(0)
}
