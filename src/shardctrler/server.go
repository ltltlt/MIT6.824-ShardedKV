package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	stateUpdateCond     *sync.Cond
	latestAppliedCmdIdx int
	maxOpIdForClerk     map[int32]int32 // used to avoid re-executing op

	// we don't have to keep below in stable storage
	// because when we restart the server, there's no longer old requests
	neededCommandIdxes map[int]struct{}
	commandIdx2Op      map[int]*Op // used to check whether the committed command is the same as we added

	dead atomic.Bool
}

// Op is raft log
type Op struct {
	// Your data here.

	OpType  int8
	OpId    int32
	ClerkId int32

	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	err := sc.retryUntilCommit(NewJoinOp(args))
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	err := sc.retryUntilCommit(NewLeaveOp(args))
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	err := sc.retryUntilCommit(NewMoveOp(args))
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// we can return if num<len(sc.configs), since configs are read only
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Config = sc.configs[args.Num]
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	// make sure we see all updates before this query
	// (some commands might still under replicate)
	err := sc.retryUntilCommit(NewQueryOp(args))
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	if err == OK {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		idx := args.Num
		if idx < 0 || idx >= len(sc.configs) {
			idx = len(sc.configs) - 1
		}
		reply.Config = sc.configs[idx]
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.dead.Store(true)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.maxOpIdForClerk = make(map[int32]int32)
	sc.stateUpdateCond = sync.NewCond(&sc.mu)
	sc.neededCommandIdxes = make(map[int]struct{})
	sc.commandIdx2Op = make(map[int]*Op)
	sc.dead.Store(false)

	go sc.goUpdateStateFromApplyCh()

	return sc
}
