package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Op is raft log
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType  int8
	Key     string
	Value   string
	OpId    int32
	ClerkId int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    atomic.Int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	persister           *raft.Persister
	state               map[string]string
	stateUpdateCond     *sync.Cond
	latestAppliedCmdIdx int
	maxOpIdForClerk     map[int32]int32 // used to avoid re-executing op

	// we don't have to keep below in stable storage
	// because when we restart the server, there's no longer old requests
	neededCommandIdxes map[int]struct{}
	commandIdx2Op      map[int]*Op // used to check whether the committed command is the same as we added
}

func (kv *KVServer) goUpdateStateFromApplyCh() {
	for kv.dead.Load() != 1 {
		msg := <-kv.applyCh
		var err error
		if msg.CommandValid {
			kv.mu.Lock()
			// it's possible that we have snapshot larger than commandIdx
			// (receive snapshot from another server)
			if kv.latestAppliedCmdIdx < msg.CommandIndex {
				kv.latestAppliedCmdIdx = msg.CommandIndex
				op := msg.Command.(Op)
				if _, ok := kv.neededCommandIdxes[msg.CommandIndex]; ok {
					kv.commandIdx2Op[msg.CommandIndex] = &op
				}
				kv.applyOpLocked(&op)
				kv.stateUpdateCond.Broadcast()

				if kv.maxraftstate >= 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					err = kv.createSnapshotLocked()
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			// normally this should be always valid
			// since all commands should be applied then we call rf.Snapshot
			// but the snapshot might be send to another server
			if msg.SnapshotIndex > kv.latestAppliedCmdIdx {
				err = kv.readSnapshotLocked(msg.Snapshot)
				if msg.SnapshotIndex != kv.latestAppliedCmdIdx {
					panic(fmt.Sprintf("snapshot index inconsistent, msg %v, decoded %v", msg.SnapshotIndex, kv.latestAppliedCmdIdx))
				}
				kv.stateUpdateCond.Broadcast()
			} else {
				kv.dprintf("Ignore snapshot endIdx %v, latest applied idx %v", msg.SnapshotIndex, kv.latestAppliedCmdIdx)
			}
			kv.mu.Unlock()
		}
		if err != nil {
			panic(err)
		}
	}
}

func (kv *KVServer) applyOpLocked(op *Op) {
	opId := kv.maxOpIdForClerk[op.ClerkId]
	if opId >= op.OpId {
		return
	}
	kv.maxOpIdForClerk[op.ClerkId] = op.OpId
	switch op.OpType {
	case OpSet:
		kv.state[op.Key] = op.Value
	case OpAppend:
		kv.state[op.Key] += op.Value
	default:
	}
}

// Get by key
// we don't have to cache the response, if client didn't receive the response
// it will retry and might get fresher data, which still matches linearizability
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err = kv.retryUntilCommit(NewGetOp(args))
	if reply.Err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		v, ok := kv.state[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = v
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = kv.retryUntilCommit(NewPutOp(args))
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = kv.retryUntilCommit(NewAppendOp(args))
}

func (kv *KVServer) retryUntilCommit(op *Op) Err {
	for {
		kv.mu.Lock()
		idx, _, ok := kv.rf.Start(*op) // use Op instead of pointer because pointer will be flattened during rpc call
		if !ok {
			kv.mu.Unlock()
			return ErrWrongLeader
		}
		kv.neededCommandIdxes[idx] = struct{}{}
		kv.mu.Unlock()
		err := kv.waitForCommandWithTimeout(idx, time.Millisecond*300)
		if err != OK {
			return err
		}
		// check whether the command is the same as ours
		kv.mu.Lock()
		// it's impossible for 1 server, two starts with same index
		// consider node 1 think it's leader and start command at 5
		// then node 2 won election, it won't remove entries 5 from 1
		// only when it needs to append new entries to node 1
		// in that case, next time 1 won election, it will only start at 6 (2 didn't append or append 1 entry)
		// or higher (2 append more entries)
		delete(kv.neededCommandIdxes, idx)
		op1, ok := kv.commandIdx2Op[idx]
		if ok && op1.ClerkId == op.ClerkId && op1.OpId == op.OpId {
			delete(kv.commandIdx2Op, idx)
			kv.mu.Unlock()
			return OK
		}
		kv.mu.Unlock()
		kv.dprintf("committed op at %v is %v didn't match mine %v, retry", idx, op1, op)
	}
}

func (kv *KVServer) waitForCommandWithTimeout(idx int, maxDuration time.Duration) Err {
	timeout := false
	ctx, stopf1 := context.WithTimeout(context.Background(), maxDuration)
	defer stopf1()
	stopf := context.AfterFunc(ctx, func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		timeout = true
		kv.stateUpdateCond.Broadcast()
	})
	defer stopf()
	// wait for applyCh send the exactly same message to us
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.latestAppliedCmdIdx < idx && kv.dead.Load() != 1 && !timeout {
		kv.stateUpdateCond.Wait()
	}
	if kv.dead.Load() == 1 {
		return ErrDead
	}
	if timeout {
		return ErrTimeout
	}
	return OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.dead.Store(1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	return 1 == kv.dead.Load()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		if err := kv.readSnapshotLocked(snapshot); err != nil {
			panic(err)
		}
	} else {
		kv.state = make(map[string]string)
		kv.latestAppliedCmdIdx = 0
		kv.maxOpIdForClerk = make(map[int32]int32)
	}
	kv.stateUpdateCond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.commandIdx2Op = make(map[int]*Op)
	kv.neededCommandIdxes = make(map[int]struct{})
	kv.persister = persister

	go kv.goUpdateStateFromApplyCh()

	return kv
}
