package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"sync"
	"sync/atomic"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType  int8
	Key     string
	Value   string
	OpId    int32
	ClerkId int32

	Timestamp       int64
	Shard           int
	ShardData       map[string]string
	GID             int
	Servers         []string
	ConfigNum       int
	MaxOpIdForClerk map[int32]int32

	OldConfig shardctrler.Config
	NewConfig shardctrler.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	persister           *raft.Persister
	states              [shardctrler.NShards]map[string]string
	stateUpdateCond     *sync.Cond
	latestAppliedCmdIdx int
	maxOpIdForClerks    [shardctrler.NShards]map[int32]int32 // used to avoid re-executing op

	// we don't have to keep below in stable storage
	// because when we restart the server, there's no longer old requests
	neededCommandIdxes map[int]struct{}
	commandIdx2Op      map[int]*Op // used to check whether the committed command is the same as we added

	dead atomic.Bool

	clerkId               int32
	opId                  atomic.Int32
	moveShardId           int32
	moveShardChan         chan *Op
	moveShardCompleteChan chan struct{}

	updateConfigTime        int64
	updateConfigCond        *sync.Cond
	triggerUpdateConfigCond *sync.Cond
	updateConfigRequests    int

	config        *shardctrler.Config
	shards        []int      // shard => server
	moveShardCond *sync.Cond // signals when moving 1 shard is completed

	configs              map[int]*shardctrler.Config // cache so we don't have to query every time
	putShardConfigNums   []int
	shardStateUpdateCond *sync.Cond
}

type moveShardRequest struct {
	configNum int
	shard     int
	servers   []string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	kv.dprintf("receive Get, key %v, shard %v", args.Key, shard)
	reply.Err = kv.retryUntilCommit(NewGetOp(args, shard), shard)
	if reply.Err == OK {
		shard := key2shard(args.Key)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		state := kv.states[shard]
		if state == nil {
			reply.Err = ErrNoKey
		} else {
			v, ok := state[args.Key]
			if !ok {
				reply.Err = ErrNoKey
			} else {
				reply.Value = v
			}
		}
	}
}

func (kv *ShardKV) PutShardData(args *PutShardDataArgs, reply *PutShardDataReply) {
	reply.Err = kv.retryUntilCommit(NewPutShardOp(args), -1)
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	var op *Op
	if args.Op == "Put" {
		op = NewPutOp(args, shard)
	} else {
		op = NewAppendOp(args, shard)
	}
	reply.Err = kv.retryUntilCommit(op, shard)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.dead.Store(true)
	kv.shardStateUpdateCond.Broadcast()
	kv.stateUpdateCond.Broadcast()
	kv.updateConfigCond.Broadcast()
	kv.moveShardCond.Broadcast()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)

	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		if err := kv.readSnapshotLocked(snapshot); err != nil {
			panic(err)
		}
	} else {
		kv.latestAppliedCmdIdx = 0
		kv.shards = make([]int, shardctrler.NShards)
		kv.putShardConfigNums = make([]int, shardctrler.NShards)
		kv.config = &shardctrler.Config{Num: -1}
	}
	kv.stateUpdateCond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.commandIdx2Op = make(map[int]*Op)
	kv.neededCommandIdxes = make(map[int]struct{})
	kv.persister = persister

	kv.updateConfigCond = sync.NewCond(&kv.mu)
	kv.triggerUpdateConfigCond = sync.NewCond(&kv.mu)

	kv.moveShardCond = sync.NewCond(&kv.mu)
	kv.configs = make(map[int]*shardctrler.Config)

	kv.moveShardChan = make(chan *Op, 1)
	kv.moveShardCompleteChan = make(chan struct{})

	kv.clerkId = int32(kv.gid)*100 + int32(kv.me)
	kv.updateConfigRequests++ // trigger on starting to pull latest config

	kv.dprintf("start server")
	kv.shardStateUpdateCond = sync.NewCond(&kv.mu)

	go kv.goUpdateStateFromApplyCh()
	go kv.goUpdateConfig()
	go kv.goTriggerUpdateConfig()

	return kv
}
