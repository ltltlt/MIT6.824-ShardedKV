package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDead        = "ErrDead"
	ErrTimeout     = "ErrTimeout"

	Debug = true
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClerkId int32
	OpId    int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClerkId int32
	OpId    int32
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardDataArgs struct {
	Shard   int
	ClerkId int32
	OpId    int32
}

type GetShardDataReply struct {
	Err  Err
	Data map[string]string
}

type DelShardDataArgs struct {
	Shard   int
	ClerkId int32
	OpId    int32
}

type DelShardDataReply struct {
	Err Err
}

type PutShardDataArgs struct {
	Shard     int
	Data      map[string]string
	ConfigNum int
	// we need to move this for shard too, or append might apply once in old server (but didn't receive reply due to network)
	// and client retry on new server, it can success
	MaxOpIdForClerk map[int32]int32

	ClerkId int32
	OpId    int32
}

type PutShardDataReply struct {
	Err Err
}

func (kv *ShardKV) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[kv g%v s%v]", kv.gid, kv.me) + fmt.Sprintf(format, a...))
	}
	return
}
func (ck *Clerk) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[kv c%v]", ck.id) + fmt.Sprintf(format, a...))
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func NewGetOp(args *GetArgs, shard int) *Op {
	return &Op{
		Key:     args.Key,
		OpType:  OpGet,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,

		Shard: shard, // used for debug
	}
}

func NewPutOp(args *PutAppendArgs, shard int) *Op {
	return &Op{
		Key:     args.Key,
		Value:   args.Value,
		OpType:  OpPut,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,

		Shard: shard,
	}
}

func NewAppendOp(args *PutAppendArgs, shard int) *Op {
	return &Op{
		Key:     args.Key,
		Value:   args.Value,
		OpType:  OpAppend,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,

		Shard: shard,
	}
}

func NewGetShardOp(args *GetShardDataArgs) *Op {
	return &Op{
		Shard:   args.Shard,
		OpType:  OpGetShard,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
}

func NewDelShardOp(shard int, clerkId int32, opId int32) *Op {
	return &Op{
		Shard:   shard,
		OpType:  OpDelShard,
		ClerkId: clerkId,
		OpId:    opId,
	}
}

func NewPutShardOp(args *PutShardDataArgs) *Op {
	return &Op{
		Shard:           args.Shard,
		ShardData:       args.Data,
		OpType:          OpPutShard,
		ConfigNum:       args.ConfigNum,
		MaxOpIdForClerk: args.MaxOpIdForClerk,

		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
}

func NewMoveShardOp(shard int, toGID int, servers []string, configNum int, clerkId int32, opId int32) *Op {
	// we don't have to store the data into log, we can pick it up in states when we try to do real operation
	// since the state should become non-writable
	return &Op{
		Shard:     shard,
		GID:       toGID,
		Servers:   servers,
		OpType:    OpMoveShard,
		ConfigNum: configNum,

		ClerkId: clerkId,
		OpId:    opId,
	}
}

func NewUpdateShardStateOp(shard int, newGID int, clerkId int32, opId int32) *Op {
	return &Op{
		Shard:  shard,
		GID:    newGID,
		OpType: OpUpdateShardState,

		ClerkId: clerkId,
		OpId:    opId,
	}
}

func NewUpdateConfigOp(oldConfig *shardctrler.Config, newConfig *shardctrler.Config, ts int64, clerkId int32, opId int32) *Op {
	return &Op{
		NewConfig: *newConfig,
		OldConfig: *oldConfig,
		OpType:    OpUpdateConfig,
		Timestamp: ts,

		ClerkId: clerkId,
		OpId:    opId,
	}
}

func NewUpdateConfigTimeOp(ts int64) *Op {
	return &Op{
		Timestamp: ts,
		OpType:    OpUpdateConfigTime,
	}
}
