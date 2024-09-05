package shardkv

import (
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

	OpPut int8 = iota
	OpGet
	OpAppend
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

func (kv *ShardKV) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[g%v s%v]", kv.gid, kv.me) + fmt.Sprintf(format, a...))
	}
	return
}
func (ck *Clerk) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[c%v]", ck.id) + fmt.Sprintf(format, a...))
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func NewGetOp(args *GetArgs) *Op {
	return &Op{
		Key:     args.Key,
		OpType:  OpGet,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
}

func NewPutOp(args *PutAppendArgs) *Op {
	return &Op{
		Key:     args.Key,
		Value:   args.Value,
		OpType:  OpPut,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
}

func NewAppendOp(args *PutAppendArgs) *Op {
	return &Op{
		Key:     args.Key,
		Value:   args.Value,
		OpType:  OpAppend,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
}
