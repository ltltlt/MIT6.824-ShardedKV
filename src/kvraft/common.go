package kvraft

import (
	"fmt"
	"log"
	"sync/atomic"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDead        = "ErrDead"
	ErrTimeout     = "ErrTimeout"
)

const Debug = true

const (
	OpSet int8 = iota
	OpGet
	OpAppend
)

const (
	MaxRetries = 1000
)

var (
	ClerkId atomic.Int32
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClerkId int32
	OpId    int32
	Leader  int
}

type PutAppendReply struct {
	Err    Err
	Leader int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int32
	OpId    int32
}

type GetReply struct {
	Err    Err
	Value  string
	Leader int
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
		OpType:  OpSet,
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

func (reply *PutAppendReply) GetErr() Err {
	return reply.Err
}

func (reply *GetReply) GetErr() Err {
	return reply.Err
}

type HasError interface {
	GetErr() Err
}

func (kv *KVServer) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[s%v]", kv.me) + fmt.Sprintf(format, a...))
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
