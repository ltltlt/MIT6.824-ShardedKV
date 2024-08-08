package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	opId atomic.Int32
	id   int32

	leader atomic.Int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = ClerkId.Add(1)
	// You'll have to add code here.

	ck.leader.Store(-1)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	opId := ck.opId.Add(1)
	args := GetArgs{Key: key, OpId: opId, ClerkId: ck.id}
	reply := GetReply{}
	ck.tryCall("Get", &args, &reply)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	opId := ck.opId.Add(1)
	args := PutAppendArgs{Key: key, Value: value, OpId: opId, ClerkId: ck.id}
	reply := PutAppendReply{}
	ck.tryCall(op, &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) tryCall(op string, args interface{}, reply HasError) {
	if leader := ck.leader.Load(); leader >= 0 {
		if ok := ck.tryWithServer(op, args, reply, int(leader)); ok {
			return
		}
	}
	rn := nrand()
	for i := 0; i < MaxRetries; i++ {
		server := (int64(i) + rn) % int64(len(ck.servers))
		if ok := ck.tryWithServer(op, args, reply, int(server)); ok {
			ck.leader.Store(int32(server))
			return
		}
		time.Sleep(time.Millisecond * 50)
	}
	panic("Reach max retry during put or append key")
}

func (ck *Clerk) tryWithServer(op string, args interface{}, reply HasError, idx int) bool {
	ck.dprintf("send %v %+v to %v", op, args, idx)
	srv := ck.servers[idx]
	if srv.Call("KVServer."+op, args, reply) {
		err := reply.GetErr()
		if err == OK || err == ErrNoKey {
			return true
		}
		ck.dprintf("failed to send %v to %v, err %v", op, idx, err)
	}
	return false
}
