package kvsrv

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	opId atomic.Int32
	id   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	return &Clerk{
		server: server,
		id:     nrand(),
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	for {
		reply := GetReply{}
		if ck.server.Call("KVServer.Get", &args, &reply) {
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Value:    value,
		Key:      key,
		OpId:     ck.opId.Add(1),
		ClientId: ck.id,
	}
	for {
		reply := PutAppendReply{}
		if ck.server.Call("KVServer."+op, &args, &reply) {
			var dummy int
			for !ck.server.Call("KVServer.UpdateStatus", &UpdateStatusArgs{
				OpId:     args.OpId,
				ClientId: args.ClientId,
			}, &dummy) {
			}
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
