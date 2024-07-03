package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data         map[string]string
	lastRequests map[int64]*Request
}

type Request struct {
	offset   int32
	response string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.checkAndUpdate(args.Key, args.Value, args.ClientId, args.OpId)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	old := kv.data[args.Key]
	reply.Value = kv.checkAndUpdate(args.Key, old+args.Value, args.ClientId, args.OpId)
}

func (kv *KVServer) UpdateStatus(args *UpdateStatusArgs, reply *int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if req := kv.lastRequests[args.ClientId]; req != nil && args.OpId >= req.offset {
		delete(kv.lastRequests, args.ClientId)
	}
}

func (kv *KVServer) checkAndUpdate(key string, value string, clientId int64, opId int32) string {
	lastRequest := kv.lastRequests[clientId]
	if lastRequest == nil || lastRequest.offset < opId {
		old := kv.data[key]
		kv.data[key] = value
		if lastRequest == nil {
			kv.lastRequests[clientId] = &Request{
				offset:   opId,
				response: old,
			}
		} else {
			lastRequest.offset = opId
			lastRequest.response = old
		}
		return old
	}
	return lastRequest.response
}

func StartKVServer() *KVServer {
	return &KVServer{
		data:         map[string]string{},
		lastRequests: map[int64]*Request{},
	}
}
