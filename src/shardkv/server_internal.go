package shardkv

import (
	"6.5840/shardctrler"
	"context"
	"fmt"
	"time"
)

func (kv *ShardKV) goUpdateStateFromApplyCh() {
	for !kv.dead.Load() {
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

func (kv *ShardKV) applyOpLocked(op *Op) {
	opId := kv.maxOpIdForClerk[op.ClerkId]
	if opId >= op.OpId {
		return
	}
	kv.maxOpIdForClerk[op.ClerkId] = op.OpId
	switch op.OpType {
	case OpPut:
		kv.state[op.Key] = op.Value
	case OpAppend:
		kv.state[op.Key] += op.Value
	default:
	}
}

func (kv *ShardKV) retryUntilCommit(op *Op) Err {
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

func (kv *ShardKV) waitForCommandWithTimeout(idx int, maxDuration time.Duration) Err {
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
	// wait for applyCh send the exact same message to us
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.latestAppliedCmdIdx < idx && !kv.dead.Load() && !timeout {
		kv.stateUpdateCond.Wait()
	}
	if kv.dead.Load() {
		return ErrDead
	}
	if timeout {
		return ErrTimeout
	}
	return OK
}

func (kv *ShardKV) goUpdateConfig() {
	for !kv.dead.Load() {
		kv.mu.Lock()
		for kv.updateConfigRequests == 0 && !kv.dead.Load() {
			kv.triggerUpdateConfigCond.Wait()
		}
		if kv.dead.Load() {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		kv.updateConfig()

		kv.mu.Lock()
		kv.updateConfigTime = time.Now()
		kv.updateConfigRequests = 0
		kv.updateConfigCond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig() {
	args := &shardctrler.QueryArgs{
		Num: -1,

		ClerkId: int32(kv.me),
		OpId:    kv.ctrlOpId.Add(1),
	}
	// Your code here.
	for {
		// try each known server.
		for _, srv := range kv.ctrlers {
			var reply shardctrler.QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK && reply.WrongLeader == false {
				kv.mu.Lock()
				for i := 0; i < len(kv.shards); i++ {
					kv.shards[i] = false
				}
				for shard, gid := range reply.Config.Shards {
					if kv.gid == gid {
						kv.shards[shard] = true
					}
				}
				kv.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) keyMatch(key string) bool {
	shard := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for time.Since(kv.updateConfigTime) > time.Millisecond*100 {
		kv.updateConfigRequests++
		kv.triggerUpdateConfigCond.Signal()
		kv.updateConfigCond.Wait()
	}
	return kv.shards[shard]
}
