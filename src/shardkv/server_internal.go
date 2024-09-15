package shardkv

import (
	"6.5840/shardctrler"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpGet int8 = iota
	OpPut
	OpAppend

	OpGetShard = iota + 10
	OpDelShard
	OpPutShard
	OpMoveShard
	OpUpdateShardState

	OpUpdateConfig = iota + 20
	OpUpdateConfigTime
)

const (
	ShardWaitForPut = -1 - iota
	ShardFreeze
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
				applyErr := kv.applyOpLocked(&op)
				if applyErr == OK {
					if _, ok := kv.neededCommandIdxes[msg.CommandIndex]; ok {
						kv.commandIdx2Op[msg.CommandIndex] = &op
					}
				} else {
					// if apply failed, it's append/put operation failed because of wrong group
					// (previously must have a freeze shard log entry but it's not observed by upper layer,
					// instead, when apply op, we found it's already fronzen, and we cannot mark it as complete)
					// we avoid setting the complete signal so upper layer know the op is not applied (though it's in raft log)
					// then upper layer will retry
					kv.dprintf("failed to apply op %v, err %v", op, applyErr)
				}
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

func (kv *ShardKV) applyDataOpLocked(op *Op) Err {
	shard := key2shard(op.Key)
	if kv.shards[shard] == ShardFreeze {
		return ErrWrongGroup
	}
	state := kv.states[shard]
	if state == nil {
		state = make(map[string]string)
		kv.states[shard] = state
	}
	maxOpIdForClerk := kv.maxOpIdForClerks[shard]
	if maxOpIdForClerk == nil {
		maxOpIdForClerk = make(map[int32]int32)
		kv.maxOpIdForClerks[shard] = maxOpIdForClerk
	}
	opId := maxOpIdForClerk[op.ClerkId]
	if opId >= op.OpId {
		kv.dprintf("ignore apply data op, seen opId %v, this opId %v, %+v", opId, op.OpId, op)
		return OK
	}
	maxOpIdForClerk[op.ClerkId] = op.OpId
	kv.dprintf("apply data op %+v", op)
	switch op.OpType {
	case OpPut:
		state[op.Key] = op.Value
	case OpAppend:
		state[op.Key] += op.Value
	case OpGet:
	}
	return OK
}

func (kv *ShardKV) applyOpLocked(op *Op) Err {
	// only op from client needs to be dedupped
	// other op are from servers, and hence won't cause duplicate
	if op.OpType == OpPut || op.OpType == OpAppend || op.OpType == OpGet {
		return kv.applyDataOpLocked(op)
	}

	if op.OpType != OpUpdateConfigTime {
		kv.dprintf("apply op %+v", op)
	}

	switch op.OpType {
	case OpPutShard:
		// might receive from multiple replica
		if op.ConfigNum > kv.putShardConfigNums[op.Shard] {
			kv.putShardConfigNums[op.Shard] = op.ConfigNum
			kv.states[op.Shard] = shardctrler.CopyMap(op.ShardData)
			kv.maxOpIdForClerks[op.Shard] = shardctrler.CopyMap(op.MaxOpIdForClerk)
			if kv.shards[op.Shard] == ShardWaitForPut {
				kv.shards[op.Shard] = kv.gid
				kv.moveShardCond.Broadcast()
			}
		} else {
			kv.dprintf("ignore putShard in configNum %v, seen configNum %v", op.ConfigNum, kv.putShardConfigNums[op.Shard])
		}
	case OpUpdateShardState:
		kv.shards[op.Shard] = op.GID
		kv.shardStateUpdateCond.Broadcast()
	case OpMoveShard:
		kv.moveShardId++
	case OpUpdateConfigTime:
		kv.updateConfigTime = op.Timestamp
	case OpUpdateConfig:
		kv.config = &op.NewConfig
		kv.updateConfigTime = op.Timestamp
		// config is updated successfully, do the gabage collection
		for shard, gid := range kv.config.Shards {
			kv.shards[shard] = gid
			if gid != kv.gid && kv.putShardConfigNums[shard] < kv.config.Num {
				// we don't own this shard in the new config
				// and we don't own it in the future (at least we don't know if we own it)
				// if we own it in future config, we should not clear the shard data
				kv.states[shard] = nil
			}
		}
	}
	return OK
}

func (kv *ShardKV) update1Config(oldConfig *shardctrler.Config, newConfig *shardctrler.Config, ts int64) Err {
	for shard, gid := range newConfig.Shards {
		oldGID := oldConfig.Shards[shard]
		if gid == oldGID {
			// do nothing
		} else if gid != kv.gid && oldGID != kv.gid {
			// change shard but me is not involved
		} else if gid == kv.gid {
			// not me => me
			if oldGID == 0 {
				kv.mu.Lock()
				kv.shards[shard] = gid // update shard state so this can be exposed
				kv.mu.Unlock()
			} else {
				kv.mu.Lock()
				if kv.putShardConfigNums[shard] < newConfig.Num {
					kv.dprintf("update config from %v to %v blocking, wait for putShard for %v", oldConfig.Num, newConfig.Num, shard)
					kv.shards[shard] = ShardWaitForPut
					kv.waitShardPutLocked(shard)
					kv.mu.Unlock()
				} else {
					// the shard is already put to me, so I don't have to wait
					kv.shards[shard] = gid
					kv.mu.Unlock()
				}
			}
		} else {
			// me => not me
			// the shard must already been put in previous config change
			// (it's blocking op, only we own the shard, then config change can proceed)
			servers := newConfig.Groups[gid]
			if servers == nil {
				servers = oldConfig.Groups[gid]
			}
			kv.dprintf("freeze shard %v and move it to %v in config %v => %v", shard, gid, oldConfig.Num, newConfig.Num)
			// make sure freeze shard and put/append operation happen in same order across raft replica
			// or different replica might see different behaviors
			if err := kv.waitShardFreeze(shard, oldGID, oldConfig.Num); err != OK {
				// either config changed or we are dead
				// either way, we restart config change process
				kv.dprintf("cannot freeze shard %v, err %v", shard, err)
				return err
			}

			// the move shard id is also replicated by raft to make sure only leader can change it monotonicly
			// let raft control the move shard id
			err := kv.retryUntilCommit(NewMoveShardOp(shard, gid, servers, newConfig.Num, -1, -1), -1)
			if err != OK {
				return err
			}
			kv.mu.Lock()
			data := kv.states[shard] // after freeze, the data won't be modified
			opId := kv.moveShardId   // use the replicated id since it increment monotonicly
			maxOpIdForClerk := kv.maxOpIdForClerks[shard]
			kv.mu.Unlock()
			kv.moveShard(newConfig.Num, shard, data, servers, opId, maxOpIdForClerk)
		}
	}
	// the shard movement is done, now we commit the change
	// it's ok if we failed to commit before crash, we will do this again without any issue
	err := kv.retryUntilCommit(NewUpdateConfigOp(oldConfig, newConfig, ts, kv.clerkId, kv.opId.Add(1)), -1)
	if err != OK && err != ErrWrongLeader {
		kv.dprintf("failed to update config after shard movement, err %v", err)
	}
	return err
}

func (kv *ShardKV) waitShardFreeze(shard int, oldGID int, configNum int) Err {
	for !kv.dead.Load() {
		kv.mu.Lock()
		if kv.shards[shard] == ShardFreeze {
			kv.mu.Unlock()
			return OK
		}
		if kv.shards[shard] != oldGID || kv.config.Num != configNum {
			// states change, we must be no leader
			kv.mu.Unlock()
			return ErrWrongLeader
		}
		idx, _, ok := kv.rf.Start(*NewUpdateShardStateOp(shard, ShardFreeze, kv.clerkId, kv.opId.Add(1)))
		if !ok {
			// wait for leader sync the op to me
			kv.waitForCondWithTimeout(func() bool {
				return kv.shards[shard] == ShardFreeze || kv.shards[shard] != oldGID || kv.config.Num != configNum
			}, kv.shardStateUpdateCond, time.Millisecond*100)
			// if timeout, might be no leader we will try to start again
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()
		_ = kv.waitForCommandWithTimeout(idx, time.Millisecond*100)
	}
	if kv.dead.Load() {
		return ErrDead
	}
	return OK
}

func (kv *ShardKV) retryUntilCommit(op *Op, shard int) Err {
	for {
		kv.mu.Lock()
		if shard >= 0 {
			if !kv.shardMatchLocked(shard) {
				kv.mu.Unlock()
				return ErrWrongGroup
			}
			kv.dprintf("check shard %v match done", shard)
		}
		idx, _, ok := kv.rf.Start(*op) // use struct instead of pointer because pointer will be flattened during rpc call
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

func (kv *ShardKV) waitForCondWithTimeout(predicate func() bool, cond *sync.Cond, timeout time.Duration) Err {
	var isTimeout atomic.Bool
	ctx, stopf1 := context.WithTimeout(context.Background(), timeout)
	defer stopf1()
	stopf := context.AfterFunc(ctx, func() {
		isTimeout.Store(true)
		cond.Broadcast()
	})
	defer stopf()
	for !predicate() && !kv.dead.Load() && !isTimeout.Load() {
		cond.Wait()
	}
	if kv.dead.Load() {
		return ErrDead
	}
	if isTimeout.Load() {
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
		kv.updateConfigRequests = 0
		kv.mu.Unlock()

		kv.updateConfig()

		kv.updateConfigCond.Broadcast()
	}
}

func (kv *ShardKV) updateConfig() {
	config := kv.queryConfig(-1)

	kv.mu.Lock()
	currConfig := kv.config
	ts := time.Now().UnixMilli()
	kv.mu.Unlock()

	nextConfigNum := currConfig.Num + 1
	if nextConfigNum > config.Num {
		// we are already up to date
		// but we still need to update time so we won't be triggered quickly
		kv.retryUntilCommit(NewUpdateConfigTimeOp(ts), -1)
		return
	}

	// update config 1 by 1, blockingly
	for ; nextConfigNum <= config.Num; nextConfigNum++ {
		newConfig := kv.queryConfig(nextConfigNum)
		err := kv.update1Config(currConfig, newConfig, ts)
		if err != OK {
			if err != ErrWrongLeader {
				kv.dprintf("failed to update config from %v to %v, err %v", currConfig.Num, nextConfigNum, err)
			}
			// we can safely skip this and wait for another another update config trigger for follower nodes
			return
		}
		currConfig = newConfig
	}
}

func (kv *ShardKV) queryConfig(num int) *shardctrler.Config {
	if num >= 0 {
		kv.mu.Lock()
		if kv.configs[num] != nil {
			kv.mu.Unlock()
			return kv.configs[num]
		}
		kv.mu.Unlock()
	}

	args := &shardctrler.QueryArgs{
		Num: num,

		ClerkId: kv.clerkId,
		OpId:    kv.opId.Add(1), // this opid can be anything, since the server will always return latest
	}
	for {
		// try each known server.
		for _, srv := range kv.ctrlers {
			var reply shardctrler.QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK && reply.WrongLeader == false {
				kv.mu.Lock()
				kv.configs[reply.Config.Num] = &reply.Config
				kv.mu.Unlock()
				return &reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// trigger update config periodly, or the config won't update in the leader
// if the client always reach out to follower
func (kv *ShardKV) goTriggerUpdateConfig() {
	for !kv.dead.Load() {
		kv.mu.Lock()
		kv.updateConfigRequests++
		kv.triggerUpdateConfigCond.Signal()
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
