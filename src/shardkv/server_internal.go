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

	OpPutShard = iota + 12
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
				// snapshot contains less information than latestAppliedLog, we do nothing
				//kv.dprintf("Ignore snapshot endIdx %v, latest applied idx %v", msg.SnapshotIndex, kv.latestAppliedCmdIdx)
			}
			kv.mu.Unlock()
		}
		if err != nil {
			panic(err)
		}
	}
}

func (kv *ShardKV) applyDataOpLocked(op *Op) Err {
	detail := op.Data.(UpdateKeyOpData)
	shard := key2shard(detail.Key)
	if kv.shards[shard] == ShardFreeze {
		return ErrWrongGroup
	}
	state := kv.shardData[shard]
	if state == nil {
		state = make(map[string]string)
		kv.shardData[shard] = state
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
		state[detail.Key] = detail.Value
	case OpAppend:
		state[detail.Key] += detail.Value
	case OpGet:
	}
	return OK
}

func (kv *ShardKV) applyOpLocked(op *Op) Err {
	// only op from client needs to be dedupped
	// other op are from servers, and won't cause duplicate
	if op.OpType == OpPut || op.OpType == OpAppend || op.OpType == OpGet {
		return kv.applyDataOpLocked(op)
	}

	kv.dprintf("apply op %+v", op)

	switch op.OpType {
	case OpPutShard:
		// might receive from multiple replica
		detail := op.Data.(PutShardOpData)
		if detail.ConfigNum > kv.putShardConfigNums[op.Shard] {
			kv.putShardConfigNums[op.Shard] = detail.ConfigNum
			kv.shardData[op.Shard] = shardctrler.CopyMap(detail.ShardData)
			kv.maxOpIdForClerks[op.Shard] = shardctrler.CopyMap(detail.MaxOpIdForClerk)
			if kv.shards[op.Shard] == ShardWaitForPut {
				kv.shards[op.Shard] = kv.gid
				kv.shardStateUpdateCond.Broadcast()
			}
		} else {
			kv.dprintf("ignore putShard in configNum %v, seen configNum %v", detail.ConfigNum, kv.putShardConfigNums[op.Shard])
		}
	case OpUpdateShardState:
		detail := op.Data.(UpdateShardStateOpData)
		kv.shards[op.Shard] = detail.GID
		kv.shardStateUpdateCond.Broadcast()
	case OpUpdateConfigTime:
		detail := op.Data.(UpdateConfigTimeOpData)
		kv.updateConfigTimes[op.Shard] = detail.Timestamp
		kv.updateConfigRequests[op.Shard] = 0
		kv.updateConfigDoneCond.Broadcast()
	case OpUpdateConfig:
		detail := op.Data.(UpdateConfigOpData)
		kv.configNums[op.Shard] = detail.NewConfigNum
		kv.updateConfigTimes[op.Shard] = detail.Timestamp
		kv.shards[op.Shard] = detail.NewGID
		// config is updated successfully, do the gabage collection
		if kv.shards[op.Shard] != kv.gid && kv.putShardConfigNums[op.Shard] < detail.NewConfigNum {
			kv.shardData[op.Shard] = nil
		}
		// since we update the config, reset the request
		kv.updateConfigRequests[op.Shard] = 0
		kv.updateConfigDoneCond.Broadcast()
	}
	return OK
}

func (kv *ShardKV) update1ConfigForShard(shard int, oldConfig *shardctrler.Config, newConfig *shardctrler.Config, ts int64) Err {
	gid := newConfig.Shards[shard]
	oldGID := oldConfig.Shards[shard]
	kv.dprintf("try to update config for shard %v from %v (%v) => %v (%v)",
		shard, oldConfig.Num, oldGID, newConfig.Num, gid)
	if gid == oldGID {
		// do nothing
	} else if gid != kv.gid && oldGID != kv.gid {
		// change shard but me is not involved
	} else if gid == kv.gid {
		// not me => me
		if oldGID == 0 {
			// from 0 to me, no shard movement
		} else {
			kv.mu.Lock()
			if kv.putShardConfigNums[shard] < newConfig.Num {
				kv.dprintf("update config from %v to %v blocking, wait for putShard for %v", oldConfig.Num, newConfig.Num, shard)
				kv.shards[shard] = ShardWaitForPut
				kv.waitShardPutLocked(shard, time.Minute*60) // it's ok for us to block forever
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

		kv.mu.Lock()
		data := shardctrler.CopyMap(kv.shardData[shard]) // after freeze, the data might be modified by read snapshot
		maxOpIdForClerk := shardctrler.CopyMap(kv.maxOpIdForClerks[shard])
		kv.mu.Unlock()

		kv.moveShard(newConfig.Num, shard, data, servers, maxOpIdForClerk)
	}
	kv.dprintf("success to move shard %v from %v (%v) => %v (%v), now commit the config change for the shard",
		shard, oldConfig.Num, oldGID, newConfig.Num, gid)
	// the shard movement is done, now we commit the change
	// it's ok if we failed to commit before crash, we will do this again without any issue
	op := NewUpdateConfigOp(shard, newConfig.Num, gid, ts, kv.clerkId, kv.opId.Add(1))
	err := kv.retryUntilCommit(op, -1, time.Second*5) // we can block here forever, instead of start another one
	if err != OK && err != ErrWrongLeader {
		kv.dprintf("failed to update config for shard %v after shard movement, err %v", shard, err)
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
		if kv.shards[shard] != oldGID || kv.configNums[shard] != configNum {
			// shardData change, we must be no leader
			kv.mu.Unlock()
			return ErrWrongLeader
		}
		idx, _, ok := kv.rf.Start(*NewUpdateShardStateOp(shard, ShardFreeze, kv.clerkId, kv.opId.Add(1)))
		if !ok {
			// wait for leader sync the op to me
			kv.waitForCondWithTimeout(func() bool {
				return kv.shards[shard] == ShardFreeze || kv.shards[shard] != oldGID || kv.configNums[shard] != configNum
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

func (kv *ShardKV) retryUntilCommit(op *Op, shard int, timeout time.Duration) Err {
	for {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			// fail fast, even though there's no leader now, the client can retry until leader found
			return ErrWrongLeader
		}
		kv.mu.Lock()
		if shard >= 0 {
			kv.dprintf("check shard %v match for op %+v", shard, op)
			if err := kv.shardMatchLocked(shard, timeout); err != OK {
				kv.mu.Unlock()
				return err
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
		err := kv.waitForCommandWithTimeout(idx, timeout)
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

func (kv *ShardKV) waitForCommandWithTimeout(idx int, timeout time.Duration) Err {
	var isTimeout atomic.Bool
	ctx, stopf1 := context.WithTimeout(context.Background(), timeout)
	defer stopf1()
	stopf := context.AfterFunc(ctx, func() {
		isTimeout.Store(true)
		kv.stateUpdateCond.Broadcast()
	})
	defer stopf()
	// wait for applyCh send the exact same message to us
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.latestAppliedCmdIdx < idx && !kv.dead.Load() && !isTimeout.Load() {
		kv.stateUpdateCond.Wait()
	}
	if kv.dead.Load() {
		return ErrDead
	}
	if isTimeout.Load() {
		return ErrTimeout
	}
	return OK
}

// wait for predictate to be true
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
	latestConfigNum := -1
	for !kv.dead.Load() {
		// optimize so each shard config doesn't need to query -1 to get latest config
		config, err := kv.queryConfig(-1)
		if err != OK {
			kv.dprintf("failed to update config, err %v", err)
			continue
		}
		if config.Num > latestConfigNum {
			kv.dprintf("found new config %v, trigger shard update config", config.Num)
			latestConfigNum = config.Num
			kv.latestConfigNum.Store(int32(latestConfigNum))
		}

		kv.mu.Lock()
		for i := 0; i < len(kv.updateConfigRequests); i++ {
			if kv.configNums[i] < latestConfigNum {
				// we might falsely trigger update config for shard, if it's migration is ongoing
				kv.updateConfigRequests[i]++
			}
		}
		kv.triggerUpdateConfigCond.Broadcast()
		kv.mu.Unlock()

		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) goUpdateConfigForShard(shard int) {
	for !kv.dead.Load() {
		kv.mu.Lock()
		for kv.updateConfigRequests[shard] == 0 && !kv.dead.Load() {
			kv.triggerUpdateConfigCond.Wait()
		}
		if kv.dead.Load() {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		err := kv.updateConfigForShard(shard)
		if err != OK {
			kv.dprintf("failed to update config for shard %v, err %v", shard, err)
			time.Sleep(time.Millisecond * 100) // we don't want it to trigger everytime since this won't reset request
		} else {
			kv.dprintf("success to update config for shard %v", shard)
		}
	}
}

func (kv *ShardKV) updateConfigForShard(shard int) Err {
	kv.dprintf("update config for shard %v start", shard)
	kv.mu.Lock()
	currConfigNum := kv.configNums[shard]
	kv.mu.Unlock()

	currConfig, err := kv.queryConfig(currConfigNum)
	if err != OK {
		return err
	}

	config, err := kv.queryConfig(int(kv.latestConfigNum.Load()))
	if err != OK {
		return err
	}
	ts := time.Now().UnixMilli()
	kv.dprintf("update config for shard %v, currConfig %v, latest config %v", shard, currConfigNum, config.Num)

	if currConfigNum >= config.Num {
		// we are already up to date
		// but we still need to update time so we won't be triggered quickly
		err := kv.retryUntilCommit(NewUpdateConfigTimeOp(shard, ts), -1, time.Second*5)
		if err != OK && err != ErrWrongLeader {
			kv.dprintf("update config time for shard %v failed, err %v", shard, err)
			// it's ok to ignore this, and we will be triggered next time
		}
		return err
	}

	// update config 1 by 1, blockingly
	for ; currConfigNum < config.Num; currConfigNum++ {
		nextConfigNum := currConfigNum + 1
		kv.dprintf("try to update config from %v to %v for shard %v", currConfigNum, nextConfigNum, shard)
		newConfig, err := kv.queryConfig(nextConfigNum)
		if err != OK {
			return err
		}
		err = kv.update1ConfigForShard(shard, currConfig, newConfig, ts)
		if err != OK {
			if err != ErrWrongLeader {
				kv.dprintf("failed to update config for shard %v from %v to %v, err %v", shard, currConfig.Num, nextConfigNum, err)
			}
			// we can safely skip this and wait for another update config trigger for follower nodes
			return err
		}
		kv.dprintf("success to update config for shard %v from %v to %v", shard, currConfig.Num, nextConfigNum)
		currConfig = newConfig
	}
	return OK
}

func (kv *ShardKV) queryConfig(num int) (*shardctrler.Config, Err) {
	args := &shardctrler.QueryArgs{
		Num: num,

		ClerkId: kv.clerkId,
		OpId:    kv.opId.Add(1), // this opid can be anything, since the server will always return latest
	}
	for {
		if num >= 0 {
			kv.mu.Lock()
			if config := kv.configCache[num]; config.Num == num {
				kv.mu.Unlock()
				return &config, OK
			}
			kv.mu.Unlock()
		}

		// try each known server.
		for _, srv := range kv.ctrlers {
			if kv.dead.Load() {
				return nil, ErrDead
			}
			var reply shardctrler.QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK && reply.WrongLeader == false {
				kv.mu.Lock()
				kv.configCache[reply.Config.Num] = reply.Config
				kv.mu.Unlock()
				return &reply.Config, OK
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
