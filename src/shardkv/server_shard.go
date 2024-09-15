package shardkv

import (
	"math/rand"
	"time"
)

// moveShard move shard from me to servers
// all replica will do the movement, and destination server should be able to dedup by configNum
// need to make sure PutShardData is serial because they share same clerkid, if 2 op happen in the same time
// with OpId 1 and OpId 2, if destination receive 2 before 1, 1 will be ignored
func (kv *ShardKV) moveShard(configNum int, shard int, data map[string]string, servers []string, moveShardOpId int32, maxOpIdForClerk map[int32]int32) {
	// wait for shard to be ready
	req := &PutShardDataArgs{
		Shard:           shard,
		Data:            data,
		ConfigNum:       configNum,
		MaxOpIdForClerk: maxOpIdForClerk,

		ClerkId: int32(kv.gid), // this group act as a unit to move shard
		OpId:    moveShardOpId,
	}
	for {
		for _, server := range servers {
			end := kv.make_end(server)
			var reply PutShardDataReply
			if ok := end.Call("ShardKV.PutShardData", req, &reply); ok {
				if reply.Err == OK {
					kv.dprintf("success to move shard %v, %v from me to %v", shard, data, server)
					return
				}
				if reply.Err != ErrWrongLeader {
					kv.dprintf("failed to move shard %v from me to %v, err %v", shard, server, reply.Err)
				}
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) shardMatch(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shardMatchLocked(shard)
}

func (kv *ShardKV) shardMatchLocked(shard int) bool {
	for time.Now().UnixMilli()-kv.updateConfigTime > 100 {
		kv.updateConfigRequests++
		kv.triggerUpdateConfigCond.Signal()
		kv.updateConfigCond.Wait()
	}

	kv.waitShardPutLocked(shard)
	if kv.shards[shard] != kv.gid {
		kv.dprintf("shard %v is not owned by me, but owned by %v", shard, kv.shards[shard])
		return false
	}
	return true
}

func (kv *ShardKV) waitShardPutLocked(shard int) {
	rnd := rand.Int()
	waitForMigration := false
	for !kv.dead.Load() && kv.shards[shard] == ShardWaitForPut {
		kv.dprintf("%v shard %v is in migration and is not available, wait...", rnd, shard)
		waitForMigration = true
		kv.moveShardCond.Wait()
	}
	if !kv.dead.Load() && waitForMigration {
		kv.dprintf("%v shard %v migration is done", rnd, shard)
	}
}
