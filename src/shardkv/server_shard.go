package shardkv

import (
	"math/rand"
	"time"
)

// putShardToServer move shard from me to servers
// all replica will do the movement, and destination server should be able to dedup by configNum
// need to make sure PutShardData is serial because they share same clerkid, if 2 op happen in the same time
// with OpId 1 and OpId 2, if destination receive 2 before 1, 1 will be ignored
func (kv *ShardKV) putShardToServer(configNum int, shard int, data map[string]string,
	servers []string, maxOpIdForClerk map[int32]int32) Err {
	// wait for shard to be ready
	req := &PutShardDataArgs{
		Shard:           shard,
		Data:            data,
		ConfigNum:       configNum,
		MaxOpIdForClerk: maxOpIdForClerk,

		ClerkId: kv.clerkId,
		OpId:    kv.opId.Add(1),
	}
	for {
		for _, server := range servers {
			kv.dprintf("try to move shard %v from me to %v", shard, server)
			end := kv.make_end(server)
			var reply PutShardDataReply
			if ok := end.Call("ShardKV.PutShardData", req, &reply); ok {
				if reply.Err == OK {
					kv.dprintf("success to move shard %v, %v from me to %v", shard, data, server)
					return OK
				}
				if reply.Err != ErrWrongLeader {
					kv.dprintf("failed to move shard %v from me to %v, err %v", shard, server, reply.Err)
				}
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) shardMatchLocked(shard int, timeout time.Duration) Err {
	if !kv.isConfigUpToDateLocked(shard) {
		if err := kv.waitConfigUpdate(shard, timeout); err != OK {
			return err
		}
	}
	kv.dprintf("config for shard %v is up to date", shard)

	if err := kv.waitShardPutLocked(shard, timeout); err != OK {
		return err
	}
	if kv.shards[shard] != kv.gid {
		kv.dprintf("shard %v is not owned by me, but owned by %v", shard, kv.shards[shard])
		return ErrWrongGroup
	}
	return OK
}

func (kv *ShardKV) waitConfigUpdate(shard int, timeout time.Duration) Err {
	kv.dprintf("config for shard %v is out of date, wait until update", shard)
	kv.updateConfigRequests[shard]++
	kv.triggerUpdateConfigCond.Broadcast()

	return kv.waitForCondWithTimeout(func() bool {
		return kv.isConfigUpToDateLocked(shard)
	}, kv.updateConfigDoneCond, timeout)
}

func (kv *ShardKV) isConfigUpToDateLocked(shard int) bool {
	return time.Now().UnixMilli()-kv.updateConfigTimes[shard] <= 100
}

func (kv *ShardKV) waitShardPutLocked(shard int, timeout time.Duration) Err {
	if kv.shards[shard] != ShardWaitForPut {
		return OK
	}
	rnd := rand.Int()
	kv.dprintf("%v shard %v is in migration and is not available, wait...", rnd, shard)
	err := kv.waitForCondWithTimeout(func() bool {
		return kv.shards[shard] != ShardWaitForPut
	}, kv.shardStateUpdateCond, timeout)
	if err == OK {
		kv.dprintf("%v shard %v migration is done", rnd, shard)
	}
	return err
}
