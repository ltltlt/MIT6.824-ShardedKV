package shardkv

import (
	"6.5840/labgob"
	"bytes"
)

func (kv *ShardKV) createSnapshotLocked() error {
	buffer := &bytes.Buffer{}
	encoder := labgob.NewEncoder(buffer)
	if err := encoder.Encode(kv.latestAppliedCmdIdx); err != nil {
		return err
	}
	if err := encoder.Encode(kv.shardData); err != nil {
		return err
	}
	if err := encoder.Encode(kv.maxOpIdForClerks); err != nil {
		return err
	}
	if err := encoder.Encode(kv.shards); err != nil {
		return err
	}
	if err := encoder.Encode(kv.putShardConfigNums); err != nil {
		return err
	}
	if err := encoder.Encode(kv.configNums); err != nil {
		return err
	}
	if err := encoder.Encode(kv.updateConfigTimes); err != nil {
		return err
	}
	kv.rf.Snapshot(kv.latestAppliedCmdIdx, buffer.Bytes())
	return nil
}

func (kv *ShardKV) readSnapshotLocked(snapshot []byte) error {
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	if err := decoder.Decode(&kv.latestAppliedCmdIdx); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.shardData); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.maxOpIdForClerks); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.shards); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.putShardConfigNums); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.configNums); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.updateConfigTimes); err != nil {
		return err
	}
	if kv.updateConfigDoneCond != nil {
		kv.updateConfigDoneCond.Broadcast()
	}
	// in case follower is waiting for putShard, and the leader send snapshot that already contain this shard
	if kv.shardStateUpdateCond != nil {
		kv.shardStateUpdateCond.Broadcast()
	}
	kv.dprintf("construct from snapshot, updateConfigTimes %v", kv.updateConfigTimes)
	return nil
}
