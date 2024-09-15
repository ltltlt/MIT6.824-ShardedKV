package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
)

func (kv *ShardKV) createSnapshotLocked() error {
	buffer := &bytes.Buffer{}
	encoder := labgob.NewEncoder(buffer)
	if err := encoder.Encode(kv.latestAppliedCmdIdx); err != nil {
		return err
	}
	if err := encoder.Encode(kv.states); err != nil {
		return err
	}
	if err := encoder.Encode(kv.maxOpIdForClerks); err != nil {
		return err
	}
	if err := encoder.Encode(kv.moveShardId); err != nil {
		return err
	}
	if err := encoder.Encode(kv.shards); err != nil {
		return err
	}
	if err := encoder.Encode(kv.putShardConfigNums); err != nil {
		return err
	}
	if err := encoder.Encode(*kv.config); err != nil {
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
	if err := decoder.Decode(&kv.states); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.maxOpIdForClerks); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.moveShardId); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.shards); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.putShardConfigNums); err != nil {
		return err
	}
	kv.config = &shardctrler.Config{}
	if err := decoder.Decode(kv.config); err != nil {
		return err
	}
	// in case follower is waiting for putShard, and the leader send snapshot that already contain this shard
	if kv.moveShardCond != nil {
		kv.moveShardCond.Broadcast()
	}
	if kv.shardStateUpdateCond != nil {
		kv.shardStateUpdateCond.Broadcast()
	}
	return nil
}
