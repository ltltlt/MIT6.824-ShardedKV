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
	kv.rf.Snapshot(kv.latestAppliedCmdIdx, buffer.Bytes())
	return nil
}

func (kv *ShardKV) readSnapshotLocked(snapshot []byte) error {
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	if err := decoder.Decode(&kv.latestAppliedCmdIdx); err != nil {
		return err
	}
	for i := 0; i < shardctrler.NShards; i++ {
		// gob won't overwrite existing slot if the encoded slot value is nil
		// decode encoded value [nil, m1, m2] in place of [m0, nil, nil] will get [m0, m1, m2]
		// this will cause challenge 1 (remove shard when not needed) failed because of snapshot size is too large
		kv.shardData[i] = nil
		kv.maxOpIdForClerks[i] = nil
		kv.shards[i] = 0 // not necessary, but make this explicitly
		kv.putShardConfigNums[i] = 0
		kv.configNums[i] = 0
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
	if kv.updateConfigDoneCond != nil {
		kv.updateConfigDoneCond.Broadcast()
	}
	// in case follower is waiting for putShard, and the leader send snapshot that already contain this shard
	if kv.shardStateUpdateCond != nil {
		kv.shardStateUpdateCond.Broadcast()
	}
	return nil
}
