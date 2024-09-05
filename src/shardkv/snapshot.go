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
	if err := encoder.Encode(kv.state); err != nil {
		return err
	}
	if err := encoder.Encode(kv.maxOpIdForClerk); err != nil {
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
	if err := decoder.Decode(&kv.state); err != nil {
		return err
	}
	if err := decoder.Decode(&kv.maxOpIdForClerk); err != nil {
		return err
	}
	return nil
}
