package shardkv

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType int8

	Shard int

	// used to dedup and make sure uniquely identify an op
	OpId    int32
	ClerkId int32

	Data interface{} // Detailed data
}

type UpdateShardStateOpData struct {
	GID int
}

type PutShardOpData struct {
	ShardData       map[string]string
	ConfigNum       int
	MaxOpIdForClerk map[int32]int32
}

type UpdateKeyOpData struct {
	Key   string
	Value string
}

type UpdateConfigOpData struct {
	NewConfigNum int
	NewGID       int
	UpdateConfigTimeOpData
}

type UpdateConfigTimeOpData struct {
	Timestamp int64
}

func NewGetOp(args *GetArgs, shard int) *Op {
	return &Op{
		OpType:  OpGet,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,

		Data: UpdateKeyOpData{
			Key: args.Key,
		},
	}
}

func NewPutOp(args *PutAppendArgs, shard int) *Op {
	return &Op{
		OpType:  OpPut,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,

		Data: UpdateKeyOpData{
			Key:   args.Key,
			Value: args.Value,
		},
	}
}

func NewAppendOp(args *PutAppendArgs, shard int) *Op {
	return &Op{
		OpType:  OpAppend,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,

		Data: UpdateKeyOpData{
			Key:   args.Key,
			Value: args.Value,
		},
	}
}

func NewPutShardOp(args *PutShardDataArgs) *Op {
	return &Op{
		Shard:  args.Shard,
		OpType: OpPutShard,
		Data: PutShardOpData{
			ShardData:       args.Data,
			MaxOpIdForClerk: args.MaxOpIdForClerk,
			ConfigNum:       args.ConfigNum,
		},

		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
}

func NewUpdateShardStateOp(shard int, newGID int, clerkId int32, opId int32) *Op {
	return &Op{
		Shard:  shard,
		OpType: OpUpdateShardState,

		Data: UpdateShardStateOpData{GID: newGID},

		ClerkId: clerkId,
		OpId:    opId,
	}
}

func NewUpdateConfigOp(shard int, newConfigNum int, newShard int, ts int64, clerkId int32, opId int32) *Op {
	detail := UpdateConfigOpData{
		NewConfigNum:           newConfigNum,
		NewGID:                 newShard,
		UpdateConfigTimeOpData: UpdateConfigTimeOpData{Timestamp: ts},
	}

	return &Op{
		OpType: OpUpdateConfig,
		Shard:  shard,

		Data:    detail,
		ClerkId: clerkId,
		OpId:    opId,
	}
}

func NewUpdateConfigTimeOp(shard int, ts int64) *Op {
	return &Op{
		Shard:  shard,
		OpType: OpUpdateConfigTime,
		Data:   UpdateConfigTimeOpData{Timestamp: ts},
	}
}
