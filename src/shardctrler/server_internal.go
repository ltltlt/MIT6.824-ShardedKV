package shardctrler

import (
	"context"
	"slices"
	"sort"
	"time"
)

const (
	OpJoin int8 = iota
	OpLeave
	OpMove
	OpQuery
)

func NewJoinOp(args *JoinArgs) *Op {
	return &Op{
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
		Servers: args.Servers,
		OpType:  OpJoin,
	}
}

func NewLeaveOp(args *LeaveArgs) *Op {
	return &Op{
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
		GIDs:    args.GIDs,
		OpType:  OpLeave,
	}
}

func NewMoveOp(args *MoveArgs) *Op {
	return &Op{
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
		Shard:   args.Shard,
		GID:     args.GID,
		OpType:  OpMove,
	}
}

func NewQueryOp(args *QueryArgs) *Op {
	return &Op{
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
		Num:     args.Num,
		OpType:  OpQuery,
	}
}

func (sc *ShardCtrler) retryUntilCommit(op *Op) Err {
	for {
		sc.mu.Lock()
		idx, _, ok := sc.rf.Start(*op) // use Op instead of pointer because pointer will be flattened during rpc call
		if !ok {
			sc.mu.Unlock()
			return ErrWrongLeader
		}
		sc.neededCommandIdxes[idx] = struct{}{}
		sc.mu.Unlock()
		err := sc.waitForCommandWithTimeout(idx, time.Millisecond*300)
		if err != OK {
			return err
		}
		// check whether the command is the same as ours
		sc.mu.Lock()
		// it's impossible for 1 server, two starts with same index
		// consider node 1 think it's leader and start command at 5
		// then node 2 won election, it won't remove entries 5 from 1
		// only when it needs to append new entries to node 1
		// in that case, next time 1 won election, it will only start at 6 (2 didn't append or append 1 entry)
		// or higher (2 append more entries)
		delete(sc.neededCommandIdxes, idx)
		op1, ok := sc.commandIdx2Op[idx]
		if ok && op1.ClerkId == op.ClerkId && op1.OpId == op.OpId {
			delete(sc.commandIdx2Op, idx)
			sc.mu.Unlock()
			return OK
		}
		sc.mu.Unlock()
		sc.dprintf("committed op at %v is %v didn't match mine %v, retry", idx, op1, op)
	}
}

func (sc *ShardCtrler) waitForCommandWithTimeout(idx int, maxDuration time.Duration) Err {
	timeout := false
	ctx, stopf1 := context.WithTimeout(context.Background(), maxDuration)
	defer stopf1()
	stopf := context.AfterFunc(ctx, func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		timeout = true
		sc.stateUpdateCond.Broadcast()
	})
	defer stopf()
	// wait for applyCh send the exactly same message to us
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for sc.latestAppliedCmdIdx < idx && !sc.dead.Load() && !timeout {
		sc.stateUpdateCond.Wait()
	}
	if sc.dead.Load() {
		return ErrDead
	}
	if timeout {
		return ErrTimeout
	}
	return OK
}

func (sc *ShardCtrler) goUpdateStateFromApplyCh() {
	for !sc.dead.Load() {
		msg := <-sc.applyCh
		var err error
		if msg.CommandValid {
			sc.mu.Lock()
			// it's possible that we have snapshot larger than commandIdx
			// (receive snapshot from another server)
			if sc.latestAppliedCmdIdx < msg.CommandIndex {
				sc.latestAppliedCmdIdx = msg.CommandIndex
				op := msg.Command.(Op)
				if _, ok := sc.neededCommandIdxes[msg.CommandIndex]; ok {
					sc.commandIdx2Op[msg.CommandIndex] = &op
				}
				sc.applyOpLocked(&op)
				sc.stateUpdateCond.Broadcast()
			}
			sc.mu.Unlock()
		}
		if err != nil {
			panic(err)
		}
	}
}

func (sc *ShardCtrler) applyOpLocked(op *Op) {
	opId := sc.maxOpIdForClerk[op.ClerkId]
	if opId >= op.OpId {
		return // see op again is not considered an error
	}
	sc.maxOpIdForClerk[op.ClerkId] = op.OpId
	switch op.OpType {
	case OpJoin:
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: CopyMap(lastConfig.Groups, op.Servers),
		}
		var newGroups []int
		for g := range op.Servers {
			newGroups = append(newGroups, g)
		}
		slices.Sort(newGroups)
		newConfig.Shards = sc.computeShardOnJoinLocked(newGroups)
		sc.dprintf("apply join op, servers %v, shards %v => %v", op.Servers, lastConfig.Shards, newConfig.Shards)
		sc.configs = append(sc.configs, newConfig)
	case OpLeave:
		lastConfig := sc.configs[len(sc.configs)-1]
		newGroups := CopyMap(lastConfig.Groups)
		for _, gid := range op.GIDs {
			delete(newGroups, gid)
		}
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: newGroups,
		}
		newConfig.Shards = sc.computeShardOnLeaveLocked(op.GIDs)
		sc.dprintf("apply leave op, gids %v, shards %v => %v", op.GIDs, lastConfig.Shards, newConfig.Shards)
		sc.configs = append(sc.configs, newConfig)
	case OpMove:
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: CopyMap(lastConfig.Groups),
			Shards: lastConfig.Shards,
		}
		oldGID := newConfig.Shards[op.Shard]
		newConfig.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, newConfig)
		sc.dprintf("apply move op, shard %v from %v to %v", op.Shard, oldGID, op.GID)
	default:
	}
}

func (sc *ShardCtrler) computeShardOnJoinLocked(newGroups []int) [NShards]int {
	lastConfig := &sc.configs[len(sc.configs)-1]
	currGroups := lastConfig.Groups
	if len(newGroups) == 0 || len(currGroups) >= NShards {
		return lastConfig.Shards
	}

	currGroups2Shards := make(map[int][]int)
	for shard, g := range lastConfig.Shards {
		currGroups2Shards[g] = append(currGroups2Shards[g], shard)
	}

	// remove existing groups
	var newGroups1 []int
	for _, g := range newGroups {
		if _, ok := currGroups2Shards[g]; !ok {
			newGroups1 = append(newGroups1, g)
		}
	}
	newGroups = newGroups1
	if len(newGroups) == 0 {
		return lastConfig.Shards
	}

	if len(currGroups)+len(newGroups) > NShards {
		newGroups = newGroups[:NShards-len(currGroups)]
	}

	// we need to ensure each group size is between minSize and minSize+1
	minSize := NShards / (len(currGroups) + len(newGroups))
	newShards := lastConfig.Shards // this will create a copy
	if len(currGroups) == 0 {
		for i, g := range newGroups {
			for shard := i * minSize; shard < (i+1)*minSize; shard++ {
				newShards[shard] = g
			}
		}
		gi := 0
		// split reminder evenly
		for shard := len(newGroups) * minSize; shard < NShards; shard++ {
			newShards[shard] = newGroups[gi]
			gi++
		}
		return newShards
	}

	var pairs []pair
	for group, shards := range currGroups2Shards {
		pairs = append(pairs, pair{group: group, shards: shards})
	}
	sort.Slice(pairs, func(i, j int) bool {
		shardCount1 := len(pairs[i].shards)
		shardCount2 := len(pairs[j].shards)
		if shardCount2 != shardCount1 {
			return shardCount2 < shardCount1 // reverse
		}
		return pairs[i].group < pairs[j].group
	})
	delta := minSize
	pi := 0
	for i := 0; i < len(newGroups); {
		g := newGroups[i]
		for pi < len(pairs) && len(pairs[pi].shards) <= minSize {
			pi++
		}
		toBeMoved := min(len(pairs[pi].shards)-minSize, delta)
		toBeMovedShards := pairs[pi].shards[:toBeMoved]
		for _, shard := range toBeMovedShards {
			newShards[shard] = g
		}
		pairs[pi].shards = pairs[pi].shards[toBeMoved:]
		delta -= toBeMoved
		if delta == 0 {
			i++
			delta = minSize
		}
	}
	return newShards
}

func (sc *ShardCtrler) computeShardOnLeaveLocked(gids []int) [NShards]int {
	lastConfig := &sc.configs[len(sc.configs)-1]
	newShards := lastConfig.Shards
	if len(gids) == len(lastConfig.Groups) {
		return [NShards]int{}
	}

	currGroups2Shards := make(map[int][]int)
	for shard, g := range lastConfig.Shards {
		currGroups2Shards[g] = append(currGroups2Shards[g], shard)
	}
	// take into account of no sharding groups
	for g := range lastConfig.Groups {
		if _, ok := currGroups2Shards[g]; !ok {
			currGroups2Shards[g] = nil
		}
	}
	var reshards []int
	for _, gid := range gids {
		reshards = append(reshards, currGroups2Shards[gid]...)
		delete(currGroups2Shards, gid)
	}
	slices.Sort(reshards)
	if len(currGroups2Shards) == 0 {
		return [NShards]int{}
	}

	var pairs []pair
	for group, shards := range currGroups2Shards {
		pairs = append(pairs, pair{group: group, shards: shards})
	}
	sort.Slice(pairs, func(i, j int) bool {
		shardCount1 := len(pairs[i].shards)
		shardCount2 := len(pairs[j].shards)
		if shardCount1 != shardCount2 {
			return shardCount1 < shardCount2
		}
		return pairs[i].group < pairs[j].group
	})
	minSize := NShards / len(pairs)
	pi := 0
	si := 0
	for si < len(reshards) && pi < len(pairs) {
		shard := reshards[si]
		currLen := len(pairs[pi].shards)
		if currLen < minSize {
			pairs[pi].shards = append(pairs[pi].shards, shard)
			newShards[shard] = pairs[pi].group
			si++
		} else {
			pi++
		}
	}
	pi = 0
	for ; si < len(reshards); si++ {
		shard := reshards[si]
		pairs[pi].shards = append(pairs[pi].shards, shard)
		newShards[shard] = pairs[pi].group
		pi++
	}
	return newShards
}

type pair struct {
	group  int
	shards []int
}
