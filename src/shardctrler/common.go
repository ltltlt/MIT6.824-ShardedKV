package shardctrler

import (
	"fmt"
	"log"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK    = "OK"
	Debug = true
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ClerkId int32
	OpId    int32
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClerkId int32
	OpId    int32
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClerkId int32
	OpId    int32
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClerkId int32
	OpId    int32
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const (
	ErrWrongLeader = "ErrWrongLeader"
	ErrDead        = "ErrDead"
	ErrTimeout     = "ErrTimeout"
)

func (sc *ShardCtrler) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[s%v]", sc.me) + fmt.Sprintf(format, a...))
	}
	return
}
func (ck *Clerk) dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(fmt.Sprintf("[c%v]", ck.id) + fmt.Sprintf(format, a...))
	}
	return
}

func copyMap[K comparable, V any](maps ...map[K]V) map[K]V {
	newMap := make(map[K]V)
	for _, m := range maps {
		for k, v := range m {
			newMap[k] = v
		}
	}
	return newMap
}
