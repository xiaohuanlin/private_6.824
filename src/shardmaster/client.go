package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader     int
	identity       int64
	sequenceNumber int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.lastLeader = 0
	ck.identity = nrand()
	ck.sequenceNumber = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Identity = ck.identity
	args.SerialNumber = ck.sequenceNumber
	ck.sequenceNumber++

	DPrintf("Clerk send query command %v", args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("Clerk get query command result %v", reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Identity = ck.identity
	args.SerialNumber = ck.sequenceNumber
	ck.sequenceNumber++

	DPrintf("Clerk send join command %v", args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("Clerk get join command %v", args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Identity = ck.identity
	args.SerialNumber = ck.sequenceNumber
	ck.sequenceNumber++

	DPrintf("Clerk send leave command %v", args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("Clerk get leave command %v", args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Identity = ck.identity
	args.SerialNumber = ck.sequenceNumber
	ck.sequenceNumber++

	DPrintf("Clerk send move command %v", args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("Clerk get move command %v", args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
