package kvraft

import (
	"../labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.identity = nrand()
	ck.sequenceNumber = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ok := false
	i := ck.lastLeader - 1
	reply := GetReply{}
	args := GetArgs{key, ck.identity, ck.sequenceNumber}
	ck.sequenceNumber += 1

	for !ok || reply.Err == ErrWrongLeader {
		i = (i + 1) % len(ck.servers)
		reply = GetReply{}
		ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
	}
	DPrintf("Clerk send GET command %v to %d and get reply", args, i)
	ck.lastLeader = i

	if reply.Err == OK {
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ok := false
	i := ck.lastLeader - 1
	reply := PutAppendReply{}
	args := PutAppendArgs{key, value, op, ck.identity, ck.sequenceNumber}
	ck.sequenceNumber += 1

	for !ok || reply.Err == ErrWrongLeader {
		i = (i + 1) % len(ck.servers)
		reply = PutAppendReply{}
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
	}
	DPrintf("Clerk send PUT or APPEND command %v to server %d and get reply %v", args, i, reply)
	ck.lastLeader = i
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
