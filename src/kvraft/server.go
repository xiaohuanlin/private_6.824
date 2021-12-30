package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	SerialNumber int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// store k-v
	dict map[string]string
	// store the channel for responseing to client
	rp map[int64]chan GeneralReply
	// store the operations of different client
	clientOperations map[int64] GeneralReply
	// max commit id
	maxCommit int64
}

func (kv *KVServer) GetInternal(args *GetArgs, reply *GeneralReply) {
	value, ok := kv.dict[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	//DPrintf("Kvserver[%d] get k: %s v: %s", kv.me, args.Key, value)
	reply.Value = value
	reply.Err = OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if rep, ok := kv.clientOperations[args.SerialNumber]; ok {
		reply.Err = rep.Err
		reply.Value = rep.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// initial channel first
	kv.mu.Lock()
	if _, ok := kv.rp[args.SerialNumber]; !ok {
		kv.rp[args.SerialNumber] = make(chan GeneralReply, 1)
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{GetOp, args.Key, "", args.SerialNumber})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Kvserver[%d] receive command %v commit index: %d", kv.me, args, index)

	kv.mu.Lock()
	channel := kv.rp[args.SerialNumber]
	kv.mu.Unlock()
	select {
	case rep := <- channel:
		DPrintf("Kvserver[%d] command %v return result %s", kv.me, args, rep.Err)
		reply.Err = rep.Err
		reply.Value = rep.Value

		kv.mu.Lock()
		delete(kv.rp, args.SerialNumber)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppendInternal(args *PutAppendArgs, reply *GeneralReply) {
	if args.Op == PutOp {
		kv.dict[args.Key] = args.Value
	} else if args.Op == AppendOp {
		kv.dict[args.Key] += args.Value
	}
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if rep, ok := kv.clientOperations[args.SerialNumber]; ok {
		reply.Err = rep.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// initial channel first
	kv.mu.Lock()
	if _, ok := kv.rp[args.SerialNumber]; !ok {
		kv.rp[args.SerialNumber] = make(chan GeneralReply, 1)
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.SerialNumber})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Kvserver[%d] receive command %v commit index: %d", kv.me, args, index)

	kv.mu.Lock()
	channel := kv.rp[args.SerialNumber]
	kv.mu.Unlock()
	select {
	case rep := <- channel:
		DPrintf("Kvserver[%d] command %v return result %v", kv.me, args, rep)
		reply.Err = rep.Err

		kv.mu.Lock()
		delete(kv.rp, args.SerialNumber)
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dict = make(map[string]string)
	kv.rp = make(map[int64]chan GeneralReply)
	kv.clientOperations = make(map[int64] GeneralReply)
	kv.maxCommit = -1
	go func() {
		for m := range kv.applyCh {
			if kv.killed() {
				return
			}
			command := m.Command.(Op)
			if m.CommandValid == false {
				// commit fail
				kv.mu.Lock()
				if _, ok := kv.rp[command.SerialNumber]; ok {
					kv.rp[command.SerialNumber] <- GeneralReply{Err: ErrWrongLeader, Value: ""}
				}
				kv.mu.Unlock()
				continue
			}
			if int64(m.CommandIndex) <= atomic.LoadInt64(&kv.maxCommit) {
				continue
			}
			atomic.StoreInt64(&kv.maxCommit, int64(m.CommandIndex))

			DPrintf("Kvserver[%d] receive reply index: %d command: %v", me, m.CommandIndex, command)

			kv.mu.Lock()
			if _, ok := kv.clientOperations[command.SerialNumber]; ok {
				kv.mu.Unlock()
				continue
			}

			var replyForChan GeneralReply
			switch command.Type {
			case PutOp:
				args := PutAppendArgs{command.Key, command.Value, PutOp, command.SerialNumber}
				kv.PutAppendInternal(&args, &replyForChan)
			case AppendOp:
				args := PutAppendArgs{command.Key, command.Value, AppendOp, command.SerialNumber}
				kv.PutAppendInternal(&args, &replyForChan)
			case GetOp:
				args := GetArgs{command.Key, command.SerialNumber}
				kv.GetInternal(&args, &replyForChan)
			}
			kv.clientOperations[command.SerialNumber] = replyForChan

			if _, ok := kv.rp[command.SerialNumber]; ok {
				kv.rp[command.SerialNumber] <- replyForChan
				DPrintf("Kvserver[%d] rp exist %d", me, m.CommandIndex)
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
