package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Type         string
	Key          string
	Value        string
	Identity     int64
	SerialNumber int64
}

type CacheEntry struct {
	SerialNumber int64
	Res          GeneralReply
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
	// store the channel for responding to clients
	rp map[int64]chan GeneralReply
	// store the operations of different clients
	opCache map[int64]CacheEntry
	// max commit id
	maxApply      int64
	snapshotTimer *raft.Timer
}

func (kv *KVServer) getInternal(args *GetArgs, reply *GeneralReply) {
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
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		if c, ok := kv.rp[args.Identity]; ok {
			close(c)
			delete(kv.rp, args.Identity)
		}
		return
	}

	kv.mu.Lock()
	if cache, ok := kv.opCache[args.Identity]; ok {
		if cache.SerialNumber >= args.SerialNumber {
			res := kv.opCache[args.Identity].Res
			reply.Err = res.Err
			reply.Value = res.Value
			DPrintf("Kvserver[%d] get from cache %v", kv.me, args)
			kv.mu.Unlock()
			return
		}
	}

	// initial channel first
	if _, ok := kv.rp[args.Identity]; !ok {
		kv.rp[args.Identity] = make(chan GeneralReply, 1)
	}
	channel := kv.rp[args.Identity]
	kv.mu.Unlock()
	go func() {
		index, _, isLeader := kv.rf.Start(Op{GetOp, args.Key, "", args.Identity, args.SerialNumber})
		if !isLeader {
			kv.mu.Lock()
			if c, ok := kv.rp[args.Identity]; ok {
				c <- GeneralReply{ErrWrongLeader, "", args.SerialNumber}
			}
			kv.mu.Unlock()
			return
		}
		DPrintf("Kvserver[%d] receive command %v commit index: %d", kv.me, args, index)
	}()

	for {
		select {
		case rep, ok := <-channel:
			if !ok {
				rep.Err = ErrWrongLeader
				return
			}
			DPrintf("Kvserver[%d] command %v return result %s sn %d", kv.me, args, rep.Err, rep.SerialNumber)
			if rep.SerialNumber == args.SerialNumber {
				reply.Err = rep.Err
				reply.Value = rep.Value
				kv.mu.Lock()
				if rep.Err == ErrWrongLeader {
					close(channel)
					delete(kv.rp, args.Identity)
				}
				kv.mu.Unlock()
				return
			}
		}
	}
}

func (kv *KVServer) putAppendInternal(args *PutAppendArgs, reply *GeneralReply) {
	if args.Op == PutOp {
		kv.dict[args.Key] = args.Value
	} else if args.Op == AppendOp {
		kv.dict[args.Key] += args.Value
	}
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		if c, ok := kv.rp[args.Identity]; ok {
			close(c)
			delete(kv.rp, args.Identity)
		}
		return
	}

	kv.mu.Lock()
	if cache, ok := kv.opCache[args.Identity]; ok {
		if cache.SerialNumber >= args.SerialNumber {
			res := kv.opCache[args.Identity].Res
			reply.Err = res.Err
			DPrintf("Kvserver[%d] get from cache %v", kv.me, args)
			kv.mu.Unlock()
			return
		}
	}

	// initial channel first
	if _, ok := kv.rp[args.Identity]; !ok {
		kv.rp[args.Identity] = make(chan GeneralReply, 1)
	}
	channel := kv.rp[args.Identity]
	kv.mu.Unlock()

	go func() {
		index, _, isLeader := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.Identity, args.SerialNumber})
		if !isLeader {
			kv.mu.Lock()
			if c, ok := kv.rp[args.Identity]; ok {
				c <- GeneralReply{ErrWrongLeader, "", args.SerialNumber}
			}
			kv.mu.Unlock()
			return
		}
		DPrintf("Kvserver[%d] receive command %v commit index: %d", kv.me, args, index)
	}()

	for {
		select {
		case rep, ok := <-channel:
			if !ok {
				rep.Err = ErrWrongLeader
				return
			}
			DPrintf("Kvserver[%d] command %v return result %s sn %d", kv.me, args, rep.Err, rep.SerialNumber)
			if rep.SerialNumber == args.SerialNumber {
				reply.Err = rep.Err
				kv.mu.Lock()
				if rep.Err == ErrWrongLeader {
					close(channel)
					delete(kv.rp, args.Identity)
				}
				kv.mu.Unlock()
				return
			}
		}
	}
}

// Create snapshot
func (kv *KVServer) createSnapshot() (int, []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.dict)
	e.Encode(kv.opCache)
	e.Encode(kv.maxApply)
	return int(kv.maxApply), w.Bytes()
}

// Read snapshot and rebuild the dict
func (kv *KVServer) readSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var dict map[string]string
	var opCache map[int64]CacheEntry
	var maxApply int64

	if d.Decode(&dict) != nil ||
		d.Decode(&opCache) != nil ||
		d.Decode(&maxApply) != nil {
		//error
		DPrintf("decode error")
	} else {
		kv.dict = dict
		kv.opCache = opCache
		kv.maxApply = maxApply
	}
	DPrintf("Kvserver[%d] read max apply %d", kv.me, kv.maxApply)
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.snapshotTimer.Cancel()
	for _, v := range kv.rp {
		close(v)
	}
	kv.rp = make(map[int64]chan GeneralReply)
	DPrintf("Kvserver[%d] has been killed", kv.me)
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
	labgob.Register(CacheEntry{})
	DPrintf("Kvserver[%d] start", me)

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dict = make(map[string]string)
	kv.rp = make(map[int64]chan GeneralReply)
	kv.opCache = make(map[int64]CacheEntry)
	kv.maxApply = -1

	go func() {
		for m := range kv.applyCh {
			if !m.CommandValid {
				if m.MsgType == raft.Snapshot {
					// Read new snapshot
					snapshot := m.Command.([]byte)
					kv.readSnapshot(snapshot)
				} else if m.MsgType == raft.Dead {
					break
				} else if m.MsgType == raft.NoLeader {
					command := m.Command.(Op)

					// commit fail
					kv.mu.Lock()
					DPrintf("Kvserver[%d] reply cancel %d", me, command.Identity)
					if channel, ok := kv.rp[command.Identity]; ok {
						channel <- GeneralReply{ErrWrongLeader, "", command.SerialNumber}
					}
					kv.mu.Unlock()
				}
				continue
			}

			command := m.Command.(Op)
			if int64(m.CommandIndex) <= atomic.LoadInt64(&kv.maxApply) {
				continue
			}
			DPrintf("Kvserver[%d] receive reply index: %d command: %v", me, m.CommandIndex, command)

			kv.mu.Lock()
			atomic.StoreInt64(&kv.maxApply, int64(m.CommandIndex))

			if cache, ok := kv.opCache[command.Identity]; ok {
				if cache.SerialNumber >= command.SerialNumber {
					kv.mu.Unlock()
					continue
				}
			}

			replyForChan := GeneralReply{SerialNumber: command.SerialNumber}
			switch command.Type {
			case PutOp:
				args := PutAppendArgs{command.Key, command.Value, PutOp, command.Identity, command.SerialNumber}
				kv.putAppendInternal(&args, &replyForChan)
			case AppendOp:
				args := PutAppendArgs{command.Key, command.Value, AppendOp, command.Identity, command.SerialNumber}
				kv.putAppendInternal(&args, &replyForChan)
			case GetOp:
				args := GetArgs{command.Key, command.Identity, command.SerialNumber}
				kv.getInternal(&args, &replyForChan)
			}
			kv.opCache[command.Identity] = CacheEntry{command.SerialNumber, replyForChan}
			if channel, ok := kv.rp[command.Identity]; ok {
				channel <- replyForChan
				DPrintf("Kvserver[%d] rp exist %d", me, m.CommandIndex)
			}
			kv.mu.Unlock()
		}
	}()

	// check if it is necessary to snapshot
	kv.snapshotTimer = raft.MakeTimer(30*time.Millisecond, func() {
		threshold := kv.maxraftstate * 2 / 3
		if threshold > 0 && persister.RaftStateSize() > threshold {
			// create snapshot
			maxApply, snapshot := kv.createSnapshot()
			if maxApply > 0 {
				DPrintf("Kvserver[%d] save max apply %d", kv.me, maxApply)
				kv.rf.DiscardLogsAndSaveSnapshot(maxApply, snapshot)
			}
		}
	})
	kv.snapshotTimer.Start(false)
	return kv
}
