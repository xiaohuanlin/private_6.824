package shardmaster

import (
	"../raft"
	"bytes"
	"sort"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "../labgob"
import "sync"
import "log"

const Debug = 0
const MaxRaftState = 1000

const (
	Join = "Join"
	Leave = "Leave"
	Move = "Move"
	Query = "Query"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	rp map[int64]chan OpReply
	// store the operations of different clients
	opCache map[int64]CacheEntry
	// max commit id
	maxApply      int64
	snapshotTimer *raft.Timer
}


type Op struct {
	// Your data here.
	Type         string
	Data 		 []byte
	Identity     int64
	SerialNumber int64
}

type OpReply struct {
	WrongLeader 	bool
	Identity     	int64
	SerialNumber 	int64
}

type GeneralReply struct {
	WrongLeader 	bool
	Err         	Err
	Data 			interface{}
	Identity 		int64
	SerialNumber 	int64
}

type CacheEntry struct {
	SerialNumber 	int64
	Res			 	GeneralReply
}

func (sm *ShardMaster) checkAndSend(args Op) bool {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if c, ok := sm.rp[args.Identity]; ok {
			close(c)
			delete(sm.rp, args.Identity)
		}
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	// initial channel first
	if _, ok := sm.rp[args.Identity]; !ok {
		sm.rp[args.Identity] = make(chan OpReply, 1)
	}

	go func() {
		index, _, isLeader := sm.rf.Start(args)
		if !isLeader {
			sm.mu.Lock()
			if c, ok := sm.rp[args.Identity]; ok {
				c <- OpReply{true, args.Identity, args.SerialNumber}
			}
			sm.mu.Unlock()
		}
		DPrintf("SMserver[%d] receive command %v commit index: %d", sm.me, args, index)
	}()
	return true
}

func (sm *ShardMaster) sendToRaft(args Op) bool {
	if !sm.checkAndSend(args) {
		return false
	}

	sm.mu.Lock()
	channel := sm.rp[args.Identity]
	sm.mu.Unlock()
	for {
		select {
		case rep, ok := <-channel:
			if !ok {
				rep.WrongLeader = true
				return false
			}
			DPrintf("SMserver[%d] command %v return result sn %d", sm.me, args, rep.SerialNumber)
			if rep.SerialNumber == args.SerialNumber {
				sm.mu.Lock()
				if rep.WrongLeader {
					close(channel)
					delete(sm.rp, args.Identity)
					sm.mu.Unlock()
					return false
				}
				sm.mu.Unlock()
				return true
			}
		}
	}
}

func (sm *ShardMaster) findInCache(identity int64, serialNumber int64) (GeneralReply, bool) {
	if cache, ok := sm.opCache[identity]; ok {
		if cache.SerialNumber >= serialNumber {
			DPrintf("SMserver[%d] get from cache %d %d", sm.me, identity, serialNumber)
			return sm.opCache[identity].Res, true
		}
	}
	return GeneralReply{}, false
}

func (sm *ShardMaster) maintainShards(groupsCount map[int][]int, freeIndex []int) [NShards]int {
	DPrintf("SMserver[%d] maintain groupcount %v freeindex %v", sm.me, groupsCount, freeIndex)
	shards := [NShards]int {}
	if len(groupsCount) == 0 {
		return shards
	}

	// to make sure it is the same for all replica, sort the input
	var sortedKeys []int
	for k := range groupsCount {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Ints(sortedKeys)

	target := NShards / len(groupsCount)
	remain := NShards % len(groupsCount)

	// free extra space
	for _, gid := range sortedKeys {
		if remain > 0 {
			if len(groupsCount[gid]) >= target + 1 {
				remain--
				freeIndex = append(freeIndex, groupsCount[gid][target+1:]...)
				groupsCount[gid] = groupsCount[gid][:target+1]
			}
		} else {
			if len(groupsCount[gid]) > target {
				freeIndex = append(freeIndex, groupsCount[gid][target:]...)
				groupsCount[gid] = groupsCount[gid][:target]
			}
		}
	}

	// set shard
	for _, gid := range sortedKeys {
		if len(groupsCount[gid]) < target {
			num := target - len(groupsCount[gid])
			groupsCount[gid] = append(groupsCount[gid], freeIndex[:num]...)
			freeIndex = freeIndex[num:]
		}
		for _, v := range groupsCount[gid] {
			shards[v] = gid
		}
	}

	if remain > 0 {
		for _, gid := range sortedKeys {
			if remain <= 0 {
				break
			}
			if len(groupsCount[gid]) == target {
				remain--
				shards[freeIndex[0]] = gid
				freeIndex = freeIndex[1:]
			}
		}
	}

	DPrintf("SMserver[%d] maintain shards %v", sm.me, shards)
	return shards
}

func serializeJoinArgs(args JoinArgs) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}

func unserializeJoinArgs(data []byte) JoinArgs {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	args := JoinArgs{}
	if d.Decode(&args) != nil {
		DPrintf("decode error")
	}
	return args
}

func serializeLeaveArgs(args LeaveArgs) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}

func unserializeLeaveArgs(data []byte) LeaveArgs {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	args := LeaveArgs{}
	if d.Decode(&args) != nil {
		DPrintf("decode error")
	}
	return args
}

func serializeMoveArgs(args MoveArgs) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}

func unserializeMoveArgs(data []byte) MoveArgs {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	args := MoveArgs{}
	if d.Decode(&args) != nil {
		DPrintf("decode error")
	}
	return args
}

func serializeQueryArgs(args QueryArgs) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}

func unserializeQueryArgs(data []byte) QueryArgs {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	args := QueryArgs{}
	if d.Decode(&args) != nil {
		DPrintf("decode error")
	}
	return args
}

func (sm *ShardMaster) joinInternal(args *JoinArgs, reply *JoinReply) {
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		return
	}

	DPrintf("SMserver[%d] do join internal %v", sm.me, args)

	reply.WrongLeader = false
	reply.Err = OK

	//do join
	lastConfig := sm.configs[len(sm.configs) - 1]

	// new groups
	groups := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		groups[k] = v
	}
	for k, v := range args.Servers {
		groups[k] = v
	}

	// new shards
	groupsCount := make(map[int][]int)
	var freeIndex []int
	for i, gid := range lastConfig.Shards {
		if gid == 0 {
			freeIndex = append(freeIndex, i)
			continue
		}
		if _, ok := groupsCount[gid]; !ok {
			groupsCount[gid] = []int {}
		}
		groupsCount[gid] = append(groupsCount[gid], i)
	}
	for gid := range groups {
		if _, ok := groupsCount[gid]; !ok {
			groupsCount[gid] = []int {}
		}
	}
	shards := sm.maintainShards(groupsCount, freeIndex)

	config := Config {len(sm.configs), shards, groups}
	DPrintf("SMserver[%d] join a new config %v", sm.me, config)
	sm.configs = append(sm.configs, config)
	sm.opCache[args.Identity] = CacheEntry{args.SerialNumber, GeneralReply{false, OK, nil, args.Identity, args.SerialNumber}}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op {Join, serializeJoinArgs(*args), args.Identity, args.SerialNumber}

	sm.mu.Lock()
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	DPrintf("SMserver[%d] start join command %v", sm.me, args)

	if !sm.sendToRaft(op) {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.joinInternal(args, reply)
}

func (sm *ShardMaster) leaveInternal(args *LeaveArgs, reply *LeaveReply) {
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		return
	}
	DPrintf("SMserver[%d] do leave internal command %v", sm.me, args)

	reply.WrongLeader = false
	reply.Err = OK
	//do leave
	lastConfig := sm.configs[len(sm.configs) - 1]

	// new groups
	groups := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		groups[k] = v
	}
	for _, gid := range args.GIDs {
		delete(groups, gid)
	}

	// new shards
	groupsCount := make(map[int][]int)
	var freeIndex []int
	for i, gid := range lastConfig.Shards {
		if gid == 0 {
			freeIndex = append(freeIndex, i)
			continue
		}
		if _, ok := groupsCount[gid]; !ok {
			groupsCount[gid] = []int {}
		}
		groupsCount[gid] = append(groupsCount[gid], i)
	}
	for gid := range groups {
		if _, ok := groupsCount[gid]; !ok {
			groupsCount[gid] = []int {}
		}
	}
	for _, gid := range args.GIDs {
		freeIndex = append(freeIndex, groupsCount[gid]...)
		delete(groupsCount, gid)
	}

	shards := sm.maintainShards(groupsCount, freeIndex)
	config := Config {len(sm.configs), shards, groups}
	DPrintf("SMserver[%d] move a new config %v", sm.me, config)
	sm.configs = append(sm.configs, config)
	sm.opCache[args.Identity] = CacheEntry{args.SerialNumber, GeneralReply{false, OK, nil, args.Identity, args.SerialNumber}}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op {Leave, serializeLeaveArgs(*args), args.Identity, args.SerialNumber}

	sm.mu.Lock()
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	DPrintf("SMserver[%d] start leave command %v", sm.me, args)

	if !sm.sendToRaft(op) {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.leaveInternal(args, reply)
}

func (sm *ShardMaster) moveInternal(args *MoveArgs, reply *MoveReply) {
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		return
	}
	DPrintf("SMserver[%d] do move internal command %v", sm.me, args)

	reply.WrongLeader = false
	reply.Err = OK
	//do move
	lastConfig := sm.configs[len(sm.configs)-1]

	// new groups
	groups := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		groups[k] = v
	}

	// new shards
	shards := [NShards]int {}
	for i, shard := range lastConfig.Shards {
		shards[i] = shard
	}
	shards[args.Shard] = args.GID

	config := Config {len(sm.configs), shards, groups}
	DPrintf("SMserver[%d] move a new config %v", sm.me, config)
	sm.configs = append(sm.configs, config)
	sm.opCache[args.Identity] = CacheEntry{args.SerialNumber, GeneralReply{false, OK, nil, args.Identity, args.SerialNumber}}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op {Move, serializeMoveArgs(*args), args.Identity, args.SerialNumber}

	sm.mu.Lock()
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	DPrintf("SMserver[%d] start move command %v", sm.me, args)

	if !sm.sendToRaft(op) {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.moveInternal(args, reply)
}

func (sm *ShardMaster) queryInternal(args *QueryArgs, reply *QueryReply) {
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		reply.Config = res.Data.(Config)
		return
	}
	DPrintf("SMserver[%d] do query internal command %v", sm.me, args)

	reply.WrongLeader = false
	reply.Err = OK
	//do query
	if args.Num >= len(sm.configs) || args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	DPrintf("SMserver[%d] query a config %v", sm.me, reply.Config)
	sm.opCache[args.Identity] = CacheEntry{args.SerialNumber, GeneralReply{false, OK, reply.Config, args.Identity, args.SerialNumber}}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op {Query, serializeQueryArgs(*args), args.Identity, args.SerialNumber}

	sm.mu.Lock()
	if res, ok := sm.findInCache(args.Identity, args.SerialNumber); ok {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	DPrintf("SMserver[%d] start query command %v", sm.me, args)

	if !sm.sendToRaft(op) {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.queryInternal(args, reply)
}

// Create snapshot
func (sm *ShardMaster) createSnapshot() (int, []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.opCache)
	maxApply := atomic.LoadInt64(&sm.maxApply)
	e.Encode(maxApply)
	return int(maxApply), w.Bytes()
}

// Read snapshot and rebuild the dict
func (sm *ShardMaster) readSnapshot(data []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configs []Config
	var opCache map[int64]CacheEntry
	var maxApply int64

	if d.Decode(&configs) != nil ||
		d.Decode(&opCache) != nil ||
		d.Decode(&maxApply) != nil {
		//error
		DPrintf("decode error")
	} else {
		if maxApply > sm.maxApply {
			sm.configs = configs
			sm.opCache = opCache
			atomic.StoreInt64(&sm.maxApply, maxApply)
		}
	}
	DPrintf("SMserver[%d] read max apply %d", sm.me, maxApply)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.snapshotTimer.Cancel()
	for _, v := range sm.rp {
		close(v)
	}
	sm.rp = make(map[int64]chan OpReply)
	DPrintf("SMserver[%d] has been killed", sm.me)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Config{})
	labgob.Register(CacheEntry{})

	sm.applyCh = make(chan raft.ApplyMsg, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	// Your code here.

	sm.rp = make(map[int64]chan OpReply)
	sm.opCache = make(map[int64]CacheEntry)
	sm.maxApply = -1

	initDone := false
	initChan := make(chan int)
	defer func() {
		<- initChan
	}()

	go func() {
		for m := range sm.applyCh {
			if !m.CommandValid {
				if m.MsgType == raft.Snapshot {
					// Read new snapshot
					DPrintf("SMserver[%d] read snapshot", me)
					snapshot := m.Command.([]byte)
					sm.readSnapshot(snapshot)
					// mark init done
					sm.mu.Lock()
					if !initDone {
						initChan <- 1
						initDone = true
					}
					sm.mu.Unlock()
				} else if m.MsgType == raft.NoLeader {
					command := m.Command.(Op)
					// commit fail
					sm.mu.Lock()
					DPrintf("SMserver[%d] reply cancel %d", me, command.Identity)
					if channel, ok := sm.rp[command.Identity]; ok {
						channel <- OpReply{true, command.Identity, command.SerialNumber}
					}
					sm.mu.Unlock()
				}
				continue
			}

			command := m.Command.(Op)
			if int64(m.CommandIndex) <= atomic.LoadInt64(&sm.maxApply) {
				continue
			}

			sm.mu.Lock()
			atomic.StoreInt64(&sm.maxApply, int64(m.CommandIndex))
			DPrintf("SMserver[%d] receive reply index: %d command: %v", me, m.CommandIndex, command)

			if channel, ok := sm.rp[command.Identity]; ok {
				channel <- OpReply{false, command.Identity, command.SerialNumber}
				DPrintf("Kvserver[%d] rp exist %d", me, m.CommandIndex)
			} else {
				switch command.Type {
				case Join:
					args := unserializeJoinArgs(command.Data)
					sm.joinInternal(&args, &JoinReply {})
				case Leave:
					args := unserializeLeaveArgs(command.Data)
					sm.leaveInternal(&args, &LeaveReply {})
				case Move:
					args := unserializeMoveArgs(command.Data)
					sm.moveInternal(&args, &MoveReply {})
				case Query:
					args := unserializeQueryArgs(command.Data)
					sm.queryInternal(&args, &QueryReply {})
				}
			}
			sm.mu.Unlock()
		}
	}()

	// check if it is necessary to snapshot
	sm.snapshotTimer = raft.MakeTimer(30*time.Millisecond, func() {
		threshold := MaxRaftState * 2 / 3
		if persister.RaftStateSize() > threshold {
			// create snapshot
			maxApply, snapshot := sm.createSnapshot()
			if maxApply > 0 {
				DPrintf("SMserver[%d] save max apply %d", sm.me, maxApply)
				sm.rf.DiscardLogsAndSaveSnapshot(maxApply, snapshot)
			}
		}
	})
	sm.snapshotTimer.Start(false)

	return sm
}
