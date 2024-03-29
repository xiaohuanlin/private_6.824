package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identity     int64
	SerialNumber int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Identity     int64
	SerialNumber int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GeneralReply struct {
	Err          Err
	Value        string
	SerialNumber int64
}
