package raft

type Timer interface {
	Make()
	Handle()
	Reset()
	Clear()
}

type VoteTimer struct {
}

func (vt *VoteTimer) Make() {

}

func (vt *VoteTimer) Handle() {

}

func (vt *VoteTimer) Reset() {

}

func (vt *VoteTimer) Clear() {

}