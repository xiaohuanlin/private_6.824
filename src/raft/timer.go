package raft

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	Reset = iota + 1
	Cancel
)

type Timer struct {
	mtx     sync.Mutex
	channel chan int
	timeout unsafe.Pointer
	handler func()
	active  int32
}

func (t *Timer) Start(now bool) {
	DPrintf("Start timer")
	go func() {
		t.mtx.Lock()
		defer t.mtx.Unlock()
		atomic.StoreInt32(&t.active, 1)
		if now {
			go t.handler()
		}

		for {
			select {
			case v := <-t.channel:
				if v == Reset {
					atomic.StoreInt32(&t.active, 1)
					DPrintf("Reset timer")
				} else if v == Cancel {
					atomic.StoreInt32(&t.active, 0)
					DPrintf("Cancel timer")
					return
				} else {
					panic("Unknown command")
				}
			case <-time.After(*(*time.Duration)(atomic.LoadPointer(&t.timeout))):
				if atomic.LoadInt32(&t.active) == 1 {
					go t.handler()
				}
			}
		}
	}()
}

func (t *Timer) Cancel() {
	DPrintf("Send cancel signal")
	if atomic.LoadInt32(&t.active) == 0 {
		return
	}
	t.channel <- Cancel
}

func (t *Timer) Reset(timeout time.Duration) {
	DPrintf("Send reset signal")
	atomic.StorePointer(&t.timeout, unsafe.Pointer(&timeout))
	t.channel <- Reset
}

func (t *Timer) ResetOrStart(timeout time.Duration) {
	DPrintf("Send reset||start signal")
	if atomic.LoadInt32(&t.active) == 1 {
		t.Reset(timeout)
	} else {
		t.Start(false)
	}
}

func MakeTimer(timeout time.Duration, handler func()) *Timer {
	t := &Timer{}
	t.mtx = sync.Mutex{}
	t.channel = make(chan int)
	atomic.StorePointer(&t.timeout, unsafe.Pointer(&timeout))
	t.handler = handler
	atomic.StoreInt32(&t.active, 0)
	return t
}
