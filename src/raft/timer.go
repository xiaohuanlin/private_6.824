package raft

import (
	"sync"
	"time"
)

const (
	Reset = iota + 1
	Cancel
)

type Timer struct {
	mtx sync.Mutex
	channel chan int
	timeout time.Duration
	handler func()
	active bool
}

func (t *Timer) Start(now bool) {
	DPrintf("Start timer")
	go func ()  {
		t.mtx.Lock()
		defer t.mtx.Unlock()
		t.active = true
		if now {
			go t.handler()
		}

		for ;; {
			select {
			case v := <- t.channel:
				if v == Reset {
					t.active = true
					DPrintf("Reset timer")
				} else if v == Cancel {
					t.active = false
					DPrintf("Cancel timer")
					return
				} else {
					panic("Unknown command")
				}
			case <-time.After(t.timeout):
				if t.active {
					go t.handler()
				}
			}
		}
	}()
}

func (t *Timer) Cancel() {
	DPrintf("Send cancel signal")
	t.channel <- Cancel
}

func (t *Timer) Reset()  {
	DPrintf("Send reset signal")
	t.channel <- Reset
}