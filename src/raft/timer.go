package raft

import (
	"log"
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
	log.Println("Start timer")
	go func ()  {
		t.mtx.Lock()
		defer t.mtx.Unlock()
		t.active = true
		if now && t.active {
			go t.handler()
		}

		for ;; {
			select {
			case v := <- t.channel:
				if v == Reset {
					log.Println("Reset timer")
				} else if v == Cancel {
					t.active = false
					log.Println("Cancel timer")
					break
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
	log.Println("Send cancel signal")
	t.channel <- Cancel
}

func (t *Timer) Reset()  {
	log.Println("Send reset signal")
	t.channel <- Reset
}