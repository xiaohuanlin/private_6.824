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
}

func (t *Timer) Start() {
	go func ()  {
		t.mtx.Lock()
		defer t.mtx.Unlock()

		for ;; {
			select {
			case v := <- t.channel:
				if v == Reset {
					log.Println("Reset timer")
				} else if v == Cancel {
					log.Println("Cancel timer")
					break
				} else {
					panic("Unknown command")
				}
			case <-time.After(t.timeout):
				t.handler()
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