package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

const RequestTimeout = time.Second

var (
	ErrNotLeader = errors.New("not leader")
	ErrTimeout   = errors.New("request timed out")
)

type Op int

const (
	OpGet Op = iota
	OpPut
	OpDelete
)

type Command struct {
	ClientID  int64
	RequestID int64
	Op        Op
	Key       string
	Value     string
}

type Result struct {
	Value string
	Found bool
}

type applied struct {
	requestID int64
	result    Result
}

type waiter struct {
	data []byte
	ch   chan Result
}

type KVStore struct {
	mu      sync.Mutex
	node    *Node
	data    map[string]string
	clients map[int64]applied
	waiters map[int]waiter
}

func NewKVStore(node *Node) *KVStore {
	kv := &KVStore{
		node:    node,
		data:    make(map[string]string),
		clients: make(map[int64]applied),
		waiters: make(map[int]waiter),
	}
	go kv.applyLoop()
	return kv
}

// Reads go through the log too, so a deposed leader can never answer with stale data.
func (kv *KVStore) Do(cmd Command) (Result, error) {
	data, _ := json.Marshal(cmd)

	kv.mu.Lock()
	index, _, isLeader := kv.node.Submit(data)
	if !isLeader {
		kv.mu.Unlock()
		return Result{}, ErrNotLeader
	}
	ch := make(chan Result, 1)
	kv.waiters[index] = waiter{data, ch}
	kv.mu.Unlock()

	select {
	case res, ok := <-ch:
		if !ok {
			return Result{}, ErrNotLeader
		}
		return res, nil
	case <-time.After(RequestTimeout):
		return Result{}, ErrTimeout
	}
}

// A different command landing at a waiter's index means leadership changed, so the waiter is failed.
func (kv *KVStore) applyLoop() {
	for msg := range kv.node.applyCh {
		kv.mu.Lock()
		var res Result
		if msg.Command != nil {
			res = kv.apply(decode(msg.Command))
		}
		if w, ok := kv.waiters[msg.CommandIndex]; ok {
			delete(kv.waiters, msg.CommandIndex)
			if bytes.Equal(w.data, msg.Command) {
				w.ch <- res
			} else {
				close(w.ch)
			}
		}
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	for _, w := range kv.waiters {
		close(w.ch)
	}
	kv.mu.Unlock()
}

func (kv *KVStore) apply(cmd Command) Result {
	if last, ok := kv.clients[cmd.ClientID]; ok && cmd.RequestID <= last.requestID {
		return last.result
	}
	var res Result
	switch cmd.Op {
	case OpGet:
		res.Value, res.Found = kv.data[cmd.Key]
	case OpPut:
		kv.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(kv.data, cmd.Key)
	}
	kv.clients[cmd.ClientID] = applied{cmd.RequestID, res}
	return res
}

func decode(data []byte) Command {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		panic(err)
	}
	return cmd
}
