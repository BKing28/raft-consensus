package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"slices"
)

var ErrNotLeader = errors.New("not leader")

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
	done func(Result, error)
}

type KVStore struct {
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
	node.apply = kv.applyMsg
	return kv
}

// Reads go through the log too, so a deposed leader can never answer with stale data.
func (kv *KVStore) Do(cmd Command, done func(Result, error)) {
	if kv.node.killed() || kv.node.role != Leader {
		done(Result{}, ErrNotLeader)
		return
	}
	data, _ := json.Marshal(cmd)
	index := kv.node.lastLogIndex() + 1
	if old, ok := kv.waiters[index]; ok {
		old.done(Result{}, ErrNotLeader)
	}
	kv.waiters[index] = waiter{data, done}
	kv.node.Submit(data)
}

// Callbacks fire in index order; map order would not be deterministic.
func (kv *KVStore) Stop() {
	for _, index := range slices.Sorted(maps.Keys(kv.waiters)) {
		kv.waiters[index].done(Result{}, ErrNotLeader)
	}
	clear(kv.waiters)
}

// A different command landing at a waiter's index means leadership changed, so the waiter is failed.
func (kv *KVStore) applyMsg(msg ApplyMsg) {
	var res Result
	if msg.Command != nil {
		res = kv.apply(decode(msg.Command))
	}
	w, ok := kv.waiters[msg.CommandIndex]
	if !ok {
		return
	}
	delete(kv.waiters, msg.CommandIndex)
	if bytes.Equal(w.data, msg.Command) {
		w.done(res, nil)
	} else {
		w.done(Result{}, ErrNotLeader)
	}
}

// Clients send one request at a time, so a request at or below the last applied ID is a retry.
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
