package raft

import (
	"fmt"
	"sync"
)

type MemStorage struct {
	mu          sync.Mutex
	currentTerm int
	votedFor    NodeID
	log         []LogEntry
}

func NewMemStorage() *MemStorage {
	return &MemStorage{votedFor: NullVote}
}

func (ms *MemStorage) GetState() (int, NodeID, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.currentTerm, ms.votedFor, nil
}

func (ms *MemStorage) SetState(currentTerm int, votedFor NodeID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.currentTerm = currentTerm
	ms.votedFor = votedFor
	return nil
}

func (ms *MemStorage) GetLogRange(lo, hi int) ([]LogEntry, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if lo < 1 || hi > len(ms.log) {
		return nil, fmt.Errorf("log range [%d,%d] out of bounds (len=%d)", lo, hi, len(ms.log))
	}
	result := make([]LogEntry, hi-lo+1)
	copy(result, ms.log[lo-1:hi])
	return result, nil
}

func (ms *MemStorage) LastIndex() (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.log), nil
}

func (ms *MemStorage) AppendLogs(entries []LogEntry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *MemStorage) TruncateFrom(index int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if index-1 < len(ms.log) {
		ms.log = ms.log[:index-1]
	}
	return nil
}
