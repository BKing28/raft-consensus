package raft

import "fmt"

type MemStorage struct {
	currentTerm int
	votedFor    NodeID
	log         []LogEntry
	snapIndex   int
	snapTerm    int
	snapshot    []byte
}

func NewMemStorage() *MemStorage {
	return &MemStorage{votedFor: NullVote}
}

func (ms *MemStorage) GetState() (int, NodeID, error) {
	return ms.currentTerm, ms.votedFor, nil
}

func (ms *MemStorage) SetState(currentTerm int, votedFor NodeID) error {
	ms.currentTerm = currentTerm
	ms.votedFor = votedFor
	return nil
}

func (ms *MemStorage) GetLogRange(lo, hi int) ([]LogEntry, error) {
	if lo <= ms.snapIndex || hi > ms.snapIndex+len(ms.log) {
		return nil, fmt.Errorf("log range [%d,%d] out of bounds (len=%d)", lo, hi, len(ms.log))
	}
	result := make([]LogEntry, hi-lo+1)
	copy(result, ms.log[lo-1-ms.snapIndex:hi-ms.snapIndex])
	return result, nil
}

func (ms *MemStorage) LastIndex() (int, error) {
	return ms.snapIndex + len(ms.log), nil
}

func (ms *MemStorage) AppendLogs(entries []LogEntry) error {
	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *MemStorage) TruncateFrom(index int) error {
	if index-1-ms.snapIndex < len(ms.log) {
		ms.log = ms.log[:index-1-ms.snapIndex]
	}
	return nil
}

func (ms *MemStorage) SaveSnapshot(index, term int, data []byte) error {
	ms.log = ms.log[min(index-ms.snapIndex, len(ms.log)):]
	ms.snapIndex, ms.snapTerm, ms.snapshot = index, term, data
	return nil
}

func (ms *MemStorage) LoadSnapshot() (int, int, []byte, error) {
	return ms.snapIndex, ms.snapTerm, ms.snapshot, nil
}
