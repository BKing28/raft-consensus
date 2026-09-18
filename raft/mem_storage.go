package raft

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

func (ms *MemStorage) GetState() (int, NodeID) {
	return ms.currentTerm, ms.votedFor
}

func (ms *MemStorage) SetState(currentTerm int, votedFor NodeID) {
	ms.currentTerm, ms.votedFor = currentTerm, votedFor
}

func (ms *MemStorage) Entries() []LogEntry {
	return ms.log
}

func (ms *MemStorage) AppendLogs(entries []LogEntry) {
	ms.log = append(ms.log, entries...)
}

func (ms *MemStorage) TruncateFrom(index int) {
	ms.log = ms.log[:min(index-1-ms.snapIndex, len(ms.log))]
}

func (ms *MemStorage) SaveSnapshot(index, term int, data []byte) {
	ms.log = ms.log[min(index-ms.snapIndex, len(ms.log)):]
	ms.snapIndex, ms.snapTerm, ms.snapshot = index, term, data
}

func (ms *MemStorage) LoadSnapshot() (int, int, []byte) {
	return ms.snapIndex, ms.snapTerm, ms.snapshot
}
