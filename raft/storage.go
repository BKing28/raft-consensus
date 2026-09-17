package raft

type Storage interface {
	GetState() (currentTerm int, votedFor NodeID, err error)
	SetState(currentTerm int, votedFor NodeID) error
	GetLogRange(lo, hi int) ([]LogEntry, error)
	LastIndex() (int, error)
	AppendLogs(entries []LogEntry) error
	TruncateFrom(index int) error
	SaveSnapshot(index, term int, data []byte) error
	LoadSnapshot() (index, term int, data []byte, err error)
}
