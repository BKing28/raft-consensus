package raft

type Storage interface {
	GetState() (currentTerm int, votedFor NodeID)
	SetState(currentTerm int, votedFor NodeID)
	Entries() []LogEntry
	AppendLogs(entries []LogEntry)
	TruncateFrom(index int)
	SaveSnapshot(index, term int, data []byte)
	LoadSnapshot() (index, term int, data []byte)
}
