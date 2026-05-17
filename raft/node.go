package raft

import "sync"

type ApplyMsg struct {
	Command      []byte
	CommandIndex int
}

type Node struct {
	mu    sync.Mutex
	id    NodeID
	peers []NodeID

	currentTerm int
	votedFor    NodeID
	log         []LogEntry

	commitIndex int
	lastApplied int
	role        Role

	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int

	transport Transport
	storage   Storage
	applyCh   chan ApplyMsg
}

func NewNode(id NodeID, peers []NodeID, transport Transport, storage Storage, applyCh chan ApplyMsg) *Node {
	return &Node{
		id:         id,
		peers:      peers,
		votedFor:   NullVote,
		log:        make([]LogEntry, 1),
		role:       Follower,
		nextIndex:  make(map[NodeID]int),
		matchIndex: make(map[NodeID]int),
		transport:  transport,
		storage:    storage,
		applyCh:    applyCh,
	}
}
