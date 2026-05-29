package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type FaultConfig struct {
	DropRate      float64
	DuplicateRate float64
	MinDelay      time.Duration
	MaxDelay      time.Duration
}

type SimTransport struct {
	mu         sync.Mutex
	nodes      map[NodeID]*Node
	partitions map[NodeID]map[NodeID]bool // partitions[a][b] == true means a cannot reach b
	faults     FaultConfig
	rng        *rand.Rand
}

func NewSimTransport(seed int64, faults FaultConfig) *SimTransport {
	return &SimTransport{
		nodes:      make(map[NodeID]*Node),
		partitions: make(map[NodeID]map[NodeID]bool),
		faults:     faults,
		rng:        rand.New(rand.NewSource(seed)),
	}
}

func (st *SimTransport) RegisterNode(id NodeID, node *Node) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.nodes[id] = node
}

func (st *SimTransport) Partition(group1, group2 []NodeID) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, a := range group1 {
		for _, b := range group2 {
			if st.partitions[a] == nil {
				st.partitions[a] = make(map[NodeID]bool)
			}
			if st.partitions[b] == nil {
				st.partitions[b] = make(map[NodeID]bool)
			}
			st.partitions[a][b] = true
			st.partitions[b][a] = true
		}
	}
}

func (st *SimTransport) Heal() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.partitions = make(map[NodeID]map[NodeID]bool)
}

func (st *SimTransport) isPartitioned(from, to NodeID) bool {
	if p, ok := st.partitions[from]; ok {
		return p[to]
	}
	return false
}

func (st *SimTransport) shouldDrop() bool {
	return st.rng.Float64() < st.faults.DropRate
}

func (st *SimTransport) shouldDuplicate() bool {
	return st.rng.Float64() < st.faults.DuplicateRate
}

func (st *SimTransport) randomDelay() time.Duration {
	if st.faults.MaxDelay <= st.faults.MinDelay {
		return st.faults.MinDelay
	}
	spread := int64(st.faults.MaxDelay - st.faults.MinDelay)
	return st.faults.MinDelay + time.Duration(st.rng.Int63n(spread))
}

func (st *SimTransport) SendRequestVote(target NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	st.mu.Lock()
	node, ok := st.nodes[target]
	partitioned := st.isPartitioned(args.CandidateID, target)
	drop := st.shouldDrop()
	dup := st.shouldDuplicate()
	delay := st.randomDelay()
	st.mu.Unlock()

	if !ok {
		return RequestVoteReply{}, fmt.Errorf("node %d not found", target)
	}
	if partitioned || drop {
		return RequestVoteReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	reply := node.HandleRequestVote(args)

	if dup {
		go func() {
			time.Sleep(delay)
			node.HandleRequestVote(args)
		}()
	}

	return reply, nil
}

func (st *SimTransport) SendAppendEntries(target NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	st.mu.Lock()
	node, ok := st.nodes[target]
	partitioned := st.isPartitioned(args.LeaderID, target)
	drop := st.shouldDrop()
	dup := st.shouldDuplicate()
	delay := st.randomDelay()
	st.mu.Unlock()

	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("node %d not found", target)
	}
	if partitioned || drop {
		return AppendEntriesReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	reply := node.HandleAppendEntries(args)

	if dup {
		go func() {
			time.Sleep(delay)
			node.HandleAppendEntries(args)
		}()
	}

	return reply, nil
}

func (st *SimTransport) SendInstallSnapshot(target NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	st.mu.Lock()
	node, ok := st.nodes[target]
	partitioned := st.isPartitioned(args.LeaderID, target)
	drop := st.shouldDrop()
	delay := st.randomDelay()
	st.mu.Unlock()

	if !ok {
		return InstallSnapshotReply{}, fmt.Errorf("node %d not found", target)
	}
	if partitioned || drop {
		return InstallSnapshotReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	// Stub — InstallSnapshot handler not yet implemented
	_ = node
	return InstallSnapshotReply{}, fmt.Errorf("not implemented")
}
