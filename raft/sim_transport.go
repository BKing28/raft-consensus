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
	ReorderRate   float64
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

// A reordered message is held an extra MaxDelay so later messages overtake it.
func (st *SimTransport) shouldReorder() bool {
	return st.rng.Float64() < st.faults.ReorderRate
}

// Resolved at delivery time so crashed nodes never process messages still in flight.
func (st *SimTransport) liveNode(id NodeID) *Node {
	st.mu.Lock()
	defer st.mu.Unlock()
	node := st.nodes[id]
	if node == nil || node.killed() {
		return nil
	}
	return node
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
	partitioned := st.isPartitioned(args.CandidateID, target)
	drop := st.shouldDrop()
	dup := st.shouldDuplicate()
	delay := st.randomDelay()
	if st.shouldReorder() {
		delay += st.faults.MaxDelay
	}
	st.mu.Unlock()

	if partitioned || drop {
		return RequestVoteReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	node := st.liveNode(target)
	if node == nil {
		return RequestVoteReply{}, fmt.Errorf("node %d down", target)
	}
	reply := node.HandleRequestVote(args)

	if dup {
		go func() {
			time.Sleep(delay)
			if node := st.liveNode(target); node != nil {
				node.HandleRequestVote(args)
			}
		}()
	}

	return reply, nil
}

func (st *SimTransport) SendAppendEntries(target NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	st.mu.Lock()
	partitioned := st.isPartitioned(args.LeaderID, target)
	drop := st.shouldDrop()
	dup := st.shouldDuplicate()
	delay := st.randomDelay()
	if st.shouldReorder() {
		delay += st.faults.MaxDelay
	}
	st.mu.Unlock()

	if partitioned || drop {
		return AppendEntriesReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	node := st.liveNode(target)
	if node == nil {
		return AppendEntriesReply{}, fmt.Errorf("node %d down", target)
	}
	reply := node.HandleAppendEntries(args)

	if dup {
		go func() {
			time.Sleep(delay)
			if node := st.liveNode(target); node != nil {
				node.HandleAppendEntries(args)
			}
		}()
	}

	return reply, nil
}

func (st *SimTransport) SendInstallSnapshot(target NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	st.mu.Lock()
	partitioned := st.isPartitioned(args.LeaderID, target)
	drop := st.shouldDrop()
	delay := st.randomDelay()
	st.mu.Unlock()

	if partitioned || drop {
		return InstallSnapshotReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	if st.liveNode(target) == nil {
		return InstallSnapshotReply{}, fmt.Errorf("node %d down", target)
	}
	// InstallSnapshot handler not yet implemented
	return InstallSnapshotReply{}, fmt.Errorf("not implemented")
}
