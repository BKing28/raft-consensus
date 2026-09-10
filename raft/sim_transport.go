package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const minReorderHold = 10 * time.Millisecond

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
	partitions map[NodeID]map[NodeID]bool
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
	return st.partitions[from][to]
}

func (st *SimTransport) randomDelay() time.Duration {
	if st.faults.MaxDelay <= st.faults.MinDelay {
		return st.faults.MinDelay
	}
	spread := int64(st.faults.MaxDelay - st.faults.MinDelay)
	return st.faults.MinDelay + time.Duration(st.rng.Int63n(spread))
}

// A reordered message is held back long enough for later messages to overtake it.
func (st *SimTransport) plan(from, to NodeID) (drop, dup bool, delay time.Duration) {
	st.mu.Lock()
	defer st.mu.Unlock()
	drop = st.rng.Float64() < st.faults.DropRate || st.isPartitioned(from, to)
	dup = st.rng.Float64() < st.faults.DuplicateRate
	delay = st.randomDelay()
	if st.rng.Float64() < st.faults.ReorderRate {
		delay += max(st.faults.MaxDelay, minReorderHold)
	}
	return drop, dup, delay
}

// Resolved after the delay so partitions and crashes during flight take effect.
func (st *SimTransport) target(from, to NodeID) (*Node, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.isPartitioned(from, to) {
		return nil, fmt.Errorf("message dropped")
	}
	node := st.nodes[to]
	if node == nil || node.killed() {
		return nil, fmt.Errorf("node %d down", to)
	}
	return node, nil
}

func (st *SimTransport) SendRequestVote(to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	from := args.CandidateID
	drop, dup, delay := st.plan(from, to)
	if drop {
		return RequestVoteReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	node, err := st.target(from, to)
	if err != nil {
		return RequestVoteReply{}, err
	}
	reply := node.HandleRequestVote(args)
	if node.killed() {
		return RequestVoteReply{}, fmt.Errorf("node %d down", to)
	}

	if dup {
		go func() {
			time.Sleep(delay)
			if node, err := st.target(from, to); err == nil {
				node.HandleRequestVote(args)
			}
		}()
	}
	return reply, nil
}

func (st *SimTransport) SendAppendEntries(to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	from := args.LeaderID
	drop, dup, delay := st.plan(from, to)
	if drop {
		return AppendEntriesReply{}, fmt.Errorf("message dropped")
	}

	time.Sleep(delay)
	node, err := st.target(from, to)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	reply := node.HandleAppendEntries(args)
	if node.killed() {
		return AppendEntriesReply{}, fmt.Errorf("node %d down", to)
	}

	if dup {
		go func() {
			time.Sleep(delay)
			if node, err := st.target(from, to); err == nil {
				node.HandleAppendEntries(args)
			}
		}()
	}
	return reply, nil
}

func (st *SimTransport) SendInstallSnapshot(to NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, fmt.Errorf("not implemented")
}
