package raft

import "time"

const minReorderHold = 10 * time.Millisecond

type FaultConfig struct {
	DropRate      float64
	DuplicateRate float64
	ReorderRate   float64
	MinDelay      time.Duration
	MaxDelay      time.Duration
}

type SimTransport struct {
	sim        *Sim
	nodes      map[NodeID]*Node
	partitions map[NodeID]map[NodeID]bool
	faults     FaultConfig
}

func NewSimTransport(sim *Sim, faults FaultConfig) *SimTransport {
	return &SimTransport{
		sim:        sim,
		nodes:      make(map[NodeID]*Node),
		partitions: make(map[NodeID]map[NodeID]bool),
		faults:     faults,
	}
}

func (st *SimTransport) RegisterNode(id NodeID, node *Node) {
	st.nodes[id] = node
}

func (st *SimTransport) Partition(group1, group2 []NodeID) {
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
	return st.faults.MinDelay + time.Duration(st.sim.rng.Int63n(spread))
}

// A reordered message is held back long enough for later messages to overtake it.
func (st *SimTransport) plan() (drop, dup bool, delay time.Duration) {
	drop = st.sim.rng.Float64() < st.faults.DropRate
	dup = st.sim.rng.Float64() < st.faults.DuplicateRate
	delay = st.randomDelay()
	if st.sim.rng.Float64() < st.faults.ReorderRate {
		delay += max(st.faults.MaxDelay, minReorderHold)
	}
	return drop, dup, delay
}

// Resolved at delivery time so partitions and crashes during flight take effect.
func (st *SimTransport) live(from, to NodeID) *Node {
	if st.isPartitioned(from, to) {
		return nil
	}
	if node := st.nodes[to]; node != nil && !node.killed() {
		return node
	}
	return nil
}

func (st *SimTransport) deliver(from, to NodeID, delay time.Duration, handle func(*Node)) {
	st.sim.After(delay, func() {
		if node := st.live(from, to); node != nil {
			handle(node)
		}
	})
}

func (st *SimTransport) send(from, to NodeID, handle func(*Node)) {
	drop, dup, delay := st.plan()
	if drop || st.isPartitioned(from, to) {
		return
	}
	st.deliver(from, to, delay, handle)
	if dup {
		st.deliver(from, to, st.randomDelay(), handle)
	}
}

func (st *SimTransport) sendReply(from, to NodeID, handle func(*Node)) {
	if st.sim.rng.Float64() < st.faults.DropRate {
		return
	}
	st.deliver(from, to, st.randomDelay(), handle)
}

func (st *SimTransport) SendRequestVote(from, to NodeID, args RequestVoteArgs) {
	st.send(from, to, func(node *Node) {
		reply := node.HandleRequestVote(args)
		st.sendReply(to, from, func(sender *Node) { sender.HandleRequestVoteReply(to, args, reply) })
	})
}

func (st *SimTransport) SendAppendEntries(from, to NodeID, args AppendEntriesArgs) {
	st.send(from, to, func(node *Node) {
		reply := node.HandleAppendEntries(args)
		st.sendReply(to, from, func(sender *Node) { sender.HandleAppendEntriesReply(to, args, reply) })
	})
}

func (st *SimTransport) SendInstallSnapshot(from, to NodeID, args InstallSnapshotArgs) {
	st.send(from, to, func(node *Node) {
		reply := node.HandleInstallSnapshot(args)
		st.sendReply(to, from, func(sender *Node) { sender.HandleInstallSnapshotReply(to, args, reply) })
	})
}
