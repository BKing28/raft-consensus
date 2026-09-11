package raft

import "time"

type Cluster struct {
	Sim       *Sim
	Nodes     []*Node
	Stores    []*KVStore
	Storage   []*MemStorage
	Transport *SimTransport
	size      int
	seed      int64
	restarts  []int
}

func NewCluster(size int, seed int64, faults FaultConfig) *Cluster {
	sim := NewSim(seed)
	c := &Cluster{
		Sim:       sim,
		Nodes:     make([]*Node, size),
		Stores:    make([]*KVStore, size),
		Storage:   make([]*MemStorage, size),
		Transport: NewSimTransport(sim, faults),
		size:      size,
		seed:      seed,
		restarts:  make([]int, size),
	}
	for i := range size {
		c.Storage[i] = NewMemStorage()
		c.createNode(NodeID(i))
	}
	return c
}

func (c *Cluster) Start() {
	for _, node := range c.Nodes {
		c.launch(node)
	}
}

func (c *Cluster) Stop() {
	for _, node := range c.Nodes {
		c.CrashNode(node.id)
	}
}

func (c *Cluster) Run(d time.Duration) {
	c.Sim.Run(d)
}

func (c *Cluster) Leader() *Node {
	var leader *Node
	leaderTerm := -1
	for _, node := range c.Nodes {
		if node.killed() {
			continue
		}
		if term, role := node.GetState(); role == Leader && term > leaderTerm {
			leader, leaderTerm = node, term
		}
	}
	return leader
}

func (c *Cluster) WaitForLeader(limit time.Duration) *Node {
	c.Sim.RunUntil(func() bool { return c.Leader() != nil }, limit)
	return c.Leader()
}

func (c *Cluster) CrashNode(id NodeID) {
	c.Nodes[id].Stop()
	c.Stores[id].Stop()
}

func (c *Cluster) RestartNode(id NodeID) {
	if !c.Nodes[id].killed() {
		c.CrashNode(id)
	}
	c.restarts[id]++
	c.createNode(id)
	c.launch(c.Nodes[id])
}

func (c *Cluster) createNode(id NodeID) {
	var peers []NodeID
	for i := range c.size {
		if NodeID(i) != id {
			peers = append(peers, NodeID(i))
		}
	}
	c.Nodes[id] = NewNode(id, peers, c.Transport, c.Storage[id], c.nodeSeed(id))
	c.Stores[id] = NewKVStore(c.Nodes[id])
}

func (c *Cluster) launch(node *Node) {
	node.Start(c.Sim.Now())
	c.Transport.RegisterNode(node.id, node)
	c.tick(node)
}

func (c *Cluster) tick(node *Node) {
	c.Sim.After(ElectionTick, func() {
		if node.killed() {
			return
		}
		node.Tick(c.Sim.Now())
		c.tick(node)
	})
}

// Distinct from the transport seed and from every other incarnation of every node.
func (c *Cluster) nodeSeed(id NodeID) int64 {
	return c.seed + 1 + int64(id) + int64(c.size)*int64(c.restarts[id])
}
