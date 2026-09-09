package raft

import "time"

type Cluster struct {
	Nodes      []*Node
	ApplyChans []chan ApplyMsg
	Storage    []*MemStorage
	Transport  *SimTransport
	size       int
	seed       int64
	restarts   []int
}

func NewCluster(size int, seed int64, faults FaultConfig) *Cluster {
	transport := NewSimTransport(seed, faults)
	nodes := make([]*Node, size)
	applyChans := make([]chan ApplyMsg, size)
	storage := make([]*MemStorage, size)

	for i := 0; i < size; i++ {
		id := NodeID(i)
		var peers []NodeID
		for j := 0; j < size; j++ {
			if j != i {
				peers = append(peers, NodeID(j))
			}
		}
		applyChans[i] = make(chan ApplyMsg, 100)
		storage[i] = NewMemStorage()
		nodes[i] = NewNode(id, peers, transport, storage[i], applyChans[i], seed+int64(i))
		transport.RegisterNode(id, nodes[i])
	}

	return &Cluster{
		Nodes:      nodes,
		ApplyChans: applyChans,
		Storage:    storage,
		Transport:  transport,
		size:       size,
		seed:       seed,
		restarts:   make([]int, size),
	}
}

func (c *Cluster) Start() {
	for _, node := range c.Nodes {
		node.Start()
	}
}

func (c *Cluster) Stop() {
	for _, node := range c.Nodes {
		node.Stop()
	}
}

func (c *Cluster) Leader() *Node {
	for _, node := range c.Nodes {
		if node.killed() {
			continue
		}
		_, role := node.GetState()
		if role == Leader {
			return node
		}
	}
	return nil
}

func (c *Cluster) WaitForLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := c.Leader(); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (c *Cluster) CrashNode(id NodeID) {
	c.Nodes[id].Stop()
}

func (c *Cluster) RestartNode(id NodeID) {
	old := c.Nodes[id]
	ch := make(chan ApplyMsg, 100)
	c.ApplyChans[id] = ch

	c.restarts[id]++
	seed := c.seed + int64(id) + int64(c.size)*int64(c.restarts[id])
	node := NewNode(old.id, old.peers, c.Transport, c.Storage[id], ch, seed)
	c.Nodes[id] = node
	node.Start()
	c.Transport.RegisterNode(id, node)
}
