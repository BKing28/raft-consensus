package raft

import (
	"sync"
	"time"
)

type Cluster struct {
	mu        sync.Mutex
	Nodes     []*Node
	Stores    []*KVStore
	Storage   []*MemStorage
	Transport *SimTransport
	size      int
	seed      int64
	restarts  []int
}

func NewCluster(size int, seed int64, faults FaultConfig) *Cluster {
	c := &Cluster{
		Nodes:     make([]*Node, size),
		Stores:    make([]*KVStore, size),
		Storage:   make([]*MemStorage, size),
		Transport: NewSimTransport(seed, faults),
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.Nodes {
		c.launch(node)
	}
}

func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.Nodes {
		node.Stop()
	}
}

func (c *Cluster) Leader() *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.Nodes {
		if node.killed() {
			continue
		}
		if _, role := node.GetState(); role == Leader {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Nodes[id].Stop()
}

func (c *Cluster) RestartNode(id NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Nodes[id].Stop()
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

// Registers only after Start so no RPC reaches a node before it has restored its state.
func (c *Cluster) launch(node *Node) {
	node.Start()
	c.Transport.RegisterNode(node.id, node)
}

// Distinct from the transport seed and from every other incarnation of every node.
func (c *Cluster) nodeSeed(id NodeID) int64 {
	return c.seed + 1 + int64(id) + int64(c.size)*int64(c.restarts[id])
}
