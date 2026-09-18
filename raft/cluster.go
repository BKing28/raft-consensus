package raft

type Cluster struct {
	Sim       *Sim
	Nodes     []*Node
	Stores    []*KVStore
	Transport *SimTransport
	storage   []*MemStorage
	seed      int64
	restarts  []int
}

func NewCluster(size int, seed int64, faults FaultConfig) *Cluster {
	sim := NewSim(seed)
	c := &Cluster{
		Sim:       sim,
		Nodes:     make([]*Node, size),
		Stores:    make([]*KVStore, size),
		Transport: NewSimTransport(sim, faults),
		storage:   make([]*MemStorage, size),
		seed:      seed,
		restarts:  make([]int, size),
	}
	for i := range size {
		c.storage[i] = NewMemStorage()
		c.createNode(NodeID(i))
	}
	return c
}

func (c *Cluster) Start() {
	for _, node := range c.Nodes {
		c.launch(node)
	}
}

func (c *Cluster) Leader() *Node {
	var leader *Node
	for _, node := range c.Nodes {
		if !node.dead && node.role == Leader && (leader == nil || node.currentTerm > leader.currentTerm) {
			leader = node
		}
	}
	return leader
}

func (c *Cluster) CrashNode(id NodeID) {
	c.Nodes[id].dead = true
	c.Stores[id].failWaiters()
}

func (c *Cluster) RestartNode(id NodeID) {
	if !c.Nodes[id].dead {
		c.CrashNode(id)
	}
	c.restarts[id]++
	c.createNode(id)
	c.launch(c.Nodes[id])
}

func (c *Cluster) createNode(id NodeID) {
	var peers []NodeID
	for i := range c.Nodes {
		if NodeID(i) != id {
			peers = append(peers, NodeID(i))
		}
	}
	c.Nodes[id] = NewNode(id, peers, c.Transport, c.storage[id], c.nodeSeed(id))
	c.Stores[id] = NewKVStore(c.Nodes[id])
}

func (c *Cluster) launch(node *Node) {
	node.Start(c.Sim.Now())
	c.Transport.RegisterNode(node.id, node)
	c.tick(node)
}

func (c *Cluster) tick(node *Node) {
	c.Sim.After(ElectionTick, func() {
		if node.dead {
			return
		}
		node.Tick(c.Sim.Now())
		c.tick(node)
	})
}

// Distinct from the transport seed and from every other incarnation of every node.
func (c *Cluster) nodeSeed(id NodeID) int64 {
	return c.seed + 1 + int64(id) + int64(len(c.Nodes))*int64(c.restarts[id])
}
