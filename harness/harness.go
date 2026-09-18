package harness

import (
	"math/rand"
	"time"

	"github.com/bking28/raft-consensus/raft"
)

const (
	RequestTimeout = 500 * time.Millisecond
	MaxThink       = 50 * time.Millisecond
	MaxBackoff     = 50 * time.Millisecond
	MaxFaultGap    = time.Second
	MaxFaultLength = 2 * time.Second
)

type Config struct {
	Seed     int64
	Nodes    int
	Clients  int
	Keys     int
	Duration time.Duration
	Faults   raft.FaultConfig
}

type Op struct {
	Cmd      raft.Command
	Invoke   time.Duration
	Complete time.Duration
	Result   raft.Result
	Done     bool
}

type harness struct {
	cfg     Config
	cluster *raft.Cluster
	rng     *rand.Rand
	down    map[raft.NodeID]bool
	history []Op
}

func Run(cfg Config) []Op {
	h := &harness{
		cfg:     cfg,
		cluster: raft.NewCluster(cfg.Nodes, cfg.Seed, cfg.Faults),
		rng:     rand.New(rand.NewSource(cfg.Seed)),
		down:    make(map[raft.NodeID]bool),
	}
	h.cluster.Start()
	for i := range cfg.Clients {
		h.startClient(int64(i + 1))
	}
	h.scheduleFault()
	h.cluster.Sim.Run(cfg.Duration)
	return h.history
}

func (h *harness) now() time.Duration {
	return h.cluster.Sim.Now()
}

func (h *harness) after(d time.Duration, fn func()) {
	h.cluster.Sim.After(d, fn)
}

func (h *harness) jitter(max time.Duration) time.Duration {
	return time.Duration(h.rng.Int63n(int64(max)))
}
