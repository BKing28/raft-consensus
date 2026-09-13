package harness

import "github.com/bking28/raft-consensus/raft"

func (h *harness) scheduleFault() {
	h.after(h.jitter(MaxFaultGap), func() {
		if h.rng.Intn(2) == 0 {
			h.partition()
		} else {
			h.crash()
		}
		h.scheduleFault()
	})
}

func (h *harness) partition() {
	var a, b []raft.NodeID
	for i := range h.cfg.Nodes {
		if h.rng.Intn(2) == 0 {
			a = append(a, raft.NodeID(i))
		} else {
			b = append(b, raft.NodeID(i))
		}
	}
	if len(a) == 0 || len(b) == 0 {
		return
	}
	h.cluster.Transport.Partition(a, b)
	h.after(h.jitter(MaxFaultLength), h.cluster.Transport.Heal)
}

// Keeps a majority alive so runs make progress; the network faults supply the rest of the chaos.
func (h *harness) crash() {
	id := raft.NodeID(h.rng.Intn(h.cfg.Nodes))
	if h.down[id] || len(h.down)+1 > (h.cfg.Nodes-1)/2 {
		return
	}
	h.down[id] = true
	h.cluster.CrashNode(id)
	h.after(h.jitter(MaxFaultLength), func() {
		delete(h.down, id)
		h.cluster.RestartNode(id)
	})
}
