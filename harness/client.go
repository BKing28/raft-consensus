package harness

import (
	"fmt"
	"time"

	"github.com/bking28/raft-consensus/raft"
)

type client struct {
	h        *harness
	id       int64
	requests int64
	attempts int
	op       int
}

func (h *harness) startClient(id int64) {
	cl := &client{h: h, id: id}
	h.after(h.jitter(MaxThink), cl.begin)
}

func (cl *client) begin() {
	cl.requests++
	cmd := raft.Command{
		ClientID:  cl.id,
		RequestID: cl.requests,
		Op:        raft.Op(cl.h.rng.Intn(3)),
		Key:       fmt.Sprint("k", cl.h.rng.Intn(cl.h.cfg.Keys)),
	}
	if cmd.Op == raft.OpPut {
		cmd.Value = fmt.Sprintf("%d-%d", cl.id, cl.requests)
	}
	cl.h.history = append(cl.h.history, Op{Cmd: cmd, Invoke: cl.h.now()})
	cl.op = len(cl.h.history) - 1
	cl.attempt()
}

// Every attempt of a request shares one history entry, since dedup makes retries the same operation.
func (cl *client) attempt() {
	idx := cl.op
	cl.attempts++
	attempt := cl.attempts
	retry := func(d time.Duration) {
		if !cl.h.history[idx].Done && attempt == cl.attempts {
			cl.h.after(d, cl.attempt)
		}
	}

	leader := cl.h.cluster.Leader()
	if leader == nil {
		retry(cl.h.jitter(MaxBackoff))
		return
	}
	cl.h.cluster.Stores[leader.ID()].Do(cl.h.history[idx].Cmd, func(res raft.Result, err error) {
		if cl.h.history[idx].Done {
			return
		}
		if err != nil {
			retry(cl.h.jitter(MaxBackoff))
			return
		}
		cl.complete(idx, res)
	})
	cl.h.after(RequestTimeout, func() { retry(0) })
}

func (cl *client) complete(idx int, res raft.Result) {
	op := &cl.h.history[idx]
	op.Complete, op.Result, op.Done = cl.h.now(), res, true
	cl.h.after(cl.h.jitter(MaxThink), cl.begin)
}
