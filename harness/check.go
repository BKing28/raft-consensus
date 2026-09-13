package harness

import (
	"math"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/bking28/raft-consensus/raft"
)

const CheckTimeout = 10 * time.Second

// Each key is an independent register, so histories are checked one key at a time.
var model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(raft.Command).Key
			byKey[key] = append(byKey[key], op)
		}
		var parts [][]porcupine.Operation
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() any { return raft.Result{} },
	Step: func(state, input, output any) (bool, any) {
		cmd := input.(raft.Command)
		switch cmd.Op {
		case raft.OpPut:
			return true, raft.Result{Value: cmd.Value, Found: true}
		case raft.OpDelete:
			return true, raft.Result{}
		default:
			return output == state, state
		}
	},
}

// Unanswered writes may have applied and keep an unbounded return time; unanswered reads constrain nothing and are dropped.
func Check(history []Op) porcupine.CheckResult {
	var ops []porcupine.Operation
	for _, op := range history {
		if !op.Done && op.Cmd.Op == raft.OpGet {
			continue
		}
		ret := int64(math.MaxInt64)
		if op.Done {
			ret = int64(op.Complete)
		}
		ops = append(ops, porcupine.Operation{
			ClientId: int(op.Cmd.ClientID),
			Input:    op.Cmd,
			Call:     int64(op.Invoke),
			Output:   op.Result,
			Return:   ret,
		})
	}
	return porcupine.CheckOperationsTimeout(model, ops, CheckTimeout)
}
