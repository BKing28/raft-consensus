package main

import (
	"flag"
	"fmt"
	"slices"
	"time"

	"github.com/bking28/raft-consensus/harness"
	"github.com/bking28/raft-consensus/raft"
)

func main() {
	cfg := harness.Config{
		Faults: raft.FaultConfig{
			DropRate:      0.1,
			DuplicateRate: 0.1,
			ReorderRate:   0.1,
			MinDelay:      time.Millisecond,
			MaxDelay:      20 * time.Millisecond,
		},
	}
	flag.Int64Var(&cfg.Seed, "seed", 1, "simulation seed")
	flag.IntVar(&cfg.Nodes, "nodes", 5, "cluster size")
	flag.IntVar(&cfg.Clients, "clients", 3, "concurrent clients")
	flag.IntVar(&cfg.Keys, "keys", 5, "distinct keys")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "virtual run time")
	flag.Parse()

	history := harness.Run(cfg)
	done := 0
	for _, op := range history {
		if op.Done {
			done++
		}
	}
	fmt.Printf("seed %d: %d requests, %d completed, %d incomplete\n", cfg.Seed, len(history), done, len(history)-done)
	fmt.Printf("linearizable: %v\n", harness.Check(history))
	fmt.Printf("replay identical: %v\n", slices.Equal(history, harness.Run(cfg)))
}
