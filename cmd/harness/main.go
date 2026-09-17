package main

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/bking28/raft-consensus/harness"
	"github.com/bking28/raft-consensus/raft"
)

const (
	Runs          = 10000
	Duration      = 30 * time.Second
	MinCompletion = 0.5
)

var mixes = []raft.FaultConfig{
	{DropRate: 0.05, DuplicateRate: 0.05, ReorderRate: 0.05, MinDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond},
	{DropRate: 0.1, DuplicateRate: 0.1, ReorderRate: 0.1, MinDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond},
	{DropRate: 0.2, DuplicateRate: 0.2, ReorderRate: 0.2, MinDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond},
}

// The whole configuration derives from the seed, so a failing seed replays exactly.
func configFor(seed int64) harness.Config {
	r := rand.New(rand.NewSource(seed))
	return harness.Config{
		Seed:     seed,
		Nodes:    3 + 2*r.Intn(3),
		Clients:  1 + r.Intn(4),
		Keys:     2 + r.Intn(4),
		Duration: Duration,
		Faults:   mixes[r.Intn(len(mixes))],
	}
}

func run(seed int64) (passed bool, summary string) {
	cfg := configFor(seed)
	history := harness.Run(cfg)
	done := 0
	for _, op := range history {
		if op.Done {
			done++
		}
	}
	result := harness.Check(history)
	passed = result == porcupine.Ok && float64(done) >= MinCompletion*float64(len(history))
	return passed, fmt.Sprintf("seed %d: %v, %d of %d completed, %+v", seed, result, done, len(history), cfg)
}

func main() {
	if len(os.Args) > 1 {
		seed, err := strconv.ParseInt(os.Args[1], 10, 64)
		if err != nil {
			fmt.Println("usage: harness [seed]")
			os.Exit(2)
		}
		_, summary := run(seed)
		fmt.Println(summary)
		fmt.Println("replay identical:", slices.Equal(harness.Run(configFor(seed)), harness.Run(configFor(seed))))
		return
	}

	start := time.Now()
	failed := 0
	for seed := int64(1); seed <= Runs; seed++ {
		if passed, summary := run(seed); !passed {
			failed++
			fmt.Println("FAILED", summary)
		}
	}
	fmt.Printf("%d seeds, %d failed, %v\n", Runs, failed, time.Since(start).Round(time.Second))
}
