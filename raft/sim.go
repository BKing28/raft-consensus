package raft

import (
	"container/heap"
	"math/rand"
	"time"
)

type event struct {
	at  time.Duration
	seq int
	fn  func()
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(event)) }

func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

type Sim struct {
	now   time.Duration
	seq   int
	queue eventQueue
	rng   *rand.Rand
}

func NewSim(seed int64) *Sim {
	return &Sim{rng: rand.New(rand.NewSource(seed))}
}

func (s *Sim) Now() time.Duration {
	return s.now
}

func (s *Sim) After(delay time.Duration, fn func()) {
	heap.Push(&s.queue, event{s.now + delay, s.seq, fn})
	s.seq++
}

func (s *Sim) Run(d time.Duration) {
	end := s.now + d
	for len(s.queue) > 0 && s.queue[0].at <= end {
		s.step()
	}
	s.now = end
}

func (s *Sim) RunUntil(cond func() bool, limit time.Duration) bool {
	end := s.now + limit
	for !cond() {
		if len(s.queue) == 0 || s.queue[0].at > end {
			s.now = end
			return false
		}
		s.step()
	}
	return true
}

func (s *Sim) step() {
	e := heap.Pop(&s.queue).(event)
	s.now = e.at
	e.fn()
}
