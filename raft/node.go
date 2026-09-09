package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)

type ApplyMsg struct {
	Command      []byte
	CommandIndex int
}

type Node struct {
	mu    sync.Mutex
	id    NodeID
	peers []NodeID

	currentTerm int
	votedFor    NodeID
	log         []LogEntry

	commitIndex int
	lastApplied int
	role        Role

	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int

	transport      Transport
	storage        Storage
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	rng            *rand.Rand
	electionTimer  *time.Timer
	dead           int32
	stopCh         chan struct{}
}

func NewNode(id NodeID, peers []NodeID, transport Transport, storage Storage, applyCh chan ApplyMsg, seed int64) *Node {
	n := &Node{
		id:         id,
		peers:      peers,
		votedFor:   NullVote,
		log:        make([]LogEntry, 1),
		role:       Follower,
		nextIndex:  make(map[NodeID]int),
		matchIndex: make(map[NodeID]int),
		transport:  transport,
		storage:    storage,
		applyCh:    applyCh,
		rng:        rand.New(rand.NewSource(seed)),
		stopCh:     make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	return n
}

func (n *Node) Start() {
	n.mu.Lock()
	n.restoreFromStorage()
	n.resetElectionTimer()
	n.mu.Unlock()
	go n.electionTicker()
	go n.applier()
}

// A node that cannot read its persisted state must not start with amnesia (fail-stop).
func (n *Node) restoreFromStorage() {
	term, votedFor, err := n.storage.GetState()
	if err != nil {
		panic(err)
	}
	n.currentTerm = term
	n.votedFor = votedFor

	lastIdx, err := n.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	if lastIdx > 0 {
		entries, err := n.storage.GetLogRange(1, lastIdx)
		if err != nil {
			panic(err)
		}
		n.log = append(n.log[:1], entries...)
	}
}

func (n *Node) Stop() {
	if !atomic.CompareAndSwapInt32(&n.dead, 0, 1) {
		return
	}
	close(n.stopCh)
	n.mu.Lock()
	n.applyCond.Broadcast()
	n.mu.Unlock()
}

func (n *Node) killed() bool {
	return atomic.LoadInt32(&n.dead) == 1
}

func (n *Node) ID() NodeID {
	return n.id
}

func (n *Node) GetState() (int, Role) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role
}

func (n *Node) lastLogIndex() int {
	return len(n.log) - 1
}

func (n *Node) lastLogTerm() int {
	return n.log[n.lastLogIndex()].Term
}

func (n *Node) randomElectionTimeout() time.Duration {
	spread := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	return ElectionTimeoutMin + time.Duration(n.rng.Int63n(spread))
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(n.randomElectionTimeout())
	}
}

func (n *Node) electionTicker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.role != Leader {
				n.startElection()
			}
			n.resetElectionTimer()
			n.mu.Unlock()
		}
	}
}

func (n *Node) persist() {
	if err := n.storage.SetState(n.currentTerm, n.votedFor); err != nil {
		panic(err)
	}
}

func (n *Node) appendToStorage(entries []LogEntry) {
	if err := n.storage.AppendLogs(entries); err != nil {
		panic(err)
	}
}

func (n *Node) truncateStorage(index int) {
	if err := n.storage.TruncateFrom(index); err != nil {
		panic(err)
	}
}

func (n *Node) becomeFollower(term int) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = NullVote
	n.persist()
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1
		n.matchIndex[peer] = 0
	}

	noop := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: nil,
	}
	n.log = append(n.log, noop)
	n.appendToStorage([]LogEntry{noop})
	n.matchIndex[n.id] = noop.Index

	n.broadcastAppendEntries()
	go n.heartbeatTicker()
}

func (n *Node) Submit(command []byte) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return -1, n.currentTerm, false
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.appendToStorage([]LogEntry{entry})
	n.matchIndex[n.id] = entry.Index
	n.broadcastAppendEntries()

	return entry.Index, n.currentTerm, true
}

func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.persist()

	votes := 1
	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()

	for _, peer := range n.peers {
		go func(peer NodeID) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply, err := n.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.killed() || n.currentTerm != term || n.role != Candidate {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) isLogUpToDate(lastIndex, lastTerm int) bool {
	myLastTerm := n.lastLogTerm()
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIndex >= n.lastLogIndex()
}

func (n *Node) heartbeatTicker() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader {
				n.mu.Unlock()
				return
			}
			n.broadcastAppendEntries()
			n.mu.Unlock()
		}
	}
}

func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.peers {
		go n.sendAppendEntries(peer)
	}
}

func (n *Node) sendAppendEntries(peer NodeID) {
	n.mu.Lock()
	if n.killed() || n.role != Leader {
		n.mu.Unlock()
		return
	}

	next := n.nextIndex[peer]
	prevLogIndex := next - 1
	prevLogTerm := 0
	if prevLogIndex > 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	var entries []LogEntry
	if next <= n.lastLogIndex() {
		entries = make([]LogEntry, len(n.log[next:]))
		copy(entries, n.log[next:])
	}

	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	reply, err := n.transport.SendAppendEntries(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.killed() || n.currentTerm != term || n.role != Leader {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		match := prevLogIndex + len(entries)
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = match + 1
		n.advanceCommitIndex()
	} else {
		if reply.ConflictTerm > 0 {
			lastOfTerm := -1
			for i := n.lastLogIndex(); i >= 1; i-- {
				if n.log[i].Term == reply.ConflictTerm {
					lastOfTerm = i
					break
				}
			}
			if lastOfTerm > 0 {
				n.nextIndex[peer] = lastOfTerm + 1
			} else {
				n.nextIndex[peer] = reply.ConflictIndex
			}
		} else if reply.ConflictIndex > 0 {
			n.nextIndex[peer] = reply.ConflictIndex
		} else {
			n.nextIndex[peer]--
		}
		if n.nextIndex[peer] < 1 {
			n.nextIndex[peer] = 1
		}
	}
}

// Only commit entries from the leader's current term
func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx <= n.lastLogIndex(); idx++ {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 {
			n.commitIndex = idx
		}
	}
	n.signalApply()
}

func (n *Node) signalApply() {
	n.applyCond.Signal()
}

func (n *Node) applier() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for n.lastApplied >= n.commitIndex && !n.killed() {
			n.applyCond.Wait()
		}
		if n.killed() {
			return
		}
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			msg := ApplyMsg{
				Command:      n.log[n.lastApplied].Command,
				CommandIndex: n.lastApplied,
			}
			n.mu.Unlock()
			select {
			case n.applyCh <- msg:
			case <-n.stopCh:
				n.mu.Lock()
				return
			}
			n.mu.Lock()
		}
	}
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply.Term = n.currentTerm

	canVote := n.votedFor == NullVote || n.votedFor == args.CandidateID
	if canVote && n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		n.persist()
		n.resetElectionTimer()
		reply.VoteGranted = true
	}

	return reply
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	} else if n.role == Candidate {
		n.role = Follower
	}
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.lastLogIndex() {
			reply.ConflictIndex = n.lastLogIndex() + 1
			return reply
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.ConflictTerm = n.log[args.PrevLogIndex].Term
			idx := args.PrevLogIndex
			for idx > 1 && n.log[idx-1].Term == reply.ConflictTerm {
				idx--
			}
			reply.ConflictIndex = idx
			return reply
		}
	}

	for i, entry := range args.Entries {
		insertIdx := args.PrevLogIndex + 1 + i
		if insertIdx <= n.lastLogIndex() {
			if n.log[insertIdx].Term != entry.Term {
				n.log = n.log[:insertIdx]
				n.truncateStorage(insertIdx)
				n.log = append(n.log, args.Entries[i:]...)
				n.appendToStorage(args.Entries[i:])
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			n.appendToStorage(args.Entries[i:])
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if n.lastLogIndex() < newCommit {
			newCommit = n.lastLogIndex()
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.signalApply()
		}
	}

	reply.Success = true
	return reply
}
