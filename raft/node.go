package raft

import (
	"math/rand"
	"time"
)

const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	ElectionTick       = 10 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)

type ApplyMsg struct {
	Command      []byte
	CommandIndex int
}

type Node struct {
	id    NodeID
	peers []NodeID

	currentTerm int
	votedFor    NodeID
	log         []LogEntry

	commitIndex int
	lastApplied int
	role        Role
	votes       map[NodeID]bool

	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int

	transport        Transport
	storage          Storage
	apply            func(ApplyMsg)
	rng              *rand.Rand
	now              time.Duration
	electionDeadline time.Duration
	nextHeartbeat    time.Duration
	dead             bool
}

func NewNode(id NodeID, peers []NodeID, transport Transport, storage Storage, seed int64) *Node {
	return &Node{
		id:         id,
		peers:      peers,
		votedFor:   NullVote,
		log:        make([]LogEntry, 1),
		role:       Follower,
		nextIndex:  make(map[NodeID]int),
		matchIndex: make(map[NodeID]int),
		transport:  transport,
		storage:    storage,
		rng:        rand.New(rand.NewSource(seed)),
	}
}

func (n *Node) Start(now time.Duration) {
	n.now = now
	n.restoreFromStorage()
	n.resetElectionTimer()
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
	n.dead = true
}

func (n *Node) killed() bool {
	return n.dead
}

func (n *Node) ID() NodeID {
	return n.id
}

func (n *Node) GetState() (int, Role) {
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
	n.electionDeadline = n.now + n.randomElectionTimeout()
}

// Handlers reset deadlines from the last tick time, so they can be up to one tick stale.
func (n *Node) Tick(now time.Duration) {
	if n.killed() {
		return
	}
	n.now = now
	if n.role != Leader && now >= n.electionDeadline {
		n.startElection()
		n.resetElectionTimer()
	}
	if n.role == Leader && now >= n.nextHeartbeat {
		n.broadcastAppendEntries()
		n.nextHeartbeat = now + HeartbeatInterval
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
	n.advanceCommitIndex()

	n.broadcastAppendEntries()
	n.nextHeartbeat = n.now + HeartbeatInterval
}

func (n *Node) Submit(command []byte) {
	if n.killed() || n.role != Leader {
		return
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.appendToStorage([]LogEntry{entry})
	n.matchIndex[n.id] = entry.Index
	n.advanceCommitIndex()
	n.broadcastAppendEntries()
}

func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.persist()
	if len(n.peers) == 0 {
		n.becomeLeader()
		return
	}

	n.votes = map[NodeID]bool{n.id: true}
	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	for _, peer := range n.peers {
		n.transport.SendRequestVote(n.id, peer, args)
	}
}

// Votes are a set because a duplicated request produces a duplicated reply.
func (n *Node) HandleRequestVoteReply(from NodeID, args RequestVoteArgs, reply RequestVoteReply) {
	if n.killed() || args.Term != n.currentTerm || n.role != Candidate {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.resetElectionTimer()
		return
	}
	if reply.VoteGranted {
		n.votes[from] = true
		if len(n.votes) > (len(n.peers)+1)/2 {
			n.becomeLeader()
		}
	}
}

func (n *Node) isLogUpToDate(lastIndex, lastTerm int) bool {
	myLastTerm := n.lastLogTerm()
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIndex >= n.lastLogIndex()
}

func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.peers {
		n.sendAppendEntries(peer)
	}
}

func (n *Node) sendAppendEntries(peer NodeID) {
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

	n.transport.SendAppendEntries(n.id, peer, AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) HandleAppendEntriesReply(from NodeID, args AppendEntriesArgs, reply AppendEntriesReply) {
	if n.killed() || args.Term != n.currentTerm || n.role != Leader {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.resetElectionTimer()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > n.matchIndex[from] {
			n.matchIndex[from] = match
		}
		n.nextIndex[from] = match + 1
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
				n.nextIndex[from] = lastOfTerm + 1
			} else {
				n.nextIndex[from] = reply.ConflictIndex
			}
		} else if reply.ConflictIndex > 0 {
			n.nextIndex[from] = reply.ConflictIndex
		} else {
			n.nextIndex[from]--
		}
		if n.nextIndex[from] < 1 {
			n.nextIndex[from] = 1
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
	n.applyCommitted()
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		n.apply(ApplyMsg{Command: n.log[n.lastApplied].Command, CommandIndex: n.lastApplied})
	}
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
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
			n.applyCommitted()
		}
	}

	reply.Success = true
	return reply
}
