package raft

import (
	"math/rand"
	"slices"
	"time"
)

const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	ElectionTick       = 10 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
	SnapshotThreshold  = 50
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
	save             func() []byte
	restore          func([]byte)
	snapshot         []byte
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

	snapIndex, snapTerm, data, err := n.storage.LoadSnapshot()
	if err != nil {
		panic(err)
	}
	if snapIndex > 0 {
		n.log[0] = LogEntry{Term: snapTerm, Index: snapIndex}
		n.snapshot = data
		n.restore(data)
		n.commitIndex = snapIndex
		n.lastApplied = snapIndex
	}

	lastIdx, err := n.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	if lastIdx > snapIndex {
		entries, err := n.storage.GetLogRange(snapIndex+1, lastIdx)
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
	return n.log[0].Index + len(n.log) - 1
}

func (n *Node) lastLogTerm() int {
	return n.log[len(n.log)-1].Term
}

func (n *Node) entry(index int) LogEntry {
	return n.log[index-n.log[0].Index]
}

func (n *Node) entriesFrom(index int) []LogEntry {
	return n.log[index-n.log[0].Index:]
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

func (n *Node) persistSnapshot() {
	if err := n.storage.SaveSnapshot(n.log[0].Index, n.log[0].Term, n.snapshot); err != nil {
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
	if next <= n.log[0].Index {
		n.transport.SendInstallSnapshot(n.id, peer, InstallSnapshotArgs{
			Term:              n.currentTerm,
			LeaderID:          n.id,
			LastIncludedIndex: n.log[0].Index,
			LastIncludedTerm:  n.log[0].Term,
			Data:              n.snapshot,
		})
		return
	}

	n.transport.SendAppendEntries(n.id, peer, AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  n.entry(next - 1).Term,
		Entries:      slices.Clone(n.entriesFrom(next)),
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
			for i := n.lastLogIndex(); i >= n.log[0].Index; i-- {
				if n.entry(i).Term == reply.ConflictTerm {
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
		if n.entry(idx).Term != n.currentTerm {
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
		n.apply(ApplyMsg{Command: n.entry(n.lastApplied).Command, CommandIndex: n.lastApplied})
	}
	if n.lastApplied-n.log[0].Index >= SnapshotThreshold {
		n.compact()
	}
}

func (n *Node) compact() {
	term := n.entry(n.lastApplied).Term
	n.log = append([]LogEntry{{Term: term, Index: n.lastApplied}}, n.entriesFrom(n.lastApplied+1)...)
	n.snapshot = n.save()
	n.persistSnapshot()
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

	if args.PrevLogIndex < n.log[0].Index {
		reply.ConflictIndex = n.log[0].Index + 1
		return reply
	}
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.lastLogIndex() {
			reply.ConflictIndex = n.lastLogIndex() + 1
			return reply
		}
		if n.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.ConflictTerm = n.entry(args.PrevLogIndex).Term
			idx := args.PrevLogIndex
			for idx > n.log[0].Index+1 && n.entry(idx-1).Term == reply.ConflictTerm {
				idx--
			}
			reply.ConflictIndex = idx
			return reply
		}
	}

	for i, entry := range args.Entries {
		insertIdx := args.PrevLogIndex + 1 + i
		if insertIdx <= n.lastLogIndex() {
			if n.entry(insertIdx).Term != entry.Term {
				n.log = n.log[:insertIdx-n.log[0].Index]
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

// Entries after a matching snapshot point may already be committed, so only a mismatched or shorter log is discarded.
func (n *Node) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	reply := InstallSnapshotReply{Term: n.currentTerm}

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

	if args.LastIncludedIndex <= n.commitIndex {
		return reply
	}

	var suffix []LogEntry
	if args.LastIncludedIndex <= n.lastLogIndex() && n.entry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		suffix = n.entriesFrom(args.LastIncludedIndex + 1)
	} else {
		n.truncateStorage(args.LastIncludedIndex + 1)
	}
	n.log = append([]LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}, suffix...)
	n.snapshot = args.Data
	n.persistSnapshot()
	n.restore(args.Data)
	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex
	return reply
}

func (n *Node) HandleInstallSnapshotReply(from NodeID, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if n.killed() || args.Term != n.currentTerm || n.role != Leader {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.resetElectionTimer()
		return
	}
	n.matchIndex[from] = max(n.matchIndex[from], args.LastIncludedIndex)
	n.nextIndex[from] = n.matchIndex[from] + 1
}
