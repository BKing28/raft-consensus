package raft

type Transport interface {
	SendRequestVote(from, to NodeID, args RequestVoteArgs)
	SendAppendEntries(from, to NodeID, args AppendEntriesArgs)
	SendInstallSnapshot(from, to NodeID, args InstallSnapshotArgs)
}
