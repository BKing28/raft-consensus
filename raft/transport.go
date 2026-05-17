package raft

type Transport interface {
	SendRequestVote(target NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(target NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
	SendInstallSnapshot(target NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}
