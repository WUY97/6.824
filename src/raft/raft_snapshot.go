package raft

import "fmt"

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || reply.Term > rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
		}
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		return
	}

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// rf.votedFor = -1

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotValid: true,
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.trimLog(args.LastIncludedIndex)

	if rf.log[0].Term != args.LastIncludedTerm {
		panic(fmt.Sprintf("Mismatched term after snapshot: %d != %d", rf.log[0].Term, rf.lastIncludedTerm))
	}

	reply.Term = rf.currentTerm

	rf.persist()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.persist()

	rf.trimLog(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[0].Term

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

func (rf *Raft) trimLog(index int) {
	sliceIndex := rf.getRelativeIndex(index)

	if sliceIndex < 0 {
		rf.log = []LogEntry{}
		return
	}

	if sliceIndex >= len(rf.log) {
		return
	}

	entries := make([]LogEntry, len(rf.log)-sliceIndex)
	copy(entries, rf.log[sliceIndex:])
	rf.log = entries

	rf.persist()
}
