package raft

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.convertToFollower(args.Term)
	}

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	message := ApplyMsg{
		CommandValid:  false,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotValid: true,
	}

	rf.applyCh <- message

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.offset = args.LastIncludedIndex + 1

	rf.persist()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastIncludedIndex {
		return false
	}

	rf.trimLog(lastIncludedIndex)

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.trimLog(index)

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

func (rf *Raft) trimLog(index int) {
	sliceIndex := index - rf.offset

	if sliceIndex < 0 {
		rf.log = []LogEntry{}
		return
	}

	if sliceIndex >= len(rf.log) {
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[sliceIndex].Term
	rf.offset = index + 1
	rf.log = rf.log[sliceIndex+1:]

	rf.persist()
}
