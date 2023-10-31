package raft

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if !rf.CondInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, args.Data) {
		return
	}

	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
	}

	offset := rf.lastIncludedIndex - rf.getLastIndex() + len(rf.log)

	if offset >= 0 && offset < len(rf.log) {
		rf.log = rf.log[offset:]
	} else if offset >= len(rf.log) {
		rf.log = []LogEntry{}
	}
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	if rf.lastIncludedTerm > lastIncludedTerm || rf.lastIncludedIndex > lastIncludedIndex {
		return false
	}

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex

	// discard old log entries
	newEntries := []LogEntry{}
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == lastIncludedTerm && i <= lastIncludedIndex {
			newEntries = rf.log[i+1:]
			break
		}
	}

	rf.log = newEntries

	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}

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

	newEntries := []LogEntry{}
	var lastIncludedTerm int
	if len(rf.log) > 0 {
		for i, entry := range rf.log {
			if i > index-rf.lastIncludedIndex {
				newEntries = append(newEntries, entry)
				if i == index-rf.lastIncludedIndex {
					lastIncludedTerm = entry.Term
				}
			}
		}
	}

	rf.log = newEntries
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.adjustNextIndex()
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

}

func (rf *Raft) adjustNextIndex() {
	for i := range rf.nextIndex {
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			rf.nextIndex[i] = rf.lastIncludedIndex + 1
		}
	}
}
