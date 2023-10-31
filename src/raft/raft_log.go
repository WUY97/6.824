package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	rf.sendToChannel(rf.heartbeatCh, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.PrevLogIndex > rf.lastIncludedIndex {
		reply.NeedSnapshot = true
		return
	}

	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	baseIndex := rf.lastIncludedIndex + 1

	if args.PrevLogIndex-baseIndex < 0 || args.PrevLogIndex-baseIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	if conflictTerm := rf.log[args.PrevLogIndex-baseIndex].Term; conflictTerm != args.PrevLogTerm {
		reply.ConflictTerm = conflictTerm
		for i := args.PrevLogIndex - 1; i >= baseIndex && rf.log[i-baseIndex].Term == conflictTerm; i-- {
			reply.ConflictIndex = i
		}
		reply.Success = false
		return
	}

	i, j := args.PrevLogIndex+1, 0
	for ; i < lastIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i-baseIndex].Term != args.Entries[j].Term {
			break
		}
	}
	rf.log = rf.log[:i-baseIndex]
	args.Entries = args.Entries[j:]
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		go rf.applyLogEntries()
	}
}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		return
	}

	if reply.NeedSnapshot {
		snapshot := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		snapshotReply := &InstallSnapshotReply{}
		go rf.sendSnapshot(server, snapshot, snapshotReply)
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		newNextIndex := rf.getLastIndex()
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.log[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}

		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = newNextIndex
		}

		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	for i := rf.getLastIndex(); i > rf.commitIndex; i-- {
		if rf.log[i].Term == rf.currentTerm {
			count := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}

			if count*2 > len(rf.peers) {
				rf.commitIndex = i
				go rf.applyLogEntries()
				break
			}
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}

	for i := range rf.peers {
		if i != rf.me {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			entries := rf.log[rf.nextIndex[i]:]
			args.Entries = make([]LogEntry, len(entries))
			// make a deep copy of the entries to send
			copy(args.Entries, entries)
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
		}
	}
}
