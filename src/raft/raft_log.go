package raft

import (
	"math"
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	reply.NeedSnapshot = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.sendToChannel(rf.heartbeatCh, true)

	absoluteLastIndex := rf.getAbsoluteLastIndex()
	relativeLastIndex := rf.getRelativeLastIndex()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.NeedSnapshot = true
		return
	}

	if args.PrevLogIndex > absoluteLastIndex {
		reply.ConflictIndex = absoluteLastIndex + 1
		return
	}

	relativePrevLogIndex := rf.getRelativeIndex(args.PrevLogIndex)
	if conflictTerm := rf.log[relativePrevLogIndex].Term; conflictTerm != args.PrevLogTerm {
		reply.ConflictTerm = conflictTerm
		for i := relativePrevLogIndex; i >= 0 && rf.log[i].Term == conflictTerm; i-- {
			reply.ConflictIndex = rf.getAbsoluteIndex(i)
		}
		return
	}

	i, j := relativePrevLogIndex+1, 0
	for ; i <= relativeLastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}
	rf.log = rf.log[:i]
	if len(args.Entries) > j {
		args.Entries = args.Entries[j:]
		rf.log = append(rf.log, args.Entries...)
	}

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(absoluteLastIndex)))
		go rf.applyLogEntries()
	}
}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		if i <= rf.lastIncludedIndex {
			continue
		}

		relativeIndex := rf.getRelativeIndex(i)
		if relativeIndex >= len(rf.log) || relativeIndex < 0 {
			continue
		}

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[relativeIndex].Command,
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

	if rf.state != Leader || reply.Term > rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
		}
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
		newNextIndex := rf.getRelativeLastIndex()
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.log[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}

		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = rf.getAbsoluteIndex(newNextIndex)
		}

		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	for i := rf.getRelativeLastIndex(); i > rf.getRelativeIndex(rf.commitIndex); i-- {
		if rf.log[i].Term == rf.currentTerm {
			count := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}

			if count*2 > len(rf.peers) {
				rf.commitIndex = rf.getAbsoluteIndex(i)
				go rf.applyLogEntries()
				break
			}
		}
	}
}
