package raft

func (rf *Raft) convertToCandidate(fromState NodeState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	rf.broadcastRequestVote()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
	}

	rf.broadcastAppendEntries()
}

func (rf *Raft) convertToFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1

	if state != Follower {
		rf.sendToChannel(rf.stepDownCh, true)
	}
}
