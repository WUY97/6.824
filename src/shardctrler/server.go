package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	latestAppliedRequest   map[int64]int64
	waitChs                map[int]chan WaitChResponse
	latestAppliedRaftIndex int
}

type WaitChResponse struct {
	err    Err
	config Config
}

type Op struct {
	// Your data here.
	Type      string
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs
	QueryArgs QueryArgs

	ClientId  int64
	RequestId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	raftIndex, _, isLeader := sc.rf.Start(Op{Type: JOIN, ClientId: args.ClientId, RequestId: args.RequestId, JoinArgs: *args})
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		sc.mu.Lock()
		_, exist := sc.waitChs[raftIndex]
		if exist {
			log.Fatalf("shardctrler server %d try to get a existing waitCh\n", sc.me)
		}
		sc.waitChs[raftIndex] = make(chan WaitChResponse, 1)
		waitCh := sc.waitChs[raftIndex]
		sc.mu.Unlock()
		select {
		case <-time.After(Timeout):
			reply.Err = ErrWrongLeader
		case response := <-waitCh:
			reply.Err = response.err
		}
		sc.mu.Lock()
		delete(sc.waitChs, raftIndex)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	raftIndex, _, isLeader := sc.rf.Start(Op{Type: LEAVE, ClientId: args.ClientId, RequestId: args.RequestId, LeaveArgs: *args})
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		sc.mu.Lock()
		_, exist := sc.waitChs[raftIndex]
		if exist {
			log.Fatalf("shardctrler server %d try to get a existing waitCh\n", sc.me)
		}
		sc.waitChs[raftIndex] = make(chan WaitChResponse, 1)
		waitCh := sc.waitChs[raftIndex]
		sc.mu.Unlock()
		select {
		case <-time.After(time.Millisecond * Timeout):
			reply.Err = ErrWrongLeader
		case response := <-waitCh:
			reply.Err = response.err
		}

		sc.mu.Lock()
		delete(sc.waitChs, raftIndex)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	raftIndex, _, isLeader := sc.rf.Start(Op{Type: MOVE, ClientId: args.ClientId, RequestId: args.RequestId, MoveArgs: *args})
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		sc.mu.Lock()
		_, exist := sc.waitChs[raftIndex]
		if exist {
			log.Fatalf("shardctrler server %d try to get a existing waitCh\n", sc.me)
		}
		sc.waitChs[raftIndex] = make(chan WaitChResponse, 1)
		waitCh := sc.waitChs[raftIndex]
		sc.mu.Unlock()
		select {
		case <-time.After(time.Millisecond * Timeout):
			reply.Err = ErrWrongLeader
		case response := <-waitCh:
			reply.Err = response.err
		}
		sc.mu.Lock()
		delete(sc.waitChs, raftIndex)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	raftIndex, _, isLeader := sc.rf.Start(Op{Type: QUERY, ClientId: args.ClientId, RequestId: args.RequestId, QueryArgs: *args})
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		sc.mu.Lock()
		_, exist := sc.waitChs[raftIndex]
		if exist {
			log.Fatalf("shardctrler server %d try to get a existing waitCh\n", sc.me)
		}
		sc.waitChs[raftIndex] = make(chan WaitChResponse, 1)
		waitCh := sc.waitChs[raftIndex]
		sc.mu.Unlock()
		select {
		case <-time.After(time.Millisecond * Timeout):
			reply.Err = ErrWrongLeader
		case response := <-waitCh:
			reply.Err = response.err
			reply.Config = response.config
		}
		sc.mu.Lock()
		delete(sc.waitChs, raftIndex)
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.latestAppliedRequest = make(map[int64]int64)
	sc.waitChs = make(map[int]chan WaitChResponse)
	sc.latestAppliedRaftIndex = 0

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if !msg.CommandValid {
			continue
		}

		index := msg.CommandIndex

		if index <= sc.latestAppliedRaftIndex {
			continue
		}

		op := msg.Command.(Op)
		waitChResponse := WaitChResponse{}

		duplicated := true
		lastId, ok := sc.latestAppliedRequest[op.ClientId]
		if !ok || lastId < op.RequestId {
			duplicated = false
			sc.latestAppliedRequest[op.ClientId] = op.RequestId
		}

		switch op.Type {
		case JOIN:
			if duplicated {
				waitChResponse.err = OK
			} else {
				sc.applyJoin(op.JoinArgs, &waitChResponse)
			}
		case LEAVE:
			if duplicated {
				waitChResponse.err = OK
			} else {
				sc.applyLeave(op.LeaveArgs, &waitChResponse)
			}
		case MOVE:
			if duplicated {
				waitChResponse.err = OK
			} else {
				sc.applyMove(op.MoveArgs, &waitChResponse)
			}
		case QUERY:
			sc.applyQuery(op.QueryArgs, &waitChResponse)
		}

		sc.mu.Lock()
		sc.latestAppliedRaftIndex = index
		waitCh, exist := sc.waitChs[index]
		sc.mu.Unlock()
		if exist {
			waitCh <- waitChResponse
		}
	}
}

func (sc *ShardCtrler) applyJoin(args JoinArgs, waitChResponse *WaitChResponse) {
	newConfig := sc.createNewConfig()

	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	newGroups := false
	for gid := range args.Servers {
		if _, ok := sc.configs[len(sc.configs)-1].Groups[gid]; !ok {
			newGroups = true
			break
		}
	}
	if !newGroups {
		return
	}

	sc.rebalanceShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)
	waitChResponse.err = OK
}

func (sc *ShardCtrler) applyLeave(args LeaveArgs, waitChResponse *WaitChResponse) {
	newConfig := sc.createNewConfig()
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}

	sc.rebalanceShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)
	waitChResponse.err = OK

}

func (sc *ShardCtrler) applyMove(args MoveArgs, waitChResponse *WaitChResponse) {
	newConfig := sc.createNewConfig()
	newConfig.Shards[args.Shard] = args.GID

	sc.configs = append(sc.configs, newConfig)
	waitChResponse.err = OK
}

func (sc *ShardCtrler) applyQuery(args QueryArgs, waitChResponse *WaitChResponse) {
	if args.Num < 0 || args.Num >= len(sc.configs)-1 {
		waitChResponse.config = sc.configs[len(sc.configs)-1]
	} else {
		waitChResponse.config = sc.configs[args.Num]
	}
	waitChResponse.err = OK
}

func (sc *ShardCtrler) createNewConfig() Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}

	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	return newConfig
}

func (sc *ShardCtrler) rebalanceShards(config *Config) {
	groupCount := len(config.Groups)
	if groupCount == 0 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	} else if groupCount == 1 {
		for gid := range config.Groups {
			for i := range config.Shards {
				config.Shards[i] = gid
			}
		}
	} else {
		shardsPerGroup := NShards / len(config.Groups)
		extraShards := NShards % len(config.Groups)

		gids := make([]int, 0, len(config.Groups))
		for gid := range config.Groups {
			gids = append(gids, gid)
		}
		sort.Ints(gids) // Ensure a consistent ordering of GIDs.

		shardIdx := 0
		for _, gid := range gids {
			// Assign the base number of shards to each group.
			for i := 0; i < shardsPerGroup; i++ {
				config.Shards[shardIdx%NShards] = gid
				shardIdx++
			}
		}

		for i := 0; i < extraShards; i++ {
			config.Shards[shardIdx%NShards] = gids[i]
			shardIdx++
		}
	}
}
