package shardctrler

import (
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

	latestRequestId map[int64]int64
	waitingOps      map[int]chan Op
}

type Op struct {
	// Your data here.
	Type      string
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	QueryArgs *QueryArgs

	ClientId  int64
	RequestId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:      JOIN,
		JoinArgs:  args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := sc.waitForApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:      LEAVE,
		LeaveArgs: args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := sc.waitForApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      MOVE,
		MoveArgs:  args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := sc.waitForApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      QUERY,
		QueryArgs: args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := sc.waitForApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
	if args.Num < 0 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
}

func (sc *ShardCtrler) waitForApplied(op Op) (bool, Op) {
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		return false, op
	}

	sc.mu.Lock()
	opCh, ok := sc.waitingOps[index]
	if !ok {
		opCh = make(chan Op, 1)
		sc.waitingOps[index] = opCh
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-opCh:
		sc.mu.Lock()
		delete(sc.waitingOps, index)
		sc.mu.Unlock()
		return sc.isSameOp(op, appliedOp), appliedOp
	case <-time.After(500 * time.Millisecond):
		return false, op
	}
}

func (sc *ShardCtrler) isSameOp(issued Op, applied Op) bool {
	return issued.ClientId == applied.ClientId && issued.RequestId == applied.RequestId
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
	sc.latestRequestId = make(map[int64]int64)
	sc.waitingOps = make(map[int]chan Op)

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if !msg.CommandValid {
			continue
		}

		sc.mu.Lock()
		index := msg.CommandIndex
		op := msg.Command.(Op)

		if op.Type == QUERY {
			sc.applyOp(&op)
		} else {
			lastId, ok := sc.latestRequestId[op.ClientId]
			if !ok || op.RequestId > lastId {
				sc.applyOp(&op)
				sc.latestRequestId[op.ClientId] = op.RequestId
			}
		}

		ch, ok := sc.waitingOps[index]
		if !ok {
			ch = make(chan Op, 1)
			sc.waitingOps[index] = ch
		}
		ch <- op
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyOp(op *Op) {
	switch op.Type {
	case JOIN:
		sc.applyJoin(op.JoinArgs)
	case LEAVE:
		sc.applyLeave(op.LeaveArgs)
	case MOVE:
		sc.applyMove(op.MoveArgs)
	case QUERY:
	default:
	}
}

func (sc *ShardCtrler) applyJoin(args *JoinArgs) {
	newConfig := sc.createNewConfig()
	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	sc.rebalanceShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyLeave(args *LeaveArgs) {
	newConfig := sc.createNewConfig()
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}

	sc.rebalanceShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyMove(args *MoveArgs) {
	newConfig := sc.createNewConfig()
	newConfig.Shards[args.Shard] = args.GID

	sc.configs = append(sc.configs, newConfig)
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
