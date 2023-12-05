package shardkv

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db              [shardctrler.NShards]map[string]string
	latestRequestId map[int64]int64
	waitingOps      map[int]chan Op

	lastApplied int

	mck *shardctrler.Clerk
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, appliedOp := kv.waitForApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = appliedOp.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := kv.waitForApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

func (kv *ShardKV) waitForApplied(op Op) (bool, Op) {
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return false, op
	}

	kv.mu.Lock()
	opCh, ok := kv.waitingOps[index]
	if !ok {
		opCh = make(chan Op, 1)
		kv.waitingOps[index] = opCh
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-opCh:
		return kv.isSameOp(op, appliedOp), appliedOp
	case <-time.After(200 * time.Millisecond):
		return false, op
	}
}

func (kv *ShardKV) isSameOp(issued Op, applied Op) bool {
	return issued.ClientId == applied.ClientId && issued.RequestId == applied.RequestId
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	for i := 0; i < shardctrler.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}
	kv.latestRequestId = make(map[int64]int64)
	kv.waitingOps = make(map[int]chan Op)

	// kv.readSnapshot(persister.ReadSnapshot())
	kv.lastApplied = 0

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()

	return kv
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)

			kv.mu.Lock()

			if index <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = index

			if op.Type == "Get" {
				kv.applyOp(&op)
			} else {
				lastId, ok := kv.latestRequestId[op.ClientId]
				if !ok || lastId < op.RequestId {
					kv.applyOp(&op)
					kv.latestRequestId[op.ClientId] = op.RequestId
				}
			}

			ch, ok := kv.waitingOps[index]
			if !ok {
				ch = make(chan Op, 1)
				kv.waitingOps[index] = ch
			}
			ch <- op

			// if kv.needSnapshot() {
			// 	kv.createSnapshot(index)
			// }
			kv.mu.Unlock()
			// } else if msg.SnapshotValid {
			// 	kv.mu.Lock()
			// 	kv.readSnapshot(msg.Snapshot)
			// 	kv.lastApplied = msg.SnapshotIndex
			// 	kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyOp(op *Op) {
	switch op.Type {
	case "Put":
		// kv.db[op.Key] = op.Value
		kv.db[key2shard(op.Key)][op.Key] = op.Value
	case "Append":
		// kv.db[op.Key] += op.Value
		kv.db[key2shard(op.Key)][op.Key] += op.Value
	case "Get":
		// op.Value = kv.db[op.Key]
		op.Value = kv.db[key2shard(op.Key)][op.Key]
	}
}

// func (kv *ShardKV) needSnapshot() bool {
// 	return kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate
// }

// func (kv *ShardKV) createSnapshot(index int) {
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	e.Encode(kv.db)
// 	e.Encode(kv.latestRequestId)
// 	snapshot := w.Bytes()
// 	kv.rf.Snapshot(index, snapshot)
// }

// func (kv *ShardKV) readSnapshot(snapshot []byte) {
// 	if snapshot == nil || len(snapshot) < 1 {
// 		return
// 	}
// 	r := bytes.NewBuffer(snapshot)
// 	d := labgob.NewDecoder(r)
// 	var db map[string]string
// 	var latestRequestId map[int64]int64
// 	if d.Decode(&db) != nil || d.Decode(&latestRequestId) != nil {
// 		panic("read snapshot error")
// 	} else {
// 		kv.db = db
// 		kv.latestRequestId = latestRequestId
// 	}
// }
