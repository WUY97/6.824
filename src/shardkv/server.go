package shardkv

import (
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	GET           = 0
	PUT           = 1
	APPEND        = 2
	UPDATE_CONFIG = 3 // WORKING -> others
	PULL_SHARD    = 4 // get data, PULLING -> WORKING
	LEAVE_SHARD   = 5 // remove data, LEAVING -> nil
	TRANSFER_DONE = 6 // confirm transfer done, WAITING -> WORKING
	EMPTY_ENTRY   = 7 // empty entry to make sure log entry is in current term
)

type OpType int

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type          OpType
	Key           string
	Value         string
	ClientId      int64
	RequestId     int64
	Config        shardctrler.Config
	ShardId       int
	ConfigVersion int
	Storage       map[string]string
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
	shards     map[int]*Shard
	prevConfig shardctrler.Config
	currConfig shardctrler.Config
	// raft log index -> wait channel
	waitChs                map[int]chan WaitChResponse
	latestAppliedRequest   map[int64]int64 // clientId -> requestId
	latestAppliedRaftIndex int

	mck *shardctrler.Clerk
}

type Shard struct {
	Storage sync.Map
	Status  ShardStatus
}

type SnapshotShard struct {
	StorageMap map[string]string
	Status     ShardStatus
}

type ShardStatus int

const (
	WORKING = 0
	PULLING = 1
	LEAVING = 2
	WAITING = 3
)

type WaitChResponse struct {
	err   Err
	value string // for Get
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	raftIndex, _, isLeader := kv.rf.Start(Op{
		Type:      GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	_, exist := kv.waitChs[raftIndex]
	if exist {
		log.Fatalf("shardkv | server %d try to get a existing waitCh\n", kv.me)
	}
	kv.waitChs[raftIndex] = make(chan WaitChResponse, 1)
	waitCh := kv.waitChs[raftIndex]
	kv.mu.Unlock()

	select {
	case <-time.After(RaftTimeOut):
		kv.DPrintf("server %d timeout when handling Get\n", kv.me)
		reply.Err = ErrWrongLeader
	case response := <-waitCh:
		reply.Value = response.value
		reply.Err = response.err
	}

	kv.mu.Lock()
	delete(kv.waitChs, raftIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var opType OpType
	if args.Op == "Put" {
		opType = PUT
	} else {
		opType = APPEND
	}
	raftIndex, _, isLeader := kv.rf.Start(Op{
		Type:      opType,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	_, exist := kv.waitChs[raftIndex]
	if exist {
		log.Fatalf("shardkv | server %d try to get a existing waitCh\n", kv.me)
	}
	kv.waitChs[raftIndex] = make(chan WaitChResponse, 1)
	waitCh := kv.waitChs[raftIndex]
	kv.mu.Unlock()

	select {
	case <-time.After(RaftTimeOut):
		kv.DPrintf("server %d timeout when handling PutAppend\n", kv.me)
		reply.Err = ErrWrongLeader
	case response := <-waitCh:
		reply.Err = response.err
	}

	kv.mu.Lock()
	delete(kv.waitChs, raftIndex)
	kv.mu.Unlock()
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
	kv.shards = make(map[int]*Shard)

	kv.currConfig = shardctrler.Config{Groups: map[int][]string{}}
	kv.prevConfig = kv.currConfig

	kv.waitChs = make(map[int]chan WaitChResponse)
	kv.latestAppliedRequest = make(map[int64]int64)
	kv.latestAppliedRaftIndex = 0

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.fetchNextConfig()
	go kv.checkEntryInCurrentTerm()

	return kv
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)

			kv.mu.Lock()
			if index <= kv.latestAppliedRaftIndex {
				kv.DPrintf("server %d get older command from applyCh\n", kv.me)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			waitChResponse := WaitChResponse{}
			switch op.Type {
			case GET:
				kv.handleGet(op, &waitChResponse)
			case PUT:
				kv.handlePut(op, &waitChResponse)
			case APPEND:
				kv.handleAppend(op, &waitChResponse)
			case UPDATE_CONFIG:
				kv.handleUpdateConfig(op, &waitChResponse)
			case PULL_SHARD:
				kv.handlePullShard(op, &waitChResponse)
			case LEAVE_SHARD:
				kv.handleLeaveShard(op, &waitChResponse)
			case TRANSFER_DONE:
				kv.handleTransferDone(op, &waitChResponse)
			case EMPTY_ENTRY:
				kv.handleEmptyEntry(&waitChResponse)
			}

			kv.latestAppliedRaftIndex = index

			if kv.needSnapshot() {
				kv.createSnapshot(index)
			}

			kv.mu.Lock()
			waitCh, exist := kv.waitChs[index]
			kv.mu.Unlock()

			if exist {
				waitCh <- waitChResponse
			}
		} else if msg.SnapshotValid {
			if msg.SnapshotIndex > kv.latestAppliedRaftIndex {
				kv.DPrintf("server %d get snapshot newer than state machine\n", kv.me)
				kv.readSnapshot(msg.Snapshot)
				kv.latestAppliedRaftIndex = msg.SnapshotIndex
			}
		}
	}
}

func (kv *ShardKV) handleGet(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard, shardExist := kv.shards[key2shard(op.Key)]
	if !shardExist || (shard.Status != WORKING && shard.Status != WAITING) {
		waitChResponse.err = ErrWrongGroup
		return
	}

	_, exist := kv.latestAppliedRequest[op.ClientId]
	if !exist {
		kv.latestAppliedRequest[op.ClientId] = -1
	}

	if op.RequestId > kv.latestAppliedRequest[op.ClientId] {
		kv.latestAppliedRequest[op.ClientId] = op.RequestId
	}

	value, keyExist := shard.Storage.Load(op.Key)
	if !keyExist {
		waitChResponse.err = ErrNoKey
		return
	}

	waitChResponse.err = OK
	waitChResponse.value = value.(string)
}

func (kv *ShardKV) handlePut(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.DPrintf("server %d put key: %s, value: %s\n", kv.me, op.Key, op.Value)

	shard, shardExist := kv.shards[key2shard(op.Key)]
	if !shardExist || (shard.Status != WORKING && shard.Status != WAITING) {
		waitChResponse.err = ErrWrongGroup
		return
	}

	_, exist := kv.latestAppliedRequest[op.ClientId]
	if !exist {
		kv.latestAppliedRequest[op.ClientId] = -1
	}

	if op.RequestId > kv.latestAppliedRequest[op.ClientId] {
		kv.latestAppliedRequest[op.ClientId] = op.RequestId
		shard.Storage.Store(op.Key, op.Value)
	}

	waitChResponse.err = OK
}

func (kv *ShardKV) handleAppend(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.DPrintf("server %d append key: %s, value: %s\n", kv.me, op.Key, op.Value)

	shard, shardExist := kv.shards[key2shard(op.Key)]
	if !shardExist || (shard.Status != WORKING && shard.Status != WAITING) {
		waitChResponse.err = ErrWrongGroup
		return
	}

	_, exist := kv.latestAppliedRequest[op.ClientId]
	if !exist {
		kv.latestAppliedRequest[op.ClientId] = -1
	}

	if op.RequestId > kv.latestAppliedRequest[op.ClientId] {
		kv.latestAppliedRequest[op.ClientId] = op.RequestId
		value, keyExist := shard.Storage.Load(op.Key)
		if keyExist {
			shard.Storage.Store(op.Key, value.(string)+op.Value)
		} else {
			shard.Storage.Store(op.Key, op.Value)
		}
	}

	waitChResponse.err = OK
}

func (kv *ShardKV) handleUpdateConfig(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currConfig.Num != op.Config.Num-1 {
		return
	}

	kv.prevConfig = kv.currConfig
	kv.currConfig = op.Config

	if op.Config.Num == 1 {
		kv.DPrintf("create first config")
		for shardId, gid := range op.Config.Shards {
			if gid == kv.gid {
				kv.shards[shardId] = &Shard{
					Storage: sync.Map{},
					Status:  WORKING,
				}
			}
		}
	} else {
		kv.DPrintf("update shards status according to new config")
		for shardId, gid := range op.Config.Shards {
			if gid == kv.gid {
				_, exist := kv.shards[shardId]
				kv.DPrintf("shard %v exist: %v\n", shardId, exist)
				if !exist {
					kv.shards[shardId] = &Shard{
						Storage: sync.Map{},
						Status:  PULLING,
					}
					go kv.pullShard(shardId, kv.currConfig.Num)
				}
			}
		}

		for shardId := range kv.shards {
			if op.Config.Shards[shardId] != kv.gid {
				kv.shards[shardId].Status = LEAVING
			}
		}
	}
}

func (kv *ShardKV) pullShard(shardId int, configVersion int) {
	var retries int
	maxRetries := 5

	for retries < maxRetries {
		kv.mu.Lock()
		kv.DPrintf("pull shard %v, configVersion %v\n", shardId, configVersion)
		if kv.currConfig.Num > configVersion {
			kv.DPrintf("config version %v > %v, stop pulling\n", kv.currConfig.Num, configVersion)
			kv.mu.Unlock()
			break
		}
		if kv.shards[shardId].Status != PULLING {
			kv.DPrintf("shard %v status is not PULLING, stop pulling\n", shardId)
			kv.mu.Unlock()
			break
		}

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.DPrintf("server %v is not leader, stop pulling\n", kv.me)
			retries++
			kv.mu.Unlock()
			time.Sleep(RetryPullInterval)
			continue
		}

		args := PullArgs{
			ConfigVersion: configVersion,
			ShardId:       shardId,
		}
		gid := kv.prevConfig.Shards[shardId]
		kv.mu.Unlock()

		if servers, ok := kv.prevConfig.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply PullReply
				ok := srv.Call("ShardKV.Pull", &args, &reply)
				if ok && (reply.Err == OK) {
					kv.mu.Lock()
					kv.rf.Start(Op{
						Type:          PULL_SHARD,
						ShardId:       shardId,
						ConfigVersion: configVersion,
						Storage:       reply.Storage,
					})
					kv.mu.Unlock()
					return
				}
				if ok && (reply.Err == ErrConfigVersion) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		retries++
		time.Sleep(RetryPullInterval)
	}
}

func (kv *ShardKV) handlePullShard(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currConfig.Num != op.ConfigVersion || kv.shards[op.ShardId].Status != PULLING {
		return
	}

	kv.DPrintf("get shard %v from ex-owner\n", op.ShardId)
	for k, v := range op.Storage {
		kv.shards[op.ShardId].Storage.Store(k, v)
	}
	kv.shards[op.ShardId].Status = WAITING
	go kv.sendLeave(op.ShardId, kv.currConfig.Num)
}

func (kv *ShardKV) sendLeave(shardId int, configVersion int) {
	var retries int
	maxRetries := 5
	for retries < maxRetries {
		kv.mu.Lock()
		if kv.currConfig.Num > configVersion {
			kv.mu.Unlock()
			break
		}
		if kv.shards[shardId].Status != WAITING {
			kv.mu.Unlock()
			break
		}
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			retries++
			time.Sleep(RetryLeaveInterval)
			continue
		}

		args := LeaveArgs{
			ConfigVersion: configVersion,
			ShardId:       shardId,
		}
		gid := kv.prevConfig.Shards[shardId]
		kv.mu.Unlock()

		if servers, ok := kv.prevConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply LeaveReply
				ok := srv.Call("ShardKV.Leave", &args, &reply)
				if ok && (reply.Err == OK) {
					kv.mu.Lock()
					kv.rf.Start(Op{
						Type:          TRANSFER_DONE,
						ShardId:       shardId,
						ConfigVersion: configVersion,
					})
					kv.mu.Unlock()
					return
				}
				if ok && (reply.Err == ErrConfigVersion) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		retries++
		time.Sleep(RetryLeaveInterval)
	}
}

func (kv *ShardKV) handleLeaveShard(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currConfig.Num == op.ConfigVersion {
		_, exist := kv.shards[op.ShardId]
		if exist {
			kv.DPrintf("delete shard %v\n", op.ShardId)
			delete(kv.shards, op.ShardId)
		}
		waitChResponse.err = OK
	} else if kv.currConfig.Num > op.ConfigVersion {
		waitChResponse.err = OK
	} else {
		waitChResponse.err = ErrConfigVersion
	}
}

func (kv *ShardKV) handleTransferDone(op Op, waitChResponse *WaitChResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currConfig.Num == op.ConfigVersion && kv.shards[op.ShardId].Status == WAITING {
		kv.DPrintf("confirm shard %v deleted from ex-owner\n", op.ShardId)
		kv.shards[op.ShardId].Status = WORKING
	}
}

func (kv *ShardKV) handleEmptyEntry(waitChResponse *WaitChResponse) {
	waitChResponse.err = OK
}

func (kv *ShardKV) fetchNextConfig() {
	for {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			time.Sleep(FetchConfigInterval)
			continue
		}

		readForNext := true
		for _, shard := range kv.shards {
			if shard.Status != WORKING {
				readForNext = false
			}
		}

		if readForNext {
			currVersion := kv.currConfig.Num
			latestConfig := kv.mck.Query(currVersion + 1)
			if latestConfig.Num > currVersion {
				if latestConfig.Num != currVersion+1 {
					log.Fatalln("Jump through some versions!")
				}
				kv.rf.Start(Op{
					Type:   UPDATE_CONFIG,
					Config: latestConfig,
				})
			}
		}
		kv.mu.Unlock()
		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) checkEntryInCurrentTerm() {
	for {
		if !kv.rf.HasLogInCurrentTerm() {
			kv.mu.Lock()
			// Empty Op to make sure the log entry is in current term
			kv.rf.Start(Op{
				Type: EMPTY_ENTRY,
			})
			kv.mu.Unlock()
		}
		time.Sleep(200 * time.Millisecond)
	}
}
