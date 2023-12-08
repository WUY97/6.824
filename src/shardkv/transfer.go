package shardkv

import (
	"log"
	"time"
)

type PullArgs struct {
	ConfigVersion int
	ShardId       int
}

type PullReply struct {
	Storage map[string]string
	Err     Err
}

type LeaveArgs struct {
	ConfigVersion int
	ShardId       int
}

type LeaveReply struct {
	Err Err
}

func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currConfig.Num != args.ConfigVersion {
		reply.Err = ErrConfigVersion
		return
	}

	shard, exist := kv.shards[args.ShardId]
	if !exist {
		reply.Err = ErrConfigVersion
		return
	}

	pulledStorage := shard.Storage
	reply.Storage = pulledStorage
	reply.Err = OK
}

func (kv *ShardKV) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	raftIndex, _, isLeader := kv.rf.Start(Op{
		Type:          LEAVE_SHARD,
		ShardId:       args.ShardId,
		ConfigVersion: kv.currConfig.Num,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if _, exist := kv.waitChs[raftIndex]; exist {
		log.Fatalf("shardkv | server %d try to get a existing waitCh\n", kv.me)
	}

	kv.waitChs[raftIndex] = make(chan WaitChResponse, 1)
	waitCh := kv.waitChs[raftIndex]
	kv.mu.Unlock()

	select {
	case <-time.After(RaftTimeOut):
		kv.DPrintf("server %d timeout when handling Leave\n", kv.me)
		reply.Err = ErrWrongLeader
	case response := <-waitCh:
		reply.Err = response.err
	}

	kv.mu.Lock()
	delete(kv.waitChs, raftIndex)
	kv.mu.Unlock()
}
