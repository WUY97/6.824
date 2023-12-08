package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int32
	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddInt64(&ck.requestId, 1)
	args.Num = num

	leader := atomic.LoadInt32(&ck.leaderId)
	serverId := leader
	for ; ; serverId = (serverId + 1) % int32(len(ck.servers)) {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			atomic.StoreInt32(&ck.leaderId, serverId)
			return reply.Config
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddInt64(&ck.requestId, 1)
	args.Servers = servers

	leader := atomic.LoadInt32(&ck.leaderId)
	serverId := leader
	for ; ; serverId = (serverId + 1) % int32(len(ck.servers)) {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			atomic.StoreInt32(&ck.leaderId, serverId)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddInt64(&ck.requestId, 1)
	args.GIDs = gids

	leader := atomic.LoadInt32(&ck.leaderId)
	serverId := leader
	for ; ; serverId = (serverId + 1) % int32(len(ck.servers)) {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			atomic.StoreInt32(&ck.leaderId, serverId)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddInt64(&ck.requestId, 1)
	args.Shard = shard
	args.GID = gid

	leader := atomic.LoadInt32(&ck.leaderId)
	serverId := leader
	for ; ; serverId = (serverId + 1) % int32(len(ck.servers)) {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			atomic.StoreInt32(&ck.leaderId, serverId)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}
