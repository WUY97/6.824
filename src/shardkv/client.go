package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int64
	leaderIds sync.Map
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderIds = sync.Map{}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	reqId := atomic.LoadInt64(&ck.requestId)
	clientId := atomic.LoadInt64(&ck.clientId)
	args := GetArgs{
		Key:       key,
		ClientId:  clientId,
		RequestId: reqId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if !ok {
			time.Sleep(100 * time.Millisecond)
			ck.config = ck.sm.Query(-1)
			continue
		}

		var oldLeaderId int
		if leaderIdIface, ok := ck.leaderIds.Load(gid); ok {
			oldLeaderId = leaderIdIface.(int)
		} else {
			oldLeaderId = 0
		}
		newLeaderId := oldLeaderId

		for {
			var reply GetReply
			ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.Get", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				atomic.AddInt64(&ck.requestId, 1)
				return reply.Value
			} else if ok && reply.Err == ErrWrongGroup {
				break
			} else {
				newLeaderId = (newLeaderId + 1) % len(servers)
				if newLeaderId == oldLeaderId {
					break
				}
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reqId := atomic.LoadInt64(&ck.requestId)
	clientId := atomic.LoadInt64(&ck.clientId)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  clientId,
		RequestId: reqId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if !ok {
			time.Sleep(100 * time.Millisecond)
			ck.config = ck.sm.Query(-1)
			continue
		}

		var oldLeaderId int
		if leaderIdIface, ok := ck.leaderIds.Load(gid); ok {
			oldLeaderId = leaderIdIface.(int)
		} else {
			oldLeaderId = 0
		}
		newLeaderId := oldLeaderId

		for {
			var reply PutAppendReply
			ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				atomic.AddInt64(&ck.requestId, 1)
				return
			} else if ok && reply.Err == ErrWrongGroup {
				break
			} else {
				newLeaderId = (newLeaderId + 1) % len(servers)
				if newLeaderId == oldLeaderId {
					break
				}
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
