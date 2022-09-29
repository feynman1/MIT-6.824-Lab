package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	mathrand "math/rand"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	//code 2A
	seqId    int
	clientId int64
	leaderId int
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
	ck.seqId = 0
	ck.leaderId = mathrand.Intn(len(ck.servers))
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqId++
	args := QueryArgs{Num: num, SeqId: ck.seqId, ClientId: ck.clientId}
	serverid := ck.leaderId
	// Your code here.
	for {
		reply := QueryReply{}
		ok := ck.servers[serverid].Call("ShardCtrler.Query", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverid
				return reply.Config
			} else if reply.Err == ErrWrongLeader {
				serverid = (serverid + 1) % len(ck.servers)
				continue
			}
		}
		serverid = (serverid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqId++
	args := JoinArgs{Servers: servers, ClientId: ck.clientId, SeqId: ck.seqId}
	leaderid := ck.leaderId
	// Your code here.
	for {
		// try each known server.
		reply := JoinReply{}
		ok := ck.servers[leaderid].Call("ShardCtrler.Join", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = leaderid
				return
			} else if reply.Err == ErrWrongLeader {
				leaderid = (leaderid + 1) % len(ck.servers)
				continue
			}
		}
		leaderid = (leaderid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqId++
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
	leaderid := ck.leaderId
	// Your code here.
	for {
		// try each known server.
		reply := LeaveReply{}
		ok := ck.servers[leaderid].Call("ShardCtrler.Leave", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = leaderid
				return
			} else if reply.Err == ErrWrongLeader {
				leaderid = (leaderid + 1) % len(ck.servers)
				continue
			}
		}
		leaderid = (leaderid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqId++
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
	leaderid := ck.leaderId
	// Your code here.
	for {
		// try each known server.
		reply := MoveReply{}
		ok := ck.servers[leaderid].Call("ShardCtrler.Move", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = leaderid
				return
			} else if reply.Err == ErrWrongLeader {
				leaderid = (leaderid + 1) % len(ck.servers)
				continue
			}
		}
		leaderid = (leaderid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
