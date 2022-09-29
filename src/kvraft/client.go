package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

//-------------------------------Data Structure------------------------------

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//code 2A
	seqId    int
	clientId int64
	leaderId int
}

//----------------------------------util-------------------------------
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//------------------------------------------Running-------------------------------------

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seqId = 0
	ck.leaderId = mathrand.Intn(len(ck.servers))
	ck.clientId = nrand()
	return ck
}

//------------------------------------------Service-------------------------------------

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	for {
		args := &GetArgs{Key: key, SeqId: ck.seqId, ClientId: ck.clientId}
		reply := &GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", args, reply)
		if ok {
			switch reply.Err {
			case ErrWrongLeader:
			case ErrNoKey:
				ck.leaderId = serverId
				return ""
			case OK:
				ck.leaderId = serverId
				return reply.Value
			}
		}
		//ck.leaderId = (ck.leaderId+1)%len(ck.servers):
		//Wrong!Get() and PutAppend() may use ck.leaderId at the same time
		serverId = (serverId + 1) % len(ck.servers)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	for {
		args := &PutAppendArgs{Key: key, Value: value, SeqId: ck.seqId, ClientId: ck.clientId}
		if op == "Put" {
			args.Op = PUT
		} else {
			args.Op = APPEND
		}
		reply := &PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
		if ok {
			switch reply.Err {
			case ErrWrongLeader:
			case OK:
				ck.leaderId = serverId
				return
			}
		}
		//ck.leaderId = (ck.leaderId+1)%len(ck.servers):
		//Wrong!Get() and PutAppend() may use ck.leaderId at the same time
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
