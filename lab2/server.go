package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KeyValueMap map[string]string

type KeyVersion map[string]rpc.Tversion

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	kval KeyValueMap
	kver KeyVersion

	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kval = make(KeyValueMap)
	kv.kver = make(KeyVersion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.kval[args.Key]; !ok{
			reply.Err = rpc.ErrNoKey
			return
	}
	reply.Value = kv.kval[args.Key]
	reply.Version = kv.kver[args.Key]
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args.Version {
	case 0:
		if _, ok := kv.kver[args.Key]; ok{
			reply.Err = rpc.ErrVersion
			return
		}
		kv.kval[args.Key] = args.Value
		kv.kver[args.Key] = 1
		reply.Err = rpc.OK
	default:
		if _, ok := kv.kval[args.Key]; !ok{
			reply.Err = rpc.ErrNoKey
			return
		}
		if args.Version != kv.kver[args.Key] {
			reply.Err = rpc.ErrVersion
			return
		}
		kv.kval[args.Key] = args.Value
		kv.kver[args.Key] = args.Version + 1
		reply.Err = rpc.OK
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
