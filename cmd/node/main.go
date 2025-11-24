package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"time"

	"labexame/ddb"
)

func main() {

	id := flag.Int("id", 0, "Node ID")
	rpcAddr := flag.String("rpc", "", "RPC address")
	raftAddr := flag.String("raft", "", "Raft address")
	shardID := flag.Int("shard", 0, "Shard ID")
	coordAddr := flag.String("coord", "127.0.0.1:9000", "Coordinator RPC address")
	flag.Parse()

	if *id == 0 || *rpcAddr == "" || *raftAddr == "" {
		log.Fatal("Missing required flags: --id --rpc --raft")
	}

	fmt.Println("=== Node ===")
	fmt.Println("ID:", *id)
	fmt.Println("RPC:", *rpcAddr)
	fmt.Println("RAFT:", *raftAddr)
	fmt.Println("Shard:", *shardID)

	peers := []string{}

	n := ddb.NewNode(*id, *rpcAddr, *raftAddr, *coordAddr, *shardID, peers)
	go n.Start()

	time.Sleep(1 * time.Second)
	registerAtCoordinator(*id, *rpcAddr, *raftAddr, *shardID, *coordAddr)

	select {} // keep running
}

func registerAtCoordinator(id int, rpcAddr, raftAddr string, shardID int, coordAddr string) {
	cli, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		log.Println("Failed to register node:", err)
		return
	}
	defer cli.Close()

	args := &ddb.RegisterNodeArgs{
		NodeID:   id,
		RPCAddr:  rpcAddr,
		RaftAddr: raftAddr,
		ShardID:  shardID,
	}
	reply := &ddb.RegisterNodeReply{}
	cli.Call("Coordinator.RegisterNode", args, reply)
}
