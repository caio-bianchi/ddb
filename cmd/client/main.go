package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"

	"labexame/ddb"
)

const COORD = "127.0.0.1:9000"

func main() {

	fmt.Println("=== Distributed DB Client ===")
	fmt.Println("Commands: put/get/del <key> [value] , quit, status")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print(">> ")
		raw, _ := reader.ReadString('\n')
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		parts := strings.Fields(raw)
		cmd := strings.ToLower(parts[0])

		switch cmd {

		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			key := parts[1]
			val := parts[2]

			cli, _ := rpc.Dial("tcp", COORD)
			args := &ddb.ClientPutArgs{Key: key, Value: val}
			reply := &ddb.ClientPutReply{}
			cli.Call("Coordinator.RPCPut", args, reply)
			cli.Close()

			if reply.Err != "" {
				fmt.Println("Error:", reply.Err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			key := parts[1]
			cli, _ := rpc.Dial("tcp", COORD)
			args := &ddb.ClientGetArgs{Key: key}
			reply := &ddb.ClientGetReply{}
			cli.Call("Coordinator.RPCGet", args, reply)
			cli.Close()

			if reply.Err != "" {
				fmt.Println("Error:", reply.Err)
			} else if !reply.Found {
				fmt.Println("(not found)")
			} else {
				fmt.Printf("%s = %s\n", key, reply.Value)
			}

		case "del":
			if len(parts) < 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			key := parts[1]

			cli, _ := rpc.Dial("tcp", COORD)
			args := &ddb.ClientDeleteArgs{Key: key}
			reply := &ddb.ClientDeleteReply{}
			cli.Call("Coordinator.RPCDelete", args, reply)
			cli.Close()

			if reply.Err != "" {
				fmt.Println("Error:", reply.Err)
			} else {
				fmt.Println("OK")
			}

		case "status":
			cli, err := rpc.Dial("tcp", COORD)
			if err != nil {
				fmt.Println("error dialing coordinator:", err)
				continue
			}

			args := &ddb.ShardStatusArgs{}
			reply := &ddb.ShardStatusReply{}
			err = cli.Call("Coordinator.RPCShardStatus", args, reply)
			cli.Close()

			if err != nil {
				fmt.Println("RPC error:", err)
				continue
			}

			for _, s := range reply.Shards {
				fmt.Printf("Shard %d:\n", s.ShardID)

				if s.Leader == nil {
					fmt.Println("  Leader: (none)")
				} else {
					status := "dead"
					if s.Leader.Alive {
						status = "alive"
					}
					fmt.Printf("  Leader: node%d (rpc %s, raft %s)  [%s]\n",
						s.Leader.NodeID, s.Leader.RPCAddr, s.Leader.RaftAddr, status)
				}

				fmt.Println("  Followers:")
				for _, n := range s.Nodes {
					if n.IsLeader {
						continue
					}
					status := "dead"
					if n.Alive {
						status = "alive"
					}
					fmt.Printf("    - node%d (rpc %s, raft %s)  [%s]\n",
						n.NodeID, n.RPCAddr, n.RaftAddr, status)
				}

				fmt.Printf("  ReplicationFactor: %d, Alive: %d/%d\n\n",
					s.Total, s.Alive, s.Total)
			}

		case "quit":
			os.Exit(0)

		default:
			fmt.Println("Unknown command.")
		}
	}
}
