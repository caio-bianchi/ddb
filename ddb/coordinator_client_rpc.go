// coordinator RPC functions called by clients
package ddb

import (
	"fmt"
	"log"
	"net/rpc"
)

// The following functions forward client requests to the appropriate shard leader
// They are called by the client via RPC

func (c *Coordinator) RPCPut(args *ClientPutArgs, reply *ClientPutReply) error {
	shard := c.determineShard(args.Key)
	leaderAddr, ok := c.leadersAddresses[shard]
	if !ok {
		reply.Err = fmt.Sprintf("no leader registered for shard %d", shard)
		return nil
	}

	log.Printf("[COORD] Forwarding PUT(%s,%s) to leader %s", args.Key, args.Value, leaderAddr)

	cli, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		reply.Err = fmt.Sprintf("failed to contact leader %s: %v", leaderAddr, err)
		return nil
	}
	defer cli.Close()

	nodeArgs := &NodePutArgs{Key: args.Key, Value: args.Value}
	nodeReply := &NodePutReply{}

	if err := cli.Call("Node.RPCPut", nodeArgs, nodeReply); err != nil {
		reply.Err = fmt.Sprintf("RPC error: %v", err)
		return nil
	}

	reply.Ok = nodeReply.Ok
	reply.Err = nodeReply.Err
	return nil
}

func (c *Coordinator) RPCGet(args *ClientGetArgs, reply *ClientGetReply) error {
	shard := c.determineShard(args.Key)
	leaderAddr, ok := c.leadersAddresses[shard]
	if !ok {
		reply.Err = fmt.Sprintf("no leader registered for shard %d", shard)
		return nil
	}

	log.Printf("[COORD] Forwarding GET(%s) to leader %s", args.Key, leaderAddr)

	cli, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		reply.Err = fmt.Sprintf("failed to contact leader %s: %v", leaderAddr, err)
		return nil
	}
	defer cli.Close()

	nodeArgs := &NodeGetArgs{Key: args.Key}
	nodeReply := &NodeGetReply{}

	if err := cli.Call("Node.RPCGet", nodeArgs, nodeReply); err != nil {
		reply.Err = fmt.Sprintf("RPC error: %v", err)
		return nil
	}

	reply.Value = nodeReply.Value
	reply.Found = nodeReply.Found
	reply.Err = nodeReply.Err
	return nil
}

func (c *Coordinator) RPCDelete(args *ClientDeleteArgs, reply *ClientDeleteReply) error {
	shard := c.determineShard(args.Key)
	leaderAddr, ok := c.leadersAddresses[shard]
	if !ok {
		reply.Err = fmt.Sprintf("[COORD] no leader registered for shard %d", shard)
		return nil
	}

	log.Printf("[COORD] Forwarding DELETE(%s) to leader %s", args.Key, leaderAddr)

	cli, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		reply.Err = fmt.Sprintf("[COORD] failed to contact leader %s: %v", leaderAddr, err)
		return nil
	}
	defer cli.Close()

	nodeArgs := &NodeDeleteArgs{Key: args.Key}
	nodeReply := &NodeDeleteReply{}

	if err := cli.Call("Node.RPCDelete", nodeArgs, nodeReply); err != nil {
		reply.Err = fmt.Sprintf("RPC error: %v", err)
		return nil
	}

	reply.Ok = nodeReply.Ok
	reply.Err = nodeReply.Err
	return nil
}

// Returns a snapshot of shards, leader, and node liveness.
func (c *Coordinator) RPCShardStatus(args *ShardStatusArgs, reply *ShardStatusReply) error {

	type shardOut struct {
		ShardID int
		Leader  *NodeStatusInfo
		Nodes   []NodeStatusInfo
		Alive   int
		Total   int
		Err     string
	}

	out := make([]shardOut, 0, c.totalShards)

	for shardID := 0; shardID < c.totalShards; shardID++ {

		nodes := c.getNodesInShard(shardID)

		leaderRPC, hasLeader := c.leadersAddresses[shardID]

		aliveCount := 0
		nodeInfos := []NodeStatusInfo{}

		// checks each node with RPCPing
		for _, ninfo := range nodes {
			ns := NodeStatusInfo{
				NodeID:   ninfo.NodeID,
				RPCAddr:  ninfo.RPCAddr,
				RaftAddr: ninfo.RaftAddr,
				IsLeader: hasLeader && ninfo.RPCAddr == leaderRPC,
				Alive:    false,
			}

			cli, err := rpc.Dial("tcp", ninfo.RPCAddr)
			if err != nil {
				ns.Alive = false
				ns.Err = err.Error()
				nodeInfos = append(nodeInfos, ns)
				continue
			}

			pingReply := &GenericOKReply{}
			callErr := cli.Call("Node.RPCPing", &struct{}{}, pingReply)
			_ = cli.Close()

			if callErr != nil {
				ns.Alive = false
				ns.Err = callErr.Error()
			} else {
				ns.Alive = pingReply.Ok
				ns.Err = pingReply.Err
			}

			if ns.Alive {
				aliveCount++
			}

			nodeInfos = append(nodeInfos, ns)
		}

		// leader info if exists
		var leaderInfo *NodeStatusInfo
		if hasLeader {
			for i := range nodeInfos {
				if nodeInfos[i].IsLeader {
					leaderInfo = &nodeInfos[i]
					break
				}
			}
			// just for sanity
			if leaderInfo == nil {
				leaderInfo = &NodeStatusInfo{
					NodeID:   -1,
					RPCAddr:  leaderRPC,
					RaftAddr: "",
					IsLeader: true,
					Alive:    false,
					Err:      "leader registered but not found among nodes",
				}
			}
		}

		out = append(out, shardOut{
			ShardID: shardID,
			Leader:  leaderInfo,
			Nodes:   nodeInfos,
			Alive:   aliveCount,
			Total:   len(nodeInfos),
			Err:     "",
		})
	}

	// prepare reply
	reply.Shards = []struct {
		ShardID int
		Leader  *NodeStatusInfo
		Nodes   []NodeStatusInfo
		Alive   int
		Total   int
		Err     string
	}{}

	for _, s := range out {
		reply.Shards = append(reply.Shards, struct {
			ShardID int
			Leader  *NodeStatusInfo
			Nodes   []NodeStatusInfo
			Alive   int
			Total   int
			Err     string
		}{
			ShardID: s.ShardID,
			Leader:  s.Leader,
			Nodes:   s.Nodes,
			Alive:   s.Alive,
			Total:   s.Total,
			Err:     s.Err,
		})
	}

	return nil
}
