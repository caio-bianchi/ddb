// coordinator RPC functions called by nodes

package ddb

import (
	"fmt"
	"log"
	"time"
)

// This function is called remotely from a Node that gets leadership
func (c *Coordinator) RegisterLeader(args *RegisterLeaderArgs, reply *RegisterLeaderReply) error {

	// Nodes cannot create shards by themselves. Coordinator is authoritative.
	if args.ShardID < 0 || args.ShardID >= c.totalShards {
		reply.Ok = false
		log.Printf("[COORD] REJECTED leader registration: shard %d does not exist (total=%d) from node %d",
			args.ShardID, c.totalShards, args.NodeID)
		return nil
	}
	// --------------------------------------------------------

	c.leadersAddresses[args.ShardID] = args.Address
	reply.Ok = true

	log.Printf("[COORD] Registered leader for shard %d: Node %d at %s\n",
		args.ShardID, args.NodeID, args.Address)

	return nil
}

// This function is called remotely from a Node that wants to register itself w/ coordinator
func (c *Coordinator) RegisterNode(args *RegisterNodeArgs, reply *RegisterNodeReply) error {

	// Nodes cannot register into shards that don't exist.
	// Coordinator is the ONLY authority over sharding.
	if args.ShardID < 0 || args.ShardID >= c.totalShards {
		reply.Ok = false
		reply.Err = fmt.Sprintf("invalid shard %d (coordinator configured with %d shards)",
			args.ShardID, c.totalShards)
		log.Printf("[COORD] REJECTED node %d: shard %d does not exist (total=%d)",
			args.NodeID, args.ShardID, c.totalShards)
		return nil
	}
	// --------------------------------------------------------

	c.mu.Lock()
	c.nodes[args.NodeID] = *args
	c.mu.Unlock()

	log.Printf("[COORD] Node registered: ID=%d RPC=%s Raft=%s shard=%d",
		args.NodeID, args.RPCAddr, args.RaftAddr, args.ShardID)

	reply.Ok = true

	// Count how many nodes exist in this shard
	nodesInShard := c.getNodesInShard(args.ShardID)

	// If this is the FIRST node of the shard, bootstrap it.
	if len(nodesInShard) == 1 {
		go func(nodeID int, shardID int) {
			time.Sleep(300 * time.Millisecond) // small delay so node finishes initRaft
			if err := c.bootstrapShard(nodeID); err != nil {
				log.Printf("[COORD] Failed to bootstrap shard %d with node %d: %v", shardID, nodeID, err)
			} else {
				log.Printf("[COORD] Bootstrap triggered for shard %d by node %d", shardID, nodeID)
			}
		}(args.NodeID, args.ShardID)

		return nil
	}

	// Otherwise, try to add as follower with retries (leader may not be registered yet).
	go c.retryAddNodeToShard(args.NodeID, args.ShardID)

	return nil
}
