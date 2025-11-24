package ddb

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Coordinator struct {
	address   string // ip + port, ex.: 127.0.0.1:9000
	rpcServer *rpc.Server
	listener  net.Listener

	totalShards      int
	leadersAddresses map[int]string           // map of shardID --> address of leader
	nodes            map[int]RegisterNodeArgs // map of nodeID --> RegisterNodeArgs

	mu sync.RWMutex
}

// Creates a new coordinator
func NewCoordinator(address string, totalShards int) *Coordinator {
	c := &Coordinator{
		address:          address,
		totalShards:      totalShards,
		leadersAddresses: make(map[int]string),
		nodes:            make(map[int]RegisterNodeArgs),
	}

	c.rpcServer = rpc.NewServer()
	if err := c.rpcServer.Register(c); err != nil {
		log.Fatalf("Failed to register coordinator RPC: %v", err)
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Coordinator failed to listen on %s: %v", address, err)
	}
	c.listener = l

	return c
}

// Returns the shard that owns a given key
func (c *Coordinator) determineShard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % c.totalShards
}

// Server loop
func (c *Coordinator) Start() {
	log.Printf("Coordinator listening at %s\n", c.address)
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			log.Println("Coordinator accept error:", err)
			return
		}
		go c.rpcServer.ServeConn(conn)
	}

}

func (c *Coordinator) Stop() {
	log.Println("Coordinator shutting down...")
	if c.listener != nil {
		_ = c.listener.Close()
	}
}

// This function adds a node to a shard by contacting the shard leader
func (c *Coordinator) AddNodeToShard(nodeID int, shardID int) error {

	// gets node info and leader address
	c.mu.RLock()
	nodeInfo, ok := c.nodes[nodeID]
	leaderAddr, hasLeader := c.leadersAddresses[shardID]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("node %d not registered", nodeID)
	}
	if !hasLeader {
		return fmt.Errorf("shard %d has no leader yet", shardID)
	}

	log.Printf("[COORD] Requesting to add node %d to shard %d", nodeID, shardID)

	// chamar líder
	cli, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		return fmt.Errorf("cannot reach leader: %v", err)
	}

	args := &AddFollowerArgs{
		NodeID:      nodeID,
		RaftAddress: nodeInfo.RaftAddr,
	}
	reply := &AddFollowerReply{}

	err = cli.Call("Node.RPCAddFollower", args, reply)
	cli.Close()
	if err != nil {
		return fmt.Errorf("leader RPC error: %v", err)
	}
	if !reply.Ok {
		return fmt.Errorf("leader refused: %v", reply.Err)
	}

	log.Printf("[COORD] Node %d successfully added to shard %d", nodeID, shardID)
	return nil
}

// returns slice of nodes registered in shard
func (c *Coordinator) getNodesInShard(shardID int) []RegisterNodeArgs {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var res []RegisterNodeArgs
	for _, info := range c.nodes {
		if info.ShardID == shardID {
			res = append(res, info)
		}
	}
	return res
}

// calls RPCBootstrap on a node (first node of shard)
func (c *Coordinator) bootstrapShard(nodeID int) error {
	c.mu.RLock()
	info, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("node %d not registered", nodeID)
	}

	cli, err := rpc.Dial("tcp", info.RPCAddr)
	if err != nil {
		return fmt.Errorf("cannot dial node %d: %v", nodeID, err)
	}
	defer cli.Close()

	reply := &BootstrapReply{}
	if err := cli.Call("Node.RPCBootstrap", &BootstrapArgs{}, reply); err != nil {
		return fmt.Errorf("RPCBootstrap error: %v", err)
	}
	if !reply.Ok {
		return fmt.Errorf("bootstrap refused: %s", reply.Err)
	}

	return nil
}

// retries AddNodeToShard a few times, waiting for leader election
func (c *Coordinator) retryAddNodeToShard(nodeID int, shardID int) {
	const attempts = 10
	const interval = 500 * time.Millisecond

	for i := 0; i < attempts; i++ {
		err := c.AddNodeToShard(nodeID, shardID)
		if err == nil {
			return
		}
		log.Printf("[COORD] Failed to add node %d to shard %d: %v (retry %d/%d)",
			nodeID, shardID, err, i+1, attempts)
		time.Sleep(interval)
	}
}
