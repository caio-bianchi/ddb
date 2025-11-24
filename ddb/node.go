package ddb

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Node struct {
	id int

	// Network
	address     string // RPC address in format ip : port
	raftAddress string // RAFT address in format ip : port

	listener  net.Listener
	rpcServer *rpc.Server

	// Communication
	coordinatorAddress string
	leaderAddress      string
	shardID            int
	peersAddresses     []string // RAFT addresses of peers in same shard, including self
	// for creation, peersAddresses must be a list in the same order for all nodes

	// Storage
	storage *Storage

	// raft
	raft          *raft.Raft
	fsm           *NodeFSM
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	transport     *raft.NetworkTransport
}

// Creates a new node
func NewNode(id int, address, raftAddress, coordinatorAddress string, shardID int, peersAddresses []string) *Node {
	n := &Node{
		id:                 id,
		address:            address,
		raftAddress:        raftAddress,
		coordinatorAddress: coordinatorAddress,
		shardID:            shardID,
		leaderAddress:      address, // ok: leaderAddress é RPC
		peersAddresses:     peersAddresses,
	}

	// RPC
	n.rpcServer = rpc.NewServer()

	// Listener
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Node %d failed to listen on %s: %v", id, address, err)
	}
	n.listener = l

	// Storage local (pré-Raft)
	store, err := NewStorage("storage_node_" + strconv.Itoa(id) + ".json")
	if err != nil {
		log.Fatalf("Node %d storage init error: %v", id, err)
	}
	n.storage = store

	return n
}

// Starts a node
func (n *Node) Start() {
	log.Printf("Node %d starting RPC at %s\n", n.id, n.address)

	if err := n.rpcServer.Register(n); err != nil {
		log.Fatalf("Node %d failed RPC registration: %v", n.id, err)
	}

	go n.acceptMultipleConnections()

	if err := n.initRaft(); err != nil {
		log.Fatalf("Node %d Raft init error: %v\n", n.id, err)
	}
}

func (n *Node) acceptMultipleConnections() error {
	log.Printf("Accepting connections on %v\n", n.listener.Addr())

	for {
		newConn, err := n.listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection. Error: ", err)
			return err
		}
		go n.handleConnection(&newConn)
	}
}

func (n *Node) handleConnection(conn *net.Conn) error {
	n.rpcServer.ServeConn(*conn)
	(*conn).Close()
	return nil
}

// node reinvidicates leadership of shard calling RegisterLeader RPC of coordinator
func (n *Node) reinvidicateLeadership() error {
	log.Printf("Node %d reinvidicating leadership to coordinator at %s\n", n.id, n.coordinatorAddress)

	client, err := rpc.Dial("tcp", n.coordinatorAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	args := &RegisterLeaderArgs{
		ShardID: n.shardID,
		NodeID:  n.id,
		Address: n.address,
	}
	reply := &RegisterLeaderReply{}

	if err := client.Call("Coordinator.RegisterLeader", args, reply); err != nil {
		return err
	}

	log.Printf("Node %d successfully reinvidicated leadership for shard %d\n", n.id, n.shardID)
	return nil
}

func (n *Node) Stop() {
	log.Printf("Node %d stopping...", n.id)
	if n.listener != nil {
		_ = n.listener.Close()
	}
}

// iniates Raft instance
func (n *Node) initRaft() error {
	config := raft.DefaultConfig()
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:  fmt.Sprintf("raft-%d", n.id),
		Level: hclog.Off,
	})

	config.LocalID = raft.ServerID(n.raftAddress) // raft ID is simply the address

	// Base dir isolated by shard and node
	// Raft will create a subdir "snapshots" inside each directory
	baseDir := filepath.Join("raft-data",
		fmt.Sprintf("shard_%d", n.shardID),
		fmt.Sprintf("node_%d", n.id),
	)

	// garante que o diretório existe antes do Raft testar permissões
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("mkdir baseDir: %v", err)
	}

	// 1 - Log Store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "raft-log.db"))
	if err != nil {
		return fmt.Errorf("logStore: %v", err)
	}
	n.logStore = logStore

	// 2 - Stable Store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "raft-stable.db"))
	if err != nil {
		return fmt.Errorf("stableStore: %v", err)
	}
	n.stableStore = stableStore

	// 3 - Snapshot Store
	snapStore, err := raft.NewFileSnapshotStore(baseDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("snapshotStore: %v", err)
	}
	n.snapshotStore = snapStore

	// 4 - Transport
	addr, err := net.ResolveTCPAddr("tcp", n.raftAddress)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(n.raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("transport: %v", err)
	}
	n.transport = transport

	// 5 - FSM
	n.fsm = &NodeFSM{storage: n.storage}

	r, err := raft.NewRaft(config, n.fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return fmt.Errorf("raft init: %v", err)
	}
	n.raft = r

	// 6 - Bootstrap cluster
	configuration := raft.Configuration{}
	for _, peerAddr := range n.peersAddresses {
		server := raft.Server{
			ID:      raft.ServerID(peerAddr),
			Address: raft.ServerAddress(peerAddr),
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		return err
	}

	// só bootstrap se houver mais de 1 node
	// nenhum node se auto-bootstrap sozinho
	if !hasState && len(n.peersAddresses) > 1 {
		r.BootstrapCluster(configuration)
	}

	// 7 - leadership callback
	go func() {
		leaderCh := r.LeaderCh()
		for {
			isLeader := <-leaderCh
			if isLeader {
				log.Printf("[RAFT] Node %d became LEADER of shard %d!", n.id, n.shardID)
				n.reinvidicateLeadership()
			}
		}
	}()

	return nil
}

// applies a Put command via Raft
func (n *Node) Put(key, value string) error {
	cmd := LogCommand{Op: "PUT", Key: key, Value: value}
	data, _ := json.Marshal(cmd)
	future := n.raft.Apply(data, 5*time.Second)
	return future.Error()
}

// applies a Delete command via Raft
func (n *Node) Delete(key string) error {
	cmd := LogCommand{Op: "DELETE", Key: key}
	data, _ := json.Marshal(cmd)
	future := n.raft.Apply(data, 5*time.Second)
	return future.Error()
}

// gets a value from local storage (no raft)
func (n *Node) Get(key string) (string, bool) {
	return n.storage.Get(key)
}

// Crash simulates a full node failure (no RPC, no Raft, no transport)
func (n *Node) Crash() {
	log.Printf("Node %d CRASH SIMULATED — stopping all services", n.id)

	// 1. Stop RPC listener
	if n.listener != nil {
		_ = n.listener.Close()
	}

	// 2. Stop Raft completely
	if n.raft != nil {
		n.raft.Shutdown()
	}

	// 3. Stop Transport (closes Raft TCP listener)
	if n.transport != nil {
		n.transport.Close()
	}
}
