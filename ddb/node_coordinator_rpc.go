// node RPC functions called by coordinator

package ddb

import (
	"fmt"
	"log"

	"github.com/hashicorp/raft"
)

// The following functions are called remotely via RPC by the coordinator

func (n *Node) RPCPut(args *NodePutArgs, reply *NodePutReply) error {

	if n.raft.State() != raft.Leader {
		reply.Err = "not leader"
		reply.Ok = false
		return nil
	}

	log.Printf("[NODE %d] RPCPut(%s, %s)", n.id, args.Key, args.Value)

	if err := n.Put(args.Key, args.Value); err != nil {
		reply.Err = err.Error()
		reply.Ok = false
		return nil
	}

	reply.Ok = true
	return nil
}

func (n *Node) RPCGet(args *NodeGetArgs, reply *NodeGetReply) error {

	if n.raft.State() != raft.Leader {
		reply.Err = "not leader"
		return nil
	}

	v, ok := n.Get(args.Key)

	reply.Value = v
	reply.Found = ok
	reply.Err = ""
	return nil
}

func (n *Node) RPCDelete(args *NodeDeleteArgs, reply *NodeDeleteReply) error {

	if n.raft.State() != raft.Leader {
		reply.Err = "not leader"
		reply.Ok = false
		return nil
	}

	log.Printf("[NODE %d] RPCDelete(%s)", n.id, args.Key)

	if err := n.Delete(args.Key); err != nil {
		reply.Err = err.Error()
		reply.Ok = false
		return nil
	}

	reply.Ok = true
	return nil
}

// adds a follower
// this function is called remotely by the coordinator
func (n *Node) RPCAddFollower(args *AddFollowerArgs, reply *AddFollowerReply) error {
	log.Printf("[NODE %d] Adding new follower %d at %s", n.id, args.NodeID, args.RaftAddress)

	// confirms leadership
	if n.raft.State() != raft.Leader {
		reply.Ok = false
		reply.Err = "not leader"
		return nil
	}

	future := n.raft.AddVoter(
		raft.ServerID(args.RaftAddress),
		raft.ServerAddress(args.RaftAddress),
		0, 0)

	if future.Error() != nil {
		reply.Ok = false
		reply.Err = future.Error().Error()
		return nil
	}

	reply.Ok = true
	return nil
}

// Called by Coordinator ONLY for the first node of a shard.
// Bootstraps a single-node Raft cluster.
// Later nodes will be added via AddVoter.
func (n *Node) RPCBootstrap(args *BootstrapArgs, reply *BootstrapReply) error {
	log.Printf("[NODE %d] RPCBootstrap() called", n.id)

	// If already has state, do nothing.
	hasState, err := raft.HasExistingState(n.logStore, n.stableStore, n.snapshotStore)
	if err != nil {
		reply.Ok = false
		reply.Err = err.Error()
		return nil
	}
	if hasState {
		reply.Ok = true
		reply.Err = ""
		return nil
	}

	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.raftAddress),
				Address: raft.ServerAddress(n.raftAddress),
			},
		},
	}

	f := n.raft.BootstrapCluster(cfg)
	if f.Error() != nil {
		reply.Ok = false
		reply.Err = fmt.Sprintf("bootstrap error: %v", f.Error())
		return nil
	}

	reply.Ok = true
	reply.Err = ""
	return nil
}

// ping RPC to check if node is alive
// called by coordinator
func (n *Node) RPCPing(args *struct{}, reply *GenericOKReply) error {
	reply.Ok = true
	reply.Err = ""
	return nil
}
