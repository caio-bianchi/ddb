package ddb

// RPC Types
type RegisterLeaderArgs struct {
	Address string
	ShardID int
	NodeID  int
}

type RegisterLeaderReply struct {
	Ok bool
}

// ---------- COORDINATOR RPC TYPES (CLIENT -> COORDINATOR) ----------

type ClientPutArgs struct {
	Key   string
	Value string
}
type ClientPutReply struct {
	Ok  bool
	Err string
}

type ClientGetArgs struct {
	Key string
}
type ClientGetReply struct {
	Value string
	Found bool
	Err   string
}

type ClientDeleteArgs struct {
	Key string
}
type ClientDeleteReply struct {
	Ok  bool
	Err string
}

// ---------- NODE RPC TYPES (COORDINATOR -> NODE) ----------

type NodePutArgs struct {
	Key   string
	Value string
}
type NodePutReply struct {
	Ok  bool
	Err string
}

type NodeGetArgs struct {
	Key string
}
type NodeGetReply struct {
	Value string
	Found bool
	Err   string
}

type NodeDeleteArgs struct {
	Key string
}
type NodeDeleteReply struct {
	Ok  bool
	Err string
}

//

type RegisterNodeArgs struct {
	NodeID   int
	RPCAddr  string
	RaftAddr string
	ShardID  int
}

type RegisterNodeReply struct {
	Ok  bool
	Err string
}

type AddFollowerArgs struct {
	NodeID      int
	RaftAddress string
}

type AddFollowerReply struct {
	Ok  bool
	Err string
}

// ---------- GENERIC OK REPLY ----------
type GenericOKReply struct {
	Ok  bool
	Err string
}

// ---------- BOOTSTRAP RPC ----------
type BootstrapArgs struct{} // vazio
type BootstrapReply = GenericOKReply

// ---------- SHARD STATUS RPC ----------

type ShardStatusArgs struct{}

type NodeStatusInfo struct {
	NodeID   int
	RPCAddr  string
	RaftAddr string
	IsLeader bool
	Alive    bool
	Err      string
}

type ShardStatusReply struct {
	Shards []struct {
		ShardID int
		Leader  *NodeStatusInfo
		Nodes   []NodeStatusInfo
		Alive   int
		Total   int
		Err     string
	}
}
