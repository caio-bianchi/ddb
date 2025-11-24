package ddb

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

type LogCommand struct {
	Op    string
	Key   string
	Value string
}

type NodeFSM struct {
	storage *Storage
}

// Apply is called when log entry is commited
// when this happens, it is safe to update memory and storage
func (f *NodeFSM) Apply(log *raft.Log) interface{} {
	var cmd LogCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		fmt.Println("FSM: failed decode", err)
		return nil
	}

	fmt.Printf("[FSM] Apply on node: %s — %v\n", time.Now().Format("15:04:05.000"), cmd)

	switch cmd.Op {
	case "PUT":
		f.storage.ApplyPut(cmd.Key, cmd.Value)
	case "DELETE":
		f.storage.ApplyDelete(cmd.Key)
	}
	return nil
}

// returns a copy of current data in memory
func (f *NodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	state := f.storage.StorageSnapshot()
	return &snapshot{state: state}, nil
}

// restores memory from a saved state
func (f *NodeFSM) Restore(rc io.ReadCloser) error {
	var state map[string]string
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	return f.storage.StorageRestore(state)
}

// snapshot is simply a state of all {key, value} pairs
type snapshot struct {
	state map[string]string
}

// when Raft decides to generate a  snapshot it calls Persist()
// we record in a raft.SnapshotSink object the complete state of the FSM
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(b); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}
