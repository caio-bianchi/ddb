package ddb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Storage struct {
	mu   sync.RWMutex
	data map[string]string
	path string
}

// NewStorage creates empty storage.
// IMPORTANT: no load here. Raft will restore state.
func NewStorage(path string) (*Storage, error) {
	s := &Storage{
		data: make(map[string]string),
		path: path,
	}
	return s, nil
}

// stores the data from memory to json file
func (s *Storage) store() error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir storage dir: %w", err)
	}

	content, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal storage json: %w", err)
	}

	if err := os.WriteFile(s.path, content, 0644); err != nil {
		return fmt.Errorf("write storage file: %w", err)
	}

	return nil
}

// gets the value for a given key THROUGH MEMORY
func (s *Storage) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	return v, ok
}

// stores data[key] = value in the memory AND transfer memory to json file
func (s *Storage) ApplyPut(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	return s.store()
}

// deletes key from memory AND transfer memory to json file
func (s *Storage) ApplyDelete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return s.store()
}

// returns a copy of current data from memory
func (s *Storage) StorageSnapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copy := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copy[k] = v
	}
	return copy
}

// restores memory from a saved state AND transfer memory to json file
func (s *Storage) StorageRestore(state map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if state == nil {
		state = make(map[string]string)
	}
	s.data = state
	return s.store()
}
