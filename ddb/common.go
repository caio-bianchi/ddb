package ddb

type Command struct {
	Op    string `json:"op"` // "SET" | "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}
