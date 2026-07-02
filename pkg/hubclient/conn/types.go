package conn

import "time"

// Handler is invoked synchronously on a worker goroutine; do not block.
type Handler func(cmd, origNid uint32, payload []byte)

// State is the session lifecycle state.
type State int

const (
	StateIdle State = iota
	StateDialing
	StateAuthing
	StateSubscribing
	StateReady
	StateClosed
)

// SessionConfig holds TCP session settings.
type SessionConfig struct {
	Addr                string
	NID                 uint32
	GID                 uint32
	User                string
	Pass                string
	WorkerNum           int
	SendQueueLen        int
	RecvQueueLen        int
	KeepaliveInterval   time.Duration
	KeepaliveMaxRetry   int
	ReconnectMaxBackoff time.Duration
	SendTimeout         time.Duration
}
