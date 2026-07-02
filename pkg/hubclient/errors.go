package hubclient

import (
	"errors"
	"fmt"
)

var (
	ErrNotReady     = errors.New("hubclient: not ready")
	ErrSendTimeout  = errors.New("hubclient: send queue timeout")
	ErrClosed       = errors.New("hubclient: closed")
	ErrAuthFailed   = errors.New("hubclient: auth failed")
	ErrAlreadyStart = errors.New("hubclient: already started")
	ErrInvalidConfig = errors.New("hubclient: invalid config")
)

// State is the client connection lifecycle state.
type State int

const (
	StateIdle State = iota
	StateDialing
	StateAuthing
	StateSubscribing
	StateReady
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateDialing:
		return "dialing"
	case StateAuthing:
		return "authing"
	case StateSubscribing:
		return "subscribing"
	case StateReady:
		return "ready"
	case StateClosed:
		return "closed"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}
