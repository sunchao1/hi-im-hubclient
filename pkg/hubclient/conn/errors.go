package conn

import (
	"errors"
)

var (
	ErrNotReady    = errors.New("conn: not ready")
	ErrSendTimeout = errors.New("conn: send queue timeout")
	ErrClosed      = errors.New("conn: closed")
	ErrAuthFailed  = errors.New("conn: auth failed")
)
