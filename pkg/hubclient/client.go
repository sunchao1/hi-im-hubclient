package hubclient

import (
	"context"
	"log/slog"
	"sort"
	"sync"

	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient/conn"
	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient/wire"
)

// Handler is invoked synchronously on a worker goroutine; do not block.
type Handler func(cmd, origNid uint32, payload []byte)

// Client is the hub TCP client API.
type Client struct {
	cfg *Config
	log *slog.Logger

	handlers   map[uint32]Handler
	subCmds    map[uint32]struct{}
	registerMu sync.Mutex

	session *conn.Session

	onReconnect   func()
	onStateChange func(State)

	startOnce sync.Once
	closeOnce sync.Once
	started   bool
	closed    bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Option configures Client.
type Option func(*Client)

// WithLogger injects a slog.Logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) {
		c.log = l
	}
}

// WithOnReconnect registers a callback after each successful reconnect handshake.
func WithOnReconnect(fn func()) Option {
	return func(c *Client) {
		c.onReconnect = fn
	}
}

// WithOnStateChange registers a lifecycle state observer.
func WithOnStateChange(fn func(State)) Option {
	return func(c *Client) {
		c.onStateChange = fn
	}
}

// New validates cfg and returns a Client. Call RegisterHandler before Start.
func New(cfg *Config, opts ...Option) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &Client{
		cfg:      cfg,
		log:      slog.Default(),
		handlers: make(map[uint32]Handler),
		subCmds:  make(map[uint32]struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	for _, cmd := range cfg.Subscribe {
		c.subCmds[cmd] = struct{}{}
	}
	return c, nil
}

// RegisterHandler must be called before Start. cmd=0 installs the default handler.
func (c *Client) RegisterHandler(cmd uint32, h Handler) error {
	c.registerMu.Lock()
	defer c.registerMu.Unlock()
	if c.started {
		return ErrAlreadyStart
	}
	c.handlers[cmd] = h
	if cmd != 0 {
		c.subCmds[cmd] = struct{}{}
	}
	return nil
}

// Start launches dial/reconnect goroutines. Non-blocking.
func (c *Client) Start(ctx context.Context) error {
	var startErr error
	c.startOnce.Do(func() {
		c.registerMu.Lock()
		if c.started {
			startErr = ErrAlreadyStart
			c.registerMu.Unlock()
			return
		}
		c.started = true
		c.registerMu.Unlock()

		c.ctx, c.cancel = context.WithCancel(ctx)
		subCmds := c.subCmdList()
		c.session = conn.NewSession(
			c.sessionConfig(),
			c.log,
			c.connHandlers(),
			subCmds,
			c.connStateObserver(),
			c.onReconnect,
		)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.session.Run(c.ctx)
		}()
	})
	return startErr
}

// Close idempotently stops the client and waits for goroutines.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed = true
		if c.cancel != nil {
			c.cancel()
		}
		if c.session != nil {
			c.session.Close()
		}
	})
	c.wg.Wait()
	return nil
}

// WaitReady blocks until AUTH+SUB complete or ctx is cancelled.
func (c *Client) WaitReady(ctx context.Context) error {
	if c.session == nil {
		return ErrNotReady
	}
	return c.session.WaitReady(ctx)
}

// Ready reports whether the client has completed handshake.
func (c *Client) Ready() bool {
	if c.session == nil {
		return false
	}
	return c.session.ReadyNow()
}

// AsyncSend enqueues a business frame. destNid==0 uses Config.NID as wire.nid.
func (c *Client) AsyncSend(cmd, destNid uint32, payload []byte) error {
	if c.session == nil {
		return ErrNotReady
	}
	nid := c.cfg.NID
	if destNid > 0 {
		nid = destNid
	}
	frame := wire.EncodeFrame(cmd, nid, wire.FlagExp, payload)
	err := c.session.EnqueueSend(frame, c.cfg.SendTimeout)
	return mapConnError(err)
}

func (c *Client) sessionConfig() conn.SessionConfig {
	return conn.SessionConfig{
		Addr:                c.cfg.Addr,
		NID:                 c.cfg.NID,
		GID:                 c.cfg.GID,
		User:                c.cfg.User,
		Pass:                c.cfg.Pass,
		WorkerNum:           c.cfg.WorkerNum,
		SendQueueLen:        c.cfg.SendQueueLen,
		RecvQueueLen:        c.cfg.RecvQueueLen,
		KeepaliveInterval:   c.cfg.KeepaliveInterval,
		KeepaliveMaxRetry:   c.cfg.KeepaliveMaxRetry,
		ReconnectMaxBackoff: c.cfg.ReconnectMaxBackoff,
		SendTimeout:         c.cfg.SendTimeout,
	}
}

func (c *Client) connHandlers() map[uint32]conn.Handler {
	handlers := c.cloneHandlers()
	out := make(map[uint32]conn.Handler, len(handlers))
	for k, v := range handlers {
		h := v
		out[k] = func(cmd, origNid uint32, payload []byte) {
			h(cmd, origNid, payload)
		}
	}
	return out
}

func (c *Client) connStateObserver() func(conn.State) {
	if c.onStateChange == nil {
		return nil
	}
	return func(st conn.State) {
		c.onStateChange(State(st))
	}
}

func mapConnError(err error) error {
	if err == nil {
		return nil
	}
	switch err {
	case conn.ErrNotReady:
		return ErrNotReady
	case conn.ErrSendTimeout:
		return ErrSendTimeout
	case conn.ErrClosed:
		return ErrClosed
	case conn.ErrAuthFailed:
		return ErrAuthFailed
	default:
		return err
	}
}

func (c *Client) cloneHandlers() map[uint32]Handler {
	c.registerMu.Lock()
	defer c.registerMu.Unlock()
	out := make(map[uint32]Handler, len(c.handlers))
	for k, v := range c.handlers {
		out[k] = v
	}
	return out
}

func (c *Client) subCmdList() []uint32 {
	c.registerMu.Lock()
	defer c.registerMu.Unlock()
	out := make([]uint32, 0, len(c.subCmds))
	for cmd := range c.subCmds {
		out = append(out, cmd)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
