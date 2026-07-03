package conn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient/wire"
)

type outbound struct {
	frame []byte
}

type inbound struct {
	cmd     uint32
	origNid uint32
	payload []byte
}

// Session manages one TCP connection: dial, handshake, keepalive, send/recv loops.
type Session struct {
	cfg     SessionConfig
	log     *slog.Logger
	handlers map[uint32]Handler
	subCmds  []uint32

	onStateChange func(State)
	onReconnect   func()

	sendq chan outbound
	recvq chan inbound
	sysq  chan outbound

	readyCh chan struct{}
	readyMu sync.Mutex
	state   atomic.Uint32

	connMu sync.Mutex
	conn   net.Conn

	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup

	kpPending atomic.Bool
	kpFails   atomic.Int32
}

// NewSession creates a session. Call Run to start dial/reconnect loop.
func NewSession(
	cfg SessionConfig,
	log *slog.Logger,
	handlers map[uint32]Handler,
	subCmds []uint32,
	onStateChange func(State),
	onReconnect func(),
) *Session {
	if log == nil {
		log = slog.Default()
	}
	return &Session{
		cfg:           cfg,
		log:           log,
		handlers:      handlers,
		subCmds:       append([]uint32(nil), subCmds...),
		onStateChange: onStateChange,
		onReconnect:   onReconnect,
		sendq:         make(chan outbound, cfg.SendQueueLen),
		recvq:         make(chan inbound, cfg.RecvQueueLen),
		sysq:          make(chan outbound, 1024),
		readyCh:       make(chan struct{}, 1),
	}
}

// Ready returns a channel closed once when AUTH+SUB handshake completes.
func (s *Session) Ready() <-chan struct{} {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	return s.readyCh
}

// WaitReady blocks until the current handshake completes.
func (s *Session) WaitReady(ctx context.Context) error {
	for {
		if s.ReadyNow() {
			return nil
		}
		s.readyMu.Lock()
		ch := s.readyCh
		s.readyMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			if s.ReadyNow() {
				return nil
			}
		}
	}
}

// ReadyNow reports whether the session is in ready state.
func (s *Session) ReadyNow() bool {
	return State(s.state.Load()) == StateReady
}

// Run dials and maintains the TCP session until ctx is cancelled or Close is called.
func (s *Session) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if s.closed.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}

		s.setState(StateDialing)
		conn, err := net.Dial("tcp", s.cfg.Addr)
		if err != nil {
			s.log.Warn("dial failed", "addr", s.cfg.Addr, "err", err)
			if !s.sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, s.cfg.ReconnectMaxBackoff)
			continue
		}
		backoff = time.Second

		s.connMu.Lock()
		s.conn = conn
		s.connMu.Unlock()

		s.setState(StateAuthing)
		if err := s.handshake(ctx, conn); err != nil {
			s.log.Warn("handshake failed", "err", err)
			_ = conn.Close()
			if !s.sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, s.cfg.ReconnectMaxBackoff)
			continue
		}

		s.markReady()
		if s.onReconnect != nil {
			s.onReconnect()
		}

		runCtx, cancel := context.WithCancel(ctx)
		errCh := make(chan error, 3)
		s.wg.Add(3)
		go func() { defer s.wg.Done(); errCh <- s.recvLoop(runCtx, conn) }()
		go func() { defer s.wg.Done(); errCh <- s.sendLoop(runCtx, conn) }()
		go func() { defer s.wg.Done(); errCh <- s.workerLoop(runCtx) }()

		var runErr error
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
		case runErr = <-errCh:
		}
		cancel()
		s.wg.Wait()

		s.connMu.Lock()
		if s.conn == conn {
			_ = s.conn.Close()
			s.conn = nil
		}
		s.connMu.Unlock()
		s.resetReady()

		if s.closed.Load() {
			return
		}
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			s.log.Warn("session ended", "err", runErr)
		}
		if !s.sleep(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, s.cfg.ReconnectMaxBackoff)
	}
}

// Close stops the session.
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.setState(StateClosed)
		s.connMu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.connMu.Unlock()
	})
}

// EnqueueSend queues a business frame for sendLoop.
func (s *Session) EnqueueSend(frame []byte, timeout time.Duration) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if !s.ReadyNow() {
		return ErrNotReady
	}
	frameCopy := append([]byte(nil), frame...)
	select {
	case s.sendq <- outbound{frame: frameCopy}:
		return nil
	case <-time.After(timeout):
		return ErrSendTimeout
	}
}

func (s *Session) handshake(ctx context.Context, conn net.Conn) error {
	authBody := wire.EncodeAuthBody(s.cfg.GID, s.cfg.User, s.cfg.Pass, s.cfg.NID)
	authFrame := wire.EncodeFrame(wire.CmdAuthReq, s.cfg.NID, wire.FlagSys, authBody)
	if err := writeAll(conn, authFrame); err != nil {
		return err
	}

	snap := &wire.Snap{}
	deadline := time.Now().Add(10 * time.Second)
	authOK := false
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		frame, err := s.readOneFrame(conn, snap)
		if err != nil {
			return err
		}
		if frame == nil {
			continue
		}
		if frame.Header.Type != wire.CmdAuthAck {
			continue
		}
		ok, err := wire.DecodeAuthAck(frame.Payload)
		if err != nil {
			return err
		}
		if !ok {
			return ErrAuthFailed
		}
		authOK = true
		break
	}
	if !authOK {
		return fmt.Errorf("auth ack timeout")
	}

	s.setState(StateSubscribing)
	for _, cmd := range s.subCmds {
		subFrame := wire.EncodeFrame(
			wire.CmdSubReq,
			s.cfg.NID,
			wire.FlagSys,
			wire.EncodeSubBody(cmd),
		)
		if err := writeAll(conn, subFrame); err != nil {
			return err
		}
		if err := s.waitSubAck(ctx, conn, snap, cmd, 10*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) waitSubAck(ctx context.Context, conn net.Conn, snap *wire.Snap, cmd uint32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		frame, err := s.readOneFrame(conn, snap)
		if err != nil {
			return err
		}
		if frame == nil {
			continue
		}
		if frame.Header.Type == wire.CmdSubAck {
			return nil
		}
	}
	return fmt.Errorf("sub ack timeout cmd=0x%X", cmd)
}

func (s *Session) recvLoop(ctx context.Context, conn net.Conn) error {
	snap := &wire.Snap{}
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.KeepaliveInterval * 2)); err != nil {
			return err
		}
		n, err := conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		snap.Append(buf[:n])
		for {
			frame, err := snap.PopFrame()
			if err != nil {
				return err
			}
			if frame == nil {
				break
			}
			if err := s.handleFrame(frame); err != nil {
				return err
			}
		}
	}
}

func (s *Session) sendLoop(ctx context.Context, conn net.Conn) error {
	ticker := time.NewTicker(s.cfg.KeepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-s.sysq:
			if err := writeAll(conn, msg.frame); err != nil {
				return err
			}

		case msg := <-s.sendq:
			if err := writeAll(conn, msg.frame); err != nil {
				return err
			}

		case <-ticker.C:
			if s.kpPending.Load() {
				if s.kpFails.Add(1) >= int32(s.cfg.KeepaliveMaxRetry) {
					return errors.New("keepalive timeout")
				}
			}
			kpFrame := wire.EncodeFrame(wire.CmdKpaliveReq, s.cfg.NID, wire.FlagSys, nil)
			select {
			case s.sysq <- outbound{frame: kpFrame}:
				s.kpPending.Store(true)
			default:
			}
		}
	}
}

func (s *Session) workerLoop(ctx context.Context) error {
	workers := s.cfg.WorkerNum
	if workers <= 0 {
		workers = 1
	}
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-s.recvq:
					s.dispatch(msg.cmd, msg.origNid, msg.payload)
				}
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (s *Session) handleFrame(frame *wire.Frame) error {
	switch frame.Header.Flag {
	case wire.FlagSys:
		return s.handleSysFrame(frame)
	default:
		payload := make([]byte, len(frame.Payload))
		copy(payload, frame.Payload)
		item := inbound{
			cmd:     frame.Header.Type,
			origNid: frame.Header.NID,
			payload: payload,
		}
		select {
		case s.recvq <- item:
			return nil
		default:
		}
		// Block rather than drop: apply TCP backpressure instead of silent loss.
		start := time.Now()
		select {
		case s.recvq <- item:
			if wait := time.Since(start); wait > 50*time.Millisecond {
				s.log.Warn("recv queue backlog",
					"cmd", fmt.Sprintf("0x%04X", frame.Header.Type),
					"wait_ms", wait.Milliseconds(),
					"payload_len", len(payload))
			}
			return nil
		case <-time.After(5 * time.Second):
			s.log.Error("recv queue full, dropping frame",
				"cmd", fmt.Sprintf("0x%04X", frame.Header.Type),
				"payload_len", len(payload))
			return nil
		}
	}
}

func (s *Session) handleSysFrame(frame *wire.Frame) error {
	switch frame.Header.Type {
	case wire.CmdKpaliveAck:
		s.kpPending.Store(false)
		s.kpFails.Store(0)
		return nil
	case wire.CmdKpaliveReq:
		ack := wire.EncodeFrame(wire.CmdKpaliveAck, s.cfg.NID, wire.FlagSys, nil)
		select {
		case s.sysq <- outbound{frame: ack}:
		default:
		}
		return nil
	case wire.CmdSubAck:
		s.log.Debug("sub ack", "payload_len", len(frame.Payload))
		return nil
	case wire.CmdAuthAck:
		return nil
	default:
		s.log.Debug("ignored sys frame", "type", frame.Header.Type)
		return nil
	}
}

func (s *Session) dispatch(cmd, origNid uint32, payload []byte) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("handler panic", "cmd", cmd, "recover", r)
		}
	}()
	h, ok := s.handlers[cmd]
	if !ok {
		h, ok = s.handlers[0]
	}
	if !ok {
		s.log.Warn("drop unknown cmd", "cmd", cmd)
		return
	}
	h(cmd, origNid, payload)
}

func (s *Session) readOneFrame(conn net.Conn, snap *wire.Snap) (*wire.Frame, error) {
	if frame, err := snap.PopFrame(); frame != nil || err != nil {
		return frame, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	snap.Append(buf[:n])
	return snap.PopFrame()
}

func (s *Session) setState(st State) {
	s.state.Store(uint32(st))
	if s.onStateChange != nil {
		s.onStateChange(st)
	}
}

func (s *Session) resetReady() {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	s.readyCh = make(chan struct{}, 1)
	s.setState(StateAuthing)
}

func (s *Session) markReady() {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	s.setState(StateReady)
	close(s.readyCh)
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		next = max
	}
	jitter := time.Duration(rand.Int63n(int64(next / 5)))
	return next + jitter
}

func (s *Session) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
