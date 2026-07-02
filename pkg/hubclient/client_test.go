package hubclient_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient"
	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient/wire"
)

func TestClientAuthSubLifecycle(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		snap := &wire.Snap{}
		buf := make([]byte, 4096)
		authOK := false
		for {
			if authOK {
				// Keep connection open until client closes.
				if _, err := conn.Read(buf); err != nil {
					return
				}
				continue
			}
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			snap.Append(buf[:n])
			for {
				frame, err := snap.PopFrame()
				if err != nil || frame == nil {
					break
				}
				if frame.Header.Type == wire.CmdAuthReq {
					ack := wire.EncodeFrame(wire.CmdAuthAck, 1, wire.FlagSys, []byte{0, 0, 0, 1})
					if _, err := conn.Write(ack); err != nil {
						return
					}
					authOK = true
				}
			}
		}
	}()

	cfg := hubclient.DefaultConfig()
	cfg.Addr = ln.Addr().String()
	cfg.NID = 40001
	cfg.GID = 1
	cfg.User = "proxy"
	cfg.Pass = "proxy"

	cli, err := hubclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli.RegisterHandler(0x030B, func(cmd, origNid uint32, payload []byte) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cli.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := cli.WaitReady(waitCtx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if !cli.Ready() {
		t.Fatal("Ready() false after WaitReady")
	}
}

func TestAsyncSendNotReady(t *testing.T) {
	cfg := hubclient.DefaultConfig()
	cfg.Addr = "127.0.0.1:1"
	cfg.NID = 1
	cfg.User = "u"
	cfg.Pass = "p"

	cli, err := hubclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.AsyncSend(1, 0, []byte("x")); err != hubclient.ErrNotReady {
		t.Fatalf("want ErrNotReady, got %v", err)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("HIIM_FORWARD_ADDR", "hub:28888")
	t.Setenv("HIIM_NID", "20001")
	t.Setenv("HIIM_AUTH_USER", "proxy")
	t.Setenv("HIIM_AUTH_PASS", "secret")
	t.Setenv("HIIM_SUB_CMDS", "0x030B,0x0101")

	cfg, err := hubclient.ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "hub:28888" || cfg.NID != 20001 || cfg.User != "proxy" || cfg.Pass != "secret" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if len(cfg.Subscribe) != 2 || cfg.Subscribe[0] != 0x030B || cfg.Subscribe[1] != 0x0101 {
		t.Fatalf("unexpected subscribe: %v", cfg.Subscribe)
	}
}
