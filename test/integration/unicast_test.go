//go:build integration

package integration_test

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sunchao1/hi-im-api/pkg/im/cmd"
	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient"
)

const benchCmd = cmd.CMD_GROUP_CHAT

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func waitTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func TestUnicastForwardToBackend(t *testing.T) {
	forwardAddr := envOr("HIIM_FORWARD_ADDR", "127.0.0.1:28888")
	backendAddr := envOr("HIIM_BACKEND_ADDR", "127.0.0.1:28889")
	user := envOr("HIIM_AUTH_USER", "proxy")
	pass := envOr("HIIM_AUTH_PASS", "proxy")

	if err := waitTCP(forwardAddr, 3*time.Second); err != nil {
		t.Skipf("hub forward not reachable at %s: %v", forwardAddr, err)
	}
	if err := waitTCP(backendAddr, 3*time.Second); err != nil {
		t.Skipf("hub backend not reachable at %s: %v", backendAddr, err)
	}

	const consumerNID = 40001
	const producerNID = 50001

	var recvCount atomic.Int32
	recvCh := make(chan struct{}, 1)

	consumerCfg := hubclient.DefaultConfig()
	consumerCfg.Addr = forwardAddr
	consumerCfg.NID = consumerNID
	consumerCfg.GID = 1
	consumerCfg.User = user
	consumerCfg.Pass = pass

	consumer, err := hubclient.New(consumerCfg)
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterHandler(benchCmd, func(cmd, origNid uint32, payload []byte) {
		recvCount.Add(1)
		select {
		case recvCh <- struct{}{}:
		default:
		}
	})

	producerCfg := hubclient.DefaultConfig()
	producerCfg.Addr = backendAddr
	producerCfg.NID = producerNID
	producerCfg.GID = 1
	producerCfg.User = user
	producerCfg.Pass = pass

	producer, err := hubclient.New(producerCfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := producer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	defer producer.Close()

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := consumer.WaitReady(waitCtx); err != nil {
		t.Fatalf("consumer WaitReady: %v", err)
	}
	if err := producer.WaitReady(waitCtx); err != nil {
		t.Fatalf("producer WaitReady: %v", err)
	}

	// Hub bridge reads IM dest nid at offset 4 (48B beehive layout), see hi-im-core bridge.cpp.
	imFrame := make([]byte, 48)
	binary.BigEndian.PutUint32(imFrame[0:4], benchCmd)
	binary.BigEndian.PutUint32(imFrame[4:8], consumerNID)

	if err := producer.AsyncSend(benchCmd, consumerNID, imFrame); err != nil {
		t.Fatalf("AsyncSend: %v", err)
	}

	select {
	case <-recvCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("consumer did not receive frame, count=%d", recvCount.Load())
	}
	if recvCount.Load() < 1 {
		t.Fatalf("recv count = %d, want >= 1", recvCount.Load())
	}
}
