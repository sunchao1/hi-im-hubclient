//go:build integration

package integration_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient"
)

func TestPublishForwardToBackend(t *testing.T) {
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

	const forwardNID = uint32(20111)
	const backendNID = uint32(30111)

	recvCh := make(chan struct{}, 1)

	consumerCfg := hubclient.DefaultConfig()
	consumerCfg.Addr = backendAddr
	consumerCfg.NID = backendNID
	consumerCfg.GID = 1
	consumerCfg.User = user
	consumerCfg.Pass = pass

	consumer, err := hubclient.New(consumerCfg)
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterHandler(benchCmd, func(cmd, origNid uint32, payload []byte) {
		select {
		case recvCh <- struct{}{}:
		default:
		}
	})

	producerCfg := hubclient.DefaultConfig()
	producerCfg.Addr = forwardAddr
	producerCfg.NID = forwardNID
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

	time.Sleep(200 * time.Millisecond)

	const imHeaderSize = 52
	const imNidOffset = 24
	imFrame := make([]byte, imHeaderSize)
	binary.BigEndian.PutUint32(imFrame[0:4], benchCmd)
	binary.BigEndian.PutUint32(imFrame[imNidOffset:imNidOffset+4], backendNID)

	if err := producer.AsyncSend(benchCmd, 0, imFrame); err != nil {
		t.Fatalf("AsyncSend: %v", err)
	}

	select {
	case <-recvCh:
	case <-time.After(10 * time.Second):
		t.Fatal("backend consumer did not receive publish frame")
	}
}
