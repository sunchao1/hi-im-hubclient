package hubclient

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds hub TCP client settings.
type Config struct {
	Addr     string
	NID      uint32
	GID      uint32
	User     string
	Pass     string
	Subscribe []uint32

	PoolSize            int
	WorkerNum           int
	SendQueueLen        int
	RecvQueueLen        int
	KeepaliveInterval   time.Duration
	KeepaliveMaxRetry   int
	ReconnectMaxBackoff time.Duration
	SendTimeout         time.Duration
}

// DefaultConfig returns production-ish defaults from the technical design.
func DefaultConfig() *Config {
	return &Config{
		GID:                 10,
		PoolSize:            1,
		WorkerNum:           4,
		SendQueueLen:        50000,
		RecvQueueLen:        50000,
		KeepaliveInterval:   30 * time.Second,
		KeepaliveMaxRetry:   3,
		ReconnectMaxBackoff: 30 * time.Second,
		SendTimeout:         time.Second,
	}
}

// ConfigFromEnv reads HIIM_* environment variables.
func ConfigFromEnv() (*Config, error) {
	cfg := DefaultConfig()

	if v := os.Getenv("HIIM_BACKEND_ADDR"); v != "" {
		cfg.Addr = v
	} else if v := os.Getenv("HIIM_FORWARD_ADDR"); v != "" {
		cfg.Addr = v
	}

	if v := os.Getenv("HIIM_NID"); v != "" {
		nid, err := parseU32(v)
		if err != nil {
			return nil, fmt.Errorf("HIIM_NID: %w", err)
		}
		cfg.NID = nid
	}

	if v := os.Getenv("HIIM_AUTH_USER"); v != "" {
		cfg.User = v
	}
	if v := os.Getenv("HIIM_AUTH_PASS"); v != "" {
		cfg.Pass = v
	}

	if v := os.Getenv("HIIM_SUB_CMDS"); v != "" {
		cmds, err := parseHexList(v)
		if err != nil {
			return nil, fmt.Errorf("HIIM_SUB_CMDS: %w", err)
		}
		cfg.Subscribe = cmds
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c == nil {
		return ErrInvalidConfig
	}
	if c.Addr == "" {
		return fmt.Errorf("%w: empty Addr", ErrInvalidConfig)
	}
	if c.NID == 0 {
		return fmt.Errorf("%w: NID must be non-zero", ErrInvalidConfig)
	}
	if c.WorkerNum <= 0 {
		c.WorkerNum = 1
	}
	if c.SendQueueLen <= 0 {
		c.SendQueueLen = 50000
	}
	if c.RecvQueueLen <= 0 {
		c.RecvQueueLen = 50000
	}
	if c.KeepaliveInterval <= 0 {
		c.KeepaliveInterval = 30 * time.Second
	}
	if c.KeepaliveMaxRetry <= 0 {
		c.KeepaliveMaxRetry = 3
	}
	if c.ReconnectMaxBackoff <= 0 {
		c.ReconnectMaxBackoff = 30 * time.Second
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = time.Second
	}
	return nil
}

func parseU32(s string) (uint32, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(s), 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func parseHexList(s string) ([]uint32, error) {
	parts := strings.Split(s, ",")
	out := make([]uint32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := parseU32(p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
