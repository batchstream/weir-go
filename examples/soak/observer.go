package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const observerStaleAfter = 150 * time.Second

type observerGuard struct {
	statusPath, heartbeatPath string
	modified, lastChange      time.Time
}

func awaitObserver(ctx context.Context, cfg options) (*observerGuard, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	guard := &observerGuard{statusPath: cfg.ObserverStatus, heartbeatPath: cfg.ObserverHeartbeat}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := guard.terminalStatus(); err != nil {
			return nil, err
		}
		info, err := os.Stat(guard.heartbeatPath)
		if err == nil {
			if !info.Mode().IsRegular() {
				return nil, errors.New("observer heartbeat is not a regular file")
			}
			guard.modified, guard.lastChange = info.ModTime(), time.Now()
			return guard, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("observer heartbeat: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("observer readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (g *observerGuard) check(now time.Time) error {
	if err := g.terminalStatus(); err != nil {
		return err
	}
	info, err := os.Stat(g.heartbeatPath)
	if err != nil {
		return fmt.Errorf("observer heartbeat: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("observer heartbeat is not a regular file")
	}
	// File timestamps only detect change. The elapsed interval uses our monotonic clock.
	if !info.ModTime().Equal(g.modified) {
		g.modified, g.lastChange = info.ModTime(), now
	}
	if now.Sub(g.lastChange) > observerStaleAfter {
		return fmt.Errorf("observer heartbeat unchanged for more than %s", observerStaleAfter)
	}
	return nil
}

func (g *observerGuard) terminalStatus() error {
	file, err := os.Open(g.statusPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil // A running observer has no terminal status yet.
	}
	if err != nil {
		return fmt.Errorf("observer status: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(data) > 65536 {
		return errors.New("observer status could not be read within its size limit")
	}
	var status struct {
		Passed *bool  `json:"passed"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(data, &status); err != nil || status.Passed == nil {
		return errors.New("observer status is malformed or missing passed")
	}
	if !*status.Passed {
		return fmt.Errorf("observer failed: %s", status.Error)
	}
	return errors.New("observer exited before the workload completed")
}
