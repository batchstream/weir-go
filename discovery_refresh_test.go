package weir

import (
	"fmt"
	"testing"
	"time"
)

func TestRefreshPrioritizesUnrenewedStoresAfterRoundDeadline(t *testing.T) {
	business := &clientTestPeer{mode: "normal"}
	peer := &discoveryPeer{business: business}
	listener := listenDiscovery(t, peer, "127.0.0.1:0")
	names := make([]string, 16)
	for i := range names {
		name := fmt.Sprintf("records-%02d", i)
		names[i] = name
		response := discoveryRecord(name, listener.address)
		response.CacheTtlMs = 5000
		peer.set(name, response)
	}
	options := OpenOptions{Seed: listener.address, Stores: names, RefreshInterval: 50 * time.Millisecond, ResolveTimeout: 2 * time.Second}
	client := openDiscovery(t, options)
	original := make(map[string]time.Time, len(names))
	for name, entry := range client.stores {
		entry.mu.RLock()
		original[name] = entry.expires
		entry.mu.RUnlock()
	}
	peer.mu.Lock()
	peer.delay = 300 * time.Millisecond
	peer.mu.Unlock()
	until := time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		for _, name := range names {
			discoveryRead(t, client, name)
		}
		time.Sleep(75 * time.Millisecond)
	}
	for name, entry := range client.stores {
		entry.mu.RLock()
		refreshed := entry.expires.After(original[name])
		entry.mu.RUnlock()
		if !refreshed {
			t.Fatalf("deadline repeatedly starved Store %s", name)
		}
	}
	started := time.Now()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("Close did not cancel active bounded refresh round")
	}
}

func TestRefreshFailuresCannotStarveHealthyTail(t *testing.T) {
	business := &clientTestPeer{mode: "normal"}
	peer := &discoveryPeer{business: business}
	listener := listenDiscovery(t, peer, "127.0.0.1:0")
	names := []string{"abandoned-a", "abandoned-b", "abandoned-c", "abandoned-d", "records"}
	for _, name := range names {
		response := discoveryRecord(name, listener.address)
		response.CacheTtlMs = 5000
		peer.set(name, response)
	}
	options := OpenOptions{Seed: listener.address, Stores: names, RefreshInterval: 50 * time.Millisecond, ResolveTimeout: 2 * time.Second}
	client := openDiscovery(t, options)
	peer.mu.Lock()
	peer.delays = make(map[string]time.Duration)
	for _, name := range names[:4] {
		peer.records[name] = nil
		peer.delays[name] = time.Second
	}
	peer.mu.Unlock()
	until := time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		discoveryRead(t, client, "records")
		time.Sleep(75 * time.Millisecond)
	}
	entry := client.stores["records"]
	entry.mu.RLock()
	valid := time.Now().Before(entry.expires)
	entry.mu.RUnlock()
	if !valid {
		t.Fatal("repeated failing control lookups starved the healthy Store")
	}
}
