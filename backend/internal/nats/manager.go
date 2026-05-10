package nats

import (
	"fmt"
	"sync"

	"nats-ui-backend/internal/config"
)

// ServerManager manages multiple NATS server connections
type ServerManager struct {
	servers map[string]*Client
	mu      sync.RWMutex
}

// NewServerManager creates a new server manager and connects to all configured servers
func NewServerManager(cfg *config.Config) (*ServerManager, error) {
	sm := &ServerManager{
		servers: make(map[string]*Client),
	}

	for _, server := range cfg.Servers {
		client, err := NewClientFromServer(server)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", server.Name, err)
		}
		sm.servers[server.Name] = client
	}

	return sm, nil
}

// Get returns a client for the given server name
func (sm *ServerManager) Get(name string) (*Client, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	client, ok := sm.servers[name]
	if !ok {
		return nil, fmt.Errorf("server %q not found", name)
	}
	return client, nil
}

// List returns all available server names
func (sm *ServerManager) List() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	names := make([]string, 0, len(sm.servers))
	for name := range sm.servers {
		names = append(names, name)
	}
	return names
}

// GetAll returns all connected clients
func (sm *ServerManager) GetAll() map[string]*Client {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string]*Client, len(sm.servers))
	for name, client := range sm.servers {
		result[name] = client
	}
	return result
}

// Close closes all connections
func (sm *ServerManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for name, client := range sm.servers {
		client.Close()
		delete(sm.servers, name)
	}
}
