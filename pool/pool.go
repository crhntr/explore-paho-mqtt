// Package pool manages a dynamic set of MQTT client connections keyed by
// client id and proxies connection and message events up to the caller
// through standard paho handler signatures.
package pool

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// disconnectQuiesce is how many milliseconds a client gets to finish
// in-flight work before its network connection is closed.
const disconnectQuiesce = 250

// Handlers are the caller's event handlers. The proxy installs them on every
// added client. Inside a handler, identify the connection that fired with
// client.OptionsReader().ClientID().
type Handlers struct {
	OnConnect      mqtt.OnConnectHandler
	ConnectionLost mqtt.ConnectionLostHandler
	Reconnecting   mqtt.ReconnectHandler
	DefaultPublish mqtt.MessageHandler
}

// Proxy is a dynamic pool of MQTT clients keyed by client id.
type Proxy struct {
	handlers  Handlers
	newClient func(*mqtt.ClientOptions) mqtt.Client

	mu      sync.Mutex
	clients map[string]mqtt.Client
}

// newProxy constructs a Proxy with an injectable client constructor for tests.
func newProxy(handlers Handlers, newClient func(*mqtt.ClientOptions) mqtt.Client) *Proxy {
	return &Proxy{
		handlers:  handlers,
		newClient: newClient,
		clients:   make(map[string]mqtt.Client),
	}
}

// New creates a Proxy and adds each of the given client options.
func New(ctx context.Context, handlers Handlers, options ...*mqtt.ClientOptions) (*Proxy, error) {
	p := newProxy(handlers, mqtt.NewClient)
	for _, o := range options {
		if err := p.Add(ctx, o); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Add connects a new client keyed by options.ClientID and waits for the
// connection to be established or ctx to end. On failure or cancellation the
// client is disconnected and nothing is pooled.
func (p *Proxy) Add(ctx context.Context, options *mqtt.ClientOptions) error {
	if options.ClientID == "" {
		return fmt.Errorf("client id must not be empty")
	}
	client := p.newClient(options)

	p.mu.Lock()
	previous, replaced := p.clients[options.ClientID]
	p.clients[options.ClientID] = client
	p.mu.Unlock()
	if replaced {
		previous.Disconnect(disconnectQuiesce)
	}

	token := client.Connect()
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			p.remove(options.ClientID, client)
			return fmt.Errorf("connecting %q: %w", options.ClientID, err)
		}
		return nil
	case <-ctx.Done():
		p.remove(options.ClientID, client)
		return fmt.Errorf("connecting %q: %w", options.ClientID, ctx.Err())
	}
}

// remove deletes the entry for clientID if it still holds client and
// disconnects client either way.
func (p *Proxy) remove(clientID string, client mqtt.Client) {
	p.mu.Lock()
	if p.clients[clientID] == client {
		delete(p.clients, clientID)
	}
	p.mu.Unlock()
	client.Disconnect(disconnectQuiesce)
}

// Remove disconnects the client with the given id and reports whether it existed.
func (p *Proxy) Remove(clientID string) bool {
	panic("not implemented")
}

// Clients iterates over a snapshot of the pooled clients keyed by client id.
// The pool may be modified while ranging.
func (p *Proxy) Clients() iter.Seq2[string, mqtt.Client] {
	return func(yield func(string, mqtt.Client) bool) {
		p.mu.Lock()
		snapshot := maps.Clone(p.clients)
		p.mu.Unlock()
		for clientID, client := range snapshot {
			if !yield(clientID, client) {
				return
			}
		}
	}
}

// Default returns client options preconfigured with the proxy's handlers.
func (p *Proxy) Default() *mqtt.ClientOptions {
	panic("not implemented")
}

// Close disconnects and removes all clients.
func (p *Proxy) Close() {
	panic("not implemented")
}
