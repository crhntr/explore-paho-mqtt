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
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// defaultDisconnectQuiesce is how long a client gets to finish in-flight
// work before its network connection is closed.
const defaultDisconnectQuiesce = 250 * time.Millisecond

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

	mu            sync.Mutex
	clients       map[string]mqtt.Client
	quiesceMillis uint
}

// SetDisconnectQuiesce sets how long a client gets to finish in-flight work
// before the proxy closes its network connection (on Remove, Close, or
// replacement by Add). Negative durations are treated as zero. The default
// is 250ms.
func (p *Proxy) SetDisconnectQuiesce(d time.Duration) {
	p.mu.Lock()
	p.quiesceMillis = uint(max(d, 0).Milliseconds())
	p.mu.Unlock()
}

// disconnect closes client's connection using the configured quiesce.
func (p *Proxy) disconnect(client mqtt.Client) {
	p.mu.Lock()
	quiesce := p.quiesceMillis
	p.mu.Unlock()
	client.Disconnect(quiesce)
}

// newProxy constructs a Proxy with an injectable client constructor for tests.
func newProxy(handlers Handlers, newClient func(*mqtt.ClientOptions) mqtt.Client) *Proxy {
	return &Proxy{
		handlers:      handlers,
		newClient:     newClient,
		clients:       make(map[string]mqtt.Client),
		quiesceMillis: uint(defaultDisconnectQuiesce.Milliseconds()),
	}
}

// New creates a Proxy and adds each of the given client options. If any of
// them fails, the already-added clients are shut down and the error returned.
func New(ctx context.Context, handlers Handlers, options ...*mqtt.ClientOptions) (*Proxy, error) {
	p := newProxy(handlers, mqtt.NewClient)
	if err := p.addAll(ctx, options...); err != nil {
		return nil, err
	}
	return p, nil
}

// addAll adds each of the given options; on failure it shuts down every
// client added so far and returns the error.
func (p *Proxy) addAll(ctx context.Context, options ...*mqtt.ClientOptions) error {
	for _, o := range options {
		if err := p.Add(ctx, o); err != nil {
			p.Close()
			return err
		}
	}
	return nil
}

// Add connects a new client keyed by options.ClientID and waits for the
// connection to be established or ctx to end. The client only enters the
// pool once connected: on connect failure or cancellation it is disconnected,
// nothing is pooled, and an existing client with the same id stays untouched.
// When the new client is pooled, a previous client with the same id is shut
// down and replaced.
func (p *Proxy) Add(ctx context.Context, options *mqtt.ClientOptions) error {
	if options == nil {
		return fmt.Errorf("options must not be nil")
	}
	if options.ClientID == "" {
		return fmt.Errorf("client id must not be empty")
	}
	p.installHandlers(options)
	client := p.newClient(options)

	token := client.Connect()
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			p.disconnect(client)
			return fmt.Errorf("connecting %q: %w", options.ClientID, err)
		}
	case <-ctx.Done():
		p.disconnect(client)
		return fmt.Errorf("connecting %q: %w", options.ClientID, ctx.Err())
	}
	// The select picks randomly when the token and ctx are ready together;
	// cancellation must win deterministically.
	if err := ctx.Err(); err != nil {
		p.disconnect(client)
		return fmt.Errorf("connecting %q: %w", options.ClientID, err)
	}

	p.mu.Lock()
	previous, replaced := p.clients[options.ClientID]
	p.clients[options.ClientID] = client
	p.mu.Unlock()
	if replaced {
		p.disconnect(previous)
	}
	return nil
}

// installHandlers overrides the handler settings on options with wrappers
// that forward to the proxy's Handlers. Wrappers no-op for nil fields so paho
// never invokes a nil handler.
func (p *Proxy) installHandlers(options *mqtt.ClientOptions) {
	handlers := p.handlers
	options.SetOnConnectHandler(func(client mqtt.Client) {
		if handlers.OnConnect != nil {
			handlers.OnConnect(client)
		}
	})
	options.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		if handlers.ConnectionLost != nil {
			handlers.ConnectionLost(client, err)
		}
	})
	options.SetReconnectingHandler(func(client mqtt.Client, options *mqtt.ClientOptions) {
		if handlers.Reconnecting != nil {
			handlers.Reconnecting(client, options)
		}
	})
	options.SetDefaultPublishHandler(func(client mqtt.Client, message mqtt.Message) {
		if handlers.DefaultPublish != nil {
			handlers.DefaultPublish(client, message)
		}
	})
}

// Remove disconnects the client with the given id and reports whether it existed.
func (p *Proxy) Remove(clientID string) bool {
	p.mu.Lock()
	client, ok := p.clients[clientID]
	delete(p.clients, clientID)
	p.mu.Unlock()
	if ok {
		p.disconnect(client)
	}
	return ok
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

// Default returns client options preconfigured with the proxy's handlers,
// connect retry, and auto reconnect. Override what you need (broker URL,
// client id, credentials, ...) and pass the result to Add.
func (p *Proxy) Default() *mqtt.ClientOptions {
	options := mqtt.NewClientOptions().
		SetConnectRetry(true).
		SetConnectRetryInterval(time.Second).
		SetAutoReconnect(true)
	p.installHandlers(options)
	return options
}

// Close disconnects and removes all clients. Close is idempotent and the
// pool remains usable: Add may be called again afterward.
func (p *Proxy) Close() {
	p.mu.Lock()
	clients := p.clients
	p.clients = make(map[string]mqtt.Client)
	p.mu.Unlock()
	for _, client := range clients {
		p.disconnect(client)
	}
}
