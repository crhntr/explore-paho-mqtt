// Package broker manages a dynamic set of MQTT client connections keyed by
// client id and proxies connection and message events up to the caller
// through standard paho handler signatures.
package broker

import (
	"cmp"
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

// Handlers are the caller's event handlers. The aggregator installs them on
// every added client. Inside a handler, identify the connection that fired
// with client.OptionsReader().ClientID().
type Handlers struct {
	NewClient      func(*mqtt.ClientOptions) mqtt.Client
	OnConnect      mqtt.OnConnectHandler
	ConnectionLost mqtt.ConnectionLostHandler
	Reconnecting   mqtt.ReconnectHandler
	DefaultPublish mqtt.MessageHandler
}

// Aggregator is a dynamic pool of MQTT clients keyed by client id.
type Aggregator struct {
	handlers  Handlers
	newClient func(*mqtt.ClientOptions) mqtt.Client

	mu            sync.Mutex
	clients       map[string]mqtt.Client
	quiesceMillis uint
}

// disconnect closes client's connection using the configured quiesce.
func (p *Aggregator) disconnect(client mqtt.Client) {
	p.mu.Lock()
	quiesce := p.quiesceMillis
	p.mu.Unlock()
	client.Disconnect(quiesce)
}

// newAggregator constructs an Aggregator with an injectable client constructor for tests.
func newAggregator(handlers Handlers, quiesce time.Duration) *Aggregator {
	if handlers.NewClient == nil {
		handlers.NewClient = mqtt.NewClient
	}
	return &Aggregator{
		handlers:      handlers,
		newClient:     handlers.NewClient,
		clients:       make(map[string]mqtt.Client),
		quiesceMillis: uint(max(cmp.Or(quiesce, defaultDisconnectQuiesce), 0).Milliseconds()),
	}
}

// New creates an Aggregator and adds each of the given client options. If any of
// them fails, the already-added clients are shut down and the error returned.
func New(ctx context.Context, quiesce time.Duration, handlers Handlers, options ...*mqtt.ClientOptions) (*Aggregator, error) {
	p := newAggregator(handlers, quiesce)
	if err := p.Add(ctx, options...); err != nil {
		return nil, err
	}
	return p, nil
}

// Add adds each of the given options; on failure it shuts down the clients
// added by this call, leaves the rest of the pool untouched, and returns
// the error.
func (p *Aggregator) Add(ctx context.Context, options ...*mqtt.ClientOptions) error {
	type addition struct {
		clientID string
		client   mqtt.Client
	}
	added := make([]addition, 0, len(options))
	for _, o := range options {
		client, err := p.addOne(ctx, o)
		if err != nil {
			for _, a := range added {
				p.removeExact(a.clientID, a.client)
			}
			return err
		}
		added = append(added, addition{clientID: o.ClientID, client: client})
	}
	return nil
}

// removeExact disconnects client and forgets it only while it is still the
// pooled client for clientID; a replacement made by a concurrent Add stays
// pooled and is left alone.
func (p *Aggregator) removeExact(clientID string, client mqtt.Client) {
	p.mu.Lock()
	pooled := p.clients[clientID] == client
	if pooled {
		delete(p.clients, clientID)
	}
	p.mu.Unlock()
	if pooled {
		p.disconnect(client)
	}
}

// addOne connects a new client keyed by options.ClientID and waits for the
// connection to be established or ctx to end. The client only enters the
// pool once connected: on connect failure or cancellation it is disconnected,
// nothing is pooled, and an existing client with the same id stays untouched.
// When the new client is pooled, a previous client with the same id is shut
// down and replaced.
func (p *Aggregator) addOne(ctx context.Context, options *mqtt.ClientOptions) (mqtt.Client, error) {
	if options == nil {
		return nil, fmt.Errorf("options must not be nil")
	}
	if options.ClientID == "" {
		return nil, fmt.Errorf("client id must not be empty")
	}
	p.installHandlers(options)
	client := p.newClient(options)

	token := client.Connect()
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			p.disconnect(client)
			return nil, fmt.Errorf("connecting %q: %w", options.ClientID, err)
		}
	case <-ctx.Done():
		p.disconnect(client)
		return nil, fmt.Errorf("connecting %q: %w", options.ClientID, ctx.Err())
	}
	// The select picks randomly when the token and ctx are ready together;
	// cancellation must win deterministically.
	if err := ctx.Err(); err != nil {
		p.disconnect(client)
		return nil, fmt.Errorf("connecting %q: %w", options.ClientID, err)
	}

	p.mu.Lock()
	previous, replaced := p.clients[options.ClientID]
	p.clients[options.ClientID] = client
	p.mu.Unlock()
	if replaced {
		p.disconnect(previous)
	}
	return client, nil
}

// installHandlers overrides the handler settings on options with wrappers
// that forward to the aggregator's Handlers. Wrappers no-op for nil fields so
// paho never invokes a nil handler.
func (p *Aggregator) installHandlers(options *mqtt.ClientOptions) {
	handlers := p.handlers
	if handlers.OnConnect != nil {
		options.SetOnConnectHandler(func(client mqtt.Client) {
			handlers.OnConnect(client)
		})
	}
	if handlers.ConnectionLost != nil {
		options.SetConnectionLostHandler(func(client mqtt.Client, err error) {
			handlers.ConnectionLost(client, err)
		})
	}
	if handlers.Reconnecting != nil {
		options.SetReconnectingHandler(func(client mqtt.Client, options *mqtt.ClientOptions) {
			handlers.Reconnecting(client, options)
		})
	}
	if handlers.DefaultPublish != nil {
		options.SetDefaultPublishHandler(func(client mqtt.Client, message mqtt.Message) {
			handlers.DefaultPublish(client, message)
		})
	}
}

// Remove disconnects the client with the given id and reports whether it existed.
func (p *Aggregator) Remove(clientID string) bool {
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
func (p *Aggregator) Clients() iter.Seq2[string, mqtt.Client] {
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

// Default returns client options preconfigured with the aggregator's handlers,
// connect retry, and auto reconnect. Override what you need (broker URL,
// client id, credentials, ...) and pass the result to Add.
func (p *Aggregator) Default() *mqtt.ClientOptions {
	options := mqtt.NewClientOptions().
		SetConnectRetry(true).
		SetConnectRetryInterval(time.Second).
		SetAutoReconnect(true)
	p.installHandlers(options)
	return options
}

// Close disconnects and removes all clients. Close is idempotent and the
// pool remains usable: Add may be called again afterward.
func (p *Aggregator) Close() {
	p.mu.Lock()
	clients := p.clients
	p.clients = make(map[string]mqtt.Client)
	p.mu.Unlock()
	for _, client := range clients {
		p.disconnect(client)
	}
}
