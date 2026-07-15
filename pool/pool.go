// Package pool manages a dynamic set of MQTT client connections keyed by
// client id and proxies connection and message events up to the caller
// through standard paho handler signatures.
package pool

import (
	"context"
	"fmt"
	"iter"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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
type Proxy struct{}

// newProxy constructs a Proxy with an injectable client constructor for tests.
func newProxy(handlers Handlers, newClient func(*mqtt.ClientOptions) mqtt.Client) *Proxy {
	panic("not implemented")
}

// New creates a Proxy and adds each of the given client options.
func New(ctx context.Context, handlers Handlers, options ...*mqtt.ClientOptions) (*Proxy, error) {
	p := &Proxy{}
	for _, o := range options {
		if err := p.Add(ctx, o); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Add connects a new client keyed by options.ClientID and waits for the
// connection to be established or ctx to end.
func (p *Proxy) Add(ctx context.Context, options *mqtt.ClientOptions) error {
	if options.ClientID == "" {
		return fmt.Errorf("client id must not be empty")
	}
	return nil
}

// Remove disconnects the client with the given id and reports whether it existed.
func (p *Proxy) Remove(clientID string) bool {
	panic("not implemented")
}

// Clients iterates over the pooled clients keyed by client id.
func (p *Proxy) Clients() iter.Seq2[string, mqtt.Client] {
	panic("not implemented")
}

// Default returns client options preconfigured with the proxy's handlers.
func (p *Proxy) Default() *mqtt.ClientOptions {
	panic("not implemented")
}

// Close disconnects and removes all clients.
func (p *Proxy) Close() {
	panic("not implemented")
}
