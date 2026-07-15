// Package pool manages a dynamic set of MQTT client connections keyed by
// client id and proxies connection and message events up to the caller
// through standard paho handler signatures.
package pool

import (
	"context"
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

// New creates a Proxy and adds each of the given client options.
func New(ctx context.Context, handlers Handlers, options ...*mqtt.ClientOptions) (*Proxy, error) {
	panic("not implemented")
}

// Add connects a new client keyed by options.ClientID and waits for the
// connection to be established or ctx to end.
func (p *Proxy) Add(ctx context.Context, options *mqtt.ClientOptions) error {
	panic("not implemented")
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
