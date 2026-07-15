package pool

import (
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/crhntr/explore-paho-mqtt/internal/fake"
)

// completedToken returns a token whose operation has already finished with err.
func completedToken(err error) *fake.Token {
	done := make(chan struct{})
	close(done)
	token := &fake.Token{}
	token.DoneReturns(done)
	token.ErrorReturns(err)
	token.WaitReturns(true)
	return token
}

// pendingToken returns a token whose operation never finishes.
func pendingToken() *fake.Token {
	token := &fake.Token{}
	token.DoneReturns(make(chan struct{}))
	return token
}

// clientRecorder builds fake clients for newProxy injection and records every
// client it creates, in creation order.
type clientRecorder struct {
	connectToken func() mqtt.Token

	mu      sync.Mutex
	created []*fake.Client
}

func (r *clientRecorder) newClient(options *mqtt.ClientOptions) mqtt.Client {
	token := mqtt.Token(completedToken(nil))
	if r.connectToken != nil {
		token = r.connectToken()
	}
	client := &fake.Client{}
	client.ConnectReturns(token)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created = append(r.created, client)
	return client
}

func (r *clientRecorder) clients() []*fake.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*fake.Client(nil), r.created...)
}
