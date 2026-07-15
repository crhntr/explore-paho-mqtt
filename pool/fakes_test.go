package pool

import (
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// fakeToken is a hand-rolled mqtt.Token. Its done channel is closed when the
// operation it represents has completed.
type fakeToken struct {
	done chan struct{}
	err  error
}

func completedToken(err error) *fakeToken {
	done := make(chan struct{})
	close(done)
	return &fakeToken{done: done, err: err}
}

func pendingToken() *fakeToken {
	return &fakeToken{done: make(chan struct{})}
}

func (t *fakeToken) Wait() bool {
	<-t.done
	return true
}

func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.done:
		return true
	case <-time.After(d):
		return false
	}
}

func (t *fakeToken) Done() <-chan struct{} { return t.done }

func (t *fakeToken) Error() error { return t.err }

// fakeClient implements the subset of mqtt.Client the pool touches. Calls to
// any other method panic via the embedded nil interface.
type fakeClient struct {
	mqtt.Client

	clientID     string
	connectToken mqtt.Token

	mu           sync.Mutex
	connectCalls int
	disconnects  []uint
}

func (c *fakeClient) Connect() mqtt.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectCalls++
	return c.connectToken
}

func (c *fakeClient) Disconnect(quiesce uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnects = append(c.disconnects, quiesce)
}

func (c *fakeClient) disconnectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.disconnects)
}

// clientRecorder builds fakeClients for newProxy injection and records every
// client it creates, keyed by creation order.
type clientRecorder struct {
	connectToken func() mqtt.Token

	mu      sync.Mutex
	created []*fakeClient
}

func (r *clientRecorder) newClient(options *mqtt.ClientOptions) mqtt.Client {
	token := mqtt.Token(completedToken(nil))
	if r.connectToken != nil {
		token = r.connectToken()
	}
	client := &fakeClient{clientID: options.ClientID, connectToken: token}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created = append(r.created, client)
	return client
}

func (r *clientRecorder) clients() []*fakeClient {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*fakeClient(nil), r.created...)
}
