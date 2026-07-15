package pool

import (
	"context"
	"errors"
	"maps"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// tokenQueue returns a connectToken func for clientRecorder that hands out
// the given tokens in order, one per created client.
func tokenQueue(tokens ...mqtt.Token) func() mqtt.Token {
	queue := make(chan mqtt.Token, len(tokens))
	for _, token := range tokens {
		queue <- token
	}
	return func() mqtt.Token { return <-queue }
}

// Attack: nil/zero-value (CWE-476).
// Hypothesis: Add dereferences options.ClientID without a nil check, so
// Add(ctx, nil) panics instead of returning an error like the empty-id case.
func TestAttack_AddNilOptions(t *testing.T) {
	recorder := &clientRecorder{}
	p := newProxy(Handlers{}, recorder.newClient)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Add(ctx, nil) panicked: %v, want an error return", r)
		}
	}()
	if err := p.Add(t.Context(), nil); err == nil {
		t.Error("Add(ctx, nil) error = nil, want an error")
	}
}

// Attack: context handling.
// Hypothesis: Add's select (pool.go:90-100) races token.Done() against
// ctx.Done(). When the connect token is already complete, both cases are
// ready and Go picks one at random, so Add with an already-canceled context
// nondeterministically returns nil and pools the client instead of failing.
func TestAttack_AddWithCanceledContextNeverSucceeds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	successes := 0
	const attempts = 200
	for i := range attempts {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)
		err := p.Add(ctx, mqtt.NewClientOptions().SetClientID("client-a"))
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("attempt %d: Add(canceled ctx) error = %v, want wrapped %v", i, err, context.Canceled)
		}
	}
	if successes != 0 {
		t.Errorf("Add(canceled ctx) returned nil on %d of %d attempts, want 0 (an already-canceled context must always fail)", successes, attempts)
	}
}

// Attack: state corruption on the failure path.
// Hypothesis: Add replaces the pooled client and disconnects it BEFORE
// connecting the new one (pool.go:81-87). When the new connect then fails or
// is canceled, remove deletes the new entry too, so a failed replacement Add
// destroys the healthy client that was already pooled under that id.
func TestAttack_FailedReplacementDestroysHealthyClient(t *testing.T) {
	connectErr := errors.New("broker unreachable")
	tests := []struct {
		name        string
		secondToken mqtt.Token
		ctx         func(t *testing.T) context.Context
	}{
		{
			name:        "connect failure",
			secondToken: completedToken(connectErr),
			ctx:         func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name:        "canceled context",
			secondToken: pendingToken(),
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &clientRecorder{connectToken: tokenQueue(completedToken(nil), tt.secondToken)}
			p := newProxy(Handlers{}, recorder.newClient)

			if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
				t.Fatalf(`first Add(client-a) error = %v, want nil`, err)
			}
			if err := p.Add(tt.ctx(t), mqtt.NewClientOptions().SetClientID("client-a")); err == nil {
				t.Fatal("second Add(client-a) error = nil, want an error")
			}

			created := recorder.clients()
			got := maps.Collect(p.Clients())
			if len(got) != 1 || got["client-a"] != mqtt.Client(created[0]) {
				t.Errorf(`Clients() after failed replacement = %v, want map with "client-a" -> the original healthy client`, got)
			}
			if calls := created[0].DisconnectCallCount(); calls != 0 {
				t.Errorf("healthy client Disconnect calls after failed replacement = %d, want 0", calls)
			}
		})
	}
}
