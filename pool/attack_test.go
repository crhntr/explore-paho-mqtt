package pool

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/crhntr/explore-paho-mqtt/internal/fake"
)

// controllableToken returns a pending token and a func that completes it
// with err. The channel close happens-before Done() observers wake, so
// setting the error first is race-free.
func controllableToken() (mqtt.Token, func(error)) {
	done := make(chan struct{})
	token := &fake.Token{}
	token.DoneReturns(done)
	return token, func(err error) {
		token.ErrorReturns(err)
		close(done)
	}
}

// waitFor polls condition until it is true or the deadline passes.
func waitFor(t *testing.T, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

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

// Robustness: the identity check in remove (pool.go:134) must keep a newer
// entry when an older Add fails after being replaced. A slow failing Add
// must not delete the client that displaced it.
func TestAttack_LateConnectFailureKeepsNewerClient(t *testing.T) {
	slowToken, complete := controllableToken()
	recorder := &clientRecorder{connectToken: tokenQueue(slowToken, completedToken(nil))}
	p := newProxy(Handlers{}, recorder.newClient)

	firstAddErr := make(chan error, 1)
	go func() {
		firstAddErr <- p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a"))
	}()
	waitFor(t, func() bool {
		created := recorder.clients()
		return len(created) == 1 && created[0].ConnectCallCount() == 1
	}, "first Add to insert its client and start connecting")

	if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
		t.Fatalf(`second Add(client-a) error = %v, want nil`, err)
	}
	complete(errors.New("late connect failure"))
	if err := <-firstAddErr; err == nil {
		t.Fatal("first Add(client-a) error = nil, want the late connect failure")
	}

	created := recorder.clients()
	got := maps.Collect(p.Clients())
	if len(got) != 1 || got["client-a"] != mqtt.Client(created[1]) {
		t.Errorf(`Clients() = %v, want map with "client-a" -> the newer client (the late failure must not delete it)`, got)
	}
	if calls := created[1].DisconnectCallCount(); calls != 0 {
		t.Errorf("newer client Disconnect calls = %d, want 0", calls)
	}
}

// Robustness: N concurrent Adds with the same client id must leave exactly
// one client pooled, never disconnected, and every displaced client
// disconnected exactly once (by its replacement).
func TestAttack_ConcurrentAddSameID(t *testing.T) {
	recorder := &clientRecorder{}
	p := newProxy(Handlers{}, recorder.newClient)

	const adds = 16
	var wg sync.WaitGroup
	for range adds {
		wg.Go(func() {
			if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
				t.Errorf("Add(client-a) error = %v, want nil", err)
			}
		})
	}
	wg.Wait()

	got := maps.Collect(p.Clients())
	if len(got) != 1 {
		t.Fatalf("Clients() after %d concurrent Adds of one id = %d entries, want 1", adds, len(got))
	}
	survivor := got["client-a"]
	if survivor == nil {
		t.Fatal(`Clients() missing "client-a" after concurrent Adds`)
	}
	displaced := 0
	for i, client := range recorder.clients() {
		if mqtt.Client(client) == survivor {
			if calls := client.DisconnectCallCount(); calls != 0 {
				t.Errorf("surviving client %d Disconnect calls = %d, want 0", i, calls)
			}
			continue
		}
		displaced++
		if calls := client.DisconnectCallCount(); calls != 1 {
			t.Errorf("displaced client %d Disconnect calls = %d, want exactly 1", i, calls)
		}
	}
	if displaced != adds-1 {
		t.Errorf("displaced clients = %d, want %d", displaced, adds-1)
	}
}

// Robustness/documentation: Close is idempotent (a second Close must not
// disconnect anyone twice) and the pool remains usable afterward — Add after
// Close succeeds and pools the client. This documents observed behavior; it
// is not stated in the Close contract.
func TestAttack_CloseSemantics(t *testing.T) {
	recorder := &clientRecorder{}
	p := newProxy(Handlers{}, recorder.newClient)
	if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
		t.Fatalf(`Add(client-a) error = %v, want nil`, err)
	}

	p.Close()
	p.Close()
	if calls := recorder.clients()[0].DisconnectCallCount(); calls != 1 {
		t.Errorf("client Disconnect calls after double Close = %d, want 1", calls)
	}

	if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-b")); err != nil {
		t.Fatalf(`Add(client-b) after Close error = %v, want nil (pool is documented-by-test as reusable)`, err)
	}
	got := maps.Collect(p.Clients())
	if len(got) != 1 || got["client-b"] == nil {
		t.Errorf(`Clients() after Add-after-Close = %v, want map with "client-b"`, got)
	}
}

// Robustness: Close racing concurrent Adds must not race the detector, and
// after a final Close every client ever created must have been disconnected
// at least once with the pool left empty (no leaked connections).
func TestAttack_CloseRacingAdd(t *testing.T) {
	recorder := &clientRecorder{}
	p := newProxy(Handlers{}, recorder.newClient)

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for range 25 {
				clientID := fmt.Sprintf("client-%d", i)
				if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID(clientID)); err != nil {
					t.Errorf("Add(%s) error = %v, want nil", clientID, err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 25 {
			p.Close()
		}
	})
	wg.Wait()
	p.Close()

	if got := maps.Collect(p.Clients()); len(got) != 0 {
		t.Errorf("Clients() after final Close = %v, want empty", got)
	}
	for i, client := range recorder.clients() {
		if calls := client.DisconnectCallCount(); calls < 1 {
			t.Errorf("client %d Disconnect calls = %d, want at least 1 (leaked connection)", i, calls)
		}
	}
}
