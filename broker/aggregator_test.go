package broker

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

//go:generate counterfeiter -generate

//counterfeiter:generate -o ../internal/fake/mqtt_client.go --fake-name=Client github.com/eclipse/paho.mqtt.golang.Client
//counterfeiter:generate -o ../internal/fake/mqtt_message.go --fake-name=Message github.com/eclipse/paho.mqtt.golang.Message
//counterfeiter:generate -o ../internal/fake/mqtt_token.go --fake-name=Token github.com/eclipse/paho.mqtt.golang.Token

func TestProxy_Add(t *testing.T) {
	t.Run("empty client id", func(t *testing.T) {
		p, err := New(t.Context(), Handlers{})
		if err != nil {
			t.Fatalf("New(Handlers{}) error = %v, want nil", err)
		}
		if err := p.Add(t.Context(), mqtt.NewClientOptions()); err == nil {
			t.Error("Add(options with empty ClientID) error = nil, want an error")
		}
	})

	t.Run("stores the connected client", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)

		err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a"))
		if err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}

		created := recorder.clients()
		if len(created) != 1 {
			t.Fatalf("clients created = %d, want 1", len(created))
		}
		if created[0].ConnectCallCount() != 1 {
			t.Errorf("Connect calls = %d, want 1", created[0].ConnectCallCount())
		}
		got := maps.Collect(p.Clients())
		if len(got) != 1 || got["client-a"] != mqtt.Client(created[0]) {
			t.Errorf(`Clients() = %v, want map with "client-a" -> the created client`, got)
		}
	})

	t.Run("replaces and shuts down a client with the same id", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)

		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
			t.Fatalf(`first Add(client-a) error = %v, want nil`, err)
		}
		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
			t.Fatalf(`second Add(client-a) error = %v, want nil`, err)
		}

		created := recorder.clients()
		if len(created) != 2 {
			t.Fatalf("clients created = %d, want 2", len(created))
		}
		if created[0].DisconnectCallCount() != 1 {
			t.Errorf("replaced client Disconnect calls = %d, want 1", created[0].DisconnectCallCount())
		}
		got := maps.Collect(p.Clients())
		if len(got) != 1 || got["client-a"] != mqtt.Client(created[1]) {
			t.Errorf(`Clients() = %v, want map with "client-a" -> the second client`, got)
		}
	})

	t.Run("connect failure pools nothing", func(t *testing.T) {
		connectErr := errors.New("broker unreachable")
		recorder := &clientRecorder{connectToken: func() mqtt.Token { return completedToken(connectErr) }}
		p := newProxy(Handlers{}, recorder.newClient)

		err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a"))
		if !errors.Is(err, connectErr) {
			t.Fatalf("Add(client-a) error = %v, want wrapped %v", err, connectErr)
		}
		if got := maps.Collect(p.Clients()); len(got) != 0 {
			t.Errorf("Clients() after failed Add = %v, want empty", got)
		}
	})

	t.Run("remove disconnects and forgets the client", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)
		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}

		if got := p.Remove("client-a"); !got {
			t.Errorf(`Remove("client-a") = %t, want true`, got)
		}
		if got := recorder.clients()[0].DisconnectCallCount(); got != 1 {
			t.Errorf("removed client Disconnect calls = %d, want 1", got)
		}
		if got := maps.Collect(p.Clients()); len(got) != 0 {
			t.Errorf("Clients() after Remove = %v, want empty", got)
		}
		if got := p.Remove("client-a"); got {
			t.Errorf(`Remove("client-a") again = %t, want false`, got)
		}
	})

	t.Run("installs the proxy handlers on the options", func(t *testing.T) {
		var (
			onConnect, connectionLost, reconnecting, defaultPublish int
			gotClient                                               mqtt.Client
			gotErr                                                  error
		)
		handlers := Handlers{
			OnConnect:      func(c mqtt.Client) { onConnect++; gotClient = c },
			ConnectionLost: func(_ mqtt.Client, err error) { connectionLost++; gotErr = err },
			Reconnecting:   func(mqtt.Client, *mqtt.ClientOptions) { reconnecting++ },
			DefaultPublish: func(mqtt.Client, mqtt.Message) { defaultPublish++ },
		}
		recorder := &clientRecorder{}
		p := newProxy(handlers, recorder.newClient)

		overridden := false
		options := mqtt.NewClientOptions().SetClientID("client-a")
		options.SetOnConnectHandler(func(mqtt.Client) { overridden = true })
		if err := p.Add(t.Context(), options); err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}

		client := recorder.clients()[0]
		lostErr := errors.New("connection reset")
		options.OnConnect(client)
		options.OnConnectionLost(client, lostErr)
		options.OnReconnecting(client, options)
		options.DefaultPublishHandler(client, nil)

		if overridden {
			t.Error("caller-set OnConnect handler was invoked, want it overridden by the proxy")
		}
		if onConnect != 1 || gotClient != mqtt.Client(client) {
			t.Errorf("Handlers.OnConnect calls = %d with client %v, want 1 with the added client", onConnect, gotClient)
		}
		if connectionLost != 1 || !errors.Is(gotErr, lostErr) {
			t.Errorf("Handlers.ConnectionLost calls = %d with error %v, want 1 with %v", connectionLost, gotErr, lostErr)
		}
		if reconnecting != 1 {
			t.Errorf("Handlers.Reconnecting calls = %d, want 1", reconnecting)
		}
		if defaultPublish != 1 {
			t.Errorf("Handlers.DefaultPublish calls = %d, want 1", defaultPublish)
		}
	})

	t.Run("default options can be overridden and added", func(t *testing.T) {
		onConnect := 0
		recorder := &clientRecorder{}
		p := newProxy(Handlers{OnConnect: func(mqtt.Client) { onConnect++ }}, recorder.newClient)

		options := p.Default()

		if !options.ConnectRetry {
			t.Error("Default().ConnectRetry = false, want true")
		}
		if !options.AutoReconnect {
			t.Error("Default().AutoReconnect = false, want true")
		}
		if options.ConnectRetryInterval != time.Second {
			t.Errorf("Default().ConnectRetryInterval = %v, want %v", options.ConnectRetryInterval, time.Second)
		}
		if options.OnReconnecting != nil || options.DefaultPublishHandler != nil {
			t.Error("only set handlers that are not null on the Handlers struct")
		}
		if options.OnConnectionLost == nil {
			t.Error("the default options set this in NewClientOptions so it is okay for it to be set even though it is not set on Handlers")
		}
		if options.OnConnect == nil {
			t.Fatal("Default().OnConnect = nil, want the proxy handler installed")
		}
		options.OnConnect(&fake.Client{})
		if onConnect != 1 {
			t.Errorf("Handlers.OnConnect calls = %d, want 1", onConnect)
		}

		if err := p.Add(t.Context(), options.SetClientID("client-a")); err != nil {
			t.Fatalf(`Add(Default() with client-a) error = %v, want nil`, err)
		}
		if got := maps.Collect(p.Clients()); len(got) != 1 {
			t.Errorf("Clients() after adding defaults = %v, want one entry", got)
		}
	})

	t.Run("initial options failure shuts down already-added clients", func(t *testing.T) {
		connectErr := errors.New("broker unreachable")
		tokens := []mqtt.Token{completedToken(nil), completedToken(connectErr)}
		recorder := &clientRecorder{connectToken: func() mqtt.Token {
			token := tokens[0]
			tokens = tokens[1:]
			return token
		}}
		p := newProxy(Handlers{}, recorder.newClient)

		err := p.addAll(t.Context(),
			mqtt.NewClientOptions().SetClientID("client-a"),
			mqtt.NewClientOptions().SetClientID("client-b"),
		)
		if !errors.Is(err, connectErr) {
			t.Fatalf("addAll(client-a, failing client-b) error = %v, want wrapped %v", err, connectErr)
		}
		if got := recorder.clients()[0].DisconnectCallCount(); got != 1 {
			t.Errorf("already-added client Disconnect calls = %d, want 1", got)
		}
		if got := maps.Collect(p.Clients()); len(got) != 0 {
			t.Errorf("Clients() after failed addAll = %v, want empty", got)
		}
	})

	t.Run("canceled context abandons the connection", func(t *testing.T) {
		recorder := &clientRecorder{connectToken: func() mqtt.Token { return pendingToken() }}
		p := newProxy(Handlers{}, recorder.newClient)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := p.Add(ctx, mqtt.NewClientOptions().SetClientID("client-a"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Add(client-a) error = %v, want wrapped %v", err, context.Canceled)
		}
		if got := maps.Collect(p.Clients()); len(got) != 0 {
			t.Errorf("Clients() after canceled Add = %v, want empty", got)
		}
		created := recorder.clients()
		if len(created) != 1 {
			t.Fatalf("clients created = %d, want 1", len(created))
		}
		if created[0].DisconnectCallCount() != 1 {
			t.Errorf("abandoned client Disconnect calls = %d, want 1", created[0].DisconnectCallCount())
		}
	})
}

func TestProxy_ConcurrentUse(t *testing.T) {
	recorder := &clientRecorder{}
	p := newProxy(Handlers{}, recorder.newClient)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			clientID := fmt.Sprintf("client-%d", i%4)
			for range 50 {
				if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID(clientID)); err != nil {
					t.Errorf("Add(%s) error = %v, want nil", clientID, err)
					return
				}
				for range p.Clients() {
				}
				p.Remove(clientID)
			}
		})
	}
	wg.Wait()
	p.Close()

	if got := maps.Collect(p.Clients()); len(got) != 0 {
		t.Errorf("Clients() after concurrent use and Close = %v, want empty", got)
	}
}

func TestProxy_SetDisconnectQuiesce(t *testing.T) {
	t.Run("configured quiesce reaches Disconnect", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)
		p.SetDisconnectQuiesce(2 * time.Second)

		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}
		p.Remove("client-a")

		client := recorder.clients()[0]
		if client.DisconnectCallCount() != 1 {
			t.Fatalf("Disconnect calls = %d, want 1", client.DisconnectCallCount())
		}
		if got := client.DisconnectArgsForCall(0); got != 2000 {
			t.Errorf("Disconnect quiesce = %d ms, want 2000", got)
		}
	})

	t.Run("default is 250 milliseconds", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)

		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}
		p.Close()

		if got := recorder.clients()[0].DisconnectArgsForCall(0); got != 250 {
			t.Errorf("Disconnect quiesce = %d ms, want 250", got)
		}
	})

	t.Run("negative durations clamp to zero", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)
		p.SetDisconnectQuiesce(-time.Second)

		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID("client-a")); err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}
		p.Remove("client-a")

		if got := recorder.clients()[0].DisconnectArgsForCall(0); got != 0 {
			t.Errorf("Disconnect quiesce = %d ms, want 0", got)
		}
	})
}

func TestProxy_Close(t *testing.T) {
	recorder := &clientRecorder{}
	p := newProxy(Handlers{}, recorder.newClient)
	for _, clientID := range []string{"client-a", "client-b"} {
		if err := p.Add(t.Context(), mqtt.NewClientOptions().SetClientID(clientID)); err != nil {
			t.Fatalf(`Add(%s) error = %v, want nil`, clientID, err)
		}
	}

	p.Close()

	for i, client := range recorder.clients() {
		if got := client.DisconnectCallCount(); got != 1 {
			t.Errorf("client %d Disconnect calls = %d, want 1", i, got)
		}
	}
	if got := maps.Collect(p.Clients()); len(got) != 0 {
		t.Errorf("Clients() after Close = %v, want empty", got)
	}
}

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
