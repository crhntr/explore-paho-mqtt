package pool

import (
	"context"
	"errors"
	"maps"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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

	t.Run("nil handler fields are safe to fire", func(t *testing.T) {
		recorder := &clientRecorder{}
		p := newProxy(Handlers{}, recorder.newClient)

		options := mqtt.NewClientOptions().SetClientID("client-a")
		if err := p.Add(t.Context(), options); err != nil {
			t.Fatalf(`Add(client-a) error = %v, want nil`, err)
		}

		client := recorder.clients()[0]
		options.OnConnect(client)
		options.OnConnectionLost(client, errors.New("connection reset"))
		options.OnReconnecting(client, options)
		options.DefaultPublishHandler(client, nil)
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
