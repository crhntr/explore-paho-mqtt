package pool

import (
	"context"
	"errors"
	"maps"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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
		if created[0].connectCalls != 1 {
			t.Errorf("Connect calls = %d, want 1", created[0].connectCalls)
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
		if created[0].disconnectCount() != 1 {
			t.Errorf("replaced client Disconnect calls = %d, want 1", created[0].disconnectCount())
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
		if created[0].disconnectCount() != 1 {
			t.Errorf("abandoned client Disconnect calls = %d, want 1", created[0].disconnectCount())
		}
	})
}
