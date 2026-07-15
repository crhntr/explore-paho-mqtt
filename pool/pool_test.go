package pool

import (
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
}
