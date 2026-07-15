package pool

import (
	"testing"
)

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
