package envelope

import (
	"testing"
	"time"
)

func TestTimestampAtFromMillis_UsesUTCPlus8(t *testing.T) {
	got := TimestampAtFromMillis(1704067200000)
	want := "2024-01-01T08:00:00+08:00"
	if got != want {
		t.Errorf("TimestampAtFromMillis()=%q, want %q", got, want)
	}
}

func TestEnsureTimestampAt_PreservesExistingValue(t *testing.T) {
	env := &Envelope{Timestamp: 1234, TimestampAt: "custom"}
	env.EnsureTimestampAt()
	if env.TimestampAt != "custom" {
		t.Errorf("TimestampAt=%q", env.TimestampAt)
	}
}

func TestTimestampAtFromTime_ZeroIsEmpty(t *testing.T) {
	if got := TimestampAtFromTime(time.Time{}); got != "" {
		t.Errorf("TimestampAtFromTime(zero)=%q", got)
	}
}
