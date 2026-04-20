// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestRotator_RecoverFrom_ColdStartDelay(t *testing.T) {
	maintenanceInterval := 10 * time.Minute
	intervalsBeforeColdStartPlanning := 5

	now := time.Now()
	clock := clock.NewMock()
	clock.Set(now)

	tests := []struct {
		name              string
		creationTime      time.Time
		jobTrackers       map[string]*JobTracker
		expectedIntervals int
	}{
		{
			name:              "no delay, past",
			creationTime:      now.Add(-time.Duration(intervalsBeforeColdStartPlanning+1) * maintenanceInterval),
			expectedIntervals: 0,
		},
		{
			name:              "partial delay, multiple",
			creationTime:      now.Add((-2 * maintenanceInterval)),
			expectedIntervals: 3, // 5 - 2 = 3
		},
		{
			name:              "partial delay, fractional",
			creationTime:      now.Add((-2 * maintenanceInterval) - maintenanceInterval/2),
			expectedIntervals: 3, // 5 - 2.5 = 2.5, ceil(2.5) = 3
		},
		{
			name:              "full delay",
			creationTime:      now,
			expectedIntervals: intervalsBeforeColdStartPlanning,
		},
		{
			name:         "existing trackers do not bypass delay",
			creationTime: now,
			jobTrackers: func() map[string]*JobTracker {
				jt, _ := newTestJobTracker(clock)
				return map[string]*JobTracker{"test": jt}
			}(),
			expectedIntervals: intervalsBeforeColdStartPlanning,
		},
		{
			name:              "clock skew, creation time in future",
			creationTime:      now.Add(maintenanceInterval),
			expectedIntervals: intervalsBeforeColdStartPlanning,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRotator(0, 0, 0, maintenanceInterval, 0, intervalsBeforeColdStartPlanning, log.NewNopLogger())
			r.clock = clock

			r.RecoverFrom(tc.jobTrackers, tc.creationTime)
			require.Equal(t, tc.expectedIntervals, r.intervalsBeforeColdStartPlanning)
		})
	}
}

func newRotatorForTest() *Rotator {
	return NewRotator(0, 0, 0, time.Minute, 0, 0, log.NewNopLogger())
}

func seedRotation(r *Rotator, tenants ...string) map[string]*TenantRotationState {
	states := make(map[string]*TenantRotationState, len(tenants))
	for _, tenant := range tenants {
		state := &TenantRotationState{}
		r.tenantStateMap[tenant] = state
		r.addToRotation(tenant, state)
		states[tenant] = state
	}
	return states
}

func rotationOrder(r *Rotator) []string {
	tenants := make([]string, 0, r.rotation.Len())
	for e := r.rotation.Front(); e != nil; e = e.Next() {
		tenants = append(tenants, e.Value.(string))
	}
	return tenants
}

func TestRotator_RemoveFromRotation_PreservesOrder(t *testing.T) {
	r := newRotatorForTest()
	states := seedRotation(r, "a", "b", "c", "d", "e")
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, rotationOrder(r))

	// Remove from the middle: remaining tenants keep their relative order.
	r.removeFromRotation(states["c"])
	require.Equal(t, []string{"a", "b", "d", "e"}, rotationOrder(r))
	require.Nil(t, states["c"].element)

	// Remove the head and the tail.
	r.removeFromRotation(states["a"])
	require.Equal(t, []string{"b", "d", "e"}, rotationOrder(r))
	r.removeFromRotation(states["e"])
	require.Equal(t, []string{"b", "d"}, rotationOrder(r))

	// Drain the rotation: cursor goes back to nil.
	r.removeFromRotation(states["b"])
	r.removeFromRotation(states["d"])
	require.Equal(t, 0, r.rotation.Len())
	require.Nil(t, r.cursor.Load())
}

func TestRotator_AdvanceCursor_RoundRobin(t *testing.T) {
	r := newRotatorForTest()
	seedRotation(r, "a", "b", "c")

	var seen []string
	for range 9 {
		seen = append(seen, r.advanceCursor().Value.(string))
	}
	require.Equal(t, []string{"a", "b", "c", "a", "b", "c", "a", "b", "c"}, seen)
}

func TestRotator_RemoveFromRotation_AdvancesCursor(t *testing.T) {
	r := newRotatorForTest()
	states := seedRotation(r, "a", "b", "c", "d", "e")

	// Consume two slots so the cursor lands on "c".
	require.Equal(t, "a", r.advanceCursor().Value.(string))
	require.Equal(t, "b", r.advanceCursor().Value.(string))
	require.Equal(t, "c", r.cursor.Load().Value.(string))

	// Removing the tenant the cursor points to advances it to the next tenant, preserving order.
	r.removeFromRotation(states["c"])
	require.Equal(t, "d", r.cursor.Load().Value.(string))

	// Round-robin continues in the original relative order.
	var seen []string
	for range 8 {
		seen = append(seen, r.advanceCursor().Value.(string))
	}
	require.Equal(t, []string{"d", "e", "a", "b", "d", "e", "a", "b"}, seen)
}

func TestRotator_RemoveFromRotation_CursorWrapsOnTailRemoval(t *testing.T) {
	r := newRotatorForTest()
	states := seedRotation(r, "a", "b", "c")

	// Advance cursor to the tail ("c").
	require.Equal(t, "a", r.advanceCursor().Value.(string))
	require.Equal(t, "b", r.advanceCursor().Value.(string))
	require.Equal(t, "c", r.cursor.Load().Value.(string))

	// Removing the tail under the cursor wraps to the front.
	r.removeFromRotation(states["c"])
	require.Equal(t, "a", r.cursor.Load().Value.(string))
}
