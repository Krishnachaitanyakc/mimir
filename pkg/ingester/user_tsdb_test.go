// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUserTSDB_acquireAppendLock(t *testing.T) {
	t.Run("should allow to acquire the lock during forced compaction if not conflicting with the compaction time range", func(t *testing.T) {
		db := &userTSDB{}

		state, err := db.acquireAppendLock(20)
		require.NoError(t, err)
		db.releaseAppendLock(state)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		_, err = db.acquireAppendLock(20)
		require.ErrorIs(t, err, errTSDBEarlyCompaction)

		state, err = db.acquireAppendLock(21)
		require.NoError(t, err)
		db.releaseAppendLock(state)
	})

	t.Run("should count all acquired locks in the inflight appends", func(t *testing.T) {
		db := &userTSDB{}

		stateActive, err := db.acquireAppendLock(20)
		require.NoError(t, err)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		stateForcedCompaction, err := db.acquireAppendLock(21)
		require.NoError(t, err)

		// Start a goroutine that will signal once in-flight appends are done.
		inFlightAppendsDone := make(chan struct{})
		go func() {
			db.inFlightAppends.Wait()
			close(inFlightAppendsDone)
		}()

		// Releasing the lock acquired while active should not signal in-flight appends done.
		db.releaseAppendLock(stateActive)
		select {
		case <-inFlightAppendsDone:
			t.Fatal("in-flight appends has been signaled as done, but there's still a lock acquired while force compacting")
		case <-time.After(100 * time.Millisecond):
		}

		// Releasing the remaining lock should signal in-flight appends done.
		db.releaseAppendLock(stateForcedCompaction)
		select {
		case <-time.After(100 * time.Millisecond):
			t.Fatal("in-flight appends have no been signaled as done, but all locks has been released")
		case <-inFlightAppendsDone:
		}
	})

	t.Run("should count only locks acquired while not force compacting in the inflight appends started before forced compaction", func(t *testing.T) {
		db := &userTSDB{}

		stateActive, err := db.acquireAppendLock(20)
		require.NoError(t, err)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		stateForcedCompaction, err := db.acquireAppendLock(21)
		require.NoError(t, err)

		// Start a goroutine that will signal once in-flight appends are done.
		inFlightAppendsWithoutForcedCompactionDone := make(chan struct{})
		go func() {
			db.inFlightAppendsStartedBeforeForcedCompaction.Wait()
			close(inFlightAppendsWithoutForcedCompactionDone)
		}()

		// Releasing the lock acquired while active should signal in-flight appends done.
		db.releaseAppendLock(stateActive)
		select {
		case <-time.After(100 * time.Millisecond):
			t.Fatal("in-flight appends started before forced compaction has not been signaled as done")
		case <-inFlightAppendsWithoutForcedCompactionDone:
		}

		db.releaseAppendLock(stateForcedCompaction)
	})
}

func TestNextForcedHeadCompactionRange(t *testing.T) {
	const blockDuration = 10

	tests := map[string]struct {
		headMinTime     int64
		headMaxTime     int64
		forcedMaxTime   int64
		expectedMinTime int64
		expectedMaxTime int64
		expectedIsValid bool
		expectedIsLast  bool
	}{
		"should compact the whole head if the range fits within the block range": {
			headMinTime:     12,
			headMaxTime:     18,
			forcedMaxTime:   50,
			expectedMinTime: 12,
			expectedMaxTime: 18,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should compact the 1st block range period if the range doesn't fit within the block range": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   50,
			expectedMinTime: 12,
			expectedMaxTime: 19,
			expectedIsValid: true,
			expectedIsLast:  false,
		},
		"should honor the forcedMaxTime when the range fits within the block range": {
			headMinTime:     12,
			headMaxTime:     19,
			forcedMaxTime:   15,
			expectedMinTime: 12,
			expectedMaxTime: 15,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should honor the forcedMaxTime when the range doesn't fit within the block range": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   15,
			expectedMinTime: 12,
			expectedMaxTime: 15,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should return no range if forcedMaxTime is smaller than headMinTime": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   11,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
		"should return a range with min time equal to max time if forcedMaxTime is equal to headMinTime": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   12,
			expectedMinTime: 12,
			expectedMaxTime: 12,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should return no range if head has min time not set": {
			headMinTime:     math.MaxInt64,
			headMaxTime:     20,
			forcedMaxTime:   12,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
		"should return no range if head has max time not set": {
			headMinTime:     0,
			headMaxTime:     math.MinInt64,
			forcedMaxTime:   12,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMinTime, actualMaxTime, actualIsValid, actualIsLast := nextForcedHeadCompactionRange(blockDuration, testData.headMinTime, testData.headMaxTime, testData.forcedMaxTime)
			assert.Equal(t, testData.expectedIsValid, actualIsValid)
			assert.Equal(t, testData.expectedIsLast, actualIsLast)

			if testData.expectedIsValid {
				assert.Equal(t, testData.expectedMinTime, actualMinTime)
				assert.Equal(t, testData.expectedMaxTime, actualMaxTime)
			}
		})
	}
}

func TestComputeOwnedSeriesAndCollectNotOwned(t *testing.T) {
	const userID = "test"

	opts := tsdb.DefaultOptions()
	opts.IsolationDisabled = true
	opts.SecondaryHashFunction = secondaryTSDBHashFunctionForUser(userID)

	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	series1 := labels.FromStrings("__name__", "foo", "n", "1")
	series2 := labels.FromStrings("__name__", "foo", "n", "2")

	app := db.Appender(context.Background())
	_, err = app.Append(0, series1, 1, 1.0)
	require.NoError(t, err)
	_, err = app.Append(0, series2, 1, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	hash1 := mimirpb.ShardByAllLabels(userID, series1)
	hash2 := mimirpb.ShardByAllLabels(userID, series2)

	udb := &userTSDB{
		db:           db,
		activeSeries: activeseries.NewActiveSeries(&asmodel.Matchers{}, time.Minute, nil),
	}

	t.Run("all owned", func(t *testing.T) {
		udb.ownedTokenRanges = ring.TokenRanges{0, math.MaxUint32}
		count, notOwned := udb.computeOwnedSeriesAndCollectNotOwned()
		require.Equal(t, 2, count)
		require.Empty(t, notOwned)
	})

	t.Run("none owned — empty token ranges", func(t *testing.T) {
		udb.ownedTokenRanges = ring.TokenRanges{}
		count, notOwned := udb.computeOwnedSeriesAndCollectNotOwned()
		require.Equal(t, 0, count)
		require.Empty(t, notOwned) // empty ranges → activeSeries.Clear(), returns early
	})

	t.Run("own only series1", func(t *testing.T) {
		udb.ownedTokenRanges = ring.TokenRanges{hash1, hash1}
		count, notOwned := udb.computeOwnedSeriesAndCollectNotOwned()
		require.Equal(t, 1, count)
		require.Len(t, notOwned, 1)
		// The not-owned ref must correspond to series2.
		idx := udb.Head().MustIndex()
		defer idx.Close()
		buf := labels.NewScratchBuilder(4)
		require.NoError(t, idx.Series(notOwned[0], &buf, nil))
		require.Equal(t, series2, buf.Labels())
	})

	t.Run("own only series2", func(t *testing.T) {
		udb.ownedTokenRanges = ring.TokenRanges{hash2, hash2}
		count, notOwned := udb.computeOwnedSeriesAndCollectNotOwned()
		require.Equal(t, 1, count)
		require.Len(t, notOwned, 1)
		idx := udb.Head().MustIndex()
		defer idx.Close()
		buf := labels.NewScratchBuilder(4)
		require.NoError(t, idx.Series(notOwned[0], &buf, nil))
		require.Equal(t, series1, buf.Labels())
	})
}

func TestCompactNotOwnedSeries(t *testing.T) {
	const userID = "test"

	series1 := labels.FromStrings("__name__", "foo", "n", "1")
	series2 := labels.FromStrings("__name__", "foo", "n", "2")
	hash1 := mimirpb.ShardByAllLabels(userID, series1)
	staleVal := math.Float64frombits(value.StaleNaN)

	opts := tsdb.DefaultOptions()
	opts.IsolationDisabled = true
	opts.SecondaryHashFunction = secondaryTSDBHashFunctionForUser(userID)

	newUDB := func(t *testing.T) *userTSDB {
		db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return &userTSDB{
			db:           db,
			activeSeries: activeseries.NewActiveSeries(&asmodel.Matchers{}, time.Minute, nil),
		}
	}

	t.Run("empty refs is a no-op", func(t *testing.T) {
		udb := newUDB(t)

		app := udb.db.Appender(context.Background())
		_, err := app.Append(0, series1, 1000, 1.0)
		require.NoError(t, err)
		_, err = app.Append(0, series2, 2000, 1.0)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		require.NoError(t, udb.compactNotOwnedSeries(nil))
		require.Equal(t, uint64(2), udb.db.Head().NumSeries())
		require.Empty(t, udb.db.Blocks())
	})

	t.Run("not-owned series are written to a block and removed from the head", func(t *testing.T) {
		udb := newUDB(t)

		// series1 receives a regular sample. series2 receives a regular sample
		// followed by a stale marker; the stale marker is what allows gcStaleSeries
		// to evict the series from the Head after compaction.
		app := udb.db.Appender(context.Background())
		_, err := app.Append(0, series1, 1000, 1.0)
		require.NoError(t, err)
		_, err = app.Append(0, series2, 2000, 1.0)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		app = udb.db.Appender(context.Background())
		_, err = app.Append(0, series2, 3000, staleVal)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		require.Equal(t, uint64(2), udb.db.Head().NumSeries())
		require.Empty(t, udb.db.Blocks())

		// Own only series1; series2 is not-owned.
		udb.ownedTokenRanges = ring.TokenRanges{hash1, hash1}
		_, notOwned := udb.computeOwnedSeriesAndCollectNotOwned()
		require.Len(t, notOwned, 1)

		require.NoError(t, udb.compactNotOwnedSeries(notOwned))

		// series2 must have been written to a block and evicted from the Head.
		require.Len(t, udb.db.Blocks(), 1)
		require.Equal(t, uint64(1), udb.db.Head().NumSeries())
	})

	t.Run("returns error when TSDB is not in active state", func(t *testing.T) {
		udb := newUDB(t)

		ok, _ := udb.changeStateToForcedCompaction(active, math.MaxInt64)
		require.True(t, ok)
		defer udb.changeState(forceCompacting, active)

		err := udb.compactNotOwnedSeries([]storage.SeriesRef{1})
		require.Error(t, err)
	})
}

func TestGetSeriesCountAndMinLocalLimit(t *testing.T) {
	tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, tsdbDB.Close())
	})

	// append some series
	app := tsdbDB.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("hello", "world"), 10, 20)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	db := userTSDB{
		db: tsdbDB,
		ownedState: ownedSeriesState{
			ownedSeriesCount: 555,
			localSeriesLimit: 10000,
		},
	}

	t.Run("using series in Head", func(t *testing.T) {
		db.useOwnedSeriesForLimits = false

		cnt, minLimit := db.getSeriesCountAndMinLocalLimit()
		require.Equal(t, 1, cnt)
		require.Equal(t, 0, minLimit)
	})

	t.Run("using owned series", func(t *testing.T) {
		db.useOwnedSeriesForLimits = true

		cnt, minLimit := db.getSeriesCountAndMinLocalLimit()
		require.Equal(t, 555, cnt)
		require.Equal(t, 10000, minLimit)
	})
}

func TestRecomputeOwnedSeries(t *testing.T) {
	limits := validation.Limits{MaxGlobalSeriesPerUser: 0}
	overrides := validation.NewOverrides(limits, nil)
	limiter := NewLimiter(overrides, newIngesterRingLimiterStrategy(nil, 3, true, "zone", overrides.IngestionTenantShardSize))

	t.Run("happy path", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}
		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			return 10
		})
		require.True(t, success)
		require.Equal(t, 1, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, but within limit", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}
		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += recomputeOwnedSeriesMaxSeriesDiff / 2
			return 10
		})

		require.True(t, success)
		require.Equal(t, 1, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, last increase is within limit", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}

		// All but last modifications of ownedSeries during compute will exceed the limit.
		mods := make([]int, recomputeOwnedSeriesMaxAttempts)
		for i := 0; i < len(mods)-1; i++ {
			mods[i] = 2 * recomputeOwnedSeriesMaxSeriesDiff
		}
		mods[len(mods)-1] = recomputeOwnedSeriesMaxSeriesDiff

		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += mods[0]
			mods = mods[1:]
			return 10
		})
		require.True(t, success)
		require.Equal(t, recomputeOwnedSeriesMaxAttempts, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, last increase is above limit, computation should retry", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}

		// All modifications of ownedSeries will exceed the limit.
		mods := make([]int, recomputeOwnedSeriesMaxAttempts)
		for i := 0; i < len(mods); i++ {
			mods[i] = 2 * recomputeOwnedSeriesMaxSeriesDiff
		}

		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += mods[0]
			mods = mods[1:]
			return 10
		})
		require.False(t, success)
		require.Equal(t, recomputeOwnedSeriesMaxAttempts, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})
}
