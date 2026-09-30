package tenantsched_test

import (
	"errors"
	"fmt"
	"sync"
	"tenantsched"
	"testing"
)

// barrier releases all waiters simultaneously so concurrent advances race
// through the scheduler's linearization point.
type barrier struct {
	once   sync.Once
	ch     chan struct{}
	readyW sync.WaitGroup
}

func newBarrier(n int) *barrier {
	b := &barrier{ch: make(chan struct{})}
	b.readyW.Add(n)
	return b
}

func (b *barrier) markReady() { b.readyW.Done() }

// waitAllReady blocks until every participant has called markReady. Only the
// test orchestrator (not the participants themselves) should call this.
func (b *barrier) waitAllReady() { b.readyW.Wait() }

func (b *barrier) fire() { b.once.Do(func() { close(b.ch) }) }

func (b *barrier) hold() { <-b.ch }

// TestConcurrentAdvanceNoDuplicateTick fires many goroutines against the exact
// same single-tick advance, over several rounds. Exactly one call per tick may
// succeed; the tick must execute exactly once.
func TestConcurrentAdvanceNoDuplicateTick(t *testing.T) {
	const goroutines = 16
	const rounds = 8
	s := mustNew(t, []tenantsched.GroupSpec{{ID: "root", Quota: 1000}})
	rev, err := s.Submit(s.Revision(), tenantsched.JobSpec{
		ID: "j", Release: 0, Work: goroutines * rounds, Deadline: 1_000_000, GroupID: "root",
	})
	if err != nil {
		t.Fatal(err)
	}
	// apply the submit at tick 0 by advancing one tick under one leader
	res, err := s.AdvanceTo(rev, 0)
	if err != nil {
		t.Fatal(err)
	}
	rev = res.Revision

	var mu sync.Mutex
	for round := 0; round < rounds; round++ {
		b := newBarrier(goroutines)
		var winners, losers, others int
		var winnerRev int64
		var wg sync.WaitGroup
		target := s.Tick() // one single tick; every goroutine races for it
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.markReady()
				b.waitAllReady()
				b.hold()
				r, aerr := s.AdvanceTo(rev, target)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case aerr == nil:
					winners++
					winnerRev = r.Revision
					if !r.Advanced || len(r.Ticks) != 1 || !r.Ticks[0].Ran {
						t.Errorf("winner result unexpected: %+v", r)
					}
				case errors.Is(aerr, tenantsched.ErrStaleRevision):
					losers++
				case errors.Is(aerr, tenantsched.ErrNoAdvance):
					others++ // a goroutine arriving after target moved past
				default:
					t.Errorf("unexpected error: %v", aerr)
				}
			}()
		}
		b.waitAllReady()
		b.fire()
		wg.Wait()

		if winners != 1 {
			t.Fatalf("round %d: winners=%d, want exactly 1 (losers=%d others=%d)",
				round, winners, losers, others)
		}
		if winners+losers+others != goroutines {
			t.Fatalf("round %d accounting %d+%d+%d != %d",
				round, winners, losers, others, goroutines)
		}
		if s.Tick() != target+1 {
			t.Fatalf("round %d: tick moved to %d, want %d", round, s.Tick(), target+1)
		}
		if winnerRev != rev+1 {
			t.Fatalf("round %d: winner rev %d want %d", round, winnerRev, rev+1)
		}
		rev = s.Revision()
	}

	// The work consumed must equal exactly the number of executed ticks.
	snap := s.Snapshot()
	ran := rounds // submit-application tick 0 ran once too
	if snap.RunTicks != int64(ran)+1 {
		t.Fatalf("run ticks %d want %d", snap.RunTicks, ran+1)
	}
	var j tenantsched.JobInfo
	for _, j = range snap.Jobs {
		if j.ID == "j" {
			break
		}
	}
	if j.Remaining != j.Work-(int64(rounds)+1) {
		t.Fatalf("remaining %d work %d: tick executed more than once?", j.Remaining, j.Work)
	}
	if len(s.Trace()) != rounds+1 {
		t.Fatalf("trace length %d want %d", len(s.Trace()), rounds+1)
	}
}

// TestConcurrentMixedOperations linearizes many submitters, migrators,
// cancellers and advancers. The outcome is non-deterministic in ordering, but
// the scheduler must stay internally consistent: trace is dense, revision
// grows monotonically and quota accounting never goes negative/over quota.
func TestConcurrentMixedOperations(t *testing.T) {
	s := mustNew(t, []tenantsched.GroupSpec{
		{ID: "root", Quota: 5},
		{ID: "a", ParentID: "root", Quota: 2},
		{ID: "b", ParentID: "root", Quota: 2},
	})

	const writers = 12
	const ticks = 30
	var wg sync.WaitGroup
	b := newBarrier(writers + 1)

	// One driver advances in single-tick steps, tolerating loss of races.
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.markReady()
		b.waitAllReady()
		b.hold()
		for s.Tick() < int64(ticks) {
			if _, err := s.AdvanceTo(s.Revision(), s.Tick()); err != nil {
				if !errors.Is(err, tenantsched.ErrStaleRevision) &&
					!errors.Is(err, tenantsched.ErrNoAdvance) {
					t.Errorf("advance: %v", err)
					return
				}
			}
		}
	}()

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b.markReady()
			b.waitAllReady()
			b.hold()
			id := fmt.Sprintf("w%d", i)
			// best-effort submit with re-reads of revision
			for tries := 0; tries < 5; tries++ {
				if _, err := s.Submit(s.Revision(), tenantsched.JobSpec{
					ID: id, Release: 0, Work: 8, Deadline: 100,
					GroupID: []string{"a", "b"}[i%2],
				}); err == nil || errors.Is(err, tenantsched.ErrTooManyJobs) {
					break
				}
			}
			// best-effort migrate / cancel / advance churn
			for n := 0; n < 10; n++ {
				switch i % 3 {
				case 0:
					_, _ = s.Migrate(s.Revision(), id, []string{"a", "b"}[(i+1)%2])
				case 1:
					_, _ = s.Cancel(s.Revision(), id)
				default:
					_, _ = s.AdvanceTo(s.Revision(), s.Tick())
				}
			}
		}(i)
	}

	b.waitAllReady()
	b.fire()
	wg.Wait()

	// Invariants after the storm:
	snap := s.Snapshot()
	if snap.Tick < 1 {
		t.Fatalf("expected progress, tick=%d", snap.Tick)
	}
	tr := s.Trace()
	for i, r := range tr {
		if r.Tick != int64(i) {
			t.Fatalf("trace not dense at index %d: tick=%d", i, r.Tick)
		}
		if r.Ran {
			for _, p := range r.Chain {
				if p.Used < 0 || p.Used > p.Quota {
					t.Fatalf("tick %d quota out of bounds: %+v", i, p)
				}
			}
		}
	}
	var ran, idle int64
	for _, r := range tr {
		if r.Ran {
			ran++
		} else {
			idle++
		}
	}
	if ran != snap.RunTicks || idle != snap.IdleTicks || ran+idle != int64(len(tr)) {
		t.Fatalf("counters ran=%d idle=%d snap=%+v", ran, idle, snap)
	}
	for _, g := range snap.Groups {
		if g.Used < 0 || g.Used > g.Quota {
			t.Fatalf("group %s accounting %+v", g.ID, g)
		}
		if g.Level < 1 || g.Level > 4 {
			t.Fatalf("group %s bad level %d", g.ID, g.Level)
		}
	}
}

// TestConcurrentCommandsBarrier fires identical migrate/cancel/submit races;
// at most one of the same-revision calls may win each.
func TestConcurrentCommandsBarrier(t *testing.T) {
	s := mustNew(t, []tenantsched.GroupSpec{
		{ID: "root", Quota: 100},
		{ID: "a", ParentID: "root", Quota: 100},
		{ID: "b", ParentID: "root", Quota: 100},
	})
	rev := s.Revision()
	const n = 20
	b := newBarrier(n)
	var wg sync.WaitGroup
	var success, stale int
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b.markReady()
			b.waitAllReady()
			b.hold()
			_, err := s.Submit(rev, tenantsched.JobSpec{
				ID: fmt.Sprintf("job%d", i), Release: 0, Work: 1, Deadline: 9, GroupID: "root",
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
			} else if errors.Is(err, tenantsched.ErrStaleRevision) {
				stale++
			} else {
				t.Errorf("unexpected err %v", err)
			}
		}(i)
	}
	b.waitAllReady()
	b.fire()
	wg.Wait()
	if success != 1 || stale != n-1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	if s.Revision() != rev+1 {
		t.Fatalf("revision %d want %d", s.Revision(), rev+1)
	}
}
