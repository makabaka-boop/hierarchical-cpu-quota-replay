package tenantsched_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"tenantsched"
)

// cmpHarness drives the real scheduler and the independent reference model
// with the same script and compares every observable result.
type cmpHarness struct {
	t   *testing.T
	sch *tenantsched.Scheduler
	ref *reference
	log []string
}

func (h *cmpHarness) note(format string, args ...any) {
	h.log = append(h.log, fmt.Sprintf(format, args...))
}

// failf aborts the test with the latest operations attached.
func (h *cmpHarness) failf(format string, args ...any) {
	h.t.Helper()
	tail := h.log
	if len(tail) > 40 {
		tail = tail[len(tail)-40:]
	}
	h.t.Fatalf(format+"\nrecent ops:\n%s", append(args, strings.Join(tail, "\n"))...)
}

func newHarness(t *testing.T, specs []tenantsched.GroupSpec) *cmpHarness {
	t.Helper()
	sch, err := tenantsched.New(specs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ref, err := newReference(specs)
	if err != nil {
		t.Fatalf("reference New: %v", err)
	}
	return &cmpHarness{t: t, sch: sch, ref: ref}
}

func (h *cmpHarness) submit(spec tenantsched.JobSpec) error {
	h.t.Helper()
	h.note("submit id=%s rel=%d work=%d dl=%d grp=%s", spec.ID, spec.Release, spec.Work, spec.Deadline, spec.GroupID)
	rev := h.sch.Revision()
	gotRev, gotErr := h.sch.Submit(rev, spec)
	wantRev, wantErr := h.ref.submit(rev, spec)
	if !errIs(gotErr, wantErr) {
		h.failf("submit %q: err=%v want %v", spec.ID, gotErr, wantErr)
	}
	if gotErr == nil && gotRev != wantRev {
		h.failf("submit %q: rev=%d want %d", spec.ID, gotRev, wantRev)
	}
	return gotErr
}

func (h *cmpHarness) migrate(id, dst string) error {
	h.t.Helper()
	h.note("migrate id=%s dst=%s", id, dst)
	rev := h.sch.Revision()
	gotRev, gotErr := h.sch.Migrate(rev, id, dst)
	wantRev, wantErr := h.ref.migrate(rev, id, dst)
	if !errIs(gotErr, wantErr) {
		h.failf("migrate %q->%q: err=%v want %v", id, dst, gotErr, wantErr)
	}
	if gotErr == nil && gotRev != wantRev {
		h.failf("migrate %q: rev=%d want %d", id, gotRev, wantRev)
	}
	return gotErr
}

func (h *cmpHarness) cancel(id string) error {
	h.t.Helper()
	h.note("cancel id=%s", id)
	rev := h.sch.Revision()
	gotRev, gotErr := h.sch.Cancel(rev, id)
	wantRev, wantErr := h.ref.cancel(rev, id)
	if !errIs(gotErr, wantErr) {
		h.failf("cancel %q: err=%v want %v", id, gotErr, wantErr)
	}
	if gotErr == nil && gotRev != wantRev {
		h.failf("cancel %q: rev=%d want %d", id, gotRev, wantRev)
	}
	return gotErr
}

func (h *cmpHarness) advance(target int64) {
	h.t.Helper()
	h.note("advance target=%d", target)
	rev := h.sch.Revision()
	if h.ref.rev != rev {
		h.failf("ref/real rev drift: %d vs %d", h.ref.rev, rev)
	}
	res, err := h.sch.AdvanceTo(rev, target)
	ran, idle, recs, rerr := h.ref.advance(rev, target)
	if !errIs(err, rerr) {
		h.failf("advance %d: err=%v want %v", target, err, rerr)
	}
	if err != nil {
		return
	}
	if res.RanTicks != ran || res.IdleTicks != idle {
		h.failf("advance->%d counts: got ran=%d idle=%d, want ran=%d idle=%d",
			target, res.RanTicks, res.IdleTicks, ran, idle)
	}
	if len(res.Ticks) != len(recs) {
		h.failf("advance->%d tick count %d != %d", target, len(res.Ticks), len(recs))
	}
	for i := range recs {
		h.compareTick(res.Ticks[i], recs[i])
	}
	h.compareState()
}

func (h *cmpHarness) compareTick(got tenantsched.TickRecord, want refTick) {
	h.t.Helper()
	if got.Tick != want.tick || got.Period != want.period {
		h.failf("tick header %+v want %+v", got, want)
	}
	if got.Ran != want.ran || got.Runnable != want.runnable || got.IdleReason != want.idleReason ||
		got.ReadyCount != want.readyCount || got.JobID != want.job || got.GroupID != want.group {
		h.failf("tick %d body mismatch:\n got  %+v\n want %+v", want.tick, got, want)
	}
	if len(got.Applied) != len(want.applied) {
		h.failf("tick %d applied len %d want %d", want.tick, len(got.Applied), len(want.applied))
	}
	for i := range want.applied {
		if !reflect.DeepEqual(got.Applied[i], want.applied[i]) {
			h.failf("tick %d applied[%d] %+v want %+v", want.tick, i, got.Applied[i], want.applied[i])
		}
	}
	gotOverdue := make([]string, 0, len(got.Overdue))
	for _, ev := range got.Overdue {
		gotOverdue = append(gotOverdue, ev.JobID)
	}
	if len(gotOverdue) != len(want.overdueJobs) {
		h.failf("tick %d overdue %v want %v", want.tick, gotOverdue, want.overdueJobs)
	}
	for i := range want.overdueJobs {
		if gotOverdue[i] != want.overdueJobs[i] {
			h.failf("tick %d overdue[%d] %q want %q", want.tick, i, gotOverdue[i], want.overdueJobs[i])
		}
	}
	if want.ran {
		h.comparePoints(want.tick, got.Chain, want.chain)
	}
	if (got.Blocked == nil) != (want.blocked == nil) {
		h.failf("tick %d blocked presence got %+v want %+v", want.tick, got.Blocked, want.blocked)
	}
	if want.blocked != nil {
		b := got.Blocked
		if b.JobID != want.blocked.job || b.GroupID != want.blocked.group ||
			b.Level != want.blocked.level || b.Quota != want.blocked.quota || b.Used != want.blocked.used {
			h.failf("tick %d blocked got %+v want %+v", want.tick, b, want.blocked)
		}
	}
	if len(got.AllBlocked) != len(want.allBlocked) {
		h.failf("tick %d allblocked len %d want %d", want.tick, len(got.AllBlocked), len(want.allBlocked))
	}
	for i := range want.allBlocked {
		a, b := got.AllBlocked[i], want.allBlocked[i]
		if a.JobID != b.job || a.GroupID != b.group || a.Level != b.level ||
			a.Quota != b.quota || a.Used != b.used {
			h.failf("tick %d allblocked[%d] got %+v want %+v", want.tick, i, a, b)
		}
	}
}

func (h *cmpHarness) comparePoints(tick int64, got []tenantsched.QuotaPoint, want []refPoint) {
	h.t.Helper()
	if len(got) != len(want) {
		h.failf("tick %d chain len %d want %d", tick, len(got), len(want))
	}
	for i := range want {
		if got[i].GroupID != want[i].id || got[i].Level != want[i].level ||
			got[i].Quota != want[i].quota || got[i].Used != want[i].used {
			h.failf("tick %d chain[%d] got %+v want %+v", tick, i, got[i], want[i])
		}
	}
}

func (h *cmpHarness) compareState() {
	h.t.Helper()
	snap := h.sch.Snapshot()
	if snap.Tick != h.ref.tick {
		h.failf("tick %d want %d", snap.Tick, h.ref.tick)
	}
	if snap.Revision != h.ref.rev {
		h.failf("rev %d want %d", snap.Revision, h.ref.rev)
	}
	if snap.Period != h.ref.period(h.ref.tick) {
		h.failf("period %d want %d", snap.Period, h.ref.period(h.ref.tick))
	}
	for _, gi := range snap.Groups {
		g := h.ref.groups[gi.ID]
		// g.used/g.period are maintained tick-by-tick identically to the
		// real implementation (reset happens inside the next executed tick),
		// so compare the fields directly.
		if gi.Level != g.level || gi.Quota != g.quota || gi.Used != g.used || gi.Period != g.period {
			h.failf("group %s got level=%d used=%d quota=%d period=%d, want level=%d used=%d quota=%d period=%d",
				gi.ID, gi.Level, gi.Used, gi.Quota, gi.Period, g.level, g.used, g.quota, g.period)
		}
	}

	// Build an effective job view from the reference incl. pending-only.
	eff := h.ref.staged()
	if len(snap.Jobs) != len(eff) {
		h.failf("job count %d want %d (snapshot=%v ref=%v)",
			len(snap.Jobs), len(eff), jobIDs(snap), mapKeys(eff))
	}
	for _, ji := range snap.Jobs {
		j, ok := eff[ji.ID]
		if !ok {
			h.failf("unexpected job %q in snapshot", ji.ID)
		}
		if ji.Release != j.release || ji.Deadline != j.deadline || ji.Work != j.work {
			h.failf("job %s static fields %+v want %+v", ji.ID, ji, j)
		}
		// A pending-only job with a queued cancel in the same batch is shown
		// as cancelled in the snapshot.
		wantCancel := j.canceled || (j.pending && j.queuedCancel)
		if ji.GroupID != j.group || ji.Remaining != j.remaining ||
			ji.Completed != j.completed || ji.Cancelled != wantCancel ||
			ji.Pending != j.pending {
			h.failf("job %s state %+v want group=%s rem=%d comp=%v canc=%v pending=%v",
				ji.ID, ji, j.group, j.remaining, j.completed, wantCancel, j.pending)
		}
		if (ji.OverdueAt >= 0) != (j.overdueAt >= 0) {
			h.failf("job %s overdue flag %d want %d", ji.ID, ji.OverdueAt, j.overdueAt)
		}
		if ji.OverdueAt >= 0 && ji.OverdueAt != j.overdueAt {
			h.failf("job %s overdueAt %d want %d", ji.ID, ji.OverdueAt, j.overdueAt)
		}
	}

	// Overdue evidence vs reference.
	gotEvidence := h.sch.OverdueEvidence()
	if len(gotEvidence) != len(h.ref.overdue) {
		h.failf("overdue evidence count %d want %d", len(gotEvidence), len(h.ref.overdue))
	}
	for i, want := range h.ref.overdue {
		got := gotEvidence[i]
		if got.JobID != want.job || got.Tick != want.tick || got.Deadline != want.deadline ||
			got.Remaining != want.rem || got.GroupID != want.group {
			h.failf("overdue[%d] got %+v want %+v", i, got, want)
		}
		h.comparePoints(want.tick, got.Chain, want.chain)
	}

	// Traces have identical length to advances.
	if len(h.sch.Trace()) != len(h.ref.trace) {
		h.failf("trace length %d want %d", len(h.sch.Trace()), len(h.ref.trace))
	}
}

func jobIDs(s tenantsched.Snapshot) []string {
	var out []string
	for _, j := range s.Jobs {
		out = append(out, j.ID)
	}
	return out
}

func mapKeys(m map[string]*refJob) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// fourLevelGroups is the canonical tree used by most scenarios:
//
//	root
//	├── a          ├── b
//	│   ├── a1     └── b1
//	└───────────── (root is level 1)
func fourLevelGroups() []tenantsched.GroupSpec {
	// Build an actual 4-level tree: root > a > a-sub > a-leaf.
	return []tenantsched.GroupSpec{
		{ID: "root", Quota: 3},
		{ID: "a", ParentID: "root", Quota: 2},
		{ID: "b", ParentID: "root", Quota: 100},
		{ID: "a1", ParentID: "a", Quota: 1},
		{ID: "a1x", ParentID: "a1", Quota: 100}, // level 4
		{ID: "b1", ParentID: "b", Quota: 100},
	}
}

// TestReferencePeriodBoundary covers ticks 0..11: quota must reset at the
// period boundary (tick 10) and the trace must show the reset behavior.
func TestReferencePeriodBoundary(t *testing.T) {
	h := newHarness(t, fourLevelGroups())
	// Job on the level-4 chain: a1 quota is 1 per period, so one run per
	// period regardless of the generous leaf/root quotas.
	h.submit(tenantsched.JobSpec{ID: "j1", Release: 0, Work: 5, Deadline: 100, GroupID: "a1x"})
	h.advance(0) // apply at boundary of tick 0
	// tick 0: runs (a1 0->1), ticks 1..9: blocked by a1, tick 10 reset: runs
	h.advance(11)

	tr := h.sch.Trace()
	if !tr[0].Ran || tr[0].JobID != "j1" {
		t.Fatalf("tick 0 should run j1: %+v", tr[0])
	}
	for tck := int64(1); tck <= 9; tck++ {
		r := tr[tck]
		if r.Ran || r.IdleReason != "quota-exhausted" || r.Blocked == nil ||
			r.Blocked.GroupID != "a1" || r.Blocked.Level != 3 {
			t.Fatalf("tick %d expected a1 blocker, got %+v", tck, r)
		}
	}
	r10 := tr[10]
	if !r10.Ran || r10.Period != 1 {
		t.Fatalf("tick 10 expected run in period 1, got %+v", r10)
	}
	// tick 11 blocked again by a1
	if tr[11].Ran || tr[11].Blocked.GroupID != "a1" {
		t.Fatalf("tick 11 expected a1 blocker: %+v", tr[11])
	}
}

// TestReferenceAncestorExhausted checks that the highest-priority ready job's
// nearest exhausted ancestor is recorded, including root-level blocking that
// starves sibling branches.
func TestReferenceAncestorExhausted(t *testing.T) {
	specs := []tenantsched.GroupSpec{
		{ID: "root", Quota: 2},
		{ID: "a", ParentID: "root", Quota: 100},
		{ID: "b", ParentID: "root", Quota: 100},
		{ID: "a1", ParentID: "a", Quota: 100},
		{ID: "b1", ParentID: "b", Quota: 100},
	}
	h := newHarness(t, specs)
	h.submit(tenantsched.JobSpec{ID: "ja", Release: 0, Work: 4, Deadline: 5, GroupID: "a1"})
	h.submit(tenantsched.JobSpec{ID: "jb", Release: 0, Work: 4, Deadline: 6, GroupID: "b1"})
	h.advance(0)
	// tick0: ja (deadline 5); tick1: root left 1 -> ja (deadline5); then root
	// exhausted; ja stays top (deadline 5) and blocker is root (level 1).
	h.advance(3)
	tr := h.sch.Trace()
	if !tr[0].Ran || tr[0].JobID != "ja" {
		t.Fatalf("tick0: %+v", tr[0])
	}
	if !tr[1].Ran || tr[1].JobID != "ja" {
		t.Fatalf("tick1: %+v", tr[1])
	}
	for _, tck := range []int64{2, 3} {
		r := tr[tck]
		if r.Ran || r.Blocked == nil || r.Blocked.GroupID != "root" || r.Blocked.Level != 1 {
			t.Fatalf("tick %d expected root block: %+v", tck, r)
		}
		if len(r.AllBlocked) != 2 {
			t.Fatalf("tick %d expected both branches blocked, got %d", tck, len(r.AllBlocked))
		}
		if r.Blocked.Used != 2 || r.Blocked.Quota != 2 {
			t.Fatalf("tick %d quota usage %+v", tck, r.Blocked)
		}
	}
}

// TestReferenceMigrateNoRefund migrates a job mid-run: quota spent at the old
// branch stays consumed and the new branch charges start from zero.
func TestReferenceMigrateNoRefund(t *testing.T) {
	specs := []tenantsched.GroupSpec{
		{ID: "root", Quota: 100},
		{ID: "a", ParentID: "root", Quota: 2},
		{ID: "b", ParentID: "root", Quota: 2},
		{ID: "a1", ParentID: "a", Quota: 100},
		{ID: "b1", ParentID: "b", Quota: 100},
	}
	h := newHarness(t, specs)
	h.submit(tenantsched.JobSpec{ID: "j", Release: 0, Work: 7, Deadline: 50, GroupID: "a1"})
	// apply submit and run tick 0: a used 0->1
	h.advance(0)
	if r := h.sch.Trace()[0]; !r.Ran || r.JobID != "j" {
		t.Fatalf("tick0: %+v", r)
	}
	// tick 1 runs again exhausting a (used=2); tick 2 blocks on a
	h.advance(2)
	tr := h.sch.Trace()
	if !tr[1].Ran || tr[1].JobID != "j" {
		t.Fatalf("tick1: %+v", tr[1])
	}
	if r2 := tr[2]; r2.Ran || r2.Blocked.GroupID != "a" {
		t.Fatalf("expected a block pre-migration: %+v", r2)
	}
	// migrate to b1; no refund to a
	h.migrate("j", "b1")
	// tick 3 applies migration and runs charging b (b used 0->1)
	h.advance(3)
	r3 := h.sch.Trace()[3]
	if !r3.Ran || r3.JobID != "j" || r3.GroupID != "b1" {
		t.Fatalf("expected run on b1 after migrate: %+v", r3)
	}
	if len(r3.Applied) != 1 || r3.Applied[0].Kind != "migrate" ||
		r3.Applied[0].From != "a1" || r3.Applied[0].Group != "b1" {
		t.Fatalf("migration event mismatch: %+v", r3.Applied)
	}
	// tick 4 runs charging b again (used=2, exhausted); tick 5 blocks on b
	h.advance(5)
	tr = h.sch.Trace()
	if !tr[4].Ran || tr[4].GroupID != "b1" {
		t.Fatalf("tick4: %+v", tr[4])
	}
	if tr[5].Ran || tr[5].Blocked.GroupID != "b" {
		t.Fatalf("expected b block after 2 charges on new branch: %+v", tr[5])
	}
	snap := h.sch.Snapshot()
	used := map[string]int64{}
	for _, g := range snap.Groups {
		used[g.ID] = g.Used
	}
	if used["a"] != 2 {
		t.Fatalf("old branch a should keep used=2 (no refund), got %d", used["a"])
	}
	if used["b"] != 2 {
		t.Fatalf("new branch b should be at used=2, got %d", used["b"])
	}
}

// TestReferenceSelectionAndCancel exercises deadline/id tie-breaking, release
// gating, cancels (live and pre-application) and completion.
func TestReferenceSelectionAndCancel(t *testing.T) {
	generous := []tenantsched.GroupSpec{
		{ID: "root", Quota: 1_000_000},
		{ID: "b", ParentID: "root", Quota: 1_000_000},
		{ID: "b1", ParentID: "b", Quota: 1_000_000},
	}
	h := newHarness(t, generous)
	// b1 is generous: use it for pure selection semantics.
	h.submit(tenantsched.JobSpec{ID: "z", Release: 0, Work: 3, Deadline: 10, GroupID: "b1"})
	h.submit(tenantsched.JobSpec{ID: "a", Release: 0, Work: 3, Deadline: 10, GroupID: "b1"})
	h.submit(tenantsched.JobSpec{ID: "late", Release: 2, Work: 2, Deadline: 10, GroupID: "b1"})
	// cancel a job queued but not yet applied
	h.submit(tenantsched.JobSpec{ID: "ghost", Release: 0, Work: 1, Deadline: 10, GroupID: "b1"})
	h.cancel("ghost")
	h.advance(0)
	tr := h.sch.Trace()
	if tr[0].JobID != "a" { // id tie-break
		t.Fatalf("tick0 tie-break: %+v", tr[0])
	}
	if len(tr[0].Applied) != 5 { // 4 submits (incl. ghost) + 1 cancel, same boundary
		t.Fatalf("applied events: %+v", tr[0].Applied)
	}
	// a (work 3) runs ticks 0,1,2 as top by id; late becomes ready at tick 2.
	h.advance(2)
	tr = h.sch.Trace()
	for tck := int64(1); tck <= 2; tck++ {
		if !tr[tck].Ran || tr[tck].JobID != "a" {
			t.Fatalf("tick %d expected a: %+v", tck, tr[tck])
		}
	}
	if tr[2].ReadyCount != 3 { // a, z, late
		t.Fatalf("tick2 ready count: %+v", tr[2])
	}
	// tick3: a finished; late < z by id
	h.advance(3)
	if r := h.sch.Trace()[3]; !r.Ran || r.JobID != "late" {
		t.Fatalf("tick3 expected late: %+v", r)
	}
	// cancel z before tick4
	h.cancel("z")
	// tick4: late finishes (work 2 -> 0), cancel event visible at boundary
	r4 := h.sch.Trace()
	h.advance(4)
	r4 = h.sch.Trace()
	if !r4[4].Ran || r4[4].JobID != "late" {
		t.Fatalf("tick4: %+v", r4[4])
	}
	if len(r4[4].Applied) != 1 || r4[4].Applied[0].Kind != "cancel" {
		t.Fatalf("tick4 applied: %+v", r4[4].Applied)
	}
	// nothing ready thereafter
	h.advance(6)
	for tck := int64(5); tck <= 6; tck++ {
		if h.sch.Trace()[tck].IdleReason != "no-ready-jobs" {
			t.Fatalf("tick %d: %+v", tck, h.sch.Trace()[tck])
		}
	}
	// operations on finished/canceled jobs rejected
	if _, err := h.sch.Cancel(h.sch.Revision(), "late"); !errorIs(err, tenantsched.ErrJobInactive) {
		t.Fatalf("cancel finished: %v", err)
	}
	if _, err := h.sch.Migrate(h.sch.Revision(), "z", "b1"); !errorIs(err, tenantsched.ErrJobInactive) {
		t.Fatalf("migrate canceled: %v", err)
	}
}

// TestReferenceOverdueEvidence verifies overdue detection timing and evidence.
func TestReferenceOverdueEvidence(t *testing.T) {
	h := newHarness(t, fourLevelGroups())
	// a1 allows 1 run per period; j needs 3 ticks with deadline 1 => overdue
	// at tick 2 after running only on tick 0.
	h.submit(tenantsched.JobSpec{ID: "slow", Release: 0, Work: 3, Deadline: 1, GroupID: "a1"})
	h.advance(2)
	ev := h.sch.OverdueEvidence()
	if len(ev) != 1 {
		t.Fatalf("expected one overdue evidence, got %d: %+v", len(ev), ev)
	}
	if ev[0].JobID != "slow" || ev[0].Tick != 2 || ev[0].Deadline != 1 || ev[0].Remaining != 2 {
		t.Fatalf("overdue evidence %+v", ev[0])
	}
	if len(ev[0].Chain) != 3 || ev[0].Chain[0].GroupID != "root" {
		t.Fatalf("overdue chain %+v", ev[0].Chain)
	}
	// overdue job never runs again and no duplicate evidence
	h.advance(12)
	if len(h.sch.OverdueEvidence()) != 1 {
		t.Fatalf("overdue evidence duplicated")
	}
	for tck := int64(2); tck <= 12; tck++ {
		r := h.sch.Trace()[tck]
		if r.Ran {
			t.Fatalf("overdue job ran at %d", tck)
		}
	}
	// tick 10 reset frees a1, but job remains overdue -> no-ready (it's excluded)
	r10 := h.sch.Trace()[10]
	if r10.IdleReason != "no-ready-jobs" {
		t.Fatalf("tick10: %+v", r10)
	}
	if _, err := h.sch.Cancel(h.sch.Revision(), "slow"); !errorIs(err, tenantsched.ErrJobInactive) {
		t.Fatalf("cancel overdue: %v", err)
	}
}

// TestRevisionConflicts verifies expected-revision handling on all commands,
// including a successful no-op advance at the current tick.
func TestRevisionConflicts(t *testing.T) {
	h := newHarness(t, fourLevelGroups())
	spec := tenantsched.JobSpec{ID: "j", Release: 0, Work: 1, Deadline: 5, GroupID: "b1"}
	if _, err := h.sch.Submit(999, spec); !errorIs(err, tenantsched.ErrStaleRevision) {
		t.Fatalf("stale submit: %v", err)
	}
	rev := h.sch.Revision()
	newRev, err := h.sch.Submit(rev, spec)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if newRev != rev+1 {
		t.Fatalf("rev %d -> %d", rev, newRev)
	}
	if _, err := h.sch.Migrate(rev, "j", "a1"); !errorIs(err, tenantsched.ErrStaleRevision) {
		t.Fatalf("stale migrate: %v", err)
	}
	if _, err := h.sch.Cancel(rev, "j"); !errorIs(err, tenantsched.ErrStaleRevision) {
		t.Fatalf("stale cancel: %v", err)
	}
	if _, err := h.sch.AdvanceTo(rev, 0); !errorIs(err, tenantsched.ErrStaleRevision) {
		t.Fatalf("stale advance: %v", err)
	}
	// stale commands did not change revision
	if h.sch.Revision() != newRev {
		t.Fatalf("revision changed on stale calls")
	}
	// advancing to the current (not-yet-executed) tick executes that tick and
	// bumps the revision exactly once.
	cur := h.sch.Tick()
	res, err := h.sch.AdvanceTo(newRev, cur)
	if err != nil || !res.Advanced || len(res.Ticks) != 1 || res.Revision != newRev+1 {
		t.Fatalf("advance to current tick: err=%v res=%+v", err, res)
	}
	if h.sch.Tick() != cur+1 {
		t.Fatalf("tick did not advance")
	}
	// re-advancing to the same (now executed) tick is rejected
	if _, err := h.sch.AdvanceTo(h.sch.Revision(), cur); !errorIs(err, tenantsched.ErrNoAdvance) {
		t.Fatalf("re-advance to executed tick: %v", err)
	}
}

func errorIs(err, target error) bool {
	if err == nil {
		return target == nil
	}
	return target != nil && (err == target || containsSentinel(err, target))
}

func containsSentinel(err, target error) bool {
	// avoid importing errors at assertion call sites inconsistently
	type wrapper interface{ Unwrap() error }
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(wrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
