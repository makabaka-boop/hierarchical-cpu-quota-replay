// Package tenantsched simulates tenant scheduling on a single CPU without
// launching any real work.
//
// Groups form a tree (at most 4 levels and 20 nodes). Every group owns a
// per-period run quota that resets every PeriodLen integer ticks. Jobs carry a
// release tick, a remaining amount of work, a deadline tick and the group they
// belong to.
//
// On every tick the scheduler first applies the changes that were committed
// before the tick started (submits, migrations, cancels), then picks one
// ready job whose own group and all ancestor groups still hold quota. Jobs
// are ordered by deadline then job ID. The chosen job runs for one tick and
// quota is charged at every level of its group chain simultaneously. If no
// job can run, the idle tick records the blocking ancestor of the most
// eligible ready job.
//
// Every mutating call carries the caller's expected revision; the scheduler
// is linearizable and stale requests are rejected with ErrStaleRevision.
// Migrating a job never refunds quota previously consumed at the old chain.
// Advancing time is serialized, so concurrent callers can never execute the
// same tick twice.
package tenantsched

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Model limits mandated by the simulation.
const (
	MaxGroups     = 20 // maximum number of groups in the tree
	MaxDepth      = 4  // maximum chain length in levels (root == level 1)
	MaxJobs       = 40 // maximum number of live jobs
	PeriodLen     = 10 // ticks per quota period
	InitialPeriod = 0  // period of ticks [0, PeriodLen)
)

// Err* are sentinel errors produced by the scheduler. They are often wrapped
// with context; use errors.Is to match them.
var (
	// ErrInvalidConfig is returned by New when the group specification does
	// not describe a valid tree within MaxGroups/MaxDepth.
	ErrInvalidConfig = errors.New("tenantsched: invalid scheduler configuration")
	// ErrStaleRevision is returned when expectedRev does not match the
	// scheduler's current revision.
	ErrStaleRevision = errors.New("tenantsched: revision mismatch")
	// ErrDuplicateID is returned when a job ID is already in use.
	ErrDuplicateID = errors.New("tenantsched: duplicate job id")
	// ErrNoSuchGroup is returned when a referenced group does not exist.
	ErrNoSuchGroup = errors.New("tenantsched: unknown group")
	// ErrNoSuchJob is returned when an operation references an unknown job.
	ErrNoSuchJob = errors.New("tenantsched: unknown job")
	// ErrJobInactive is returned when an operation targets a completed,
	// cancelled or overdue job.
	ErrJobInactive = errors.New("tenantsched: job is not active")
	// ErrTooManyJobs is returned when MaxJobs live jobs would be exceeded.
	ErrTooManyJobs = errors.New("tenantsched: job limit exceeded")
	// ErrInvalidArgument is returned for malformed parameters.
	ErrInvalidArgument = errors.New("tenantsched: invalid argument")
	// ErrNoAdvance is returned when the target tick is in the past.
	ErrNoAdvance = errors.New("tenantsched: target tick before current tick")
)

// GroupSpec describes one group at construction time. The root uses the empty
// string as ParentID. Quota is the amount of run ticks granted to the group
// for every period.
type GroupSpec struct {
	ID       string
	ParentID string
	Quota    int64
}

// JobSpec describes a job submitted to the scheduler.
type JobSpec struct {
	ID       string
	Release  int64  // earliest tick the job is ready to run
	Work     int64  // remaining amount of work measured in ticks
	Deadline int64  // last tick on which completion is still on time
	GroupID  string // group the job belongs to
}

// job is the mutable per-job state.
type job struct {
	spec      JobSpec
	remaining int64
	cancelled bool
	completed bool
	overdueAt int64 // tick on which the job was first observed overdue, -1 otherwise
}

func (j *job) active() bool {
	return !j.cancelled && !j.completed && j.overdueAt < 0
}

// gstate is the mutable per-group state.
type gstate struct {
	spec   GroupSpec
	used   int64 // quota consumed in the current period
	period int64 // period for which used is accounted
}

func (g *gstate) available(period int64) int64 {
	if g.period != period {
		return g.spec.Quota // not reset yet this period => full quota
	}
	return g.spec.Quota - g.used
}

// changeKind enumerates the queued commit kinds.
type changeKind int

const (
	chSubmit changeKind = iota
	chMigrate
	chCancel
)

// change is a committed but not yet applied request. The request is validated
// against a staged view when it is committed; applying it on the tick boundary
// cannot fail (the no-op of a stale chain is handled by tick order).
type change struct {
	kind   changeKind
	jobID  string
	spec   JobSpec // submit
	group  string  // migrate
	expect int64
	rev    int64 // revision after this commit
}

// GroupInfo is an immutable view of a group inside a Snapshot.
type GroupInfo struct {
	ID       string
	ParentID string
	Quota    int64
	Used     int64
	Period   int64
	Level    int
}

// JobInfo is an immutable view of a job inside a Snapshot.
type JobInfo struct {
	ID        string
	Release   int64
	Remaining int64
	Work      int64
	Deadline  int64
	GroupID   string
	Completed bool
	Cancelled bool
	OverdueAt int64 // -1 unless the job became overdue
	Pending   bool  // committed, but not yet applied at a tick boundary
}

// Snapshot is an immutable summary of scheduler state.
type Snapshot struct {
	Tick        int64
	Period      int64
	Revision    int64
	Groups      []GroupInfo
	Jobs        []JobInfo
	PendingN    int
	IdleTicks   int64
	RunTicks    int64
	OverdueJobs []string
}

// ChangeEvent records that a committed change became visible at a tick.
type ChangeEvent struct {
	Kind   string // "submit" | "migrate" | "cancel"
	JobID  string
	Group  string // new group for migrate / submit, otherwise empty
	From   string // previous group for migrate
	Commit int64  // revision under which the change was committed
}

// BlockedJob records a ready job that could not be scheduled and the nearest
// group on its chain with no remaining quota.
type BlockedJob struct {
	JobID   string
	GroupID string // nearest root-first group on the chain without quota
	Level   int    // level of the blocking group, root == 1
	Quota   int64
	Used    int64
}

// QuotaPoint is one entry of a group's post-tick quota status.
type QuotaPoint struct {
	GroupID string
	Level   int
	Quota   int64
	Used    int64
}

// OverdueEvidence captures the moment a job missed its deadline.
type OverdueEvidence struct {
	JobID     string
	Tick      int64 // tick at which the job was observed overdue
	Deadline  int64
	Remaining int64
	GroupID   string
	Chain     []QuotaPoint // group chain, root first, with quota at detection time
}

// TickRecord is the per-tick trace entry.
type TickRecord struct {
	Tick       int64
	Period     int64
	Applied    []ChangeEvent
	Overdue    []OverdueEvidence
	Ran        bool
	JobID      string
	GroupID    string
	Chain      []QuotaPoint // chain of the running job, root first, post-charge quota
	Blocked    *BlockedJob  // most eligible ready job when the tick idles on quota
	AllBlocked []BlockedJob // every ready job blocked by an exhausted ancestor, eligibility order
	ReadyCount int          // number of released, unfinished, non-overdue jobs
	Runnable   bool         // whether the most eligible ready job had quota
	IdleReason string       // "no-ready-jobs" | "quota-exhausted"
}

// AdvanceResult summarizes an AdvanceTo call.
type AdvanceResult struct {
	FromTick  int64 // tick before the advance (first executed tick)
	ToTick    int64 // tick after the advance
	Ticks     []TickRecord
	RanTicks  int64
	IdleTicks int64
	Revision  int64
	Advanced  bool // false when target tick equalled the current tick
}

// Scheduler is the simulated single-CPU tenant scheduler. All methods are
// safe for concurrent use; state transitions are serialized.
type Scheduler struct {
	mu     *mutex // sync.Mutex, kept in a named field for readable docs
	groups map[string]*gstate
	order  []string            // deterministic group order
	parent map[string]string   // group id -> parent id ("" for root)
	chains map[string][]string // group id -> chain root first, includes the group itself

	jobs    map[string]*job
	pending []change

	tick int64 // next tick to execute
	rev  int64

	trace   []TickRecord
	overdue []OverdueEvidence

	runTicks  int64
	idleTicks int64
}

// New constructs a scheduler from the group table. The returned scheduler sits
// just before tick 0 with revision 1.
func New(groups []GroupSpec) (*Scheduler, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("%w: no groups", ErrInvalidConfig)
	}
	if len(groups) > MaxGroups {
		return nil, fmt.Errorf("%w: %d groups exceeds limit %d", ErrInvalidConfig, len(groups), MaxGroups)
	}

	gs := make(map[string]*gstate, len(groups))
	parent := make(map[string]string, len(groups))
	var roots int
	for _, s := range groups {
		if s.ID == "" {
			return nil, fmt.Errorf("%w: empty group id", ErrInvalidConfig)
		}
		if _, dup := gs[s.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate group id %q", ErrInvalidConfig, s.ID)
		}
		if s.Quota < 0 {
			return nil, fmt.Errorf("%w: group %q has negative quota", ErrInvalidConfig, s.ID)
		}
		if s.ParentID == "" {
			roots++
		}
		gs[s.ID] = &gstate{spec: s, period: InitialPeriod}
		parent[s.ID] = s.ParentID
	}
	if roots != 1 {
		return nil, fmt.Errorf("%w: tree must have exactly one root, found %d", ErrInvalidConfig, roots)
	}

	chains := make(map[string][]string, len(groups))
	for id := range gs {
		chain, err := buildChain(id, parent)
		if err != nil {
			return nil, err
		}
		if len(chain) > MaxDepth {
			return nil, fmt.Errorf("%w: group %q is %d levels deep, limit is %d", ErrInvalidConfig, id, len(chain), MaxDepth)
		}
		chains[id] = chain
	}

	order := make([]string, 0, len(groups))
	for _, s := range groups {
		order = append(order, s.ID)
	}

	return &Scheduler{
		mu:     newMutex(),
		groups: gs,
		order:  order,
		parent: parent,
		chains: chains,
		jobs:   make(map[string]*job),
		tick:   0,
		rev:    1,
	}, nil
}

func buildChain(id string, parent map[string]string) ([]string, error) {
	seen := make(map[string]bool)
	var rev []string
	cur := id
	for cur != "" {
		if seen[cur] {
			return nil, fmt.Errorf("%w: cycle through group %q", ErrInvalidConfig, cur)
		}
		if _, ok := parent[cur]; !ok {
			return nil, fmt.Errorf("%w: group %q references unknown parent %q", ErrInvalidConfig, id, cur)
		}
		seen[cur] = true
		rev = append(rev, cur)
		cur = parent[cur]
	}
	chain := make([]string, len(rev))
	for i, g := range rev {
		chain[len(rev)-1-i] = g
	}
	return chain, nil
}

// Revision returns the current linearization revision.
func (s *Scheduler) Revision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev
}

// Tick returns the next tick that will be executed.
func (s *Scheduler) Tick() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

func (s *Scheduler) checkRevLocked(expected int64) error {
	if expected != s.rev {
		return fmt.Errorf("%w: expected %d, current %d", ErrStaleRevision, expected, s.rev)
	}
	return nil
}

// stagedJob is the effective job view used to validate commits that are
// queued for the next tick boundary.
type stagedJob struct {
	j        *job
	pending  bool // introduced by a queued submit
	cancel   bool // removed by a queued cancel
	newGroup string
	migrated bool
}

func (s *Scheduler) stagedLocked() map[string]stagedJob {
	m := make(map[string]stagedJob, len(s.jobs)+len(s.pending))
	for id, j := range s.jobs {
		m[id] = stagedJob{j: j}
	}
	for _, c := range s.pending {
		switch c.kind {
		case chSubmit:
			m[c.jobID] = stagedJob{j: &job{spec: c.spec, remaining: c.spec.Work, overdueAt: -1}, pending: true}
		case chMigrate:
			v := m[c.jobID]
			v.newGroup = c.group
			v.migrated = true
			m[c.jobID] = v
		case chCancel:
			v := m[c.jobID]
			v.cancel = true
			m[c.jobID] = v
		}
	}
	return m
}

// activeStagedCount counts jobs that are effectively live: not cancelled and
// not finished or overdue.
func activeStagedCount(m map[string]stagedJob) int {
	n := 0
	for _, v := range m {
		if v.cancel {
			continue
		}
		if v.j.active() {
			n++
		}
	}
	return n
}

// Submit commits a job for introduction at the next tick boundary.
// It returns the new revision.
func (s *Scheduler) Submit(expectedRev int64, spec JobSpec) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkRevLocked(expectedRev); err != nil {
		return s.rev, err
	}
	if spec.ID == "" {
		return s.rev, fmt.Errorf("%w: empty job id", ErrInvalidArgument)
	}
	if spec.Work <= 0 {
		return s.rev, fmt.Errorf("%w: work must be positive, got %d", ErrInvalidArgument, spec.Work)
	}
	if spec.Release < 0 {
		return s.rev, fmt.Errorf("%w: negative release tick", ErrInvalidArgument)
	}
	if spec.Deadline < 0 {
		return s.rev, fmt.Errorf("%w: negative deadline tick", ErrInvalidArgument)
	}
	if _, ok := s.groups[spec.GroupID]; !ok {
		return s.rev, fmt.Errorf("%w: %q", ErrNoSuchGroup, spec.GroupID)
	}
	staged := s.stagedLocked()
	if v, ok := staged[spec.ID]; ok && !v.cancel {
		return s.rev, fmt.Errorf("%w: %q", ErrDuplicateID, spec.ID)
	}
	if activeStagedCount(staged) >= MaxJobs {
		return s.rev, fmt.Errorf("%w: live jobs limited to %d", ErrTooManyJobs, MaxJobs)
	}
	s.rev++
	s.pending = append(s.pending, change{
		kind: chSubmit, jobID: spec.ID, spec: spec,
		expect: expectedRev, rev: s.rev,
	})
	return s.rev, nil
}

// Migrate commits a job reassignment to dstGroup; it becomes visible at the
// next tick boundary. Quota previously consumed at the job's old chain is
// never refunded. It returns the new revision.
func (s *Scheduler) Migrate(expectedRev int64, jobID, dstGroup string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkRevLocked(expectedRev); err != nil {
		return s.rev, err
	}
	if jobID == "" {
		return s.rev, fmt.Errorf("%w: empty job id", ErrInvalidArgument)
	}
	if _, ok := s.groups[dstGroup]; !ok {
		return s.rev, fmt.Errorf("%w: %q", ErrNoSuchGroup, dstGroup)
	}
	staged := s.stagedLocked()
	v, ok := staged[jobID]
	if !ok || v.cancel {
		return s.rev, fmt.Errorf("%w: %q", ErrNoSuchJob, jobID)
	}
	if !v.pending && !v.j.active() {
		return s.rev, fmt.Errorf("%w: %q", ErrJobInactive, jobID)
	}
	current := v.j.spec.GroupID
	if v.migrated {
		current = v.newGroup
	}
	if current == dstGroup {
		// A same-group migration is a committed no-op change; it still bumps
		// the revision like any other linearized mutation.
		s.rev++
		s.pending = append(s.pending, change{
			kind: chMigrate, jobID: jobID, group: dstGroup,
			expect: expectedRev, rev: s.rev,
		})
		return s.rev, nil
	}
	s.rev++
	s.pending = append(s.pending, change{
		kind: chMigrate, jobID: jobID, group: dstGroup,
		expect: expectedRev, rev: s.rev,
	})
	return s.rev, nil
}

// Cancel commits a job cancellation, visible at the next tick boundary.
// A job that has been queued but not yet introduced can be cancelled too.
// It returns the new revision.
func (s *Scheduler) Cancel(expectedRev int64, jobID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkRevLocked(expectedRev); err != nil {
		return s.rev, err
	}
	if jobID == "" {
		return s.rev, fmt.Errorf("%w: empty job id", ErrInvalidArgument)
	}
	staged := s.stagedLocked()
	v, ok := staged[jobID]
	if !ok {
		return s.rev, fmt.Errorf("%w: %q", ErrNoSuchJob, jobID)
	}
	if v.cancel {
		return s.rev, fmt.Errorf("%w: %q already cancelled", ErrJobInactive, jobID)
	}
	if !v.pending && !v.j.active() {
		return s.rev, fmt.Errorf("%w: %q", ErrJobInactive, jobID)
	}
	s.rev++
	s.pending = append(s.pending, change{
		kind: chCancel, jobID: jobID,
		expect: expectedRev, rev: s.rev,
	})
	return s.rev, nil
}

// AdvanceTo executes every tick up to and including targetTick under the
// single call's linearization. Committed changes are applied at each tick
// boundary. Concurrent callers are serialized; stale callers are rejected and
// no tick is ever executed twice. Advancing to a tick before the current one
// fails with ErrNoAdvance.
func (s *Scheduler) AdvanceTo(expectedRev, targetTick int64) (*AdvanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if targetTick < s.tick {
		return nil, fmt.Errorf("%w: target %d < next tick %d", ErrNoAdvance, targetTick, s.tick)
	}
	if err := s.checkRevLocked(expectedRev); err != nil {
		return nil, err
	}

	from := s.tick
	recs := make([]TickRecord, 0, targetTick-from+1)
	var ran, idle int64
	for s.tick <= targetTick {
		rec := s.runTickLocked()
		recs = append(recs, rec)
		if rec.Ran {
			ran++
		} else {
			idle++
		}
		// Every executed tick is a new linearization point.
		s.rev++
		s.tick++
	}
	s.runTicks += ran
	s.idleTicks += idle
	return &AdvanceResult{
		FromTick:  from,
		ToTick:    s.tick,
		Ticks:     recs,
		RanTicks:  ran,
		IdleTicks: idle,
		Revision:  s.rev,
		Advanced:  true,
	}, nil
}

func periodOf(tick int64) int64 {
	if tick >= 0 {
		return tick / PeriodLen
	}
	return -((-tick + PeriodLen - 1) / PeriodLen)
}

// runTickLocked executes exactly one tick while the scheduler lock is held.
func (s *Scheduler) runTickLocked() TickRecord {
	t := s.tick
	period := periodOf(t)

	rec := TickRecord{Tick: t, Period: period}

	// 1. Apply every committed change at the tick boundary.
	rec.Applied = s.applyChangesLocked(period)

	// 2. Reset group quota for the period of this tick.
	for _, g := range s.groups {
		if g.period != period {
			g.period = period
			g.used = 0
		}
	}

	// 3. Detect jobs that missed their deadline before selection runs.
	for _, id := range s.deterministicJobIDsLocked() {
		j := s.jobs[id]
		if j.active() && t > j.spec.Deadline && j.remaining > 0 {
			j.overdueAt = t
			ev := s.overdueEvidenceLocked(j, t)
			rec.Overdue = append(rec.Overdue, ev)
			s.overdue = append(s.overdue, ev)
		}
	}

	// 4. Gather ready jobs (released, unfinished, not overdue/cancelled).
	ready := s.readyJobsLocked(t)
	rec.ReadyCount = len(ready)

	// 5. Select the most eligible job (deadline asc, job id asc).
	sort.Slice(ready, func(i, k int) bool {
		a, b := ready[i], ready[k]
		if a.spec.Deadline != b.spec.Deadline {
			return a.spec.Deadline < b.spec.Deadline
		}
		return a.spec.ID < b.spec.ID
	})

	var chosen *job
	if len(ready) > 0 {
		chosen = ready[0]
		chain := s.chains[chosen.spec.GroupID]
		blocker := s.blockingGroupLocked(chain, period)
		if blocker == "" {
			// 6a. Run one tick and charge quota at every level at once.
			group := chosen.spec.GroupID
			for _, gid := range chain {
				s.groups[gid].used++
			}
			chosen.remaining--
			if chosen.remaining == 0 {
				chosen.completed = true
			}
			rec.Ran = true
			rec.JobID = chosen.spec.ID
			rec.GroupID = group
			rec.Chain = s.chainQuotaLocked(chain, period)
			rec.Runnable = true
			rec.IdleReason = ""
		} else {
			rec.Runnable = false
			rec.IdleReason = "quota-exhausted"
			rec.Blocked = s.blockedJobLocked(chosen, blocker, period)
			rec.AllBlocked = s.allBlockedLocked(ready, period)
		}
	} else {
		rec.Runnable = false
		rec.IdleReason = "no-ready-jobs"
	}

	s.trace = append(s.trace, rec)
	return rec
}

// applyChangesLocked introduces queued changes at a tick boundary.
func (s *Scheduler) applyChangesLocked(period int64) []ChangeEvent {
	if len(s.pending) == 0 {
		return nil
	}
	events := make([]ChangeEvent, 0, len(s.pending))
	pending := s.pending
	s.pending = nil
	for _, c := range pending {
		switch c.kind {
		case chSubmit:
			j := &job{spec: c.spec, remaining: c.spec.Work, overdueAt: -1}
			s.jobs[c.jobID] = j
			events = append(events, ChangeEvent{
				Kind: "submit", JobID: c.jobID, Group: c.spec.GroupID, Commit: c.rev,
			})
		case chMigrate:
			j := s.jobs[c.jobID]
			if j != nil && j.active() {
				from := j.spec.GroupID
				j.spec.GroupID = c.group
				events = append(events, ChangeEvent{
					Kind: "migrate", JobID: c.jobID, Group: c.group, From: from, Commit: c.rev,
				})
			}
		case chCancel:
			j := s.jobs[c.jobID]
			if j != nil {
				j.cancelled = true
				events = append(events, ChangeEvent{
					Kind: "cancel", JobID: c.jobID, Commit: c.rev,
				})
			}
		}
	}
	return events
}

func (s *Scheduler) deterministicJobIDsLocked() []string {
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *Scheduler) readyJobsLocked(t int64) []*job {
	var ready []*job
	for _, id := range s.deterministicJobIDsLocked() {
		j := s.jobs[id]
		if !j.active() || j.remaining <= 0 {
			continue
		}
		if j.spec.Release > t {
			continue
		}
		ready = append(ready, j)
	}
	return ready
}

// blockingGroupLocked returns the nearest root-first group on the chain with
// no quota left in period, or "" when the whole chain has quota.
func (s *Scheduler) blockingGroupLocked(chain []string, period int64) string {
	for _, gid := range chain {
		if s.groups[gid].available(period) <= 0 {
			return gid
		}
	}
	return ""
}

func (s *Scheduler) levelOfLocked(gid string) int {
	return len(s.chains[gid])
}

func (s *Scheduler) blockedJobLocked(j *job, blocker string, period int64) *BlockedJob {
	g := s.groups[blocker]
	return &BlockedJob{
		JobID:   j.spec.ID,
		GroupID: blocker,
		Level:   s.levelOfLocked(blocker),
		Quota:   g.spec.Quota,
		Used:    g.used,
	}
}

// allBlockedLocked reports every ready job whose chain contains an exhausted
// group, in the supplied (deadline, id) order.
func (s *Scheduler) allBlockedLocked(ready []*job, period int64) []BlockedJob {
	var out []BlockedJob
	for _, j := range ready {
		chain := s.chains[j.spec.GroupID]
		if blocker := s.blockingGroupLocked(chain, period); blocker != "" {
			bj := s.blockedJobLocked(j, blocker, period)
			out = append(out, *bj)
		}
	}
	return out
}

func (s *Scheduler) chainQuotaLocked(chain []string, period int64) []QuotaPoint {
	out := make([]QuotaPoint, 0, len(chain))
	for _, gid := range chain {
		g := s.groups[gid]
		used := g.used
		if g.period != period {
			used = 0
		}
		out = append(out, QuotaPoint{
			GroupID: gid,
			Level:   s.levelOfLocked(gid),
			Quota:   g.spec.Quota,
			Used:    used,
		})
	}
	return out
}

func (s *Scheduler) overdueEvidenceLocked(j *job, t int64) OverdueEvidence {
	return OverdueEvidence{
		JobID:     j.spec.ID,
		Tick:      t,
		Deadline:  j.spec.Deadline,
		Remaining: j.remaining,
		GroupID:   j.spec.GroupID,
		Chain:     s.chainQuotaLocked(s.chains[j.spec.GroupID], periodOf(t)),
	}
}

// Trace returns a copy of the per-tick trace so far.
func (s *Scheduler) Trace() []TickRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TickRecord, len(s.trace))
	copyTickRecords(s.trace, out)
	return out
}

// OverdueEvidence returns a copy of every overdue observation so far in the
// order they occurred (tick, then job ID).
func (s *Scheduler) OverdueEvidence() []OverdueEvidence {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OverdueEvidence, len(s.overdue))
	copyOverdue(s.overdue, out)
	return out
}

func copyTickRecords(src, dst []TickRecord) {
	for i := range src {
		dst[i] = src[i]
		dst[i].Applied = append([]ChangeEvent(nil), src[i].Applied...)
		dst[i].Overdue = append([]OverdueEvidence(nil), src[i].Overdue...)
		if src[i].Chain != nil {
			dst[i].Chain = append([]QuotaPoint(nil), src[i].Chain...)
		}
		if src[i].Blocked != nil {
			b := *src[i].Blocked
			dst[i].Blocked = &b
		}
		if src[i].AllBlocked != nil {
			dst[i].AllBlocked = append([]BlockedJob(nil), src[i].AllBlocked...)
		}
	}
}

func copyOverdue(src, dst []OverdueEvidence) {
	for i := range src {
		dst[i] = src[i]
		dst[i].Chain = append([]QuotaPoint(nil), src[i].Chain...)
	}
}

// Snapshot returns an immutable copy of the scheduler state.
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups := make([]GroupInfo, 0, len(s.order))
	for _, id := range s.order {
		g := s.groups[id]
		groups = append(groups, GroupInfo{
			ID:       g.spec.ID,
			ParentID: g.spec.ParentID,
			Quota:    g.spec.Quota,
			Used:     g.used,
			Period:   g.period,
			Level:    s.levelOfLocked(g.spec.ID),
		})
	}

	jobs := make([]JobInfo, 0, len(s.jobs)+len(s.pending))
	for _, id := range s.deterministicJobIDsLocked() {
		jobs = append(jobs, jobInfoLocked(s.jobs[id], false))
	}
	// Add jobs introduced by queued submits that have not reached a tick
	// boundary yet. A queued cancel of such a job is shown as cancelled.
	pendingLive := make(map[string]bool)
	pendingCancelled := make(map[string]bool)
	pendingMigrated := make(map[string]string)
	for _, c := range s.pending {
		switch c.kind {
		case chSubmit:
			if s.jobs[c.jobID] == nil {
				pendingLive[c.jobID] = true
			}
		case chCancel:
			if s.jobs[c.jobID] == nil {
				pendingCancelled[c.jobID] = true
			}
		case chMigrate:
			if s.jobs[c.jobID] == nil {
				pendingMigrated[c.jobID] = c.group
			}
		}
	}
	pendingIDs := make([]string, 0, len(pendingLive))
	for id := range pendingLive {
		pendingIDs = append(pendingIDs, id)
	}
	sort.Strings(pendingIDs)
	for _, id := range pendingIDs {
		// recover the spec from the queued submit
		var spec JobSpec
		for _, c := range s.pending {
			if c.kind == chSubmit && c.jobID == id {
				spec = c.spec
				break
			}
		}
		group := spec.GroupID
		if g := pendingMigrated[id]; g != "" {
			group = g
		}
		jobs = append(jobs, JobInfo{
			ID: id, Release: spec.Release, Remaining: spec.Work,
			Work: spec.Work, Deadline: spec.Deadline, GroupID: group,
			Cancelled: pendingCancelled[id], OverdueAt: -1, Pending: true,
		})
	}

	overdueIDs := make([]string, 0)
	for _, id := range s.deterministicJobIDsLocked() {
		if s.jobs[id].overdueAt >= 0 {
			overdueIDs = append(overdueIDs, id)
		}
	}

	return Snapshot{
		Tick:        s.tick,
		Period:      periodOf(s.tick),
		Revision:    s.rev,
		Groups:      groups,
		Jobs:        jobs,
		PendingN:    len(s.pending),
		IdleTicks:   s.idleTicks,
		RunTicks:    s.runTicks,
		OverdueJobs: overdueIDs,
	}
}

func jobInfoLocked(j *job, pending bool) JobInfo {
	overdue := j.overdueAt
	return JobInfo{
		ID:        j.spec.ID,
		Release:   j.spec.Release,
		Remaining: j.remaining,
		Work:      j.spec.Work,
		Deadline:  j.spec.Deadline,
		GroupID:   j.spec.GroupID,
		Completed: j.completed,
		Cancelled: j.cancelled,
		OverdueAt: overdue,
		Pending:   pending,
	}
}

// FormatTrace renders the trace as a human-readable table, one line per tick,
// followed by overdue evidence (if any).
func FormatTrace(recs []TickRecord, overdue []OverdueEvidence) string {
	var b strings.Builder
	b.WriteString("tick period  action                              detail\n")
	for _, r := range recs {
		detail := "idle"
		switch {
		case r.Ran:
			detail = fmt.Sprintf("run   job=%-8q group=%-8q", r.JobID, r.GroupID)
		case r.IdleReason == "no-ready-jobs":
			detail = "idle  (no ready jobs)"
		default:
			bk := r.Blocked
			detail = fmt.Sprintf("idle  blocked-by=%q level=%d quota=%d used=%d ready-job=%q",
				bk.GroupID, bk.Level, bk.Quota, bk.Used, bk.JobID)
		}
		line := fmt.Sprintf("%4d %6d  %-35s %s", r.Tick, r.Period, traceAction(r), detail)
		b.WriteString(strings.TrimRight(line, " "))
		b.WriteString("\n")
	}
	if len(overdue) > 0 {
		b.WriteString("\noverdue evidence:\n")
		for _, ev := range overdue {
			fmt.Fprintf(&b, "  job=%q tick=%d deadline=%d remaining=%d group=%q chain=%s\n",
				ev.JobID, ev.Tick, ev.Deadline, ev.Remaining, ev.GroupID, formatChain(ev.Chain))
		}
	}
	return b.String()
}

func traceAction(r TickRecord) string {
	if len(r.Applied) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(r.Applied))
	for _, e := range r.Applied {
		switch e.Kind {
		case "submit":
			parts = append(parts, "submit:"+e.JobID+"->"+e.Group)
		case "migrate":
			parts = append(parts, "migrate:"+e.JobID+":"+e.From+"->"+e.Group)
		case "cancel":
			parts = append(parts, "cancel:"+e.JobID)
		}
	}
	return strings.Join(parts, ",")
}

func formatChain(points []QuotaPoint) string {
	parts := make([]string, 0, len(points))
	for _, p := range points {
		parts = append(parts, fmt.Sprintf("%s[%d/%d]", p.GroupID, p.Used, p.Quota))
	}
	return strings.Join(parts, ">")
}
