package tenantsched_test

import (
	"errors"
	"fmt"
	"sort"

	"tenantsched"
)

// refGroup / refJob are the reference model's own state shapes. The model is
// written from scratch (no shared code with the package internals) so that it
// can catch implementation drift.
type refGroup struct {
	id, parent  string
	quota, used int64
	period      int64
	level       int
}

type refJob struct {
	id                  string
	release, deadline   int64
	work, remaining     int64
	group               string
	completed, canceled bool
	overdueAt           int64
	pending             bool // introduced but not yet applied
	queuedCancel        bool // a cancel is queued in the same (not yet applied) batch
}

type refChange struct {
	kind  string // "submit" | "migrate" | "cancel"
	jobID string
	spec  tenantsched.JobSpec
	group string
	rev   int64
}

type refPoint struct {
	id    string
	level int
	quota int64
	used  int64
}

type refBlocked struct {
	job, group  string
	level       int
	quota, used int64
}

type refTick struct {
	tick, period int64
	applied      []tenantsched.ChangeEvent
	overdueJobs  []string
	ran          bool
	job, group   string
	chain        []refPoint
	blocked      *refBlocked
	allBlocked   []refBlocked
	readyCount   int
	runnable     bool
	idleReason   string
}

type refOverdue struct {
	job                 string
	tick, deadline, rem int64
	group               string
	chain               []refPoint
}

// reference is an independent tick-by-tick model.
type reference struct {
	groups  map[string]*refGroup
	order   []string
	chains  map[string][]string
	jobs    map[string]*refJob
	pending []refChange
	tick    int64
	rev     int64
	trace   []refTick
	overdue []refOverdue
}

func newReference(specs []tenantsched.GroupSpec) (*reference, error) {
	// Build the real scheduler once solely to validate the same config
	// rules; the reference keeps its own group table afterwards.
	if _, err := tenantsched.New(specs); err != nil {
		return nil, err
	}
	r := &reference{
		groups: map[string]*refGroup{},
		chains: map[string][]string{},
		jobs:   map[string]*refJob{},
		rev:    1,
	}
	parent := map[string]string{}
	for _, s := range specs {
		r.groups[s.ID] = &refGroup{id: s.ID, parent: s.ParentID, quota: s.Quota, period: 0}
		parent[s.ID] = s.ParentID
		r.order = append(r.order, s.ID)
	}
	for id := range r.groups {
		cur := id
		var rev []string
		for cur != "" {
			rev = append(rev, cur)
			cur = parent[cur]
		}
		chain := make([]string, len(rev))
		for i, g := range rev {
			chain[len(rev)-1-i] = g
		}
		r.chains[id] = chain
		r.groups[id].level = len(chain)
	}
	return r, nil
}

func (r *reference) period(t int64) int64 { return t / 10 }

func (r *reference) staged() map[string]*refJob {
	m := map[string]*refJob{}
	for id, j := range r.jobs {
		cp := *j
		m[id] = &cp
	}
	// represent pending-only jobs and follow pending mutations
	pendingOnly := map[string]*refJob{}
	for _, c := range r.pending {
		switch c.kind {
		case "submit":
			if _, live := r.jobs[c.jobID]; !live {
				pendingOnly[c.jobID] = &refJob{
					id: c.spec.ID, release: c.spec.Release, deadline: c.spec.Deadline,
					work: c.spec.Work, remaining: c.spec.Work, group: c.spec.GroupID,
					overdueAt: -1, pending: true,
				}
			}
		}
	}
	for id, j := range pendingOnly {
		m[id] = j
	}
	for _, c := range r.pending {
		switch c.kind {
		case "migrate":
			if j := m[c.jobID]; j != nil && !j.queuedCancel {
				j.group = c.group
			}
		case "cancel":
			if j := m[c.jobID]; j != nil {
				// Same-batch cancellation: the job is still active for
				// validation purposes but queued for removal.
				j.queuedCancel = true
			}
		}
	}
	return m
}

func (r *reference) expectRev(rev int64) error {
	if rev != r.rev {
		return fmt.Errorf("%w: ref expected %d got %d", tenantsched.ErrStaleRevision, rev, r.rev)
	}
	return nil
}

func (r *reference) submit(rev int64, spec tenantsched.JobSpec) (int64, error) {
	if err := r.expectRev(rev); err != nil {
		return r.rev, err
	}
	if spec.ID == "" {
		return r.rev, tenantsched.ErrInvalidArgument
	}
	if spec.Work <= 0 || spec.Release < 0 || spec.Deadline < 0 {
		return r.rev, tenantsched.ErrInvalidArgument
	}
	if _, ok := r.groups[spec.GroupID]; !ok {
		return r.rev, tenantsched.ErrNoSuchGroup
	}
	st := r.staged()
	if j, ok := st[spec.ID]; ok && !j.queuedCancel {
		return r.rev, tenantsched.ErrDuplicateID
	}
	alive := 0
	for _, j := range st {
		if j.queuedCancel {
			continue
		}
		if !j.completed && !j.canceled && j.overdueAt < 0 {
			alive++
		}
	}
	if alive >= 40 {
		return r.rev, tenantsched.ErrTooManyJobs
	}
	r.rev++
	r.pending = append(r.pending, refChange{kind: "submit", jobID: spec.ID, spec: spec, rev: r.rev})
	return r.rev, nil
}

func (r *reference) migrate(rev int64, id, dst string) (int64, error) {
	if err := r.expectRev(rev); err != nil {
		return r.rev, err
	}
	if id == "" {
		return r.rev, tenantsched.ErrInvalidArgument
	}
	if _, ok := r.groups[dst]; !ok {
		return r.rev, tenantsched.ErrNoSuchGroup
	}
	st := r.staged()
	j, ok := st[id]
	if !ok {
		return r.rev, tenantsched.ErrNoSuchJob
	}
	// A queued cancel in the current batch makes the job effectively gone.
	if j.queuedCancel {
		return r.rev, tenantsched.ErrNoSuchJob
	}
	if j.canceled || j.completed || j.overdueAt >= 0 {
		return r.rev, tenantsched.ErrJobInactive
	}
	r.rev++
	r.pending = append(r.pending, refChange{kind: "migrate", jobID: id, group: dst, rev: r.rev})
	return r.rev, nil
}

func (r *reference) cancel(rev int64, id string) (int64, error) {
	if err := r.expectRev(rev); err != nil {
		return r.rev, err
	}
	if id == "" {
		return r.rev, tenantsched.ErrInvalidArgument
	}
	st := r.staged()
	j, ok := st[id]
	if !ok {
		return r.rev, tenantsched.ErrNoSuchJob
	}
	if j.queuedCancel {
		return r.rev, tenantsched.ErrJobInactive
	}
	if j.canceled || j.completed || j.overdueAt >= 0 {
		return r.rev, tenantsched.ErrJobInactive
	}
	r.rev++
	r.pending = append(r.pending, refChange{kind: "cancel", jobID: id, rev: r.rev})
	return r.rev, nil
}

func (r *reference) chainPoints(id string, period int64) []refPoint {
	var out []refPoint
	for _, gid := range r.chains[id] {
		g := r.groups[gid]
		used := g.used
		if g.period != period {
			used = 0
		}
		out = append(out, refPoint{id: gid, level: g.level, quota: g.quota, used: used})
	}
	return out
}

func (r *reference) blocker(id string, period int64) string {
	for _, gid := range r.chains[id] {
		g := r.groups[gid]
		avail := g.quota
		if g.period == period {
			avail = g.quota - g.used
		}
		if avail <= 0 {
			return gid
		}
	}
	return ""
}

func (r *reference) sortedJobIDs() []string {
	ids := make([]string, 0, len(r.jobs))
	for id := range r.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *reference) runTick() refTick {
	t := r.tick
	p := r.period(t)
	rec := refTick{tick: t, period: p}

	// 1. apply committed changes
	left := r.pending
	r.pending = nil
	for _, c := range left {
		switch c.kind {
		case "submit":
			r.jobs[c.jobID] = &refJob{
				id: c.spec.ID, release: c.spec.Release, deadline: c.spec.Deadline,
				work: c.spec.Work, remaining: c.spec.Work, group: c.spec.GroupID,
				overdueAt: -1,
			}
			rec.applied = append(rec.applied, tenantsched.ChangeEvent{
				Kind: "submit", JobID: c.jobID, Group: c.spec.GroupID, Commit: c.rev,
			})
		case "migrate":
			j := r.jobs[c.jobID]
			if j != nil && !j.completed && !j.canceled && j.overdueAt < 0 {
				from := j.group
				j.group = c.group
				rec.applied = append(rec.applied, tenantsched.ChangeEvent{
					Kind: "migrate", JobID: c.jobID, Group: c.group, From: from, Commit: c.rev,
				})
			}
		case "cancel":
			if j := r.jobs[c.jobID]; j != nil {
				j.canceled = true
				rec.applied = append(rec.applied, tenantsched.ChangeEvent{
					Kind: "cancel", JobID: c.jobID, Commit: c.rev,
				})
			}
		}
	}

	// 2. period reset
	for _, g := range r.groups {
		if g.period != p {
			g.period = p
			g.used = 0
		}
	}

	// 3. overdue detection in id order
	for _, id := range r.sortedJobIDs() {
		j := r.jobs[id]
		if !j.completed && !j.canceled && j.overdueAt < 0 && j.remaining > 0 && t > j.deadline {
			j.overdueAt = t
			rec.overdueJobs = append(rec.overdueJobs, id)
			r.overdue = append(r.overdue, refOverdue{
				job: id, tick: t, deadline: j.deadline, rem: j.remaining,
				group: j.group, chain: r.chainPoints(j.group, p),
			})
		}
	}

	// 4. ready jobs
	var ready []*refJob
	for _, id := range r.sortedJobIDs() {
		j := r.jobs[id]
		if j.completed || j.canceled || j.overdueAt >= 0 || j.remaining <= 0 {
			continue
		}
		if j.release <= t {
			ready = append(ready, j)
		}
	}
	rec.readyCount = len(ready)

	// 5. selection: deadline asc, id asc
	sort.SliceStable(ready, func(i, k int) bool {
		if ready[i].deadline != ready[k].deadline {
			return ready[i].deadline < ready[k].deadline
		}
		return ready[i].id < ready[k].id
	})

	if len(ready) == 0 {
		rec.idleReason = "no-ready-jobs"
	} else {
		top := ready[0]
		if b := r.blocker(top.group, p); b != "" {
			rec.idleReason = "quota-exhausted"
			g := r.groups[b]
			rec.blocked = &refBlocked{
				job: top.id, group: b, level: g.level, quota: g.quota, used: g.used,
			}
			for _, j := range ready {
				if b2 := r.blocker(j.group, p); b2 != "" {
					g2 := r.groups[b2]
					rec.allBlocked = append(rec.allBlocked, refBlocked{
						job: j.id, group: b2, level: g2.level, quota: g2.quota, used: g2.used,
					})
				}
			}
		} else {
			for _, gid := range r.chains[top.group] {
				r.groups[gid].used++
			}
			top.remaining--
			if top.remaining == 0 {
				top.completed = true
			}
			rec.ran = true
			rec.job = top.id
			rec.group = top.group
			rec.chain = r.chainPoints(top.group, p)
			rec.runnable = true
		}
	}

	r.trace = append(r.trace, rec)
	r.tick++
	r.rev++
	return rec
}

func (r *reference) advance(rev, target int64) (ran, idle int64, recs []refTick, err error) {
	if target < r.tick {
		return 0, 0, nil, tenantsched.ErrNoAdvance
	}
	if err := r.expectRev(rev); err != nil {
		return 0, 0, nil, err
	}
	for r.tick <= target {
		rec := r.runTick()
		recs = append(recs, rec)
		if rec.ran {
			ran++
		} else {
			idle++
		}
	}
	return ran, idle, recs, nil
}

// errIs compares errors by their sentinel; nil matches nil.
func errIs(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	sentinels := []error{
		tenantsched.ErrInvalidConfig, tenantsched.ErrStaleRevision, tenantsched.ErrDuplicateID,
		tenantsched.ErrNoSuchGroup, tenantsched.ErrNoSuchJob, tenantsched.ErrJobInactive,
		tenantsched.ErrTooManyJobs, tenantsched.ErrInvalidArgument, tenantsched.ErrNoAdvance,
	}
	for _, s := range sentinels {
		if errors.Is(a, s) != errors.Is(b, s) {
			return false
		}
	}
	return true
}
