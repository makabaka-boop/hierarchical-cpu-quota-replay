package tenantsched_test

import (
	"strings"
	"testing"

	"tenantsched"
)

func mustNew(t *testing.T, specs []tenantsched.GroupSpec) *tenantsched.Scheduler {
	t.Helper()
	s, err := tenantsched.New(specs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name  string
		specs []tenantsched.GroupSpec
		want  error
	}{
		{
			name:  "empty",
			specs: nil,
			want:  tenantsched.ErrInvalidConfig,
		},
		{
			name: "two roots",
			specs: []tenantsched.GroupSpec{
				{ID: "r1"}, {ID: "r2"},
			},
			want: tenantsched.ErrInvalidConfig,
		},
		{
			name: "unknown parent",
			specs: []tenantsched.GroupSpec{
				{ID: "root"}, {ID: "a", ParentID: "ghost"},
			},
			want: tenantsched.ErrInvalidConfig,
		},
		{
			name: "cycle",
			specs: []tenantsched.GroupSpec{
				{ID: "root"},
				{ID: "a", ParentID: "b"},
				{ID: "b", ParentID: "a"},
			},
			want: tenantsched.ErrInvalidConfig,
		},
		{
			name: "too deep",
			specs: []tenantsched.GroupSpec{
				{ID: "root"},
				{ID: "l2", ParentID: "root"},
				{ID: "l3", ParentID: "l2"},
				{ID: "l4", ParentID: "l3"},
				{ID: "l5", ParentID: "l4"},
			},
			want: tenantsched.ErrInvalidConfig,
		},
		{
			name: "negative quota",
			specs: []tenantsched.GroupSpec{
				{ID: "root", Quota: -1},
			},
			want: tenantsched.ErrInvalidConfig,
		},
		{
			name:  "duplicate group",
			specs: []tenantsched.GroupSpec{{ID: "root"}, {ID: "root"}},
			want:  tenantsched.ErrInvalidConfig,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tenantsched.New(tc.specs); !errorIs(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("too many groups", func(t *testing.T) {
		specs := []tenantsched.GroupSpec{{ID: "root", Quota: 100}}
		for i := 0; i < tenantsched.MaxGroups; i++ { // root + 20 children == 21
			specs = append(specs, tenantsched.GroupSpec{
				ID: strings.Repeat("g", 1) + itoa(i), ParentID: "root", Quota: 1,
			})
		}
		if _, err := tenantsched.New(specs); !errorIs(err, tenantsched.ErrInvalidConfig) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("max depth and size accepted", func(t *testing.T) {
		specs := []tenantsched.GroupSpec{
			{ID: "root", Quota: 100},
			{ID: "l2", ParentID: "root", Quota: 100},
			{ID: "l3", ParentID: "l2", Quota: 100},
			{ID: "l4", ParentID: "l3", Quota: 100},
		}
		s := mustNew(t, specs)
		snap := s.Snapshot()
		levels := map[string]int{}
		for _, g := range snap.Groups {
			levels[g.ID] = g.Level
		}
		if levels["l4"] != 4 || levels["root"] != 1 {
			t.Fatalf("levels %+v", levels)
		}
	})
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func TestJobLimits(t *testing.T) {
	s := mustNew(t, []tenantsched.GroupSpec{{ID: "root", Quota: 1}})
	rev := s.Revision()
	for i := 0; i < tenantsched.MaxJobs; i++ {
		var err error
		rev, err = s.Submit(rev, tenantsched.JobSpec{
			ID: "j" + itoa(i), Release: 1000, Work: 1, Deadline: 2000, GroupID: "root",
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	if _, err := s.Submit(rev, tenantsched.JobSpec{
		ID: "onemore", Release: 0, Work: 1, Deadline: 1, GroupID: "root",
	}); !errorIs(err, tenantsched.ErrTooManyJobs) {
		t.Fatalf("got %v", err)
	}

	// invalid arguments
	base := tenantsched.JobSpec{ID: "x", Release: 0, Work: 1, Deadline: 1, GroupID: "root"}
	for mutate := range 9 {
		spec := base
		switch mutate {
		case 0:
			spec.ID = ""
		case 1:
			spec.Work = 0
		case 2:
			spec.Work = -3
		case 3:
			spec.Release = -1
		case 4:
			spec.Deadline = -1
		case 5:
			spec.GroupID = "nope"
		case 6:
			spec.ID = "j0" // live (pending) duplicate
		case 7:
			spec.ID = ""
		case 8:
			spec.GroupID = ""
		}
		if _, err := s.Submit(s.Revision(), spec); err == nil {
			t.Fatalf("case %d expected error", mutate)
		}
	}

	// unknown-job operations
	if _, err := s.Migrate(s.Revision(), "ghost", "root"); !errorIs(err, tenantsched.ErrNoSuchJob) {
		t.Fatalf("migrate unknown: %v", err)
	}
	if _, err := s.Cancel(s.Revision(), "ghost"); !errorIs(err, tenantsched.ErrNoSuchJob) {
		t.Fatalf("cancel unknown: %v", err)
	}
	if _, err := s.Submit(s.Revision(), tenantsched.JobSpec{
		ID: "g", Release: 0, Work: 1, Deadline: 1, GroupID: "ghost",
	}); !errorIs(err, tenantsched.ErrNoSuchGroup) {
		t.Fatalf("submit unknown group: %v", err)
	}
}

func TestAdvanceGuard(t *testing.T) {
	s := mustNew(t, []tenantsched.GroupSpec{{ID: "root", Quota: 100}})
	if _, err := s.AdvanceTo(s.Revision(), 5); err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if _, err := s.AdvanceTo(s.Revision(), 2); !errorIs(err, tenantsched.ErrNoAdvance) {
		t.Fatalf("backwards advance: %v", err)
	}
}
