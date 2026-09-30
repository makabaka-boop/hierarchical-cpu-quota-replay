package tenantsched_test

import (
	"fmt"

	"tenantsched"
)

// ExampleScheduler drives a compact scenario and prints the per-tick trace:
// two runs exhaust a tight mid-level quota, an idle tick records the blocking
// ancestor, then a migration moves the job to a different branch.
func ExampleScheduler() {
	groups := []tenantsched.GroupSpec{
		{ID: "root", Quota: 100},
		{ID: "a", ParentID: "root", Quota: 2},
		{ID: "b", ParentID: "root", Quota: 100},
		{ID: "a1", ParentID: "a", Quota: 100},
	}
	s, err := tenantsched.New(groups)
	if err != nil {
		panic(err)
	}
	rev, err := s.Submit(s.Revision(), tenantsched.JobSpec{
		ID: "j", Release: 0, Work: 4, Deadline: 100, GroupID: "a1",
	})
	if err != nil {
		panic(err)
	}
	// Ticks 0 and 1 run, consuming a's whole quota of 2; tick 2 idles.
	res, err := s.AdvanceTo(rev, 2)
	if err != nil {
		panic(err)
	}
	// Migrate the blocked job onto the b branch (no refund to a).
	rev, err = s.Migrate(res.Revision, "j", "b")
	if err != nil {
		panic(err)
	}
	// Tick 3 applies the migration and runs; tick 4 finishes the job.
	if _, err := s.AdvanceTo(rev, 4); err != nil {
		panic(err)
	}
	fmt.Print(tenantsched.FormatTrace(s.Trace(), s.OverdueEvidence()))
	// Output:
	// tick period  action                              detail
	//    0      0  submit:j->a1                        run   job="j"      group="a1"
	//    1      0  -                                   run   job="j"      group="a1"
	//    2      0  -                                   idle  blocked-by="a" level=2 quota=2 used=2 ready-job="j"
	//    3      0  migrate:j:a1->b                     run   job="j"      group="b"
	//    4      0  -                                   run   job="j"      group="b"
}
