package tenantsched_test

import (
	"math/rand"
	"tenantsched"
	"testing"
)

// TestRandomDifferential drives both implementations with identical random
// scripts. After every command the error/revision must match, and after every
// advance the complete per-tick trace, quota accounting, job state and overdue
// evidence must match. Several seeds are used deterministically.
func TestRandomDifferential(t *testing.T) {
	specs := fourLevelGroups()
	allGroups := []string{"root", "a", "b", "a1", "a1x", "b1"}
	leaves := []string{"a1x", "b1", "b", "a"} // mixes tight (a1 chain) and wide quotas
	jobIDs := make([]string, 18)
	for i := range jobIDs {
		jobIDs[i] = "j" + itoa(i)
	}

	for _, seed := range []int64{1, 2, 7, 42, 99, 2026} {
		seed := seed
		t.Run("seed"+itoa64(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			h := newHarness(t, specs)

			var liveIDs []string
			addLive := func(id string) {
				if !contains(liveIDs, id) {
					liveIDs = append(liveIDs, id)
				}
			}

			for op := 0; op < 500; op++ {
				switch rng.Intn(10) {
				case 0, 1, 2, 3: // submit ~40%
					id := jobIDs[rng.Intn(len(jobIDs))]
					rel := int64(rng.Intn(8))
					work := int64(1 + rng.Intn(6))
					dl := rel + int64(rng.Intn(10))
					grp := leaves[rng.Intn(len(leaves))]
					if err := h.submit(tenantsched.JobSpec{
						ID: id, Release: rel, Work: work, Deadline: dl, GroupID: grp,
					}); err == nil {
						addLive(id)
					}
				case 4, 5: // migrate ~20%
					if len(liveIDs) > 0 {
						id := liveIDs[rng.Intn(len(liveIDs))]
						dst := allGroups[rng.Intn(len(allGroups))]
						h.migrate(id, dst)
					}
				case 6: // cancel ~10%
					if len(liveIDs) > 0 {
						h.cancel(liveIDs[rng.Intn(len(liveIDs))])
					}
				default: // advance ~30%
					h.advance(h.sch.Tick() + int64(rng.Intn(4)))
				}
			}
			// Drain to past the next period boundary to force resets.
			h.advance(h.sch.Tick() + 12)
			// And a final long run to settle completions / overdue accounting.
			h.advance(h.sch.Tick() + 25)

			// compareState already ran after every advance.
		})
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func itoa64(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
