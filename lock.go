package tenantsched

import "sync"

// mutex is the single lock serializing every scheduler transition. Submits,
// migrations, cancels and advances all linearize under it, so a tick's
// changes + selection + charging happen atomically and two advances can
// never execute the same tick.
type mutex = sync.Mutex

func newMutex() *mutex { return &sync.Mutex{} }
