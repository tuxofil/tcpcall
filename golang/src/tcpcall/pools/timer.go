/**
Pool of reused timers.
Intended to reduce allocations.
*/

package pools

import "time"

var (
	// Size of the pool to store timers in
	gTimerPoolSize = 500
	// Pool of timers
	gTimerPool = make(chan *time.Timer, gTimerPoolSize)
)

// GetFreeTimer will reset free timer to specified duration and return it
func GetFreeTimer(dur time.Duration) (t *time.Timer) {
	select {
	case t = <-gTimerPool:
		t.Reset(dur)
	default:
		t = time.NewTimer(dur)
	}
	return
}

// AppendToTimer stops and puts timer to pool
func AppendToTimer(t *time.Timer) {
	if !t.Stop() {
		if len(t.C) > 0 {
			<-t.C
		}
	}
	select {
	case gTimerPool <- t:
	default:
	}
}
