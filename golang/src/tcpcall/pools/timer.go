package pools

import (
	"time"
)

var (
	// Size of channel to store timers in
	timerChanSize = 500
	tPool         chan *time.Timer
)

func init() {
	tPool = make(chan *time.Timer, timerChanSize)
}

// GetFreeTimer will reset free timer to specified duration and return it
func GetFreeTimer(dur time.Duration) (t *time.Timer) {
	select {
	case t = <-tPool:
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
	case tPool <- t:
	default:
	}
}
