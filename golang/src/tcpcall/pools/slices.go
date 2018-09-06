package pools

import "sync"

var (
	// Will be used to set size only for new buffers
	BuffChanSize = 500
	// Step for slices
	//
	// Change with caution: may leave some buffers unavailable!
	SplitVal = 4
	// Single dimensional
	packetChan  = make(map[int]chan []byte)
	packetChanM sync.RWMutex
)

func GetFreeBuffer(nLen int) []byte {
	// Get more than needed to fulfill or expectations
	modulo := nLen / SplitVal
	if nLen%SplitVal != 0 {
		modulo++
	}
	packetChanM.RLock()
	chn := packetChan[modulo]
	if chn == nil {
		chn = make(chan []byte, BuffChanSize)
		packetChanM.RUnlock()
		packetChanM.Lock()
		packetChan[modulo] = chn
		packetChanM.Unlock()
	} else {
		packetChanM.RUnlock()
	}
	var buf []byte
	select {
	case buf = <-chn:
	default:
		buf = make([]byte, SplitVal*modulo)
	}
	return buf[:nLen]
}

// Works best when inserting buffer after getting it from GetFreeBuffer
//
// Will drop buffer if no or full channel
func AppendToBuffer(buf []byte) {
	packetChanM.RLock()
	select {
	case packetChan[cap(buf)/SplitVal] <- buf:
	default:
	}
	packetChanM.RUnlock()
}
