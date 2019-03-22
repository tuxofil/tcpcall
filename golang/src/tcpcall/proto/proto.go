/*
TCP Request-Reply Bridge - network protocol.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package proto

import (
	"encoding/binary"
	"io"
	"sync"
	"time"
)

const (
	REQUEST              = 0
	REPLY                = 1
	ERROR                = 2
	CAST                 = 3
	FLOW_CONTROL_SUSPEND = 4
	FLOW_CONTROL_RESUME  = 5
	UPLINK_CAST          = 6
)

type Packet struct {
	// Packet type identifier
	Type int
	// Packet headers. Actual length depends on packet type.
	Headers [12]byte
	// Packet data. Some packet types doesn't support it.
	Data []byte
}

var (
	// Shorthand for byte order
	gOrder = binary.BigEndian
	// packet sequence number generator state
	gSeq   uint32
	gSeqMu sync.Mutex
)

// Generate sequence number (aka packet ID).
func GenSeqNum() uint32 {
	gSeqMu.Lock()
	res := gSeq
	gSeq++
	gSeqMu.Unlock()
	return res
}

// Write packet to the stream.
// Implements io.WriterTo interface.
func (p *Packet) WriteTo(writer io.Writer) (int64, error) {
	headerLen := 4
	switch p.Type {
	case REQUEST:
		headerLen = 12
	case FLOW_CONTROL_SUSPEND:
		headerLen = 8
	case FLOW_CONTROL_RESUME, UPLINK_CAST:
		headerLen = 0
	}
	packetLen := 1 + headerLen + len(p.Data)
	if err := binary.Write(writer, gOrder, uint32(packetLen)); err != nil {
		return 0, err
	}
	if err := binary.Write(writer, gOrder, byte(p.Type)); err != nil {
		return 4, err
	}
	if 0 < headerLen {
		if n, err := writer.Write(p.Headers[:headerLen]); err != nil {
			return int64(1 + n), err
		}
	}
	if 0 < len(p.Data) {
		if n, err := writer.Write(p.Data); err != nil {
			return int64(5 + headerLen + n), err
		}
	}
	return int64(4 + packetLen), nil
}

// Decode and return SeqNum field.
// Valid for: Request, Reply, Error, Cast
func (p *Packet) SeqNum() uint32 {
	return gOrder.Uint32(p.Headers[:4])
}

// Decode and return Deadline field.
// Valid for: Request
func (p *Packet) RequestDeadline() time.Time {
	millis := int64(gOrder.Uint64(p.Headers[4:]))
	return time.Unix(0, millis*1000)
}

// Decode and return Duration field.
// Valid for: FlowControlSuspend
func (p *Packet) SuspendDuration() time.Duration {
	millis := gOrder.Uint64(p.Headers[:])
	return time.Duration(millis * 1000000)
}

// Create new request packet.
func NewRequest(seqNum uint32, deadline time.Time, data []byte) Packet {
	p := Packet{Type: REQUEST, Data: data}
	gOrder.PutUint32(p.Headers[:], uint32(seqNum))
	gOrder.PutUint64(p.Headers[4:], uint64(deadline.UnixNano()/1000))
	return p
}

// Create new reply packet.
func NewReply(seqNum uint32, data []byte) Packet {
	p := Packet{Type: REPLY, Data: data}
	gOrder.PutUint32(p.Headers[:], uint32(seqNum))
	return p
}

// Create new error packet.
func NewError(seqNum uint32, data []byte) Packet {
	p := Packet{Type: ERROR, Data: data}
	gOrder.PutUint32(p.Headers[:], uint32(seqNum))
	return p
}

// Create new cast packet.
func NewCast(seqNum uint32, data []byte) Packet {
	p := Packet{Type: CAST, Data: data}
	gOrder.PutUint32(p.Headers[:], uint32(seqNum))
	return p
}

// Create new suspend (flow control) packet.
func NewSuspend(duration time.Duration) Packet {
	p := Packet{Type: FLOW_CONTROL_SUSPEND}
	gOrder.PutUint64(p.Headers[:], uint64(duration/1000000))
	return p
}

// Create new resume (flow control) packet.
func NewResume() Packet {
	return Packet{Type: FLOW_CONTROL_RESUME}
}

// Create new uplink cast packet.
func NewUplinkCast(data []byte) Packet {
	return Packet{Type: UPLINK_CAST, Data: data}
}
