/*
TCP Request-Reply Bridge - network protocol.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package proto

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"tcpcall/pools"
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

type Packet interface {
	Encode() [][]byte
}

type PacketRequest struct {
	SeqNum   uint32
	Deadline time.Time
	Request  [][]byte
}

type PacketReply struct {
	SeqNum uint32
	Reply  [][]byte
}

type PacketError struct {
	SeqNum uint32
	Reason [][]byte
}

type PacketCast struct {
	SeqNum  uint32
	Request [][]byte
}

type PacketFlowControlSuspend struct {
	Duration time.Duration
}

type PacketFlowControlResume struct {
}

type PacketUplinkCast struct {
	Data [][]byte
}

// packet sequence number generator state
var (
	gSeq *uint32
)

func init() {
	var v uint32
	gSeq = &v
}

// Create new request packet.
func NewRequest(request [][]byte, deadline time.Time) *PacketRequest {
	return &PacketRequest{getSeqNum(), deadline, request}
}

// Create new cast packet.
func NewCast(data [][]byte) *PacketCast {
	return &PacketCast{0, data}
}

// Encode Request packet for network.
func (p PacketRequest) Encode() [][]byte {
	res := make([][]byte, len(p.Request)+1)
	res[0] = pools.GetFreeBuffer(13) // packet header
	res[0][0] = REQUEST
	binary.BigEndian.PutUint32(res[0][1:], uint32(p.SeqNum))
	binary.BigEndian.PutUint64(res[0][5:], uint64(p.Deadline.UnixNano()/1000))
	copy(res[1:], p.Request)
	return res
}

// Encode Reply packet for network.
func (p PacketReply) Encode() [][]byte {
	res := make([][]byte, len(p.Reply)+1)
	res[0] = pools.GetFreeBuffer(5)
	res[0][0] = REPLY
	binary.BigEndian.PutUint32(res[0][1:], uint32(p.SeqNum))
	copy(res[1:], p.Reply)
	return res
}

// Encode Cast packet for network.
func (p PacketCast) Encode() [][]byte {
	res := make([][]byte, len(p.Request)+1)
	res[0] = pools.GetFreeBuffer(5)
	res[0][0] = CAST
	binary.BigEndian.PutUint32(res[0][1:], uint32(p.SeqNum))
	copy(res[1:], p.Request)
	return res
}

// Encode Error packet for network.
func (p PacketError) Encode() [][]byte {
	res := make([][]byte, len(p.Reason)+1)
	res[0] = pools.GetFreeBuffer(5)
	res[0][0] = ERROR
	binary.BigEndian.PutUint32(res[0][1:], uint32(p.SeqNum))
	copy(res[1:], p.Reason)
	return res
}

// Encode Suspend packet for network.
func (p PacketFlowControlSuspend) Encode() [][]byte {
	res := make([][]byte, 1)
	res[0] = pools.GetFreeBuffer(9)
	res[0][0] = FLOW_CONTROL_SUSPEND
	binary.BigEndian.PutUint64(res[0][1:], uint64(p.Duration.Nanoseconds()/1000000))
	return res
}

// Encode Resume packet for network.
func (p PacketFlowControlResume) Encode() [][]byte {
	return [][]byte{[]byte{FLOW_CONTROL_RESUME}}
}

// Encode Uplink Cast packet for network.
func (p PacketUplinkCast) Encode() [][]byte {
	res := make([][]byte, len(p.Data)+1)
	res[0] = []byte{UPLINK_CAST}
	copy(res[1:], p.Data)
	return res
}

// Decode network packet.
func Decode(bytes []byte) (ptype int, packet interface{}, err error) {
	if len(bytes) == 0 {
		return -1, nil, errors.New("bad packet: empty")
	}
	ptype = int(bytes[0])
	switch ptype {
	case REQUEST:
		if len(bytes) < 13 {
			return -1, nil, errors.New("bad Request packet: header too small")
		}
		seqnum := binary.BigEndian.Uint32(bytes[1:])
		micros := binary.BigEndian.Uint64(bytes[5:13])
		deadline := time.Unix(0, int64(micros*1000))
		return ptype, &PacketRequest{seqnum, deadline, [][]byte{bytes[13:]}}, nil
	case CAST:
		if len(bytes) < 5 {
			return -1, nil, errors.New("bad Cast packet: header too small")
		}
		seqnum := binary.BigEndian.Uint32(bytes[1:])
		return ptype, &PacketCast{seqnum, [][]byte{bytes[5:]}}, nil
	case REPLY:
		if len(bytes) < 5 {
			return -1, nil, errors.New("bad Reply packet: header too small")
		}
		seqnum := binary.BigEndian.Uint32(bytes[1:])
		return ptype, &PacketReply{seqnum, [][]byte{bytes[5:]}}, nil
	case ERROR:
		if len(bytes) < 5 {
			return -1, nil, errors.New("bad Error packet: header too small")
		}
		seqnum := binary.BigEndian.Uint32(bytes[1:])
		return ptype, &PacketError{seqnum, [][]byte{bytes[5:]}}, nil
	case FLOW_CONTROL_SUSPEND:
		if len(bytes) < 9 {
			return -1, nil, errors.New("bad Suspend packet: header too small")
		}
		millis := binary.BigEndian.Uint64(bytes[1:])
		return ptype, &PacketFlowControlSuspend{time.Millisecond * time.Duration(millis)}, nil
	case FLOW_CONTROL_RESUME:
		return ptype, &PacketFlowControlResume{}, nil
	case UPLINK_CAST:
		return ptype, &PacketUplinkCast{[][]byte{bytes[1:]}}, nil
	}
	return -1, nil, errors.New("Not implemented")
}

// Generate sequence number (aka packet ID).
func getSeqNum() uint32 {
	return atomic.AddUint32(gSeq, 1) - 1
}
