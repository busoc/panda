package panda

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"time"
)

const (
	CCSDSLength = 6
	ESALength   = 10
	UMILength   = 21
)

type Packet interface {
	Timestamp() time.Time

	Bytes() ([]byte, error)
	Payload() []byte
}

type Telemetry struct {
	CCSDSHeader
	ESAHeader
	Data []byte
	Sum  uint16
}

func (t Telemetry) Bytes() ([]byte, error) {
	var b []byte
	w := new(bytes.Buffer)

	b, _ = encodeCCSDS(t.CCSDSHeader)
	w.Write(b)
	b, _ = encodeESA(t.ESAHeader)
	w.Write(b)

	w.Write(t.Data)

	return w.Bytes(), nil
}

func (t Telemetry) Payload() []byte {
	return t.Data
}

type Parameter struct {
	UMIHeader
	Data []byte
}

func (p Parameter) Value() interface{} {
	switch p.Type {
	default:
		return p.Data
	case Int32, Long:
		v := binary.BigEndian.Uint32(p.Data)
		return int32(v)
	case Float64, Real, Exponent, Decimal:
		v := binary.BigEndian.Uint64(p.Data)
		return math.Float64frombits(v)
	case Binary8, BinaryN:
		return "-"
	case String8, StringN:
		v := bytes.Trim(p.Data, "\x00")
		return string(v)
	case DateTime, Time:
		return "-"
	case Bit:
		return p.Data[0]
	}
}

func (p Parameter) Bytes() ([]byte, error) {
	w := new(bytes.Buffer)

	b, _ := encodeUMI(p.UMIHeader)
	w.Write(b)
	w.Write(p.Data)

	return w.Bytes(), nil
}

func (p Parameter) Payload() []byte {
	return p.Data
}

type CCSDSHeader struct {
	Pid     uint16
	Segment uint16
	Length  uint16
}

func (c CCSDSHeader) SegmentationFlag() CCSDSPacketSegmentation {
	return CCSDSPacketSegmentation(c.Segment & 0xC000 >> 14)
}

func (c CCSDSHeader) Apid() int {
	return int(c.Pid) & 0x07FF
}

func (c CCSDSHeader) Sequence() int {
	return int(c.Segment) & 0x3FFF
}

func (c CCSDSHeader) Len() int {
	return int(c.Length) + 1
}

type ESAHeader struct {
	Coarse  uint32
	Fine    uint8
	Control uint8
	Sid     uint32
}

func (e ESAHeader) PacketType() ESAPacketType {
	return ESAPacketType(e.Control & 0x0f)
}

func (e ESAHeader) Timestamp() time.Time {
	ns := time.Duration(e.Fine) * time.Millisecond

	t := time.Unix(int64(e.Coarse), ns.Nanoseconds()).UTC()
	// return t.Add(epoch)
	return t
}

func (e ESAHeader) Sum() bool {
	return (e.Control>>5)&0x01 == 1
}

type UMIHeader struct {
	State  UMIPacketState
	Orbit  [4]byte
	Code   [6]byte
	Type   UMIDataType
	Unit   uint16
	Coarse uint32
	Fine   uint8
	Length uint16
}

func (u UMIHeader) Timestamp() time.Time {
	ns := time.Duration(u.Fine) * time.Millisecond

	t := time.Unix(int64(u.Coarse), ns.Nanoseconds()).UTC()
	// return t.Add(epoch)
	return t
}

func decodeCCSDS(bs []byte) (CCSDSHeader, error) {
	var c CCSDSHeader

	r := bytes.NewReader(bs)
	binary.Read(r, binary.BigEndian, &c.Pid)
	binary.Read(r, binary.BigEndian, &c.Segment)
	binary.Read(r, binary.BigEndian, &c.Length)

	return c, nil
}

func encodeCCSDS(c CCSDSHeader) ([]byte, error) {
	w := new(bytes.Buffer)
	binary.Write(w, binary.BigEndian, c.Pid)
	binary.Write(w, binary.BigEndian, c.Segment)
	binary.Write(w, binary.BigEndian, c.Length)

	return w.Bytes(), nil
}

func decodeESA(bs []byte) (ESAHeader, error) {
	var e ESAHeader

	r := bytes.NewReader(bs)
	binary.Read(r, binary.BigEndian, &e.Coarse)
	binary.Read(r, binary.BigEndian, &e.Fine)
	binary.Read(r, binary.BigEndian, &e.Control)
	binary.Read(r, binary.BigEndian, &e.Sid)

	return e, nil
}

func encodeESA(e ESAHeader) ([]byte, error) {
	w := new(bytes.Buffer)
	binary.Write(w, binary.BigEndian, e.Coarse)
	binary.Write(w, binary.BigEndian, e.Fine)
	binary.Write(w, binary.BigEndian, e.Control)
	binary.Write(w, binary.BigEndian, e.Sid)

	return w.Bytes(), nil
}

func decodeUMI(bs []byte) (UMIHeader, error) {
	var u UMIHeader

	r := bytes.NewReader(bs)
	binary.Read(r, binary.BigEndian, &u.State)
	r.Read(u.Orbit[:])
	r.Read(u.Code[:])
	binary.Read(r, binary.BigEndian, &u.Type)
	binary.Read(r, binary.BigEndian, &u.Unit)
	binary.Read(r, binary.BigEndian, &u.Coarse)
	binary.Read(r, binary.BigEndian, &u.Fine)
	binary.Read(r, binary.BigEndian, &u.Length)

	return u, nil
}

func encodeUMI(u UMIHeader) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.BigEndian, u.State)
	w.Write(u.Orbit[:])
	w.Write(u.Code[:])
	binary.Write(w, binary.BigEndian, u.Type)
	binary.Write(w, binary.BigEndian, u.Unit)
	binary.Write(w, binary.BigEndian, u.Coarse)
	binary.Write(w, binary.BigEndian, u.Fine)
	binary.Write(w, binary.BigEndian, u.Length)

	return w.Bytes(), nil
}

type reader struct {
	io.Reader
	err  error
	size int64
}

func newReader(b []byte) *reader {
	return &reader{Reader: bytes.NewReader(b)}
}

func (r *reader) ReadLE(v interface{}) (int64, error) {
	return r.read(v, binary.LittleEndian)
}

func (r *reader) ReadBE(v interface{}) (int64, error) {
	return r.read(v, binary.BigEndian)
}

func (r *reader) Read(b []byte) (int, error) {
	if r.err != nil {
		return int(r.size), r.err
	}
	c, err := r.Reader.Read(b)
	if err != nil {
		r.err = err
	} else {
		r.size += int64(c)
	}
	return c, err
}

func (r *reader) read(v interface{}, e binary.ByteOrder) (int64, error) {
	defer func() {
		if r.err != nil {
			return
		}
		r.size += int64(binary.Size(v))
	}()
	if r.err != nil {
		return r.size, r.err
	}
	if err := binary.Read(r.Reader, binary.LittleEndian, v); err != nil {
		r.err = err
	}
	return r.size, nil
}
