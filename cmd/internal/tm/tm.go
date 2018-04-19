package tm

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/busoc/panda"
)

type raw []byte

func (r raw) Timestamp() time.Time {
	return time.Now()
}

func (r raw) Bytes() ([]byte, error) {
	return r.Payload(), nil
}

func (r raw) Payload() []byte {
	return r
}

func Open(a string) (io.Reader, error) {
	if _, _, err := net.SplitHostPort(a); err == nil {
		return mud.Listen("tm", a)
	}
	return mud.Walk("tm", a)
}

func Filter(r io.Reader, d mud.Decoder) <-chan mud.Telemetry {
	q := make(chan mud.Telemetry)
	go func() {
		source := mud.NewReader(r, d)
		defer func() {
			close(q)
			source.Close()
		}()
		for {
			p, err := source.Read()
			switch err {
			case nil:
				if p, ok := p.(mud.Telemetry); ok {
					q <- p
				}
			case mud.ErrDone:
				return
			}
		}
	}()
	return q
}

func Packets(addr string, apid int, pids []uint32) (<-chan mud.Telemetry, error) {
	r, err := Open(addr)
	if err != nil {
		return nil, err
	}
	return Filter(r, NewDecoder(apid, pids)), nil
}

type Decoder struct {
	pid     []byte
	sources [][]byte
	decoder mud.Decoder
}

func NewDecoder(apid int, ps []uint32) mud.Decoder {
	d := mud.DecodeTM()
	if apid <= 0 && len(ps) == 0 {
		return d
	}
	var is [][]byte
	for _, p := range ps {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, p)

		is = append(is, bs)
	}

	var pid []byte
	if apid > 0 {
		pid = make([]byte, 2)
		binary.BigEndian.PutUint16(pid, uint16(1<<12|1<<11|apid))
	}

	return Decoder{pid, is, d}
}

func (d Decoder) Decode(bs []byte) (int, mud.Packet, error) {
	if len(d.pid) > 0 && !bytes.HasPrefix(bs, d.pid) {
		return len(bs), nil, mud.ErrSkip
	}
	if len(d.sources) == 0 {
		return d.decoder.Decode(bs)
	}
	ix := mud.CCSDSLength + mud.ESALength
	for _, s := range d.sources {
		if bytes.Equal(bs[ix-len(s):ix], s) {
			return d.decoder.Decode(bs)
		}
	}
	return len(bs), nil, mud.ErrSkip
}
