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
		return panda.Listen("tm", a)
	}
	return panda.Walk("tm", a)
}

func Filter(r io.Reader, d panda.Decoder) <-chan panda.Telemetry {
	q := make(chan panda.Telemetry)
	go func() {
		source := panda.NewReader(r, d)
		defer func() {
			close(q)
			source.Close()
		}()
		for {
			p, err := source.Read()
			switch err {
			case nil:
				if p, ok := p.(panda.Telemetry); ok {
					q <- p
				}
			case panda.ErrDone:
				return
			}
		}
	}()
	return q
}

func Packets(addr string, apid int, pids []uint32) (<-chan panda.Telemetry, error) {
	r, err := Open(addr)
	if err != nil {
		return nil, err
	}
	return Filter(r, NewDecoder(apid, pids)), nil
}

type Decoder struct {
	pid     []byte
	sources [][]byte
	decoder panda.Decoder
}

func NewDecoder(apid int, ps []uint32) panda.Decoder {
	d := panda.DecodeTM()
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

func (d Decoder) Decode(bs []byte) (int, panda.Packet, error) {
	if len(d.pid) > 0 && !bytes.HasPrefix(bs, d.pid) {
		return len(bs), nil, panda.ErrSkip
	}
	if len(d.sources) == 0 {
		return d.decoder.Decode(bs)
	}
	ix := panda.CCSDSLength + panda.ESALength
	for _, s := range d.sources {
		if bytes.Equal(bs[ix-len(s):ix], s) {
			return d.decoder.Decode(bs)
		}
	}
	return len(bs), nil, panda.ErrSkip
}
