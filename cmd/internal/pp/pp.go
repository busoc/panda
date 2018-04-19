package pp

import (
	"bytes"
	"io"
	"net"

	"github.com/busoc/panda"
)

func Open(a string) (io.Reader, error) {
	if _, _, err := net.SplitHostPort(a); err == nil {
		return mud.Listen("pp", a)
	}
	return mud.Walk("pp", a)
}

func Filter(r io.Reader, d mud.Decoder) <-chan mud.Parameter {
	q := make(chan mud.Parameter)
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
				if p, ok := p.(mud.Parameter); ok {
					q <- p
				}
			case mud.ErrDone:
				return
			}
		}
	}()
	return q
}

func Packets(addr string, codes []uint64) (<-chan mud.Parameter, error) {
	r, err := Open(addr)
	if err != nil {
		return nil, err
	}
	return Filter(r, NewDecoder(codes)), nil
}

type Decoder struct {
	codes   [][]byte
	decoder mud.Decoder
}

func NewDecoder(cs []uint64) mud.Decoder {
	if len(cs) == 0 {
		return mud.DecodePP()
	}
	d := mud.DecodePP()
	var vs [][]byte
	for _, c := range cs {
		vs = append(vs, Itob(c))
	}
	return Decoder{vs, d}
}

func (d Decoder) Decode(bs []byte) (int, mud.Packet, error) {
	i, p, err := d.decoder.Decode(bs)
	if err != nil {
		return i, nil, err
	}
	u, ok := p.(mud.Parameter)
	if !ok {
		return i, nil, mud.ErrSkip
	}
	for _, c := range d.codes {
		if bytes.Equal(c, u.Code[:]) {
			return i, p, nil
		}
	}
	return i, nil, mud.ErrSkip
}

func Itob(v uint64) []byte {
	bs := make([]byte, 16)

	i := len(bs) - 1
	for ; v > 0; i-- {
		bs[i], v = byte(v&0xFF), v>>8
	}
	return bs[i+1:]
}
