package pp

import (
	"bytes"
	"io"
	"net"

	"github.com/busoc/panda"
)

func Open(a string) (io.Reader, error) {
	if _, _, err := net.SplitHostPort(a); err == nil {
		return panda.Listen("pp", a)
	}
	return panda.Walk("pp", a)
}

func Filter(r io.Reader, d panda.Decoder) <-chan panda.Parameter {
	q := make(chan panda.Parameter)
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
				if p, ok := p.(panda.Parameter); ok {
					q <- p
				}
			case panda.ErrDone:
				return
			}
		}
	}()
	return q
}

func Packets(addr string, codes []uint64) (<-chan panda.Parameter, error) {
	r, err := Open(addr)
	if err != nil {
		return nil, err
	}
	return Filter(r, NewDecoder(codes)), nil
}

type Decoder struct {
	codes   [][]byte
	decoder panda.Decoder
}

func NewDecoder(cs []uint64) panda.Decoder {
	if len(cs) == 0 {
		return panda.DecodePP()
	}
	d := panda.DecodePP()
	var vs [][]byte
	for _, c := range cs {
		vs = append(vs, Itob(c))
	}
	return Decoder{vs, d}
}

func (d Decoder) Decode(bs []byte) (int, panda.Packet, error) {
	i, p, err := d.decoder.Decode(bs)
	if err != nil {
		return i, nil, err
	}
	u, ok := p.(panda.Parameter)
	if !ok {
		return i, nil, panda.ErrSkip
	}
	for _, c := range d.codes {
		if bytes.Equal(c, u.Code[:]) {
			return i, p, nil
		}
	}
	return i, nil, panda.ErrSkip
}

func Itob(v uint64) []byte {
	bs := make([]byte, 16)

	i := len(bs) - 1
	for ; v > 0; i-- {
		bs[i], v = byte(v&0xFF), v>>8
	}
	return bs[i+1:]
}
