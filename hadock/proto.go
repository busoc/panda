package hadock

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/busoc/panda"
)

var (
	ErrUnsupportedProtocol   = errors.New("unsupported protocol")
	ErrUnsupportedVMUVersion = errors.New("unsupported vmu version")
)

const (
	OPS  = 255
	TEST = 0
	SIM1 = 1
	SIM2 = 2
)

const Preamble uint32 = 0xf82e3553

//const Preamble uint32 = 0x5335e2f8

const (
	HadockVersion1 = iota
	HadockVersion2
)

var errFinalFragment = errors.New("packet completed")

const PacketLength = 10

type Module interface {
	Process(uint8, mud.HRPacket) error
}

type multiModule struct {
	ms []Module
}

func Process(ms []Module) Module {
	ps := make([]Module, len(ms))
	copy(ps, ms)
	return multiModule{ps}
}

func (m multiModule) Process(i uint8, p mud.HRPacket) error {
	var err error
	for _, m := range m.ms {
		if e := m.Process(i, p); err == nil && e != nil {
			err = e
		}
	}
	return err
}

type Packet struct {
	Protocol uint8
	Version  uint8
	Instance uint8
	Sequence uint16
	Length   uint32
	Payload  []byte
	Sum      uint16

	Curr uint16
	Last uint16
}

func DecodeCompressedPackets(r io.Reader, is []uint8) <-chan *Packet {
	q := make(chan *Packet)
	go func() {
		if c, ok := r.(io.Closer); ok {
			defer c.Close()
		}
		g, _ := gzip.NewReader(r)
		defer g.Close()
		bs := make([]byte, 4*1024*1024)

		var vs []byte
		for {
			n, err := g.Read(bs)
			switch err {
			case nil:
				vs = bs[:n]
			case gzip.ErrHeader, gzip.ErrChecksum:
			case io.EOF, io.ErrUnexpectedEOF:
				return
			default:
			}
			version := binary.BigEndian.Uint16(g.Header.Extra)
			q <- &Packet{
				Version:  uint8((version >> 8) & 0x0F),
				Instance: uint8(version & 0xFF),
				Payload:  vs,
			}
		}
	}()
	return q
}

func DecodeBinaryPackets(r io.Reader, is []uint8) <-chan *Packet {
	q := make(chan *Packet)
	go func() {
		if c, ok := r.(io.Closer); ok {
			defer c.Close()
		}
		defer close(q)

		sort.Slice(is, func(i, j int) bool { return is[i] < is[j] })
		for {
			p, err := DecodePacket(r)
			switch err {
			case nil:
				ix := sort.Search(len(is), func(i int) bool {
					return is[i] >= p.Instance
				})
				if len(is) > 0 && (ix >= len(is) || is[ix] != p.Instance) {
					break
				}
			case io.EOF, ErrUnsupportedProtocol, ErrUnsupportedVMUVersion:
				return
			default:
				continue
			}
			q <- p
		}
	}()
	return q
}

func EncodePacket(p *Packet) ([]byte, error) {
	w := new(bytes.Buffer)
	if p.Version != HadockVersion1 {
		return nil, fmt.Errorf("not yet implemented")
	}
	binary.Write(w, binary.BigEndian, uint8(p.Protocol<<4|p.Version))
	binary.Write(w, binary.BigEndian, p.Instance)
	binary.Write(w, binary.BigEndian, p.Sequence)
	binary.Write(w, binary.BigEndian, p.Length)
	w.Write(p.Payload)
	binary.Write(w, binary.BigEndian, p.Sum)

	return w.Bytes(), nil
}

func DecodePacket(r io.Reader) (*Packet, error) {
	prefix, err := readPreamble(r)
	if err != nil {
		return nil, err
	}
	p := new(Packet)
	p.Protocol, p.Version, p.Instance = uint8(prefix>>12), uint8(prefix>>8)&0x0F, uint8(prefix&0xFF)
	switch p.Protocol {
	default:
		return nil, ErrUnsupportedProtocol
	case HadockVersion1:
		err = readPacket(r, p)
	case HadockVersion2:
		ps := make([]*Packet, 0, 256)
		for {
			v := new(Packet)
			final, err := readFragment(r, v)
			if err != nil {
				return nil, err
			}
			ps = append(ps, v)
			if final {
				p.Sequence, p.Length = v.Sequence, v.Length
				break
			}
			x, err := readPreamble(r)
			if err != nil {
				return nil, err
			}
			if x != prefix {
				return nil, fmt.Errorf("version mismatched: expected: %x, got %x", prefix, x)
			}
		}
		sort.Slice(ps, func(i, j int) bool { return ps[i].Curr < ps[j].Curr })
		for i := range ps {
			p.Payload = append(p.Payload, ps[i].Payload...)
		}
	}
	return p, err
}

func readPreamble(r io.Reader) (uint16, error) {
	var (
		preamble uint32
		prefix   uint16
	)
	if err := binary.Read(r, binary.BigEndian, &preamble); err != nil {
		return 0, err
	}
	if preamble != Preamble {
		return 0, fmt.Errorf("invalid preamble: expected %x, got %x", Preamble, preamble)
	}
	if err := binary.Read(r, binary.BigEndian, &prefix); err != nil {
		return 0, err
	}
	return prefix, nil
}

func readFragment(r io.Reader, p *Packet) (bool, error) {
	if err := binary.Read(r, binary.BigEndian, &p.Curr); err != nil {
		return false, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Last); err != nil {
		return false, err
	}
	return p.Curr == p.Last, readPacket(r, p)
}

func readPacket(r io.Reader, p *Packet) error {
	if err := binary.Read(r, binary.BigEndian, &p.Sequence); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Length); err != nil {
		return err
	}
	p.Payload = make([]byte, int(p.Length))
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return err
	}
	return binary.Read(r, binary.BigEndian, &p.Sum)
}
