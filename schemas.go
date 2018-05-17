package panda

import (
  "encoding/binary"
  "fmt"
  "sort"
  "time"

  "github.com/busoc/panda/internal/buffer"
)

type Item struct {
	Label     string `json:"name"`
	Comment   string `json:"comment"`
	Type      string `json:"type"`
	Offset    int    `json:"position"`
	Length    int    `json:"length"`
	Ignore    bool   `json:"-"`
	Endianess string `json:"-"`

	Raw interface{} `json:"-"`
}

func (i Item) Position() int {
	ix := i.Offset / 8
	return ix
}

func (i Item) Extract(b *buffer.Buffer) (Item, error) {
	var e binary.ByteOrder
	switch i.Endianess {
	case "big", "be", "":
		e = binary.BigEndian
	case "little", "le":
		e = binary.LittleEndian
	default:
		return i, fmt.Errorf("unsupported endianess %s", i.Endianess)
	}
	var err error

	v := i
	switch i.Type {
	case "bool":
		b, err := b.ReadUint8(v.Offset, v.Length, e)
		if b == 1 && err == nil {
			v.Raw = true
		}
	case "uchar":
		v.Raw, err = b.ReadUint8(v.Offset, v.Length, e)
	case "ushort", "":
		v.Raw, err = b.ReadUint16(v.Offset, v.Length, e)
	case "ulong":
		v.Raw, err = b.ReadUint32(v.Offset, v.Length, e)
	case "char":
		v.Raw, err = b.ReadInt8(v.Offset, v.Length, e)
	case "short":
		v.Raw, err = b.ReadInt16(v.Offset, v.Length, e)
	case "long":
		v.Raw, err = b.ReadInt32(v.Offset, v.Length, e)
	case "float":
		v.Raw, err = b.ReadFloat(v.Offset, e)
	default:
		return i, fmt.Errorf("unsupported type %s", i.Type)
	}
	return v, err
}

type Schema struct {
	Name    string   `json:"name"`
	Offset  int64    `json:"offset"`
	Sources []uint32 `json:"sources"`
	Apid    uint16   `json:"apid"`
	Items   []Item   `json:"parameters"`

	Lastmod time.Time `json:"lastmod"`
	Sum     string    `json:"md5sum"`
}

func (s Schema) Extract(p Telemetry) ([]Item, error) {
	sort.Slice(s.Sources, func(i, j int) bool { return s.Sources[i] < s.Sources[j] })
	ix := sort.Search(len(s.Sources), func(i int) bool {
		return s.Sources[i] >= p.ESAHeader.Sid
	})
	if ix >= len(s.Sources) || s.Sources[ix] != p.ESAHeader.Sid {
		return nil, ErrSkip
	}

	buf := buffer.NewBuffer(p.Payload())
	if err := buf.Discard(s.Offset / 8); err != nil {
		return nil, err
	}
	var is []Item
	for i := range s.Items {
		i, err := s.Items[i].Extract(buf)
		if err != nil {
			return nil, err
		}
		if !i.Ignore {
			is = append(is, i)
		}
	}
	return is, nil
}
