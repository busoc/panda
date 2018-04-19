package panda

import (
	"bufio"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

type Measurement struct {
	Name  string
	Value interface{}
}

type Item struct {
	Name      string
	Type      string
	Offset    int
	Length    int
	Repeat    int
	Ignore    bool
	Endianess binary.ByteOrder
}

func (i *Item) UnmarshalXML(d *xml.Decoder, s xml.StartElement) error {
	v := struct {
		Name      string `xml:"name,attr"`
		Type      string `xml:"type,attr"`
		Offset    uint   `xml:"offset,attr"`
		Length    uint   `xml:"length,attr"`
		Repeat    uint   `xml:"repeat,attr"`
		Endianess string `xml:"endianess,attr"`
	}{}
	if err := d.DecodeElement(&v, &s); err != nil {
		return err
	}
	if v.Repeat == 0 {
		v.Repeat++
	}
	i.Name, i.Type = v.Name, v.Type
	i.Offset, i.Length, i.Repeat = int(v.Offset), int(v.Length), int(v.Repeat)
	i.Ignore = v.Name == "" || v.Name == "spare"
	switch e := strings.ToLower(v.Endianess); e {
	default:
		return fmt.Errorf("endianess: invalid value %s", e)
	case "little", "le":
		i.Endianess = binary.LittleEndian
	case "big", "be", "":
		i.Endianess = binary.BigEndian
	}
	return nil
}

func (i *Item) Extract(r io.Reader) ([]Measurement, error) {
	ms := make([]Measurement, 0, i.Repeat)
	for j := 0; j < i.Repeat; j++ {
		var (
			err error
			v   interface{}
		)
		switch t := strings.ToLower(i.Type); t {
		default:
			return nil, fmt.Errorf("unsupported type %s", t)
		case "long":
			v, err = readSignedLong(r, i.Endianess)
		case "short":
			v, err = readSignedShort(r, i.Endianess)
		case "char":
			v, err = readSignedChar(r, i.Endianess)
		case "ulong":
			v, err = readUnsignedLong(r, i.Endianess)
		case "ushort":
			v, err = readUnsignedShort(r, i.Endianess)
		case "uchar":
			v, err = readUnsignedChar(r, i.Endianess)
		case "bytea", "string":
			v, err = readBytes(r, i.Length)
		}
		if err != nil {
			return nil, err
		}
		if i.Ignore {
			continue
		}
		m := Measurement{i.Name, v}
		ms = append(ms, m)
	}
	return ms, nil
}

type Layout struct {
	Name    string
	Offset  int
	Repeat  int
	items   []*Item
	layouts []*Layout
}

func (l *Layout) UnmarshalXML(d *xml.Decoder, s xml.StartElement) error {
	v := struct {
		Name    string    `xml:"id,attr"`
		Offset  uint      `xml:"offset,attr"`
		Repeat  uint      `xml:"repeat,attr"`
		Items   []*Item   `xml:"item"`
		Layouts []*Layout `xml:"layout"`
	}{}
	if err := d.DecodeElement(&v, &s); err != nil {
		return err
	}
	if m := v.Offset % 8; m != 0 {
		return fmt.Errorf("invalid offset provided")
	}
	if v.Repeat == 0 {
		v.Repeat++
	}
	l.Name = v.Name
	l.Offset, l.Repeat = int(v.Offset/8), int(v.Repeat)
	l.items = v.Items
	l.layouts = v.Layouts
	return nil
}

func (l *Layout) Extract(r io.Reader) ([]Measurement, error) {
	rs := bufio.NewReader(r)
	n, err := rs.Discard(l.Offset)
	if err != nil {
		return nil, err
	}
	if n < l.Offset {
		return nil, fmt.Errorf("too few bytes discarded (%d < %d)", n, l.Offset)
	}
	ms := make([]Measurement, 0)
	for j := 0; j < l.Repeat; j++ {
		for _, i := range l.items {
			vs, err := i.Extract(r)
			if err != nil {
				return nil, err
			}
			ms = append(ms, vs...)
		}
	}
	return ms, nil
}

func readUnsignedChar(r io.Reader, e binary.ByteOrder) (uint8, error) {
	var v uint8
	return v, binary.Read(r, e, &v)
}

func readUnsignedShort(r io.Reader, e binary.ByteOrder) (uint16, error) {
	var v uint16
	return v, binary.Read(r, e, &v)
}

func readUnsignedLong(r io.Reader, e binary.ByteOrder) (uint32, error) {
	var v uint32
	return v, binary.Read(r, e, &v)
}

func readSignedChar(r io.Reader, e binary.ByteOrder) (int8, error) {
	var v int8
	return v, binary.Read(r, e, &v)
}

func readSignedShort(r io.Reader, e binary.ByteOrder) (int16, error) {
	var v int16
	return v, binary.Read(r, e, &v)
}

func readSignedLong(r io.Reader, e binary.ByteOrder) (int32, error) {
	var v int32
	return v, binary.Read(r, e, &v)
}

func readFloat(r io.Reader, e binary.ByteOrder) (float32, error) {
	var v float32
	return v, binary.Read(r, e, &v)
}

func readBytes(r io.Reader, i int) ([]byte, error) {
	bs := make([]byte, i)
	n, err := r.Read(bs)
	return bs[:n], err
}
