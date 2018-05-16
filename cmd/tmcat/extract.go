package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/busoc/panda"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

var ErrInvalidPosition = errors.New("invalid position")

func runExtract(cmd *cli.Command, args []string) error {
	log.SetOutput(os.Stdout)

	zero := cmd.Flag.Bool("z", false, "")
	count := cmd.Flag.Uint("n", 0, "count")
	config := cmd.Flag.String("c", "", "config")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(*config)
	if err != nil {
		return err
	}
	var s Schema
	if err := toml.NewDecoder(f).Decode(&s); err != nil {
		return err
	}
	f.Close()

	queue, err := FetchPackets(cmd.Flag.Arg(0), 0, nil)
	if err != nil {
		return err
	}
	var n uint
	index := (panda.CCSDSLength / 2) + (panda.ESALength / 2) + (s.Offset / 16)
	if !*zero {
		index++
	}
	for p := range queue {
		if *count > 0 && n >= *count {
			break
		}
		vs, err := s.Extract(p)
		switch err {
		case nil:
			n++
		case panda.ErrSkip:
			continue
		default:
			return err
		}
		for _, v := range vs {
			if v.Length == 0 {
				v.Length = binary.Size(v.Raw) * 8
			}
			w := index + int64(v.Position()/2)
			result, err := v.Calibrate()
			if err != nil {
				return err
			}
			log.Printf("%3d | %2d | %2d | %-32s | %-6s | %16v | %16v", w, v.Length, v.Offset%16, v.Label, v.Type, v.Raw, result)
		}
		log.Println("===")
	}
	return nil
}

type Item struct {
	Label     string `toml:"name" json:"name"`
	Comment   string `toml:"comment" json:"comment"`
	Type      string `toml:"type" json:"type"`
	Offset    int    `toml:"position" json:"position"`
	Length    int    `toml:"length" json:"length"`
	Ignore    bool   `toml:"ignore" json:"-"`
	Endianess string `toml:"endianess" json:"-"`

	Method *Domain `toml:"calibration"`

	Raw interface{} `toml:"-" json:"-"`
}

func (i Item) Calibrate() (interface{}, error) {
	if i.Method == nil {
		return i.Raw, nil
	}
	return i.Method.Transform(i.Raw)
}

func (i Item) Position() int {
	ix := i.Offset / 8
	return ix
}

func (i Item) Extract(b *buffer) (Item, error) {
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
	Name    string   `toml:"name" json:"name"`
	Offset  int64    `toml:"offset" json:"offset"`
	Sources []uint32 `toml:"source" json:"sources"`
	Apid    uint16   `toml:"apid" json:"apid"`
	Items   []Item   `toml:"item" json:"parameters"`

	Lastmod time.Time `json:"lastmod"`
	Sum     string    `json:"md5sum"`
}

func (s Schema) Extract(p panda.Telemetry) ([]Item, error) {
	sort.Slice(s.Sources, func(i, j int) bool { return s.Sources[i] < s.Sources[j] })
	ix := sort.Search(len(s.Sources), func(i int) bool {
		return s.Sources[i] >= p.ESAHeader.Sid
	})
	if ix >= len(s.Sources) || s.Sources[ix] != p.ESAHeader.Sid {
		return nil, panda.ErrSkip
	}

	buf := NewBuffer(p.Payload())
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

type buffer struct {
	inner *bytes.Reader
	index int64
}

func NewBuffer(bs []byte) *buffer {
	return &buffer{inner: bytes.NewReader(bs)}
}

func (b *buffer) Discard(n int64) error {
	bs := make([]byte, b.inner.Size()-n)
	if _, err := b.inner.ReadAt(bs, n); err != nil {
		return err
	}
	b.inner.Reset(bs)
	return nil
}

func (b *buffer) ReadFloat(pos int, order binary.ByteOrder) (i float32, err error) {
	ix, _ := index(pos)

	var u uint32
	if err = b.readValue(&u, ix, binary.Size(u)*8, order); err != nil {
		return
	}
	i = math.Float32frombits(u)
	return
}

func (b *buffer) ReadInt32(pos, count int, order binary.ByteOrder) (i int32, err error) {
	ix, offset := index(pos)
	if err = b.readValue(&i, ix, count, order); err != nil {
		return
	}
	if count == 0 {
		return
	}
	delta := uint32(binary.Size(i)*8 - offset - count)
	mask := ((1 << uint32(count)) - 1) << delta
	i = (i & int32(mask)) >> delta

	return
}

func (b *buffer) ReadInt16(pos, count int, order binary.ByteOrder) (i int16, err error) {
	ix, offset := index(pos)
	if err = b.readValue(&i, ix, count, order); err != nil {
		return
	}
	if count == 0 {
		return
	}
	delta := uint16(binary.Size(i)*8 - offset - count)
	mask := ((1 << uint16(count)) - 1) << delta
	i = (i & int16(mask)) >> delta
	return
}

func (b *buffer) ReadInt8(pos, count int, order binary.ByteOrder) (i int8, err error) {
	ix, offset := index(pos)
	if err = b.readValue(&i, ix, count, order); err != nil {
		return
	}
	if count == 0 {
		return
	}
	delta := uint8(binary.Size(i)*8 - offset - count)
	mask := ((1 << uint8(count)) - 1) << delta
	i = (i & int8(mask)) >> delta

	return
}

func (b *buffer) ReadUint32(pos, count int, order binary.ByteOrder) (i uint32, err error) {
	ix, offset := index(pos)
	if err = b.readValue(&i, ix, count, order); err != nil {
		return
	}
	if count == 0 {
		return
	}
	delta := uint32(binary.Size(i)*8 - offset - count)
	mask := ((1 << uint32(count)) - 1) << delta
	i = (i & uint32(mask)) >> delta

	return
}

func (b *buffer) ReadUint16(pos, count int, order binary.ByteOrder) (i uint16, err error) {
	ix, offset := index(pos)
	if err = b.readValue(&i, ix, count, order); err != nil {
		return
	}
	if count == 0 {
		return
	}
	delta := uint16(binary.Size(i)*8 - offset - count)
	mask := ((1 << uint16(count)) - 1) << delta
	i = (i & uint16(mask)) >> delta
	return
}

func (b *buffer) ReadUint8(pos, count int, order binary.ByteOrder) (i uint8, err error) {
	ix, offset := index(pos)
	if err = b.readValue(&i, ix, count, order); err != nil {
		return
	}
	if count == 0 {
		return
	}
	delta := uint8(binary.Size(i)*8 - offset - count)
	mask := ((1 << uint8(count)) - 1) << delta
	i = (i & uint8(mask)) >> delta

	return
}

func (b *buffer) readValue(i interface{}, x, n int, e binary.ByteOrder) error {
	if int64(x) >= b.inner.Size() {
		return ErrInvalidPosition
	}
	if s := binary.Size(i) * 8; s <= 0 || n > s {
		return fmt.Errorf("unsupported type %T", i)
	}
	if _, err := b.inner.Seek(int64(x), io.SeekStart); err != nil {
		return err
	}
	return binary.Read(b.inner, e, i)
}

type Transformer interface {
	Transform(interface{}) (interface{}, error)
}

type Pair struct {
	X int64
	Y float64
}

type Polynomial []Pair

func (p Polynomial) Transform(r interface{}) (interface{}, error) {
	v, err := toFloat(r)
	if err != nil {
		return r, err
	}
	var t float64
	for i := range p {
		t += math.Pow(v, float64(p[i].X)) * p[i].Y
	}
	return t, nil
}

type PointPair []Pair

func (p PointPair) Transform(r interface{}) (interface{}, error) {
	return 0.0, nil
}

type Enum struct {
	X int64
	Y string
}

type Enumeration []Enum

func (e Enumeration) Transform(r interface{}) (interface{}, error) {
	v, err := toInt(r)
	if err != nil {
		return r, err
	}
	for i := range e {
		if e[i].X == v {
			return e[i].Y, nil
		}
	}
	return "***", fmt.Errorf("undefined value %v", r)
}

func toInt(v interface{}) (int64, error) {
  var (
    i int64
    err error
  )
  e := reflect.ValueOf(v)
  switch k := e.Kind(); {
  case isInt(k):
    i = e.Int()
  case isUint(k):
    i = int64(e.Uint())
  case isFloat(k):
    i = int64(e.Float())
  default:
    err = fmt.Errorf("\"%v\" can not be converted to int64 (%s)", v, k)
  }
  return i, err
}

func toFloat(v interface{}) (float64, error) {
  var (
    i float64
    err error
  )
  e := reflect.ValueOf(v)
  switch k := e.Kind(); {
  case isInt(k):
    i = float64(e.Int())
  case isUint(k):
    i = float64(e.Uint())
  case isFloat(k):
    i = e.Float()
  default:
    err = fmt.Errorf("\"%v\" can not be converted to int64 (%s)", v, k)
  }
  return i, err
}

func isInt(k reflect.Kind) bool {
	return k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64
}

func isUint(k reflect.Kind) bool {
	return k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64
}

func isFloat(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}

type Method string

type Set struct {
	method    *Method
	Transform Transformer
}

func (s *Set) UnmarshalOption(d *toml.Decoder) error {
	switch m := string(*s.method); m {
	case "enum", "enumeration":
		vs := make([]Enum, 0)
		if err := d.DecodeElement(&vs); err != nil {
			return err
		}
		sort.Slice(vs, func(i, j int) bool {
			return vs[i].X < vs[j].X
		})
		s.Transform = Enumeration(vs)
	case "poly", "polynomial":
		vs := make([]Pair, 0)
		if err := d.DecodeElement(&vs); err != nil {
			return err
		}
		sort.Slice(vs, func(i, j int) bool {
			return vs[i].X < vs[j].X
		})
		s.Transform = Polynomial(vs)
	case "pair", "pointpair":
		vs := make([]Pair, 0)
		if err := d.DecodeElement(&vs); err != nil {
			return err
		}
		sort.Slice(vs, func(i, j int) bool {
			return vs[i].X < vs[j].X
		})
		s.Transform = PointPair(vs)
	default:
		return fmt.Errorf("unsupported calibration method")
	}
	return nil
}

func (m *Method) UnmarshalOption(d *toml.Decoder) error {
	var v string
	if err := d.DecodeElement(&v); err != nil {
		return err
	}
	*m = Method(v)
	return nil
}

type Domain struct {
	Transformer
}

func (t *Domain) UnmarshalTOML(d *toml.Decoder) error {
	var meth Method
	v := struct {
		Option *Method `toml:"method"`
		Domain *Set    `toml:"domain"`
	}{
		Option: &meth,
		Domain: &Set{method: &meth},
	}
	if err := d.DecodeElement(&v); err != nil {
		return err
	}
	t.Transformer = v.Domain.Transform
	return nil
}

func index(n int) (int, int) {
	return n / 8, n % 8
}
