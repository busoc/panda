package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/hadock"
	"github.com/midbel/toml"
)

const magic = 0x90

const metaLength = 74

type metadata struct {
	Source      uint8     `xml:"originator-id"`
	Sequence    uint32    `xml:"originator-seq-no"`
	Acquisition time.Time `xml:"acquisition-time"`
	Auxiliary   time.Time `xml:"auxiliary-time"`
	X           uint16    `xml:"source-x-size"`
	Y           uint16    `xml:"source-y-size"`
	Format      uint8     `xml:"format"`
	Drop        uint16    `xml:"fdrp"`
	OffsetX     uint16    `xml:"roi-x-offset"`
	SizeX       uint16    `xml:"roi-x-size"`
	OffsetY     uint16    `xml:"roi-y-offset"`
	SizeY       uint16    `xml:"roi-y-size"`
	ScaleX      uint16    `xml:"scale-x-size"`
	ScaleY      uint16    `xml:"scale-y-size"`
	Ratio       uint8     `xml:"scale-far"`
	UPI         string    `xml:"user-packet-info"`
}

type converter struct {
	dir    string
	origin string
	alt    bool
	granul int
}

func New(f string) (hadock.Module, error) {
	r, err := os.Open(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	c := struct {
		Origin  string `toml:"origin"`
		Datadir string `toml:"datadir"`
		Type    string `toml:"type"`
		Granul  int    `toml:"interval"`
	}{}
	if err := toml.NewDecoder(r).Decode(&c); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.Datadir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &converter{dir: c.Datadir, origin: c.Origin, granul: c.Granul}, nil
}

func (c *converter) Process(i uint8, p mud.HRPacket) error {
	if p.Version() != mud.VMUProtocol2 || p.Origin() != c.origin {
		return nil
	}
	dir, _ := joinPath(c.dir, p, i, c.granul, c.alt)
	if err := os.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	buf, dat, err := process(p)
	if err != nil {
		return err
	}
	e := ".csv"
	if !dat {
		e = ".ini"
	}

	n := p.Filename() + e
	if err := ioutil.WriteFile(path.Join(dir, n), buf.Bytes(), 0644); err != nil {
		return err
	}
	if dat {
		e := xml.NewEncoder(buf)
		e.Indent("", "\t")
		buf.Reset()
		if err := e.Encode(decodeMeta(p.Payload())); err != nil {
			return err
		}
		return ioutil.WriteFile(path.Join(dir, n+".xml"), buf.Bytes(), 0644)
	}
	return nil
}

func process(p mud.HRPacket) (*bytes.Buffer, bool, error) {
	buf := new(bytes.Buffer)

	bs := p.Payload()
	if bs[0] != magic {
		buf.Write(bs)
		return buf, false, nil
	}
	r := bytes.NewReader(bs[metaLength:])
	n, err := r.ReadByte()
	if err != nil {
		return nil, false, err
	}
	w := csv.NewWriter(buf)

	vs := make([]string, int(n)+1)
	vs[0] = "t"
	for i, j := int(n), 0; j < i; j++ {
		var v uint16
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, false, err
		}
		vs[j+1] = fmt.Sprintf("g2(t, %d)", v)
	}
	w.Write(vs)
	if err := w.Error(); err != nil {
		return nil, false, err
	}
	for i := 0; r.Len() > 0; i++ {
		vs[0] = strconv.Itoa(i)
		for j := 0; j < int(n); j++ {
			var v float32
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return nil, false, err
			}
			vs[j+1] = strconv.FormatFloat(float64(v), 'f', -1, 32)
		}
		w.Write(vs)
	}
	w.Flush()
	return buf, true, w.Error()
}

func decodeMeta(bs []byte) metadata {
	r := bytes.NewReader(bs[1:])

	var (
		m metadata
		d uint64
	)
	binary.Read(r, binary.LittleEndian, &d)
	m.Acquisition = mud.AdjustGenerationTime(int64(d)) // mud.GPS.Add(time.Duration(d))

	binary.Read(r, binary.LittleEndian, &m.Sequence)

	binary.Read(r, binary.LittleEndian, &d)
	m.Auxiliary = mud.AdjustGenerationTime(int64(d)) // mud.GPS.Add(time.Duration(d))

	binary.Read(r, binary.LittleEndian, &m.Source)
	binary.Read(r, binary.LittleEndian, &m.X)
	binary.Read(r, binary.LittleEndian, &m.Y)
	binary.Read(r, binary.LittleEndian, &m.Format)
	binary.Read(r, binary.LittleEndian, &m.Drop)
	binary.Read(r, binary.LittleEndian, &m.OffsetX)
	binary.Read(r, binary.LittleEndian, &m.SizeX)
	binary.Read(r, binary.LittleEndian, &m.OffsetY)
	binary.Read(r, binary.LittleEndian, &m.SizeY)
	binary.Read(r, binary.LittleEndian, &m.ScaleX)
	binary.Read(r, binary.LittleEndian, &m.ScaleY)
	binary.Read(r, binary.LittleEndian, &m.Ratio)

	upi := make([]byte, 32)
	io.ReadFull(r, upi)
	if bs := bytes.Trim(upi, "\x00"); len(bs) > 0 {
		m.UPI = string(bs)
	}

	return m
}

func joinPath(base string, v mud.HRPacket, i uint8, g int, a bool) (string, error) {
	switch i {
	case hadock.TEST:
		base = path.Join(base, "TEST")
	case hadock.SIM1, hadock.SIM2:
		base = path.Join(base, "SIM"+fmt.Sprint(i))
	case hadock.OPS:
		base = path.Join(base, "OPS")
	default:
		base = path.Join(base, "DATA")
	}
	var t time.Time
	switch v := v.(type) {
	case *mud.Table:
		base, t = path.Join(base, "sciences"), v.VMUHeader.Timestamp()
	case *mud.Image:
		base, t = path.Join(base, "images"), v.VMUHeader.Timestamp()
	}
	if v.IsRealtime() {
		base = path.Join(base, "realtime", v.Origin())
	} else {
		base = path.Join(base, "playback", v.Origin())
	}
	if t.IsZero() || a {
		t = v.Timestamp()
	}

	return joinPathTime(base, t, g), nil
}

func joinPathTime(base string, t time.Time, g int) string {
	t = mud.AdjustGenerationTime(t.Unix())
	y := fmt.Sprintf("%04d", t.Year())
	d := fmt.Sprintf("%03d", t.YearDay())
	h := fmt.Sprintf("%02d", t.Hour())
	base = path.Join(base, y, d, h)
	if m := t.Truncate(time.Second * time.Duration(g)); g > 0 {
		base = path.Join(base, fmt.Sprintf("%02d", m.Minute()))
	}
	return base
}
