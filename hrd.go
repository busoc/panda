package panda

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"strings"
	"time"

	img "github.com/busoc/panda/internal/image"
	"github.com/busoc/panda/internal/science"
)

type Exporter interface {
	Filename() string
	Export(io.Writer, string) error
	ExportRaw(io.Writer) error
}

type HRPacket interface {
	Packet
	Exporter
	Stream() Channel
	Origin() string
	Format() string
	Sequence() uint32
	Version() int
	IsRealtime() bool
}

type Four interface {
	FCC() uint32
}

type Bitmap interface {
	Four
	X() uint16
	Y() uint16
}

const (
	HRDPHeaderLength = 14
	HRDLSyncLength   = 8
	VMUHeaderLength  = 16
	IDHeaderLengthV1 = 72
	IDHeaderLengthV2 = 76
	SDHeaderLengthV2 = 56
	SDHeaderLengthV1 = 8
)

const UPILen = 32

const (
	VMUProtocol1 = iota + 1
	VMUProtocol2
)

var (
	MMA  = []byte("MMA ")
	CORR = []byte("CORR")
	SYNC = []byte("SYNC")
	RAW  = []byte("RAW ")
	Y800 = []byte("Y800")
	Y16B = []byte("Y16 ")
	Y16L = []byte("Y16L")
	I420 = []byte("I420")
	YUY2 = []byte("YUY2")
	RGB  = []byte("RGB ")
	JPEG = []byte("JPEG")
	PNG  = []byte("PNG ")
	H264 = []byte("H264")
	SVS  = []byte("SVS ")
	TIFF = []byte("TIFF")
)

const (
	RUBUnit uint8 = 0x36
	Alv1          = 0x39
	Alv2          = 0x40
	SMDUnit       = 0x41
	LRSD          = 0x51
	LCP           = 0x90
)

func Valid(p HRPacket) bool {
	var sum uint32
	switch p := p.(type) {
	case *Table:
		sum = p.Sum
	case *Image:
		sum = p.Sum
	default:
		return false
	}
	bs, err := p.Bytes()
	if err != nil {
		return false
	}
	var cmp uint32
	for i := range bs[:len(bs)-binary.Size(cmp)] {
		cmp += uint32(bs[i])
	}
	return cmp == sum
}

func DecodeHR(v int) (Decoder, error) {
	switch v {
	default:
		return nil, fmt.Errorf("unsupported vmu protocol version: %d", v)
	case VMUProtocol1:
		return DecoderFunc(decodeVMUv1), nil
	case VMUProtocol2:
		return DecoderFunc(decodeVMUv2), nil
	}
}

func decodeVMUv2(bs []byte) (int, Packet, error) {
	if len(bs) <= VMUHeaderLength+IDHeaderLengthV2 || len(bs) <= VMUHeaderLength+SDHeaderLengthV2 {
		idh := VMUHeaderLength + IDHeaderLengthV2
		sdh := VMUHeaderLength + SDHeaderLengthV2
		return len(bs), nil, fmt.Errorf("packet size to short: %d (sciences: %d bytes, images: %d bytes)", len(bs), sdh, idh)
	}
	ix := VMUHeaderLength
	v, err := decodeVMU(bs[:ix])
	if err != nil {
		return len(bs), nil, err
	}

	var p Packet
	switch v.Channel {
	default:
		return len(bs), nil, fmt.Errorf("unknown channel %d", v.Channel)
	case Video1, Video2:
		h, err := decodeIDHv2(bs[ix : ix+IDHeaderLengthV2])
		if err != nil {
			return len(bs), nil, err
		}
		ix += IDHeaderLengthV2
		vs := make([]byte, len(bs)-ix-4)
		copy(vs, bs[ix:len(bs)-4])
		p = &Image{
			VMUHeader: &v,
			IDH:       &h,
			Data:      vs,
			Sum:       binary.LittleEndian.Uint32(bs[len(bs)-4:]),
		}
	case Science:
		h, err := decodeSDHv2(bs[ix : ix+SDHeaderLengthV2])
		if err != nil {
			return len(bs), nil, err
		}
		ix += SDHeaderLengthV2
		vs := make([]byte, len(bs)-ix-4)
		copy(vs, bs[ix:len(bs)-4])
		p = &Table{
			VMUHeader: &v,
			SDH:       &h,
			Data:      vs,
			Sum:       binary.LittleEndian.Uint32(bs[len(bs)-4:]),
		}
	}
	return len(bs), p, nil
}

func decodeVMUv1(bs []byte) (int, Packet, error) {
	ix := VMUHeaderLength
	v, err := decodeVMU(bs[:ix])
	if err != nil {
		return len(bs), nil, err
	}

	var p Packet
	switch v.Channel {
	default:
		return len(bs), nil, fmt.Errorf("unknown channel %d", v.Channel)
	case Video1, Video2:
		h, err := decodeIDHv1(bs[ix : ix+IDHeaderLengthV1])
		if err != nil {
			return len(bs), nil, err
		}
		ix += IDHeaderLengthV1
		vs := make([]byte, len(bs)-ix-4)
		copy(vs, bs[ix:len(bs)-4])
		p = &Image{VMUHeader: &v, IDH: &h, Data: vs}
	case Science:
		h, err := decodeSDHv1(bs[ix : ix+SDHeaderLengthV1])
		if err != nil {
			return len(bs), nil, err
		}
		ix += SDHeaderLengthV1
		vs := make([]byte, len(bs)-ix-4)
		copy(vs, bs[ix:len(bs)-4])
		p = &Table{VMUHeader: &v, SDH: &h, Data: vs}
	}
	return len(bs), p, nil
}

type Channel uint8

const (
	Video1 Channel = iota + 1
	Video2
	Science
)

func (c Channel) String() string {
	switch c {
	default:
		return "***"
	case Video1:
		return "vic1"
	case Video2:
		return "vic2"
	case Science:
		return "lrsd"
	}
}

type VMUHeader struct {
	Channel  Channel
	Source   uint8
	Sequence uint32
	Coarse   uint32
	Fine     uint16
}

func (v *VMUHeader) Stream() Channel {
	return v.Channel
}

func (v *VMUHeader) Timestamp() time.Time {
	ms := time.Duration(v.Fine) * time.Millisecond
	t := time.Unix(int64(v.Coarse), 0).Add(ms)

	// return t.Add(epoch).UTC()
	return t.UTC()
}

func (v *VMUHeader) Generated() time.Time {
	return v.Timestamp()
}

func decodeVMU(bs []byte) (VMUHeader, error) {
	var v VMUHeader

	r := bytes.NewReader(bs)
	binary.Read(r, binary.LittleEndian, &v.Channel)
	binary.Read(r, binary.LittleEndian, &v.Source)
	binary.Read(r, binary.LittleEndian, new(uint16))
	binary.Read(r, binary.LittleEndian, &v.Sequence)
	binary.Read(r, binary.LittleEndian, &v.Coarse)
	binary.Read(r, binary.LittleEndian, &v.Fine)
	binary.Read(r, binary.LittleEndian, new(uint16))

	return v, nil
}

func encodeVMU(v VMUHeader) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.LittleEndian, v.Channel)
	binary.Write(w, binary.LittleEndian, v.Source)
	binary.Write(w, binary.LittleEndian, uint16(0))
	binary.Write(w, binary.LittleEndian, v.Sequence)
	binary.Write(w, binary.LittleEndian, v.Coarse)
	binary.Write(w, binary.LittleEndian, v.Fine)
	binary.Write(w, binary.LittleEndian, uint16(0))

	return w.Bytes(), nil
}

type SDHv1 struct {
	Sequence uint32
}

func (s SDHv1) Format() string {
	return Science.String()
}

func decodeSDHv1(bs []byte) (SDHv1, error) {
	var s SDHv1
	r := bytes.NewReader(bs)
	binary.Read(r, binary.LittleEndian, &s.Sequence)
	binary.Read(r, binary.LittleEndian, new(uint32))
	return s, nil
}

func encodeSDHv1(s SDHv1) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.LittleEndian, s.Sequence)
	binary.Write(w, binary.LittleEndian, uint32(0))

	return w.Bytes(), nil
}

func (s SDHv1) FCC() uint32 {
	return binary.BigEndian.Uint32(MMA)
}

type IDHv1 struct {
	XMLName xml.Name `xml:"metadata"`

	Sequence  uint32
	Coarse    uint32
	Fine      uint16
	Part      uint8
	Video     uint8
	Type      uint8
	Rate      float32
	Pixels    uint32
	Region    uint64
	LineDrop  uint8
	FrameDrop uint16
	Info      [32]byte
}

func (i *IDHv1) Format() string {
	switch i.Type {
	default:
		return "raw"
	case 1:
		return "jpg"
	case 2:
		return "gray"
	case 3:
		return "ycbcr"
	case 4:
		return "rgb"
	case 5:
		return "tiff"
	}
}

func (i *IDHv1) FCC() uint32 {
	var v uint32
	switch i.Type {
	default:
		v = binary.BigEndian.Uint32(RAW)
	case 1:
		v = binary.BigEndian.Uint32(JPEG)
	case 2:
		v = binary.BigEndian.Uint32(Y800)
	case 3:
		v = binary.BigEndian.Uint32(YUY2)
	case 4:
		v = binary.BigEndian.Uint32(RGB)
	}
	return v
}

func (i *IDHv1) X() uint16 {
	return uint16(i.Pixels & 0xFFFF)
}

func (i *IDHv1) Y() uint16 {
	return uint16(i.Pixels >> 16)
}

func (i *IDHv1) MarshalXML(e *xml.Encoder, s xml.StartElement) error {
	xy := struct {
		X uint16 `xml:"x"`
		Y uint16 `xml:"y"`
	}{i.X(), i.Y()}

	rx, ry := uint32(i.Region>>32), uint32(i.Region&0xFFFFFFFF)
	rs := struct {
		OffsetX uint16 `xml:"offset-x"`
		SizeX   uint16 `xml:"size-x"`
		OffsetY uint16 `xml:"offset-y"`
		SizeY   uint16 `xml:"size-y"`
	}{uint16(rx >> 16), uint16(rx & 0xFFFF), uint16(ry >> 16), uint16(ry & 0xFFFF)}

	ds := struct {
		Line  uint8  `xml:"line-drop"`
		Frame uint16 `xml:"frame-drop"`
	}{i.LineDrop, i.FrameDrop}

	e.EncodeElement(i.Sequence, xml.StartElement{Name: xml.Name{Local: "sequence"}})
	e.EncodeElement(i.Timestamp(), xml.StartElement{Name: xml.Name{Local: "timestamp"}})
	e.EncodeElement(i.Part, xml.StartElement{Name: xml.Name{Local: "portion"}})
	e.EncodeElement(i.Video, xml.StartElement{Name: xml.Name{Local: "video"}})
	e.EncodeElement(i.Type, xml.StartElement{Name: xml.Name{Local: "type"}})
	e.EncodeElement(i.Rate, xml.StartElement{Name: xml.Name{Local: "rate"}})
	e.EncodeElement(xy, xml.StartElement{Name: xml.Name{Local: "pixels"}})
	e.EncodeElement(rs, xml.StartElement{Name: xml.Name{Local: "region"}})
	e.EncodeElement(ds, xml.StartElement{Name: xml.Name{Local: "drop"}})
	if bs := bytes.Trim(i.Info[:], "\x00"); len(bs) > 0 {
		e.EncodeElement(string(bs), xml.StartElement{Name: xml.Name{Local: "info"}})
	}

	return nil
}

func (i *IDHv1) Timestamp() time.Time {
	ms := time.Duration(i.Fine) * time.Millisecond
	t := time.Unix(int64(i.Coarse), 0).Add(ms)

	// return t.Add(epoch).UTC()
	return t.UTC()
}

func decodeIDHv1(bs []byte) (IDHv1, error) {
	var i IDHv1

	r := bytes.NewReader(bs)
	binary.Read(r, binary.LittleEndian, &i.Sequence)
	binary.Read(r, binary.LittleEndian, &i.Coarse)
	binary.Read(r, binary.LittleEndian, &i.Fine)
	binary.Read(r, binary.LittleEndian, &i.Part)
	binary.Read(r, binary.LittleEndian, &i.Video)
	binary.Read(r, binary.LittleEndian, &i.Type)
	binary.Read(r, binary.LittleEndian, &i.Rate)
	binary.Read(r, binary.LittleEndian, &i.Pixels)
	binary.Read(r, binary.LittleEndian, &i.Region)
	binary.Read(r, binary.LittleEndian, &i.LineDrop)
	binary.Read(r, binary.LittleEndian, &i.FrameDrop)
	binary.Read(r, binary.LittleEndian, new(uint64))
	r.Read(i.Info[:])

	return i, nil
}

func encodeIDHv1(i IDHv1) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.LittleEndian, i.Sequence)
	binary.Write(w, binary.LittleEndian, i.Coarse)
	binary.Write(w, binary.LittleEndian, i.Fine)
	binary.Write(w, binary.LittleEndian, i.Part)
	binary.Write(w, binary.LittleEndian, i.Video)
	binary.Write(w, binary.LittleEndian, i.Type)
	binary.Write(w, binary.LittleEndian, i.Rate)
	binary.Write(w, binary.LittleEndian, i.Pixels)
	binary.Write(w, binary.LittleEndian, i.Region)
	binary.Write(w, binary.LittleEndian, i.LineDrop)
	binary.Write(w, binary.LittleEndian, i.FrameDrop)
	binary.Write(w, binary.LittleEndian, uint64(0))
	w.Write(i.Info[:])

	return w.Bytes(), nil
}

type SDHv2 struct {
	Properties  uint8
	Sequence    uint16
	Originator  uint32
	Acquisition time.Duration
	Auxiliary   time.Duration
	Id          uint8
	Info        [32]byte
}

func (s SDHv2) Format() string {
	switch s.Id {
	default:
		return "raw"
	case Alv1, Alv2:
		return "corr"
	case LRSD:
		return "mma"
	case RUBUnit, SMDUnit:
		return "sync"
	}
}

func (s SDHv2) FCC() uint32 {
	switch s.Id {
	default:
		return binary.BigEndian.Uint32(RAW)
	case LRSD:
		return binary.BigEndian.Uint32(MMA)
	case Alv1, Alv2:
		return binary.BigEndian.Uint32(CORR)
	case SMDUnit, RUBUnit:
		return binary.BigEndian.Uint32(SYNC)
	case LCP:
		return binary.BigEndian.Uint32(SVS)
	}
}

func decodeSDHv2(bs []byte) (SDHv2, error) {
	var s SDHv2

	r := bytes.NewReader(bs)
	binary.Read(r, binary.LittleEndian, &s.Properties)
	binary.Read(r, binary.LittleEndian, &s.Sequence)
	binary.Read(r, binary.LittleEndian, &s.Originator)
	binary.Read(r, binary.LittleEndian, &s.Acquisition)
	binary.Read(r, binary.LittleEndian, &s.Auxiliary)
	binary.Read(r, binary.LittleEndian, &s.Id)
	r.Read(s.Info[:])

	return s, nil
}

func encodeSDHv2(s SDHv2) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.LittleEndian, s.Properties)
	binary.Write(w, binary.LittleEndian, s.Sequence)
	binary.Write(w, binary.LittleEndian, s.Originator)
	binary.Write(w, binary.LittleEndian, s.Acquisition)
	binary.Write(w, binary.LittleEndian, s.Auxiliary)
	binary.Write(w, binary.LittleEndian, s.Id)
	w.Write(s.Info[:])

	return w.Bytes(), nil
}

func (s SDHv2) Acquire() (time.Time, time.Time) {
	return GPS.Add(s.Auxiliary).UTC(), s.Timestamp()
}

func (s SDHv2) Timestamp() time.Time {
	return GPS.Add(s.Acquisition).UTC()
}

type IDHv2 struct {
	XMLName     xml.Name `xml:"metadata"`
	Properties  uint8
	Sequence    uint16
	Originator  uint32
	Acquisition time.Duration
	Auxiliary   time.Duration
	Id          uint8
	Type        uint8
	Pixels      uint32
	Region      uint64
	Dropping    uint16
	Scaling     uint32
	Ratio       uint8
	Info        [32]byte
}

func decodeIDHv2(bs []byte) (IDHv2, error) {
	var i IDHv2

	r := bytes.NewReader(bs)
	binary.Read(r, binary.LittleEndian, &i.Properties)
	binary.Read(r, binary.LittleEndian, &i.Sequence)
	binary.Read(r, binary.LittleEndian, &i.Originator)
	binary.Read(r, binary.LittleEndian, &i.Acquisition)
	binary.Read(r, binary.LittleEndian, &i.Auxiliary)
	binary.Read(r, binary.LittleEndian, &i.Id)
	binary.Read(r, binary.LittleEndian, &i.Type)
	binary.Read(r, binary.LittleEndian, &i.Pixels)
	binary.Read(r, binary.LittleEndian, &i.Region)
	binary.Read(r, binary.LittleEndian, &i.Dropping)
	binary.Read(r, binary.LittleEndian, &i.Scaling)
	binary.Read(r, binary.LittleEndian, &i.Ratio)
	r.Read(i.Info[:])

	return i, nil
}

func encodeIDHv2(i IDHv2) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.LittleEndian, i.Properties)
	binary.Write(w, binary.LittleEndian, i.Sequence)
	binary.Write(w, binary.LittleEndian, i.Originator)
	binary.Write(w, binary.LittleEndian, i.Acquisition)
	binary.Write(w, binary.LittleEndian, i.Auxiliary)
	binary.Write(w, binary.LittleEndian, i.Id)
	binary.Write(w, binary.LittleEndian, i.Type)
	binary.Write(w, binary.LittleEndian, i.Pixels)
	binary.Write(w, binary.LittleEndian, i.Region)
	binary.Write(w, binary.LittleEndian, i.Dropping)
	binary.Write(w, binary.LittleEndian, i.Scaling)
	binary.Write(w, binary.LittleEndian, i.Ratio)
	w.Write(i.Info[:])

	return w.Bytes(), nil
}

func (i *IDHv2) X() uint16 {
	return uint16(i.Pixels & 0xFFFF)
}

func (i *IDHv2) Y() uint16 {
	return uint16(i.Pixels >> 16)
}

func (i *IDHv2) MarshalXML(e *xml.Encoder, s xml.StartElement) error {
	sp := struct {
		Id   uint8 `xml:"properties"`
		Type uint8 `xml:"type"`
	}{i.Properties & 0x0F, i.Properties >> 4}
	qs := struct {
		Stream     uint16 `xml:"stream"`
		Originator uint32 `xml:"originator"`
	}{i.Sequence, i.Originator}
	xy := struct {
		X uint16 `xml:"x"`
		Y uint16 `xml:"y"`
	}{i.X(), i.Y()}

	rx, ry := uint32(i.Region&0xFFFFFFFF), uint32(i.Region>>32)
	rs := struct {
		OffsetX uint16 `xml:"offset-x"`
		SizeX   uint16 `xml:"size-x"`
		OffsetY uint16 `xml:"offset-y"`
		SizeY   uint16 `xml:"size-y"`
	}{uint16(rx & 0xFFFF), uint16(rx >> 16), uint16(ry & 0xFFFF), uint16(ry >> 16)}

	cs := struct {
		X     uint32 `xml:"size-x"`
		Y     uint32 `xml:"size-y"`
		Ratio uint8  `xml:"force-aspect-ratio"`
	}{X: i.Scaling & 0x0000FFFF, Y: i.Scaling >> 16, Ratio: i.Ratio}

	// x, a := i.Acquire()

	e.EncodeElement(sp, xml.StartElement{Name: xml.Name{Local: "stream"}})
	e.EncodeElement(qs, xml.StartElement{Name: xml.Name{Local: "sequences"}})
	e.EncodeElement(i.Timestamp(), xml.StartElement{Name: xml.Name{Local: "timestamp"}})
	e.EncodeElement(i.Auxiliary.Nanoseconds(), xml.StartElement{Name: xml.Name{Local: "auxiliary"}})
	e.EncodeElement(i.Type, xml.StartElement{Name: xml.Name{Local: "type"}})
	e.EncodeElement(xy, xml.StartElement{Name: xml.Name{Local: "pixels"}})
	e.EncodeElement(rs, xml.StartElement{Name: xml.Name{Local: "region"}})
	e.EncodeElement(i.Dropping, xml.StartElement{Name: xml.Name{Local: "dropping"}})
	e.EncodeElement(cs, xml.StartElement{Name: xml.Name{Local: "scaling"}})
	if bs := bytes.Trim(i.Info[:], "\x00"); len(bs) > 0 {
		e.EncodeElement(string(bs), xml.StartElement{Name: xml.Name{Local: "info"}})
	}

	return nil
}

func (i IDHv2) Format() string {
	switch i.Type {
	default:
		return "raw"
	case 1:
		return "gray"
	case 2:
		return "gray16be"
	case 3:
		return "gray16le"
	case 4:
		return "yuy2"
	case 5:
		return "i420"
	case 6:
		return "rgb"
	case 7:
		return "jpg"
	case 8:
		return "png"
	case 9:
		return "h264"
	}
}

func (i *IDHv2) FCC() uint32 {
	var v uint32
	switch i.Type {
	default:
		v = binary.BigEndian.Uint32(RAW)
	case 1:
		v = binary.BigEndian.Uint32(Y800)
	case 2:
		v = binary.BigEndian.Uint32(Y16B)
	case 3:
		v = binary.BigEndian.Uint32(Y16L)
	case 4:
		v = binary.BigEndian.Uint32(YUY2)
	case 5:
		v = binary.BigEndian.Uint32(I420)
	case 6:
		v = binary.BigEndian.Uint32(RGB)
	case 7:
		v = binary.BigEndian.Uint32(JPEG)
	case 8:
		v = binary.BigEndian.Uint32(PNG)
	case 9:
		v = binary.BigEndian.Uint32(H264)
	}
	return v
}

func (i IDHv2) Acquire() (time.Time, time.Time) {
	return GPS.Add(i.Auxiliary).UTC(), i.Timestamp()
}

func (i IDHv2) Timestamp() time.Time {
	return GPS.Add(i.Acquisition).UTC()
}

type Table struct {
	*VMUHeader
	SDH  interface{}
	Data []byte
	Sum  uint32
}

func (t *Table) Format() string {
	switch v := t.SDH.(type) {
	case *SDHv1:
		return v.Format()
	case *SDHv2:
		return v.Format()
	default:
		return t.Channel.String()
	}
}

func (t *Table) Sequence() uint32 {
	switch v := t.SDH.(type) {
	default:
		return t.VMUHeader.Sequence
	case *SDHv1:
		return v.Sequence
	case *SDHv2:
		return v.Originator
	}
}

func (t *Table) Origin() string {
	var id int
	switch v := t.SDH.(type) {
	default:
		id = int(t.VMUHeader.Source)
	case *SDHv1:
		id = int(t.VMUHeader.Source)
	case *SDHv2:
		id = int(v.Id)
	}
	return fmt.Sprintf("%02x", id)
}

func (t *Table) IsRealtime() bool {
	switch s := t.SDH.(type) {
	default:
		return true
	case *SDHv1:
		return true
	case *SDHv2:
		return t.VMUHeader.Source == s.Id
	}
}

func (t *Table) Version() int {
	switch t.SDH.(type) {
	default:
		return VMUProtocol1
	case *SDHv1:
		return VMUProtocol1
	case *SDHv2:
		return VMUProtocol2
	}
}

func (t *Table) Filename() string {
	var (
		id, seq int
		delta   time.Duration
	)

	upi := "SCIENCE"
	switch v := t.SDH.(type) {
	default:
		id = int(t.VMUHeader.Channel)
	case *SDHv1:
		id, seq = int(t.VMUHeader.Channel), int(v.Sequence)
		delta = time.Second
	case *SDHv2:
		id, seq = int(v.Id), int(v.Originator)
		if bs := bytes.Trim(v.Info[:], "\x00"); len(bs) > 0 {
			upi = strings.Replace(string(bs), " ", "-", -1)
		}
		delta = AdjustTime(t.VMUHeader.Timestamp(), false).Sub(v.Timestamp())
	}
	offset := int64(delta.Minutes())
	n := t.Timestamp().Format("20060102_150405")

	ext := "dat"
	if !Valid(t) {
		ext += ".bad"
	}
	return fmt.Sprintf("%04x_%s_%d_%06d_%s_%09d.%s", id, upi, t.Stream(), seq, n, offset, ext)
}

func (t *Table) Timestamp() time.Time {
	switch v := t.SDH.(type) {
	default:
		return t.VMUHeader.Timestamp()
	case *SDHv1:
		return t.VMUHeader.Timestamp()
	case *SDHv2:
		return v.Timestamp()
	}
}

func (t *Table) Payload() []byte {
	bs := make([]byte, len(t.Data))
	copy(bs, t.Data)
	return bs
}

func (t *Table) Bytes() ([]byte, error) {
	var vs []byte
	w := new(bytes.Buffer)
	vs, _ = encodeVMU(*t.VMUHeader)
	w.Write(vs)
	switch s := t.SDH.(type) {
	default:
		return nil, fmt.Errorf("unsupported science header type")
	case *SDHv1:
		vs, _ = encodeSDHv1(*s)
	case *SDHv2:
		vs, _ = encodeSDHv2(*s)
	}
	w.Write(vs)
	w.Write(t.Data)
	binary.Write(w, binary.LittleEndian, t.Sum)

	return w.Bytes(), nil
}

func (t *Table) Export(w io.Writer, _ string) error {
	n := t.Timestamp()
	switch s := t.SDH.(type) {
	case *SDHv1:
		return science.ExportScienceData(w, t.Data, n)
	case *SDHv2:
		switch s.Id {
		case LCP:
			return science.ExportSVSData(w, t.Data)
		case LRSD:
			return science.ExportScienceData(w, t.Data, n)
		case SMDUnit, RUBUnit:
			return science.ExportSyncUnit(w, t.Data, n)
		default:
			return t.ExportRaw(w)
		}
	default:
		return t.ExportRaw(w)
	}
}

func (t *Table) ExportRaw(w io.Writer) error {
	_, err := w.Write(t.Data)
	return err
}

type Image struct {
	*VMUHeader
	IDH  interface{}
	Data []byte
	Sum  uint32
}

func (i *Image) Format() string {
	switch v := i.IDH.(type) {
	case *IDHv1:
		return v.Format()
	case *IDHv2:
		return v.Format()
	default:
		return i.Channel.String()
	}
}

func (i *Image) Sequence() uint32 {
	switch v := i.IDH.(type) {
	default:
		return i.VMUHeader.Sequence
	case *IDHv1:
		return v.Sequence
	case *IDHv2:
		return v.Originator
	}
}

func (i *Image) Origin() string {
	var id int
	switch v := i.IDH.(type) {
	default:
		id = int(i.VMUHeader.Source)
	case *IDHv1:
		id = int(v.Video)
	case *IDHv2:
		id = int(v.Id)
	}
	return fmt.Sprintf("%02x", id)
}

func (i *Image) IsRealtime() bool {
	switch v := i.IDH.(type) {
	default:
		return true
	case *IDHv1:
		return i.VMUHeader.Source == v.Video
	case *IDHv2:
		return i.VMUHeader.Source == v.Id
	}
}

func (i *Image) Version() int {
	switch i.IDH.(type) {
	default:
		return VMUProtocol1
	case *IDHv1:
		return VMUProtocol1
	case *IDHv2:
		return VMUProtocol2
	}
}

func (i *Image) Filename() string {
	var (
		id, seq int
		ext     string
		delta   time.Duration
	)
	upi := "IMG"
	switch v := i.IDH.(type) {
	default:
		id, ext = int(i.VMUHeader.Channel), "raw"
	case *IDHv1:
		id, ext, seq = int(i.VMUHeader.Channel), v.Format(), int(v.Sequence)
		if bs := bytes.Trim(v.Info[:], "\x00"); len(bs) > 0 {
			upi = strings.Replace(string(bs), " ", "-", -1)
		}
		delta = AdjustTime(i.VMUHeader.Timestamp(), false).Sub(v.Timestamp())
	case *IDHv2:
		id, ext, seq = int(v.Id), v.Format(), int(v.Originator)
		if bs := bytes.Trim(v.Info[:], "\x00"); len(bs) > 0 {
			upi = strings.Replace(string(bs), " ", "-", -1)
		}
		delta = AdjustTime(i.VMUHeader.Timestamp(), false).Sub(v.Timestamp())
	}
	n := i.Timestamp().Format("20060102_150405")
	offset := int64(delta.Minutes())
	if !Valid(i) {
		ext += ".bad"
	}
	return fmt.Sprintf("%04x_%s_%d_%06d_%s_%09d.%s", id, upi, i.Stream(), seq, n, offset, ext)
}

func (i *Image) Timestamp() time.Time {
	switch v := i.IDH.(type) {
	default:
		return i.VMUHeader.Timestamp()
	case *IDHv1:
		return v.Timestamp()
	case *IDHv2:
		return v.Timestamp()
	}
}

func (i *Image) Bytes() ([]byte, error) {
	var vs []byte
	w := new(bytes.Buffer)
	vs, _ = encodeVMU(*i.VMUHeader)
	w.Write(vs)
	switch i := i.IDH.(type) {
	default:
		return nil, fmt.Errorf("unsupported image header type")
	case *IDHv1:
		vs, _ = encodeIDHv1(*i)
	case *IDHv2:
		vs, _ = encodeIDHv2(*i)
	}
	w.Write(vs)
	w.Write(i.Data)
	binary.Write(w, binary.LittleEndian, i.Sum)

	return w.Bytes(), nil
}

func (i *Image) Payload() []byte {
	bs := make([]byte, len(i.Data))
	copy(bs, i.Data)
	return bs
}

func (i *Image) Export(w io.Writer, f string) error {
	switch v := i.IDH.(type) {
	default:
		return fmt.Errorf("unknown version")
	case *IDHv1:
		x := &xy{
			X:    int(v.Pixels & 0xFFFF), //int(v.Pixels >> 16),
			Y:    int(v.Pixels >> 16),    //int(v.Pixels & 0xFFFF),
			Data: i.Data,
		}
		return exportImageMk1(w, f, v.Type, x)
	case *IDHv2:
		x := &xy{
			X:    int(v.Pixels & 0xFFFF), //int(v.Pixels >> 16),
			Y:    int(v.Pixels >> 16),    //int(v.Pixels & 0xFFFF),
			Data: i.Data,
		}
		return exportImageMk2(w, f, v.Type, x)
	}
}

func (i *Image) ExportRaw(w io.Writer) error {
	_, err := w.Write(i.Payload())
	return err
}

type xy struct {
	X    int
	Y    int
	Data []byte
}

func exportImageMk1(w io.Writer, f string, t uint8, x *xy) error {
	var (
		err error
		i   image.Image
	)
	switch t {
	default:
		return fmt.Errorf("unsupported image type %d", t)
	case 1: //jpeg
		_, err = w.Write(x.Data)
		return err
	case 2: //monochrome
		i = img.ImageGray8(x.X, x.Y, x.Data)
	case 3: //s-video
		i = img.ImageLBR(x.X, x.Y, x.Data)
	case 4: //rgb
		i = img.ImageRGB(x.X, x.Y, x.Data)
		//case 5: //kodack
	}
	switch f {
	default:
		return fmt.Errorf("unsupported image type %s", f)
	case "jpg":
		err = jpeg.Encode(w, i, nil)
	case "png", "":
		err = png.Encode(w, i)
	}
	return err
}

func exportImageMk2(w io.Writer, f string, t uint8, x *xy) error {
	var (
		err error
		i   image.Image
	)
	switch t {
	default:
		return fmt.Errorf("unsupported image type %d", t)
	case 1: //gray8
		i = img.ImageGray8(x.X, x.Y, x.Data)
	case 2: //gray16-BE
		i = img.ImageGray16(x.X, x.Y, x.Data, binary.BigEndian)
	case 3: //gray16-LE
		i = img.ImageGray16(x.X, x.Y, x.Data, binary.LittleEndian)
	case 4: //yuy2
		i = img.ImageLBR(x.X, x.Y, x.Data)
	case 5: //i420
		i = img.ImageI420(x.X, x.Y, x.Data)
	case 6: //rgb
		i = img.ImageRGB(x.X, x.Y, x.Data)
	case 7, 8: //jpeg, png
		_, err = w.Write(x.Data)
		return err
	case 9: //h264
	}
	if i == nil {
		_, err = w.Write(x.Data)
		return err
	}
	switch f {
	default:
		return fmt.Errorf("unsupported image type %s", f)
	case "jpg", "jpeg":
		err = jpeg.Encode(w, i, nil)
	case "png", "":
		err = png.Encode(w, i)
	}
	return err
}
