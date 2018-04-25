package panda

import (
	"bufio"
	"errors"
	"io"
	"time"
)

var (
	GPS  = time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC)
	UNIX = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

var (
	ErrDone     = errors.New("done")
	ErrSkip     = errors.New("skip")
	ErrTooShort = errors.New("not enough bytes available")
)

var BufferSize = 1024 * 1024 * 4

const (
	TagPP = 0x06
	TagTM = 0x0B
)

type Decoder interface {
	Decode([]byte) (int, Packet, error)
}

type DecoderFunc func([]byte) (int, Packet, error)

func (d DecoderFunc) Decode(bs []byte) (int, Packet, error) {
	return d(bs)
}

func DecodePP() Decoder {
	f := func(bs []byte) (int, Packet, error) {
		if len(bs) < UMILength {
			return len(bs), nil, ErrTooShort
		}
		var (
			pp  Parameter
			err error
		)
		if pp.UMIHeader, err = decodeUMI(bs[:UMILength]); err != nil {
			return len(bs), nil, err
		}
		pp.Data = make([]byte, pp.UMIHeader.Length)
		copy(pp.Data, bs[UMILength:])

		return UMILength + len(pp.Data), pp, nil
	}
	return DecoderFunc(f)
}

func DecodeTM() Decoder {
	f := func(bs []byte) (int, Packet, error) {
		if len(bs) < CCSDSLength+ESALength {
			return 0, nil, ErrTooShort
		}
		var (
			tm  Telemetry
			err error
		)
		if tm.CCSDSHeader, err = decodeCCSDS(bs[:CCSDSLength]); err != nil {
			return len(bs), nil, err
		}
		if tm.ESAHeader, err = decodeESA(bs[CCSDSLength : CCSDSLength+ESALength]); err != nil {
			return len(bs), nil, err
		}
		tm.Data = make([]byte, tm.CCSDSHeader.Length+1-ESALength)
		copy(tm.Data, bs[CCSDSLength+ESALength:])

		// return CCSDSLength + ESALength + len(tm.Data), tm, nil
		return 0, tm, nil
	}
	return DecoderFunc(f)
}

type Reader struct {
	reader io.Reader
	queue  <-chan Packet
}

func NewReader(r io.Reader, d Decoder) *Reader {
	q := make(chan Packet)
	go readAll(bufio.NewReader(r), d, q)
	return &Reader{
		reader: r,
		queue:  q,
	}
}

func NewReaderWithSize(r io.Reader, s int, d Decoder) *Reader {
	return NewReader(bufio.NewReaderSize(r, s), d)
}

func (r *Reader) Read() (Packet, error) {
	p, ok := <-r.queue
	if !ok {
		return nil, ErrDone
	}
	return p, nil
}

func (r *Reader) Close() error {
	if c, ok := r.reader.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func readAll(r io.Reader, d Decoder, q chan<- Packet) {
	defer close(q)
	bs := make([]byte, BufferSize)
	for {
		n, err := r.Read(bs)
		if err != nil {
			return
		}
		if n == 0 {
			continue
		}
		for i, vs := 0, bs[:n]; i < len(vs); {
			c, p, err := d.Decode(vs[i:])
			switch err {
			case nil:
				q <- p
			case ErrSkip:
			default:
				return
			}
			if c <= 0 {
				break
			}
			i += c
		}
	}
}
