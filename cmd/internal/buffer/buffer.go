package buffer

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/busoc/panda"
)

type Buffer interface {
	Write(panda.Packet) (int, int, error)
	Flush(time.Time) error
}

type flat struct {
	datadir string
	prefix  string

	compat   bool
	compress bool

	count    uint64
	sequence uint64
	buf      *bytes.Buffer
}

func New(i, d string, c bool) Buffer {
	return &flat{
		datadir: d,
		compat:  c,
		prefix:  i,
		buf:     new(bytes.Buffer),
	}
}

func (f *flat) Write(p panda.Packet) (int, int, error) {
	bs, err := p.Bytes()
	if err != nil {
		return int(atomic.LoadUint64(&f.count)), f.buf.Len(), err
	}
	if f.compat {
		switch p.(type) {
		case panda.Parameter:
			binary.Write(f.buf, binary.LittleEndian, uint32(len(bs)))
		case panda.Telemetry:
			t := time.Now()
			binary.Write(f.buf, binary.LittleEndian, uint32(len(bs)+6))
			binary.Write(f.buf, binary.BigEndian, uint8(0x09))
			binary.Write(f.buf, binary.BigEndian, uint32(t.Unix()))
			binary.Write(f.buf, binary.BigEndian, uint8(0x09))
		}
	}
	_, err = f.buf.Write(bs)
	if err != nil {
		return int(atomic.LoadUint64(&f.count)), f.buf.Len(), err
	}
	return int(atomic.AddUint64(&f.count, 1)), f.buf.Len(), nil
}

func (f *flat) Flush(t time.Time) error {
	if f.buf.Len() == 0 {
		return nil
	}
	defer func() {
		f.buf.Reset()
		atomic.StoreUint64(&f.count, 0)
	}()
	s, c := atomic.AddUint64(&f.sequence, 1), atomic.LoadUint64(&f.count)

	if t.IsZero() {
		t = time.Now()
	}
	n := fmt.Sprintf("%s_%06d_%06d_%s.dat", f.prefix, s, c, t.Add(panda.GPS.Sub(panda.UNIX)).Format("20060102_150405"))

	w, err := os.Create(filepath.Join(f.datadir, n))
	if err != nil {
		return err
	}
	defer w.Close()

	var z io.Writer
	if f.compress {
		g := gzip.NewWriter(w)
		defer g.Close()

		z = g
	} else {
		z = w
	}
	if _, err := io.Copy(z, f.buf); err != nil {
		return err
	}
	return nil
}
