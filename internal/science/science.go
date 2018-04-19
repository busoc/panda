package science

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"time"
)

const (
	syncChunkLength = 18
	lrsdChunkLength = 32
)

func ExportSyncUnit(w io.Writer, bs []byte, t time.Time) error {
	c := csv.NewWriter(w)
	for i, r := 1, bytes.NewReader(bs); r.Len() > 0; i++ {
		rs := make([]string, 7)
		rs[0] = t.Format(time.RFC3339)
		rs[1] = fmt.Sprint(i)
		rs[2] = fmt.Sprint(r.Size() / syncChunkLength)

		for i := 3; i < 5; i++ {
			var x uint8
			if err := binary.Read(r, binary.BigEndian, &x); err != nil {
				return err
			}
			rs[i] = fmt.Sprint(x)
		}
		for i := 5; i < len(rs); i++ {
			var x int64
			if err := binary.Read(r, binary.BigEndian, &x); err != nil {
				return err
			}
			rs[i] = fmt.Sprint(x)
		}

		if err := c.Write(rs); err != nil {
			return err
		}
	}
	c.Flush()
	return c.Error()
}

func ExportScienceData(w io.Writer, bs []byte, t time.Time) error {
	c := csv.NewWriter(w)
	for i, r := 1, bytes.NewReader(bs); r.Len() > 0; i++ {
		rs := make([]string, 35)
		rs[0] = t.Format(time.RFC3339)
		rs[1] = fmt.Sprint(i)
		rs[2] = fmt.Sprint(r.Size() / lrsdChunkLength)
		for i := 3; i < len(rs); i++ {
			var v uint16
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return err
			}
			rs[i] = fmt.Sprint(v)
		}
		if err := c.Write(rs); err != nil {
			return err
		}
	}
	c.Flush()
	return c.Error()
}
