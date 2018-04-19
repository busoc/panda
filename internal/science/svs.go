package science

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

const (
	metaLength = 74
	svsMagic   = 0x90
)

func ExportSVSData(w io.Writer, bs []byte) error {
	if bs[0] != svsMagic {
		_, err := w.Write(bs)
		return err
	}
	r := bytes.NewReader(bs[metaLength:])
	n, err := r.ReadByte()
	if err != nil {
		return err
	}
	c := csv.NewWriter(w)

	vs := make([]string, int(n)+1)
	vs[0] = "t"
	for i, j := int(n), 0; j < i; j++ {
		var v uint16
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return err
		}
		vs[j+1] = fmt.Sprintf("g2(t, %d)", v)
	}
	c.Write(vs)
	for i := 0; r.Len() > 0; i++ {
		vs[0] = strconv.Itoa(i)
		for j := 0; j < int(n); j++ {
			var v float32
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return err
			}
			vs[j+1] = strconv.FormatFloat(float64(v), 'f', -1, 32)
		}
		c.Write(vs)
	}
	c.Flush()
	return c.Error()
}
