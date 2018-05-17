package buffer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var ErrInvalidPosition = errors.New("invalid position")

type Buffer struct {
	inner *bytes.Reader
	index int64
}

func NewBuffer(bs []byte) *Buffer {
	return &Buffer{inner: bytes.NewReader(bs)}
}

func (b *Buffer) Discard(n int64) error {
	bs := make([]byte, b.inner.Size()-n)
	if _, err := b.inner.ReadAt(bs, n); err != nil {
		return err
	}
	b.inner.Reset(bs)
	return nil
}

func (b *Buffer) ReadFloat(pos int, order binary.ByteOrder) (i float32, err error) {
	ix, _ := index(pos)

	var u uint32
	if err = b.readValue(&u, ix, binary.Size(u)*8, order); err != nil {
		return
	}
	i = math.Float32frombits(u)
	return
}

func (b *Buffer) ReadInt32(pos, count int, order binary.ByteOrder) (i int32, err error) {
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

func (b *Buffer) ReadInt16(pos, count int, order binary.ByteOrder) (i int16, err error) {
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

func (b *Buffer) ReadInt8(pos, count int, order binary.ByteOrder) (i int8, err error) {
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

func (b *Buffer) ReadUint32(pos, count int, order binary.ByteOrder) (i uint32, err error) {
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

func (b *Buffer) ReadUint16(pos, count int, order binary.ByteOrder) (i uint16, err error) {
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

func (b *Buffer) ReadUint8(pos, count int, order binary.ByteOrder) (i uint8, err error) {
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

func (b *Buffer) readValue(i interface{}, x, n int, e binary.ByteOrder) error {
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

func index(n int) (int, int) {
	return n / 8, n % 8
}
