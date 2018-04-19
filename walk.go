package mud

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func Walk(p, s string) (io.Reader, error) {
	// if i, err := os.Stat(s); err != nil || !i.IsDir() {
	// 	return nil, fmt.Errorf("not a directory")
	// }
	var skip int
	switch p {
	default:
		return nil, fmt.Errorf("unsupported: %s", p)
	case "tm":
		skip = 10
	case "pp":
		skip = 0
	case "hr", "hrd", "vmu":
		skip = 26
	}

	done := make(chan struct{})
	next := walk(s, done)

	return &walker{
		next: next,
		done: done,
		skip: skip,
	}, nil
}

type walker struct {
	sc   *bufio.Scanner
	rc   io.ReadCloser
	skip int

	next <-chan io.ReadCloser

	once sync.Once
	done chan struct{}
}

func (w *walker) Read(b []byte) (int, error) {
	select {
	case <-w.done:
		return 0, ErrDone
	default:
		p, err := w.read()
		if err != nil {
			return 0, err
		}
		return copy(b, p), nil
	}
}

func (w *walker) read() ([]byte, error) {
	if w.sc == nil {
		r, ok := <-w.next
		if !ok {
			return nil, ErrDone
		}
		w.rc = r
		w.sc = bufio.NewScanner(w.rc)
		w.sc.Buffer(make([]byte, 4096), 1024*1024*1024)
		w.sc.Split(scan(w.skip))
	}
	if !w.sc.Scan() {
		w.rc.Close()
		if err := w.sc.Err(); err != nil {
			return nil, err
		}
		w.sc = nil
		return w.read()
	}
	return w.sc.Bytes(), nil
}

func (w *walker) Close() error {
	var (
		err error
		ok  bool
	)
	w.once.Do(func() {
		if w.rc != nil {
			err = w.rc.Close()
		}
		ok = true

		close(w.done)
	})
	if ok {
		return err
	}
	return ErrDone
}

func scan(s int) bufio.SplitFunc {
	if s == 0 {
		s = 4
	}
	return func(buf []byte, ateof bool) (int, []byte, error) {
		if len(buf) < 4 {
			return 0, nil, nil
		}
		length := int(binary.LittleEndian.Uint32(buf[:4]) + 4)
		if len(buf) < length {
			return 0, nil, nil
		}
		b := make([]byte, length)
		copy(b, buf[:length])

		return length, b[s:], nil
	}
}

func walk(s string, done <-chan struct{}) <-chan io.ReadCloser {
	q := make(chan io.ReadCloser)
	go func() {
		defer close(q)
		filepath.Walk(s, func(p string, i os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if i.IsDir() {
				return nil
			}
			f, err := os.Open(p)
			if err != nil {
				return err
			}

			select {
			case <-done:
				return ErrDone
			case q <- f:
				return nil
			}
		})
	}()
	return q
}
