package hadock

import (
	"compress/gzip"
	"io"
	"net"
	"time"
)

type flusher interface {
	Flush() error
}

type proxy struct {
	net.Conn
	level  int
	writer io.Writer
}

func DialProxy(a, e string) (io.WriteCloser, error) {
	c, err := net.Dial("tcp", a)
	if err != nil {
		return nil, err
	}
	p := &proxy{Conn: c, writer: c}
	z := true
	switch e {
	default:
		z = false
	case "no":
		p.level = gzip.NoCompression
	case "speed":
		p.level = gzip.BestSpeed
	case "best":
		p.level = gzip.BestCompression
	case "default":
		p.level = gzip.DefaultCompression
	}
	if z {
		p.writer, _ = gzip.NewWriterLevel(p.writer, p.level)
	}
	return p, nil
}

func (p *proxy) Write(bs []byte) (int, error) {
	_, err := p.writer.Write(bs)
	if err == nil {
		if f, ok := p.writer.(flusher); ok {
			err = f.Flush()
		}
		return len(bs), nil
	}
	if err, ok := err.(net.Error); ok && !err.Temporary() {
		a := p.Conn.RemoteAddr()
		c, err := net.DialTimeout(a.Network(), a.String(), time.Millisecond*250)
		if err == nil {
			p.Conn.Close()
			p.Conn, p.writer = c, c
			if _, ok := p.writer.(flusher); ok {
				p.writer, _ = gzip.NewWriterLevel(p.writer, p.level)
			}
		}
	}
	return len(bs), nil
}
