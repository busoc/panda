package mud

import (
	"fmt"
	"io"
	"net"
)

func Listen(p, s string) (io.Reader, error) {
	a, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenMulticastUDP("udp", nil, a)
	if err != nil {
		return nil, err
	}
	var (
		tag  byte
		skip int
	)
	switch p {
	default:
		return nil, fmt.Errorf("unsupported: %s", p)
	case "tm":
		tag, skip = TagTM, 10
	case "pp":
		tag, skip = TagPP, 12
	}
	return &conn{c, tag, skip}, nil
}

type conn struct {
	net.Conn
	tag  byte
	skip int
}

func (c *conn) Read(bs []byte) (int, error) {
	t := make([]byte, len(bs))
	r, err := c.Conn.Read(t)
	if r == 0 || t[0] != c.tag {
		return r, ErrSkip
	}
	return copy(bs, t[c.skip:r]), err
}
