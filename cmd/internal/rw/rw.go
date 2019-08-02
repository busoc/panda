package rw

import (
	"crypto/md5"
	"fmt"
	"io"
)

type noDuplicate struct {
	digests map[string]struct{}
	inner   io.Writer
}

func NoDuplicate(w io.Writer) io.Writer {
	ds := make(map[string]struct{})
	return &noDuplicate{digests: ds, inner: w}
}

func (n *noDuplicate) Write(bs []byte) (int, error) {
	s := fmt.Sprintf("%x", md5.Sum(bs))
	if _, ok := n.digests[s]; ok {
		return len(bs), nil
	}
	n.digests[s] = struct{}{}
	return n.inner.Write(bs)
}
