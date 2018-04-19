package distrib

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

type Query interface {
	fmt.Stringer
	Write(string, io.Writer) error
}

func next(base string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for n := q.Start; n.Before(q.End); n = n.Add(time.Minute * 5) {
			y, d, h := n.Year(), n.YearDay(), n.Hour()
			n := fmt.Sprintf("rt_%02d_%02d.dat", n.Minute(), n.Minute()+4)
			p := filepath.Join(base, fmt.Sprintf("%04d", y), fmt.Sprintf("%03d", d), fmt.Sprintf("%02d", h), n)
			if i, err := os.Stat(p); err == nil && i.Mode().IsRegular() {
				ch <- p
			}
		}
	}()
	return ch
}

func writeResponse(w *http.ResponseWriter, q Query, d string) error {
	buf := new(bytes.Buffer)
	if err := q.Write(d, buf); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/octet-stream")
	w.Header().Set("content-length", fmt.Sprint(buf.Len()))
	w.Header().Set("content-disposition", fmt.Sprintf("attachment; filename=\"%s\"", s.String()))
	if _, err := io.Copy(w, buf); err != nil {
		log.Println(err)
	}
	return err
}
