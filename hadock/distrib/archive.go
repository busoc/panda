package distrib

import (
	"archive/tar"
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type downloader string

type query struct {
	File  string
	Type  string
	Level int64
	Flat  bool
	Meta  bool
}

func Download(d string) (http.Handler, error) {
	i, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	if !i.IsDir() {
		return nil, fmt.Errorf("not a directory", d)
	}
	return downloader(d), nil
}

func (d downloader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q, err := parseQuery(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n := fmt.Sprintf("attachment; filename=%s.%s", q.File, q.Type)
	switch p := filepath.Join(string(d), r.URL.Path); q.Type {
	case "tar":
		w.Header().Set("content-disposition", n)
		w.Header().Set("content-type", "application/x-tar")
		writeTar(w, p, string(d), q.Flat, q.Meta)
	case "zip":
		w.Header().Set("content-disposition", n)
		w.Header().Set("content-type", "application/zip")
		writeZip(w, p, string(d), q.Flat, q.Meta)
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

func writeTar(w io.Writer, datadir, strip string, flat, meta bool) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	err := filepath.Walk(datadir, func(p string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() || (!meta && filepath.Ext(p) == ".xml") {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		n := strings.TrimPrefix(p, strip)
		if flat {
			n = filepath.Base(p)
		}
		h := &tar.Header{
			Name:    n,
			Size:    i.Size(),
			Mode:    0644,
			ModTime: i.ModTime(),
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		_, err = io.Copy(tw, f)
		return err
	})
	return err
}

func writeZip(w io.Writer, datadir, strip string, flat, meta bool) error {
	zw := zip.NewWriter(w)
	defer zw.Close()

	err := filepath.Walk(datadir, func(p string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() || (!meta && filepath.Ext(p) == ".xml") {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		n := strings.TrimPrefix(p, strip)
		if flat {
			n = filepath.Base(p)
		}
		w, err := zw.Create(n)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, f)
		return err
	})
	return err
}

func parseQuery(r *http.Request) (*query, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	q := new(query)
	q.File = r.Form.Get("filename")
	if q.File == "" {
		q.File = "archive_" + time.Now().Format("20060102150405")
	}
	q.Type = r.Form.Get("type")
	q.Meta, _ = strconv.ParseBool(r.Form.Get("meta"))
	q.Level, _ = strconv.ParseInt(r.Form.Get("level"), 10, 64)

	return q, nil
}
