package distrib

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type browser string

type info struct {
	Name    string    `json:"name" xml:"name"`
	Mod     time.Time `json:"lastmod" xml:"lastmod,attr"`
	Size    int64     `json:"size" xml:"size,attr"`
	Regular bool      `json:"regular" xml:"file"`

	Acq    time.Time `json:"dtstamp" xml:"info>acquisition"`
	Ori    int       `json:"oid" xml:"info>oid"`
	Seq    int       `json:"sequence" xml:"info>sequence"`
	Format string    `json:"format" xml:"info>format"`
}

func Browse(d string) (http.Handler, error) {
	i, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	if !i.IsDir() {
		return nil, fmt.Errorf("not a directory", d)
	}
	return browser(d), nil
}

func (b browser) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queue, err := readDir(filepath.Join(string(b), r.URL.Path))
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	is := make([]info, 0, 1000)
	for i := range queue {
		is = append(is, *i)
	}
	ws := new(bytes.Buffer)
	switch a := r.Header.Get("accept"); {
	default:
		w.WriteHeader(http.StatusNotAcceptable)
	case isAcceptable(a, "application/json"):
		err = json.NewEncoder(ws).Encode(is)
	case isAcceptable(a, "application/xml"):
		v := struct {
			XMLName xml.Name `xml:"archive"`
			Infos   []info   `xml:"product"`
		}{Infos: is}
		err = xml.NewEncoder(ws).Encode(v)
	case isAcceptable(a, "text/csv"):
		c := csv.NewWriter(ws)
		for _, i := range is {
			rs := []string{i.Name, i.Mod.Format(time.RFC3339), fmt.Sprint(i.Size)}
			if err := c.Write(rs); err != nil {
				break
			}
		}
		c.Flush()
		err = c.Error()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	io.Copy(w, ws)
}

func readDir(p string) (<-chan *info, error) {
	infos, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, err
	}
	q := make(chan *info)
	go func(is []os.FileInfo) {
		split := func(r rune) bool {
			return r == '_' || r == '.'
		}
		defer close(q)
		for _, i := range is {
			if filepath.Ext(i.Name()) == ".xml" {
				continue
			}
			n := &info{
				Name:    filepath.Base(i.Name()),
				Size:    i.Size(),
				Mod:     i.ModTime(),
				Regular: !i.IsDir(),
			}
			if !i.IsDir() {
				fs := strings.FieldsFunc(n.Name, split)
				n.Ori, _ = strconv.Atoi(fs[0])
				n.Seq, _ = strconv.Atoi(fs[1])
				n.Acq, _ = time.Parse("20060102_150405", fs[2]+"_"+fs[3])
				n.Format = fs[4]
			}
			q <- n
		}
	}(infos)
	return q, nil
}
