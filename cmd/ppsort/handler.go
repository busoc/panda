package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/busoc/panda/cmd/internal/pool"
	"github.com/busoc/panda/cmd/internal/pp"
)

type query struct {
	Codes []uint64  `json:"codes"`
	Start time.Time `json:"dtstart"`
	End   time.Time `json:"dtend"`

	filename string
}

func Validate(r io.Reader, d, i time.Duration) (*query, error) {
	q := new(query)
	if err := json.NewDecoder(r).Decode(q); err != nil {
		return nil, err
	}
	n := time.Now()
	if delta := n.Sub(q.End); delta < d {
		return nil, fmt.Errorf("invalid interval")
	}
	if delta := q.End.Sub(q.Start); delta >= i {
		return nil, fmt.Errorf("invalid interval")
	}
	return q, nil
}

func (q *query) Write(d string, w io.Writer) error {
	for p := range q.next(d) {
		r, err := pp.Packets(p, q.Codes)
		if err != nil {
			return err
		}
		for p := range r {
			bs, e := p.Bytes()
			if e != nil {
				continue
			}
			if _, err := w.Write(bs); err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *query) next(base string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for n := q.Start; n.Before(q.End); n = n.Add(time.Minute * 5) {
			y, d, h := n.Year(), n.YearDay(), n.Hour()
			n := fmt.Sprintf("rt_%02d_%02d.dat", n.Minute(), n.Minute()+4)
			p := filepath.Join(base, fmt.Sprintf("%04d", y), fmt.Sprintf("%03d", d), fmt.Sprintf("%02d", h), n)
			if i, err := os.Stat(p); err != nil || !i.Mode().IsRegular() {
				continue
			}
			ch <- p
		}
	}()
	return ch
}

func (q *query) UnmarshalJSON(bs []byte) error {
	v := struct {
		Codes []string  `json:"codes"`
		Start time.Time `json:"dtstart"`
		End   time.Time `json:"dtend"`
		Name  string    `json:"filename"`
	}{}
	if err := json.Unmarshal(bs, &v); err != nil {
		return err
	}
	if len(v.Codes) == 0 {
		return fmt.Errorf("no umi codes provided")
	}
	for _, c := range v.Codes {
		if c, err := strconv.ParseUint(c, 0, 64); err != nil {
			return err
		} else {
			q.Codes = append(q.Codes, c)
		}
	}
	if v.Start.After(v.End) {
		return fmt.Errorf("invalid interval: %s < %s", q.End, q.Start)
	}
	t := time.Minute * 5
	q.filename, q.Start, q.End = v.Name, v.Start.Truncate(t), v.End.Add(t).Truncate(t)
	return nil
}

func (q *query) String() string {
	if q.filename != "" {
		return q.filename
	}
	const p = "20060102_150405"
	return fmt.Sprintf("pp_%s_%s.dat", q.Start.Format(p), q.End.Format(p))
}

type Archive struct {
	Datadir  string
	Delay    time.Duration
	Interval time.Duration
}

func (a *Archive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("content-type") != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	q, err := Validate(r.Body, a.Delay, a.Interval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	buf := new(bytes.Buffer)
	if err := q.Write(a.Datadir, buf); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.Header().Set("content-disposition", fmt.Sprintf("attachment; filename=\"%s\"", q.String()))
	io.Copy(w, buf)
}

func (a *Archive) UnmarshalJSON(bs []byte) error {
	v := struct {
		Datadir  string `json:"datadir"`
		Delay    uint   `json:"delay"`
		Interval uint   `json:"interval"`
	}{}
	if err := json.Unmarshal(bs, &v); err != nil {
		return err
	}
	if i, err := os.Stat(v.Datadir); err != nil || !i.IsDir() {
		return fmt.Errorf("%s not a directory", v.Datadir)
	}
	if a == nil {
		a = new(Archive)
	}
	a.Datadir = v.Datadir
	if v.Interval == 0 {
		v.Interval = 6 * 3600
	}
	if v.Delay == 0 {
		v.Delay = 3 * 3600
	}
	a.Interval, a.Delay = time.Duration(v.Interval), time.Duration(v.Delay)

	return nil
}

type Handler struct {
	*pool.Pool
	now time.Time
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		data interface{}
	)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	if h := r.Header.Get("Access-Control-Request-Headers"); len(h) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", h)
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
		w.WriteHeader(http.StatusOK)
		return
	}
	switch r.Method {
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	case http.MethodGet:
		data = h.Status()
	case http.MethodPost:
		if _, n := path.Split(r.URL.Path); n == "" {
			err = h.Register(io.LimitReader(r.Body, 65536))
		} else {
			err = h.Start(n)
		}
	case http.MethodDelete:
		_, n := path.Split(r.URL.Path)
		err = h.Stop(n)
	}
	if err != nil {
		v := struct {
			Err  string `json:"error"`
			Code int    `json:"code"`
		}{
			Err:  err.Error(),
			Code: http.StatusBadRequest,
		}
		json.NewEncoder(w).Encode(v)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if data == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) Status() []pool.State {
	vs := h.Pool.Status()
	if len(vs) == 0 {
		return nil
	}
	return vs
}

func (h *Handler) Start(n string) error {
	return h.Pool.Start(n)
}

func (h *Handler) Stop(n string) error {
	return h.Pool.Stop(n)
}

func (h *Handler) Register(r io.Reader) error {
	v := struct {
		*Worker
		Auto bool `json:"auto"`
	}{Worker: new(Worker)}
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return err
	}
	return h.Pool.Register(v.Worker, v.Auto)
}
