package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/busoc/panda/cmd/internal/pool"
	"github.com/busoc/panda/cmd/internal/tm"
	"github.com/busoc/panda/cmd/internal/rw"
)

type query struct {
	Apid     int
	Start    time.Time
	End      time.Time
	filename string
}

func Validate(r io.Reader, t time.Time, d, i time.Duration, ids []int) (*query, error) {
	q := new(query)
	if err := json.NewDecoder(r).Decode(q); err != nil {
		return nil, err
	}
	ix := sort.SearchInts(ids, q.Apid)
	if len(ids) > 0 && ix >= len(ids) || ids[ix] != q.Apid {
		return nil, fmt.Errorf("invalid apid - no data available for %d", q.Apid)
	}
	if !t.IsZero() && (q.Start.Before(t) || q.End.Before(t)) {
		return nil, fmt.Errorf("invalid interval - no data available before %s", t)
	}
	n := time.Now()
	if delta := n.Sub(q.End); delta < d {
		return nil, fmt.Errorf("invalid interval: delay %s (min: %s)", delta, d)
	}
	if delta := q.End.Sub(q.Start); delta >= i {
		return nil, fmt.Errorf("invalid interval: interval %s (max: %s)", delta, i)
	}
	return q, nil
}

func (q *query) Write(d string, w io.Writer) error {
	for p := range q.next(d) {
		r, err := tm.Packets(p, q.Apid, nil)
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
			if i, err := os.Stat(p); err == nil && i.Mode().IsRegular() {
				ch <- p
			}
		}
	}()
	return ch
}

func (q *query) UnmarshalJSON(bs []byte) error {
	v := struct {
		Apid  int       `json:"apid"`
		Start time.Time `json:"dtstart"`
		End   time.Time `json:"dtend"`
		Name  string    `json:"filename"`
	}{}
	if err := json.Unmarshal(bs, &v); err != nil {
		return err
	}
	if v.Start.After(v.End) {
		return fmt.Errorf("invalid interval: %s < %s", q.End, q.Start)
	}
	t := time.Minute * 5
	q.filename, q.Apid, q.Start, q.End = v.Name, v.Apid, v.Start.Truncate(t), v.End.Add(t).Truncate(t)
	return nil
}

func (q *query) String() string {
	if q.filename != "" {
		return q.filename
	}
	const p = "20060102_150405"
	return fmt.Sprintf("tm_%06d_%s_%s.dat", q.Apid, q.Start.Format(p), q.End.Format(p))
}

type Archive struct {
	Datadir  string
	Delay    time.Duration
	Interval time.Duration
	Apids    []int
	Date     time.Time
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
	q, err := Validate(r.Body, a.Date, a.Delay, a.Interval, a.Apids)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	buf := new(bytes.Buffer)
	if err := q.Write(a.Datadir, rw.NoDuplicate(buf)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.Header().Set("content-length", fmt.Sprint(buf.Len()))
	w.Header().Set("content-disposition", fmt.Sprintf("attachment; filename=\"%s\"", q.String()))
	if _, err := io.Copy(w, buf); err != nil {
		log.Println(err)
	}
}

func (a *Archive) UnmarshalJSON(bs []byte) error {
	v := struct {
		Datadir  string    `json:"datadir"`
		Delay    uint      `json:"delay"`
		Interval uint      `json:"interval"`
		Date     time.Time `json:"date"`
		Apids    []uint16  `json:"apid"`
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
	a.Date = v.Date
	for _, i := range v.Apids {
		a.Apids = append(a.Apids, int(i))
	}
	sort.Sort(sort.IntSlice(a.Apids))
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
	Include string
	now     time.Time
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
	if err := h.Pool.Register(v.Worker, v.Auto); err != nil {
		return err
	}
	if i, err := os.Stat(h.Include); err == nil && i.IsDir() {
		if f, err := os.Create(filepath.Join(h.Include, v.Worker.Id+".json")); err == nil {
			json.NewEncoder(f).Encode(v.Worker)
			f.Close()
		}
	}
	return nil
}
