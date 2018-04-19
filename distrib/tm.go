package distrib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/busoc/panda"
)

type CCSDSArchive struct {
	Apids    []int
	Datadir  string
	Delay    time.Duration
	Interval time.Duration
	Limit    time.Time
}

func (a *CCSDSArchive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("content-type") != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	q, err := a.validate(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeResponse(w, q, a.Datadir)
}

func (a *CCSDSArchive) validate(r io.ReadCloser) (Query, error) {
	defer r.Close()

	q := new(ccsdsQuery)
	if err := json.NewDecoder(r).Decode(q); err != nil {
		return nil, err
	}
	ix := sort.SearchInts(ids, q.Apid)
	if len(ids) > 0 && ix >= len(ids) || ids[ix] != q.Apid {
		return nil, fmt.Errorf("invalid apid - no data available for %d", q.Apid)
	}
	if !t.IsZero() && (q.Start.Before(a.Date) || q.End.Before(a.Date)) {
		return nil, fmt.Errorf("invalid interval - no data available before %s", a.Date)
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

type ccsdsQuery struct {
	Apid     int
	Start    time.Time
	End      time.Time
	filename string
}

func (q *ccsdsQuery) Write(d string, w io.Writer) error {
	for p := range next(d) {
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

func (q *ccsdsQuery) UnmarshalJSON(bs []byte) error {
	v := struct {
		Apid  int       `json:"apid"`
		Start time.Time `json:"dtstart"`
		End   time.Time `json:"dtend"`
		Name  string    `json:"filename"`
	}{}
	if err := json.Unmarshal(bs, &v); err != nil {
		return err
	}
	if q.Apid >= 1<<11 {
		return fmt.Errorf("invalid apid %d", q.Apid)
	}
	if v.Start.After(v.End) {
		return fmt.Errorf("invalid interval: %s < %s", q.End, q.Start)
	}
	t := time.Minute * 5
	q.filename, q.Apid, q.Start, q.End = v.Name, v.Apid, v.Start.Truncate(t), v.End.Truncate(t)
	return nil
}

func (q *query) String() string {
	if q.filename != "" {
		return q.filename
	}
	const p = "20060102_150405"
	return fmt.Sprintf("tm_%06d_%s_%s.dat", q.Apid, q.Start.Format(p), q.End.Format(p))
}
