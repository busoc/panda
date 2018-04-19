package distrib

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/busoc/panda/cmd/internal/pp"
)

type UMIArchive struct {
	Datadir  string
	Delay    time.Duration
	Interval time.Duration
}

func (a *UMIArchive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (a *UMIArchive) validate(r io.ReadCloser) (Query, error) {
	defer r.Close()
	q := new(umiQuery)
	if err := json.NewDecoder(r).Decode(q); err != nil {
		return nil, err
	}
	n := time.Now()
	if delta := n.Sub(q.End); delta < a.Delay {
		return nil, fmt.Errorf("invalid interval")
	}
	if delta := q.End.Sub(q.Start); delta < a.Interval {
		return nil, fmt.Errorf("invalid interval")
	}
	return q, nil
}

type umiQuery struct {
	Codes []uint64
	Start time.Time
	End   time.Time

	filename string
}

func (q *umiQuery) Write(d string, w io.Writer) error {
	for p := range next(d) {
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

func (q *umiQuery) UnmarshalJSON(bs []byte) error {
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
