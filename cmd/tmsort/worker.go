package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/cmd/internal/buffer"
	"github.com/busoc/panda/cmd/internal/pool"
	"github.com/busoc/panda/cmd/internal/tm"
)

type Worker struct {
	Id string

	Apid    int
	Sources []uint32
	Every   time.Duration

	Sequence uint64
	Count    uint64
	Size     uint64
	Last     time.Time

	reader io.Reader

	logger *log.Logger
}

func NewWorker(n string, a int, e time.Duration) *Worker {
	if len(n) == 0 {
		n = fmt.Sprint(a)
	}
	return &Worker{
		Id:     n,
		Apid:   a,
		Every:  e,
		logger: log.New(os.Stderr, fmt.Sprintf("[%s] ", n), log.LstdFlags),
	}
}

func (w *Worker) String() string {
	return w.Id
}

func (w *Worker) Status() pool.State {
	return pool.State{
		Id:      w.Id,
		Count:   int(w.Count),
		Size:    int(w.Size),
		Last:    w.Last,
		Running: w.reader != nil,
	}
}

func (w *Worker) RunBuffer(a string, b buffer.Buffer) error {
	if w.reader != nil {
		return fmt.Errorf("%s already running", w.Id)
	}
	if r, err := tm.Open(a); err != nil {
		return err
	} else {
		w.reader = r
	}
	q := tm.Filter(w.reader, tm.NewDecoder(w.Apid, w.Sources))
	return w.sortPackets(q, b)
}

func (w *Worker) Run(a, d string, c bool) error {
	if w.reader != nil {
		return fmt.Errorf("%s already running", w.Id)
	}
	if r, err := tm.Open(a); err != nil {
		return err
	} else {
		w.reader = r
	}
	var prev time.Time

	w.logger.Printf("start sorting packets from %s", a)
	defer w.logger.Printf("done sorting packets from %s", a)

	buf := buffer.New(w.Id, d, c)
	for p := range tm.Filter(w.reader, tm.NewDecoder(w.Apid, w.Sources)) {
		t := p.Timestamp()
		if prev.IsZero() {
			prev = t
		}
		if t.Sub(prev) >= w.Every {
			if err := buf.Flush(prev); err != nil {
				w.logger.Printf("failed to write packets: %s", err)
			} else {
				if w.Count > 0 {
					w.logger.Printf("%d packets written to %s (%.2fKB)", w.Count, d, float64(w.Size)/1024.0)
				}
			}
			w.Count, w.Size, prev = 0, 0, t
		}
		c, s, _ := buf.Write(p)
		w.Count, w.Size, w.Last = uint64(c), w.Size+uint64(s), t
	}
	return buf.Flush(prev)
}

func (w *Worker) Close() error {
	if w.reader == nil {
		return fmt.Errorf("%s not yet running", w.Id)
	}
	var err error
	if c, ok := w.reader.(io.Closer); ok {
		err = c.Close()
	}
	w.reader = nil
	return err
}

func (w *Worker) UnmarshalJSON(bs []byte) error {
	v := struct {
		Prefix  string   `json:"prefix"`
		Apid    int      `json:"apid"`
		Every   int      `json:"every"`
		Sources []uint32 `json:"sources"`
	}{}
	if err := json.Unmarshal(bs, &v); err != nil {
		return err
	}
	if len(v.Prefix) == 0 {
		return fmt.Errorf("worker without prefix")
	}
	if v.Every <= 0 {
		return fmt.Errorf("given interval too short for %s", v.Prefix)
	}
	w.Id = v.Prefix
	w.Apid = v.Apid
	w.Sources = v.Sources
	w.Every = time.Second * time.Duration(v.Every)

	w.logger = log.New(os.Stderr, fmt.Sprintf("[%s] ", w.Id), log.LstdFlags)

	return nil
}

func (w *Worker) sortPackets(queue <-chan mud.Telemetry, buf buffer.Buffer) error {
	var prev time.Time
	for p := range queue {
		t := p.Timestamp()
		if prev.IsZero() {
			prev = t
		}
		c, s, _ := buf.Write(p)
		w.Count, w.Size, w.Last = uint64(c), w.Size+uint64(s), t
		if t.Sub(prev) < w.Every {
			continue
		}
		if err := buf.Flush(prev); err != nil {
			w.logger.Printf("failed to write packets: %s", err)
		} else {
			if w.Count > 0 {
				w.logger.Printf("%d packets written (%.2fKB)", w.Count, float64(w.Size)/1024.0)
			}
		}
		w.Count, w.Size, prev = 0, 0, t
	}
	return buf.Flush(prev)
}
