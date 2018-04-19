package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/busoc/panda"
)

type Acquirer interface {
	Acquire() (time.Time, time.Time)
}

type formats []string

func (fs *formats) String() string {
	return fmt.Sprint(*fs)
}

func (fs *formats) Set(vs string) error {
	for _, n := range strings.Split(vs, ",") {
		*fs = append(*fs, strings.ToLower(n))
	}
	sort.SliceStable(*fs, func(i, j int) bool {
		return (*fs)[i] < (*fs)[j]
	})
	return nil
}

type channels []mud.Channel

func (cs *channels) String() string {
	return fmt.Sprint(*cs)
}

func (cs *channels) Set(vs string) error {
	for _, n := range strings.Split(vs, ",") {
		var c mud.Channel
		switch n {
		case "vic1":
			c = mud.Video1
		case "vic2":
			c = mud.Video2
		case "lrsd":
			c = mud.Science
		default:
			return fmt.Errorf("unknown channel %q", n)
		}
		*cs = append(*cs, c)
	}
	sort.SliceStable(*cs, func(i, j int) bool {
		return (*cs)[i] > (*cs)[j]
	})
	return nil
}

type source string

func (s *source) String() string {
	return string(*s)
}

func (s *source) Set(v string) error {
	switch v {
	case "realtime", "playback", "", "*":
		*s = source(v)
	default:
		return fmt.Errorf("unknown source %q", v)
	}
	return nil
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)

	var (
		fs       formats
		cs       channels
		src      source
		version  int
		limit    int
		parallel int
		raw      bool
		flat     bool
	)
	flag.Var(&fs, "t", "formats")
	flag.Var(&cs, "c", "channels")
	flag.Var(&src, "s", "source")
	flag.IntVar(&parallel, "p", 4, "parallel workers")
	flag.IntVar(&version, "u", mud.VMUProtocol2, "vmu version")
	flag.IntVar(&limit, "m", 0, "limit")
	flag.BoolVar(&flat, "f", false, "flat")
	flag.BoolVar(&raw, "r", false, "raw")
	flag.Parse()

	queue, err := Packets(flag.Arg(0), string(src), version, []string(fs), []mud.Channel(cs))
	if err != nil {
		log.Fatalln(err)
	}

	if parallel < 0 {
		parallel = 0
	}
	sema := make(chan struct{}, parallel)
	var (
		curr int
		wg   sync.WaitGroup
	)
	for p := range queue {
		if limit > 0 && curr >= limit {
			break
		}
		curr++
		sema <- struct{}{}
		wg.Add(1)
		go func(d string, p mud.HRPacket) {
			defer func() {
				<-sema
				wg.Done()
			}()
			if !flat {
				d = filepath.Join(d, p.Stream().String(), p.Format(), timePath(p.Timestamp()))
				if err := os.MkdirAll(d, 0755); err != nil && !os.IsExist(err) {
					log.Println(err)
					return
				}
			}
			w, err := os.Create(filepath.Join(d, p.Filename()))
			if err != nil {
				log.Println(err)
				return
			}
			err = p.Export(w, "")
			w.Close()
			if err != nil {
				os.Remove(w.Name())
			}

			if raw {
				w, err := os.Create(filepath.Join(d, p.Filename()+".raw"))
				if err != nil {
					log.Println(err)
					return
				}
				err = p.ExportRaw(w)
				w.Close()
				if err != nil {
					os.Remove(w.Name())
				}
			}
		}(flag.Arg(1), p)
	}
	wg.Wait()
}

func timePath(t time.Time) string {
	y := fmt.Sprintf("%04d", t.Year())
	d := fmt.Sprintf("%03d", t.YearDay())
	h := fmt.Sprintf("%02d", t.Hour())

	return filepath.Join(y, d, h)
}

func Packets(a, s string, v int, fs []string, cs []mud.Channel) (<-chan mud.HRPacket, error) {
	d, err := mud.DecodeHR(v)
	if err != nil {
		return nil, err
	}
	w, err := mud.Walk("hr", a)
	if err != nil {
		return nil, err
	}
	q := make(chan mud.HRPacket)
	go func() {
		r := mud.NewReader(w, d)
		defer func() {
			close(q)
			r.Close()
		}()
		for {
			p, err := r.Read()
			switch err {
			case mud.ErrDone:
				return
			case nil:
			default:
				log.Fatalln(err)
			}
			v, ok := p.(mud.HRPacket)
			if !ok {
				continue
			}
			fx := sort.Search(len(fs), func(i int) bool {
				return fs[i] <= v.Format()
			})
			if len(fs) > 0 && (fx >= len(fs) || fs[fx] != v.Format()) {
				continue
			}
			sx := sort.Search(len(cs), func(i int) bool {
				return cs[i] <= v.Stream()
			})
			if len(cs) > 0 && (sx >= len(cs) || cs[sx] != v.Stream()) {
				continue
			}
			ok = false
			switch r := v.IsRealtime(); s {
			case "realtime":
				ok = r
			case "playback":
				ok = !r
			default:
				ok = true
			}
			if !ok {
				continue
			}
			q <- v
		}
	}()
	return q, nil
}
