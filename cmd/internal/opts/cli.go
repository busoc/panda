package opts

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Gap struct {
	aos time.Duration
	los time.Duration

	mu   sync.Mutex
	curr uint8
}

func (g *Gap) Next() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if mod := g.curr % 2; mod != 0 && g.los > 0 {
		return g.los
	}
	return g.aos
}

func (g *Gap) Wait() (time.Duration, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.curr++
	if mod := g.curr % 2; mod != 0 && g.los > 0 {
		return g.los, false
	}
	return g.aos, true
}

func (g *Gap) IsZero() bool {
	return g.aos == 0 && g.los == 0
}

func (g *Gap) String() string {
	return fmt.Sprintf("%s:%s", g.aos, g.los)
}

func (g *Gap) Set(v string) error {
	vs := strings.Split(v, ":")
	var err error
	g.aos, err = time.ParseDuration(vs[0])
	if err != nil {
		return err
	}
	g.los, err = time.ParseDuration(vs[1])
	if err != nil {
		return err
	}
	return nil
}

type SIDSet []uint32

func (i *SIDSet) Set(vs string) error {
	if f, err := os.Open(vs); err == nil {
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			t := s.Text()
			if t == "" {
				continue
			}
			if err := i.parse(t); err != nil {
				return err
			}
		}
		return s.Err()
	}
	for _, v := range strings.Split(vs, ",") {
		if err := i.parse(v); err != nil {
			return err
		}
	}
	return nil
}

func (i *SIDSet) parse(v string) error {
	n, err := strconv.ParseUint(v, 0, 64)
	if err != nil {
		return err
	}
	*i = append(*i, uint32(n))
	return nil
}

func (i *SIDSet) String() string {
	return fmt.Sprint(*i)
}

type UMISet []uint64

func (i *UMISet) Set(vs string) error {
	if f, err := os.Open(vs); err == nil {
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			t := s.Text()
			if t == "" {
				continue
			}
			if err := i.parse(t); err != nil {
				return err
			}
		}
		return s.Err()
	}
	for _, v := range strings.Split(vs, ",") {
		if err := i.parse(v); err != nil {
			return err
		}
	}
	return nil
}

func (i *UMISet) String() string {
	return fmt.Sprint(*i)
}

func (i *UMISet) parse(v string) error {
	n, err := strconv.ParseUint(v, 0, 64)
	if err != nil {
		return err
	}
	*i = append(*i, uint64(n))
	return nil
}
