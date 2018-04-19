package main

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/hadock"
	"github.com/midbel/toml"
)

const Timeout = time.Second * 5

const pattern = "%s | %s | %s | %d | %d"

type monitor struct {
	datadir  string
	delta    time.Duration
	channels []mud.Channel

	first  time.Time
	last   time.Time
	starts uint32
	ends   uint32

	size  uint64
	count uint64

	file   *os.File
	logger *log.Logger
}

func New(f string) (hadock.Module, error) {
	r, err := os.Open(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	c := struct {
		Datadir   string        `toml:"datadir"`
		Every     uint          `toml:"delta"`
		Rotate    uint          `toml:"rotate"`
		Aggregate bool          `toml:"aggregate"`
		Channels  []mud.Channel `toml:"channels"`
	}{}
	if err := toml.NewDecoder(r).Decode(&c); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.Datadir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	t := Timeout
	if c.Every > 0 {
		t = time.Second * time.Duration(c.Every)
	}
	p := filepath.Join(c.Datadir, time.Now().Format("20060102")) + ".log"
	g, err := os.Create(p)
	if err != nil {
		return nil, err
	}
	m := &monitor{
		datadir: c.Datadir,
		delta:   t,
		file:    g,
		logger:  log.New(g, "", log.LstdFlags),
	}
	t = time.Hour
	if c.Rotate > 0 {
		t = time.Second * time.Duration(c.Rotate)
	}
	go m.rotate(c.Aggregate, t)

	return m, nil
}

func (m *monitor) Process(i uint8, p mud.HRPacket) error {
	if p.Version() != mud.VMUProtocol2 {
		return nil
	}
	var (
		v  *mud.VMUHeader
		hs uint64
	)
	switch p := p.(type) {
	case *mud.Image:
		v = p.VMUHeader
		hs = mud.IDHeaderLengthV2
	case *mud.Table:
		v = p.VMUHeader
		hs = mud.SDHeaderLengthV2
	}
	m.last, m.ends = v.Timestamp(), v.Sequence
	if m.first.IsZero() {
		m.first, m.starts = m.last, m.ends
	}
	delta := m.ends - m.starts
	if d := m.last.Sub(m.first); d > 0 && d > m.delta && (delta > 1 && delta > m.ends) {
		m.logger.Printf(pattern, "g", m.first, m.last, m.size, m.count)
		m.size, m.count = 0, 0
		m.first, m.starts = m.last, m.ends
	}
	m.count++
	m.size += mud.VMUHeaderLength + hs

	return nil
}

func (m *monitor) rotate(a bool, d time.Duration) {
	e := time.NewTicker(time.Minute * 5)
	r := time.NewTicker(d)
	for {
		select {
		case <-e.C:
			if !a {
				break
			}
			m.logger.Printf(pattern, "a", m.first.Format(time.RFC3339), m.last.Format(time.RFC3339), m.size, m.count)
		case n := <-r.C:
			p := filepath.Join(m.datadir, n.Format("20060102")) + ".log"
			f, err := os.Create(p)
			if err != nil {
				break
			}
			m.logger.SetOutput(f)

			m.file.Close()
			m.file = f
		}
	}
}
