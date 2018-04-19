package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"plugin"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/hadock"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

type module struct {
	Location string   `json:"location"`
	Config   []string `json:"config"`
}

type storer struct {
	Disabled    bool   `toml:"disabled"`
	Scheme      string `toml:"type"`
	Location    string `toml:"location"`
	Hard        string `toml:"link"`
	Raw         bool   `toml:"raw"`
	Granularity uint   `toml:"interval"`
}

type proxy struct {
	Addr  string `toml:"address"`
	Level string `toml:"level"`
}

type pool struct {
	Interval  uint       `toml:"interval"`
	Notifiers []notifier `toml:"notifiers"`
}

type notifier struct {
	Scheme   string        `toml:"type"`
	Location string        `toml:"location"`
	Source   string        `toml:"source"`
	Instance int32         `toml:"instance"`
	Channels []mud.Channel `toml:"channels"`
}

type decodeFunc func(io.Reader, []uint8) <-chan *hadock.Packet

func runListen(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	c := struct {
		Addr      string   `toml:"address"`
		Mode      string   `toml:"mode"`
		Proxy     proxy    `toml:"proxy"`
		Instances []uint8  `toml:"instances"`
		Age       uint     `toml:"age"`
		Stores    []storer `toml:"storage"`
		Pool      pool     `toml:"pool"`
		Modules   []module `toml:"module"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	} else {
		f.Close()
	}
	fs, err := setupStorage(c.Stores)
	if err != nil {
		return err
	}
	pool, err := setupPool(c.Pool)
	if err != nil {
		return err
	}
	ms, err := setupModules(c.Modules)
	if err != nil {
		return err
	}
	var df decodeFunc
	switch c.Mode {
	case "rfc1952", "gzip":
		df = hadock.DecodeCompressedPackets
	case "binary", "":
		df = hadock.DecodeBinaryPackets
	case "binary+gzip":
		df = func(r io.Reader, is []uint8) <-chan *hadock.Packet {
			if _, ok := r.(io.ByteReader); ok {
				r = bufio.NewReader(r)
			}
			if rs, err := gzip.NewReader(r); err == nil {
				//defer rs.Close()
				r = rs
			}
			return hadock.DecodeBinaryPackets(r, is)
		}
	default:
		return fmt.Errorf("unsupported working mode %s", c.Mode)
	}
	ps, err := ListenPackets(c.Addr, c.Proxy, df, c.Instances)
	if err != nil {
		return err
	}
	queue := make(chan *hadock.Item, 100)
	defer close(queue)
	go func() {
		for i := range queue {
			if err := ms.Process(uint8(i.Instance), i.HRPacket); err != nil {
				log.Println(err)
			}
		}
	}()

	age := time.Second * time.Duration(c.Age)
	for i := range Convert(ps) {
		if err := fs.Store(uint8(i.Instance), i.HRPacket); err != nil {
			log.Println(err)
		}
		if age == 0 || time.Since(i.Timestamp()) <= age {
			pool.Notify(i)
		}
		queue <- i
	}
	return nil
}

func Convert(ps <-chan *hadock.Packet) <-chan *hadock.Item {
	q := make(chan *hadock.Item)
	go func() {
		ds := make(map[int]mud.Decoder)
		for _, v := range []int{mud.VMUProtocol1, mud.VMUProtocol2} {
			d, err := mud.DecodeHR(v)
			if err != nil {
				continue
			}
			ds[v] = d
		}

		defer close(q)
		for p := range ps {
			d, ok := ds[int(p.Version)]
			if !ok {
				continue
			}
			_, v, err := d.Decode(p.Payload)
			if err != nil {
				log.Println(err)
			}
			switch v.(type) {
			case *mud.Table, *mud.Image:
				hr := v.(mud.HRPacket)
				q <- &hadock.Item{int32(p.Instance), hr}
			default:
				continue
			}
		}
	}()
	return q
}

func ListenPackets(a string, p proxy, decode decodeFunc, is []uint8) (<-chan *hadock.Packet, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	q := make(chan *hadock.Packet, 100)
	go func() {
		defer func() {
			s.Close()
			close(q)
		}()
		for {
			c, err := s.Accept()
			if err != nil {
				return
			}
			if c, ok := c.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
				c.SetKeepAlivePeriod(time.Second * 90)
			}
			go func(c net.Conn) {
				defer c.Close()

				var r io.Reader = c
				if c, err := hadock.DialProxy(p.Addr, p.Level); err == nil {
					defer c.Close()
					r = io.TeeReader(r, c)
				}
				for p := range decode(r, is) {
					q <- p
				}
				log.Printf("connection closed: %s", c.RemoteAddr())
			}(c)
		}
	}()
	return q, nil
}

func setupModules(ms []module) (hadock.Module, error) {
	var ps []hadock.Module
	for _, m := range ms {
		p, err := plugin.Open(m.Location)
		if err != nil {
			return nil, err
		}
		n, err := p.Lookup("New")
		if err != nil {
			return nil, err
		}
		switch n := n.(type) {
		case func(string) (hadock.Module, error):
			for _, c := range m.Config {
				i, err := n(c)
				if err != nil {
					continue
				}
				ps = append(ps, i)
			}
		case func() (hadock.Module, error):
			i, err := n()
			if err != nil {
				continue
			}
			ps = append(ps, i)
		default:
			return nil, fmt.Errorf("invalid module function: %T", n)
		}
	}
	return hadock.Process(ps), nil
}

func setupPool(p pool) (*hadock.Pool, error) {
	delay := time.Second * time.Duration(p.Interval)

	ns := make([]hadock.Notifier, 0, len(p.Notifiers))
	for _, v := range p.Notifiers {
		var (
			err error
			n   hadock.Notifier
		)
		o := &hadock.Options{
			Source:   v.Source,
			Instance: v.Instance,
			Channels: v.Channels,
		}
		switch v.Scheme {
		default:
			continue
		case "udp":
			n, err = hadock.NewExternalNotifier(v.Scheme, v.Location, o)
		case "logger":
			var w io.Writer
			switch v.Location {
			default:
				f, e := os.Create(v.Location)
				if e != nil {
					return nil, err
				}
				w = f
			case "/dev/null":
				w = ioutil.Discard
			case "":
				w = os.Stdout
			}
			n, err = hadock.NewDebuggerNotifier(w, o)
		}
		if err != nil {
			return nil, err
		}
		ns = append(ns, n)
	}
	return hadock.NewPool(ns, delay), nil
}

func setupStorage(vs []storer) (hadock.Storage, error) {
	if len(vs) == 0 {
		return nil, fmt.Errorf("no storage defined! abort")
	}
	fs := make([]hadock.Storage, 0, len(vs))
	for _, v := range vs {
		if v.Disabled {
			continue
		}
		var (
			err error
			s   hadock.Storage
		)
		switch v.Scheme {
		default:
			continue
		case "file":
			if err := os.MkdirAll(v.Location, 0755); err != nil {
				break
			}
			if err := os.MkdirAll(v.Hard, 0755); v.Hard != "" && err != nil {
				break
			}
			s, err = hadock.NewLocalStorage(v.Location, v.Hard, int(v.Granularity), v.Raw)
		case "http":
			s, err = hadock.NewHTTPStorage(v.Location, int(v.Granularity))
		case "hrdp":
			if err := os.MkdirAll(v.Location, 0755); err != nil {
				break
			}
			s, err = hadock.NewHRDPStorage(v.Location, 2)
		}
		if err != nil {
			return nil, err
		}
		fs = append(fs, s)
	}
	return hadock.Multistore(fs...), nil
}
