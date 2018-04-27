package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"golang.org/x/net/websocket"

	"github.com/busoc/panda"
	"github.com/busoc/panda/cmd/internal/opts"
	"github.com/busoc/panda/cmd/internal/tm"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

var commands = []*cli.Command{
	{
		Run:   runShow,
		Usage: "show [-a] [-g] [-s] <source>",
		Alias: []string{"dump"},
		Short: "dump packet headers",
	},
	{
		Run:   runDistrib,
		Usage: "distrib <config.toml>",
		Short: "",
	},
	{
		Run:   runReplay,
		Usage: "replay [-d] [-r] [-l] <group>",
		Short: "",
	},
	{
		Run:   runExtract,
		Usage: "extract [-c] [-n] <source>",
		Short: "",
	},
}

const helpText = `{{.Name}} prints TM packet headers.

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{printf "  %-9s %s" .String .Short}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

func main() {
	log.SetFlags(0)
	if err := cli.Run(commands, usage, nil); err != nil {
		log.Fatalln(err)
	}
}

func usage() {
	data := struct {
		Name     string
		Commands []*cli.Command
	}{
		Name:     filepath.Base(os.Args[0]),
		Commands: commands,
	}
	t := template.Must(template.New("help").Parse(helpText))
	t.Execute(os.Stderr, data)

	os.Exit(2)
}

func runShow(cmd *cli.Command, args []string) error {
	const pattern = "%s | %6d | %12s | %4d | %6d | %9d | %-16s | % x | %3s | %6d |%x\n"

	var pids opts.SIDSet
	cmd.Flag.Var(&pids, "p", "type")
	apid := cmd.Flag.Int("a", -1, "apid")
	sum := cmd.Flag.Bool("s", false, "sum")
	gps := cmd.Flag.Bool("g", false, "gps")
	debug := cmd.Flag.Bool("b", false, "debug")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	queue, err := FetchPackets(cmd.Flag.Arg(0), *apid, pids)
	if err != nil {
		return err
	}
	gaps := make(map[int]panda.Telemetry)
	for p := range queue {
		var s []byte
		if *sum {
			if bs, err := p.Bytes(); err == nil {
				is := md5.Sum(bs)
				s = is[:]
			}
		}
		var warning string

		c, e := p.CCSDSHeader, p.ESAHeader
		prev := gaps[c.Apid()]
		delta := c.Sequence() - prev.CCSDSHeader.Sequence()
		if !(delta == 1 || delta == -(1<<14)+1) {
			warning = "gap"
		}
		if delta < 0 && e.Timestamp().Sub(prev.ESAHeader.Timestamp()) >= time.Second {
			delta = (1 << 14) - 1 + c.Sequence() - prev.CCSDSHeader.Sequence()
		}
		fmt.Printf(pattern,
			panda.AdjustTime(e.Timestamp(), *gps).Format("2006-01-02T15:04:05.000Z"),
			c.Sequence(),
			c.SegmentationFlag(),
			c.Apid(),
			c.Len(),
			e.Sid,
			e.PacketType(),
			p.Data[:4],
			warning,
			delta,
			s,
		)
		if *debug {
			fmt.Println(hex.Dump(p.Payload()))
		}
		gaps[c.Apid()] = p
	}
	return nil
}

func runDistrib(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	c := struct {
		Addr   string   `toml:"addr"`
		Client int32    `toml:"client"`
		Groups []*group `toml:"group"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	f.Close()

	for _, g := range c.Groups {
		g.limit = c.Client
		var prefix string
		if _, _, err := net.SplitHostPort(g.Addr); err == nil {
			prefix = "/realtime/"
		} else {
			prefix = "/replay/"
		}
		http.Handle(prefix+filepath.Clean(g.Name), websocket.Handler(g.Handle))
	}
	return http.ListenAndServe(c.Addr, nil)
}

var codec = websocket.Codec{
	Marshal: func(v interface{}) ([]byte, byte, error) {
		p, ok := v.(panda.Telemetry)
		if !ok {
			return nil, websocket.UnknownFrame, fmt.Errorf("%T", p)
		}
		bs, err := p.Bytes()
		return bs, websocket.BinaryFrame, err
	},
}

type group struct {
	Name     string    `toml:"name"`
	Addr     string    `toml:"addr"`
	Apid     int       `toml:"apid"`
	Sources  []uint32  `toml:"source"`
	Date     time.Time `toml:"limit"`
	Delay    int64     `toml:"delay"`
	Interval int64     `toml:"interval"`

	limit int32
	count int32
}

func (g *group) handleRealtime(r *http.Request) (<-chan panda.Telemetry, int, error) {
	q, err := tm.Packets(g.Addr, g.Apid, g.Sources)
	return q, 0, err
}

func (g *group) handleReplay(r *http.Request) (<-chan panda.Telemetry, int, error) {
	rate := 1
	q := r.URL.Query()
	rate = 1
	if r, err := strconv.ParseUint(q.Get("rate"), 10, 64); err == nil && (r >= 1 && r <= 5) {
		rate = int(r)
	}
	n := time.Now().UTC().Truncate(time.Minute * 5)
	dtend := n.Add(time.Duration(-g.Delay) * time.Second)
	dtstart := dtend.Add(time.Duration(-g.Interval) * time.Second)
	if d, err := time.Parse(time.RFC3339, q.Get("dtstart")); err == nil {
		dtstart = d.Truncate(time.Minute * 5)
	}
	if d, err := time.Parse(time.RFC3339, q.Get("dtend")); err == nil {
		dtend = d.Add(time.Minute * 5).Truncate(time.Minute * 5)
	}
	apid := g.Apid
	if r, err := strconv.ParseInt(q.Get("apid"), 10, 64); err == nil {
		apid = int(r)
	}
	queue := make(chan panda.Telemetry)
	go func() {
		defer close(queue)
		for w := dtstart; w.Before(dtend); w = w.Add(time.Minute * 5) {
			y, d, h, m := w.Year(), w.YearDay(), w.Hour(), w.Minute()
			n := fmt.Sprintf("rt_%02d_%02d.dat", m, m+4)
			p := filepath.Join(g.Addr, fmt.Sprintf("%04d", y), fmt.Sprintf("%03d", d), fmt.Sprintf("%02d", h), n)
			q, err := tm.Packets(p, apid, g.Sources)
			if err != nil {
				return
			}
			for p := range q {
				queue <- p
			}
		}
	}()
	return queue, rate, nil
}

func (g *group) Handle(ws *websocket.Conn) {
	defer ws.Close()

	curr := atomic.AddInt32(&g.count, 1)
	defer atomic.AddInt32(&g.count, -1)
	if g.limit > 0 && curr >= int32(g.limit) {
		return
	}
	var (
		prev  time.Time
		delta time.Duration
		rate  int
		err   error
		queue <-chan panda.Telemetry
	)
	if r := ws.Request(); strings.HasPrefix(r.URL.Path, "/replay/") {
		queue, rate, err = g.handleReplay(r)
	} else {
		queue, rate, err = g.handleRealtime(r)
	}
	if err != nil {
		return
	}
	for p := range queue {
		if !prev.IsZero() && rate == 0 && prev.After(p.Timestamp()) {
			continue
		}
		wait(delta, rate)
		if err := codec.Send(ws, p); err != nil {
			return
		}
		if !prev.IsZero() && rate > 0 {
			delta = p.Timestamp().Sub(prev)
		}
		prev = p.Timestamp()
	}
}

func wait(d time.Duration, r int) {
	if d == 0 {
		return
	}
	d = d / time.Duration(r)
	if d < time.Millisecond*10 {
		d = time.Millisecond * 10
	}
	time.Sleep(d)
}

func runReplay(cmd *cli.Command, args []string) error {
	var gap opts.Gap
	cmd.Flag.Var(&gap, "g", "gap")
	loop := cmd.Flag.Int("l", 0, "loop")
	rate := cmd.Flag.Int("r", 0, "rate")
	datadir := cmd.Flag.String("d", os.TempDir(), "datadir")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	c, err := net.Dial("udp", cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer c.Close()
	for i := 0; *loop <= 0 || i < *loop; i++ {
		if err := replay(c, *datadir, *rate, gap); err != nil {
			return err
		}
	}

	return nil
}

func replay(c net.Conn, datadir string, rate int, gap opts.Gap) error {
	if rate == 0 {
		rate++
	}
	if !gap.IsZero() {
		c = &conn{
			Conn:   c,
			gap:    gap,
			writer: c,
			after:  time.After(gap.Next()),
		}
	}
	queue, err := tm.Packets(datadir, 0, nil)
	if err != nil {
		return err
	}
	var (
		prev  time.Time
		delta time.Duration
		buf   bytes.Buffer
	)
	for p := range queue {
		time.Sleep(delta / time.Duration(rate))

		n := time.Duration(time.Now().UnixNano())
		s, ns := n/time.Second, n/time.Millisecond

		binary.Write(&buf, binary.BigEndian, uint8(panda.TagTM))
		binary.Write(&buf, binary.BigEndian, uint32(s))
		binary.Write(&buf, binary.BigEndian, uint8(ns)%255)
		binary.Write(&buf, binary.BigEndian, uint32(0))
		bs, err := p.Bytes()
		if err != nil {
			return err
		}
		buf.Write(bs)
		if _, err := io.Copy(c, &buf); err != nil {
			return err
		}
		if !prev.IsZero() {
			delta = p.Timestamp().Sub(prev)
			if delta > time.Hour {
				delta = 0
			}
		}
		prev = p.Timestamp()
	}
	return nil
}

type conn struct {
	net.Conn
	gap opts.Gap

	writer io.Writer
	after  <-chan time.Time
}

func (c *conn) Write(bs []byte) (int, error) {
	select {
	case <-c.after:
		var (
			d time.Duration
			w io.Writer
		)
		if t, ok := c.gap.Wait(); !ok {
			w, d = ioutil.Discard, t
		} else {
			w, d = c.Conn, t
		}
		c.writer, c.after = w, time.After(d)
	default:
	}
	return c.writer.Write(bs)
}
