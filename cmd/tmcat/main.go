package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

func FetchPackets(s string, apid int, pids []uint32) (<-chan panda.Telemetry, error) {
	if i, err := os.Stat(s); err == nil && i.IsDir() {
		return tm.Packets(s, apid, pids)
	}
	if u, err := url.Parse(s); err == nil && u.Scheme == "ws" {
		o := *u
		o.Scheme = "http"
		c, err := websocket.Dial(u.String(), "", o.String())
		if err != nil {
			return nil, err
		}
		return tm.Filter(c, tm.NewDecoder(apid, pids)), nil
	}

	i, _, err := net.SplitHostPort(s)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(i); ip != nil && ip.IsMulticast() {
		return tm.Packets(s, apid, pids)
	}
	return nil, fmt.Errorf("can not fetch packets from %s", s)
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
		http.Handle("/"+g.Name+"/", websocket.Handler(g.Handle))
	}
	return http.ListenAndServe(c.Addr, nil)
}

type group struct {
	Name    string   `toml:"name"`
	Addr    string   `toml:"addr"`
	Apid    int      `toml:"apid"`
	Sources []uint32 `toml:"source"`

	limit int32
	count int32
}

func (g *group) Handle(ws *websocket.Conn) {
	defer ws.Close()

	curr := atomic.AddInt32(&g.count, 1)
	defer atomic.AddInt32(&g.count, -1)
	if g.limit > 0 && curr >= int32(g.limit) {
		return
	}
	c := websocket.Codec{
		Marshal: func(v interface{}) ([]byte, byte, error) {
			bs := v.([]byte)
			return bs, websocket.BinaryFrame, nil
		},
	}
	queue, err := tm.Packets(g.Addr, g.Apid, g.Sources)
	if err != nil {
		return
	}
	for p := range queue {
		bs, err := p.Bytes()
		if err != nil {
			continue
		}
		if err := c.Send(ws, bs); err != nil {
			return
		}
	}
}

func runReplay(cmd *cli.Command, args []string) error {
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
		if err := replay(c, *datadir, *rate); err != nil {
			return err
		}
	}

	return nil
}

func replay(c net.Conn, datadir string, rate int) error {
	if rate == 0 {
		rate++
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
		}
		prev = p.Timestamp()
	}
	return nil
}
