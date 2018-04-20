package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/gorilla/websocket"

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
		Short: "",
	},
	{
		Run:   runDistrib,
		Usage: "distrib <config.toml>",
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
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	queue, err := tm.Packets(cmd.Flag.Arg(0), *apid, pids)
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
		gaps[c.Apid()] = p
	}
	return nil
}

func runDistrib(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(flag.Arg(0))
	if err != nil {
		return err
	}
	v := struct {
		Addr    string `toml:"server"`
		Group   string `toml:"group"`
		Clients int32  `toml:"clients"`
		Apids   []int  `toml:"apid"`
	}{}
	if err := toml.NewDecoder(f).Decode(&v); err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	http.Handle("/", distribute(v.Group, v.Clients))
	return http.ListenAndServe(v.Addr, nil)
}

func distribute(a string, c int32) http.Handler {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
	var count int32
	f := func(w http.ResponseWriter, r *http.Request) {
		curr := atomic.AddInt32(&count, 1)
		if c > 0 && curr >= c {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		defer atomic.AddInt32(&count, -1)

		q := r.URL.Query()
		apid, err := strconv.Atoi(q.Get("apid"))
		if err != nil && len(q.Get("apid")) > 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		queue, err := tm.Packets(a, apid, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for p := range queue {
			bs, err := p.Bytes()
			if err != nil {
				continue
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, bs); err != nil {
				return
			}
		}
	}
	return http.HandlerFunc(f)
}
