package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"text/template"

	"github.com/gorilla/websocket"

	"github.com/busoc/panda/cmd/internal/opts"
	"github.com/busoc/panda/cmd/internal/pp"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

var commands = []*cli.Command{
	{
		Run:   runShow,
		Usage: "show [-a] [-g] [-s] <source>",
		Alias: []string{"dump"},
		Short: "",
	},
	{
		Run:   runDistrib,
		Usage: "distrib <config.toml>",
		Short: "",
	},
}

const helpText = `{{.Name}} prints PP packet headers.

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
	const pattern = "%s | %-12s | %x | %-12s | %6d | %5d | %-24x | %-v\n"

	var codes opts.UMISet
	cmd.Flag.Var(&codes, "u", "umi code")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	queue, err := pp.Packets(cmd.Flag.Arg(0), codes)
	if err != nil {
		return err
	}
	for p := range queue {
		u := p.UMIHeader
		fmt.Printf(pattern,
			u.Timestamp().Format("2006-01-02T15:04:05.000Z"),
			u.State,
			u.Code,
			u.Type,
			u.Unit,
			u.Length,
			p.Data,
			p.Value(),
		)
	}
	return nil
}

// func runDistrib(cmd *cli.Command, args []string) error {
// 	if err := cmd.Flag.Parse(args); err != nil {
// 		return err
// 	}
// 	f, err := os.Open(cmd.Flag.Arg(0))
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()
// 	c := struct {
// 		Addr     string `toml:"address"`
// 		Datadir  string `toml:"datadir"`
// 		Interval int    `toml:"interval"`
// 		Delay    int    `toml:"delay"`
// 	}{}
// 	if err := toml.NewDecoder(f).Decode(&c); err != nil {
// 		return err
// 	}
// 	if i, err := os.Stat(c.Datadir); !(err == nil || i.IsDir()) {
// 		return fmt.Errorf("invalid datadir %s")
// 	}
// 	a := distrib.UMIArchive{
// 		Datadir:  c.Datadir,
// 		Delay:    time.Duration(c.Delay) * time.Second,
// 		Interval: time.Duration(c.Interval) * time.Second,
// 	}
// 	return http.ListenAndServe(c.Addr, a)
// }

func runDistrib(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	c := struct {
		Addr    string `toml:"server"`
		Group   string `toml:"group"`
		Clients int32  `toml:"clients"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	f.Close()

	http.Handle("/", distribute(c.Group, c.Clients))
	return http.ListenAndServe(c.Addr, nil)
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
		var cs []uint64
		for _, v := range q["umi[]"] {
			c, err := strconv.ParseUint(v, 0, 64)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			cs = append(cs, c)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		queue, err := pp.Packets(a, cs)
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
