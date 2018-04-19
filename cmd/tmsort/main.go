package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"text/template"
	"time"

	"github.com/busoc/panda/cmd/internal/pool"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

var commands = []*cli.Command{
	{
		Usage: "dispatch <config.json>",
		Short: "filter telemetry packets coming from multicast group",
		Run:   runDispatch,
	},
	{
		Usage: "filter [-a] [-n] [-d] [-c] [-e] <path...>",
		Short: "filter telemetry packets from the HRDP archive",
		Run:   runFilter,
	},
	{
		Usage: "distrib <config.toml>",
		Short: "",
		Run:   runDistrib,
	},
}

const helpText = `{{.Name}} captures and filter TM packets from multicast stream
or from HRDP archive

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{printf "  %-9s %s" .String .Short}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

func main() {
	log.SetFlags(0)
	usage := func() {
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
	if err := cli.Run(commands, usage, nil); err != nil {
		log.Fatalln(err)
	}
}

func runDistrib(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return fmt.Errorf("unable to open configuration file: %s", err)
	}
	defer f.Close()

	c := struct {
		Addr     string `toml:"address"`
		Prefix   string `toml:"prefix"`
		Datadir  string `toml:"datadir"`
		Apids    []int  `toml:"apids"`
		Delay    int    `toml:"delay"`
		Interval int    `toml:"interval"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return fmt.Errorf("invalid settings provided: %s", err)
	}
	a := &Archive{
		Datadir:  c.Datadir,
		Apids:    c.Apids,
		Delay:    time.Duration(c.Delay) * time.Second,
		Interval: time.Duration(c.Interval) * time.Second,
	}
	http.Handle("/", a)
	return http.ListenAndServe(c.Addr, nil)
}

func runDispatch(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return fmt.Errorf("unable to open configuration file: %s", err)
	}
	defer f.Close()

	v := struct {
		Addr    string    `json:"addr"`
		Datadir string    `json:"datadir"`
		Monitor string    `json:"monitor"`
		Prefix  string    `json:"prefix"`
		Auto    bool      `json:"auto"`
		Workers []*Worker `json:"workers"`
	}{}
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		return fmt.Errorf("invalid settings provided: %s", err)
	}
	ws := make([]pool.Worker, len(v.Workers))
	for i := range v.Workers {
		ws[i] = v.Workers[i]
	}
	p, err := pool.New(v.Addr, v.Datadir, ws)
	if err != nil {
		return err
	}
	if _, _, err := net.SplitHostPort(v.Monitor); err == nil {
		http.Handle(joinPath(v.Prefix, "workers"), &Handler{now: time.Now(), Pool: p})
		s := &http.Server{Addr: v.Monitor, Handler: nil}
		go func() {
			defer s.Close()
			log.Printf("start monitoring and controlling at %s", s.Addr)
			if err := s.ListenAndServe(); err != nil {
				log.Println(err)
			}
		}()
	} else {
		v.Auto = true
	}
	return p.Run(v.Auto)
}

func joinPath(p, s string) string {
	p = path.Join("/", p, s)
	return path.Clean(p) + "/"
}

func runFilter(cmd *cli.Command, args []string) error {
	apid := cmd.Flag.Int("a", -1, "apid")
	label := cmd.Flag.String("n", "", "label")
	datadir := cmd.Flag.String("d", os.TempDir(), "datadir")
	every := cmd.Flag.Duration("e", time.Minute*5, "every")
	parallel := cmd.Flag.Int("p", 4, "parallel processing")
	when := cmd.Flag.Duration("w", 0, "when")
	//flat := cmd.Flag.Bool("f", true, "flat layout")

	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	if *parallel <= 0 {
		*parallel = 1
	}

	var wg sync.WaitGroup

	sema := make(chan struct{}, *parallel)
	for _, a := range cmd.Flag.Args() {
		if *when > 0 {
			t := time.Now().UTC().Add(-*when).Truncate(time.Hour)
			a = filepath.Join(a, fmt.Sprintf("%04d", t.Year()), fmt.Sprintf("%03d", t.YearDay()), fmt.Sprintf("%02d", t.Hour()))
		}
		sema <- struct{}{}
		wg.Add(1)
		go func(a string) {
			log.Printf("start sorting TMs from %s (stored to %s)", a, *datadir)
			w := NewWorker(*label, *apid, *every)
			if err := w.Run(a, *datadir, false); err != nil {
				log.Println(err)
			}
			wg.Done()
			<-sema
			log.Printf("done sorting TMs from %s", a)
		}(a)
	}
	wg.Wait()
	return nil
}
