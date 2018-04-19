package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/busoc/panda/hadock/distrib"
	"github.com/gorilla/handlers"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

func runDistrib(cmd *cli.Command, args []string) error {
	quiet := cmd.Flag.Bool("q", false, "quiet mode")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()

	c := struct {
		Address string   `toml:"address"`
		Rate    int      `toml:"ratelimit"`
		Rawdir  string   `toml:"rawdir"`
		Datadir string   `toml:"datadir"`
		Groups  []string `toml:"groups"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	i, err := os.Stat(c.Rawdir)
	if err != nil {
		return err
	}
	if !i.IsDir() {
		return fmt.Errorf("%s not a directory", c.Rawdir)
	}
	if h, err := distrib.Browse(c.Rawdir); err == nil {
		http.Handle("/browse/", http.StripPrefix("/browse/", h))
	} else {
		log.Println("browse:", err)
	}
	if h, err := distrib.Monitor(c.Groups); err == nil {
		http.Handle("/monitor/", h)
	} else {
		log.Println("monitor:", err)
	}
	if h, err := distrib.Fetch(c.Rawdir, c.Datadir); err == nil {
		http.Handle("/products/", http.StripPrefix("/products/", distrib.Limit(h, c.Rate)))
	} else {
		log.Println("products:", err)
	}
	if h, err := distrib.Download(c.Rawdir); err == nil {
		http.Handle("/archives/", http.StripPrefix("/archives/", h))
	} else {
		log.Println("archives:", err)
	}
	opts := []handlers.CORSOption{
		handlers.AllowedHeaders([]string{"if-modified-since"}),
		handlers.ExposedHeaders([]string{"last-modified"}),
	}
	h := handlers.CORS(opts...)(http.DefaultServeMux)
	if !*quiet {
		h = handlers.LoggingHandler(os.Stderr, h)
	}
	return http.ListenAndServe(c.Address, h)
}
