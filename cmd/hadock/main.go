package main

import (
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/midbel/cli"
)

const helpText = `{{.Name}} process VMU packets.

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{printf "  %-9s %s" .String .Short}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

var commands = []*cli.Command{
	{
		Usage: "replay [-r] [-s] [-m] [-t] [-d] <host:port>",
		Short: "send VMU packets throught the network from a HRDP archive",
		Run:   runReplay,
	},
	{
		Usage: "listen <hdk.toml>",
		Short: "store packets in archive",
		Run:   runListen,
	},
	{
		Usage: "distrib <hdk.toml>",
		Short: "distribute files stored in archive",
		Run:   runDistrib,
	},
	{
		Usage: "monitor <group...>",
		Short: "monitor hadock activities",
		Run:   runMonitor,
	},
}

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
