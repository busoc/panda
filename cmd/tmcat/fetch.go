package main

import (
	"fmt"
	"net"
	"net/url"
	"os"

	"github.com/busoc/panda"
	"github.com/busoc/panda/cmd/internal/tm"
	"golang.org/x/net/websocket"
)

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
