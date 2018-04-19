package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/hadock"
	"github.com/midbel/cli"
)

func runMonitor(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	r, err := readMessages(cmd.Flag.Args())
	if err != nil {
		return err
	}
	for {
		m, err := decodeMessage(r)
		if err != nil {
			log.Fatalln(err)
		}
		mode := "realtime"
		if !m.Realtime {
			mode = "playback"
		}
		log.Printf("%s | %9d | %3d | %d | %12s | %9d | %12s | %6.3g | %s | %s | %s",
			m.Origin,
			m.Sequence,
			m.Instance,
			m.Channel,
			mode,
			m.Count,
			m.Elapsed,
			float64(m.Count)/m.Elapsed.Seconds(),
			mud.AdjustGenerationTime(m.Generated).Format(time.RFC3339),
			time.Unix(m.Acquired, 0).Format(time.RFC3339),
			m.Reference,
		)
	}
}

func readMessages(gs []string) (io.Reader, error) {
	pr, pw := io.Pipe()
	for _, g := range gs {
		a, err := net.ResolveUDPAddr("udp", g)
		if err != nil {
			return nil, err
		}
		c, err := net.ListenMulticastUDP("udp", nil, a)
		if err != nil {
			return nil, err
		}
		go func(rc io.ReadCloser) {
			defer rc.Close()
			for {
				_, err := io.Copy(pw, rc)
				e, ok := err.(net.Error)
				if !ok {
					log.Println(err)
					return
				}
				if !(e.Temporary() || e.Timeout()) {
					log.Println(err)
					return
				}
			}
		}(c)
	}
	return bufio.NewReader(pr), nil
}

func decodeMessage(r io.Reader) (*hadock.Message, error) {
	var err error
	m := new(hadock.Message)

	if m.Origin, err = readString(r); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Sequence); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Channel); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Realtime); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Count); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Elapsed); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Generated); err != nil {
		return nil, err
	}
	if err = binary.Read(r, binary.BigEndian, &m.Acquired); err != nil {
		return nil, err
	}
	if m.Reference, err = readString(r); err != nil {
		return nil, err
	}
	if m.UPI, err = readString(r); err != nil {
		return nil, err
	}
	return m, nil
}

func readString(r io.Reader) (string, error) {
	var z uint16
	if err := binary.Read(r, binary.BigEndian, &z); err != nil {
		return "", err
	}
	bs := make([]byte, int(z))
	if _, err := r.Read(bs); err != nil {
		return "", err
	}
	return string(bs), nil
}
