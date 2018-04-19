package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/hadock"
	"github.com/midbel/cli"
	"github.com/midbel/rustine/sum"
)

func runReplay(cmd *cli.Command, args []string) error {
	rate := cmd.Flag.Int("r", 0, "rate")
	size := cmd.Flag.Int("s", 0, "chunk size")
	mode := cmd.Flag.Int("m", hadock.OPS, "mode")
	gap := cmd.Flag.Int("g", 0, "gap")
	vmu := cmd.Flag.Int("t", mud.VMUProtocol2, "vmu version")
	datadir := cmd.Flag.String("d", "", "archive")
	gz := cmd.Flag.Bool("z", false, "rfc1952")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	c, err := Replay(cmd.Flag.Arg(0), *size, *vmu, *mode, *gz)
	if err != nil {
		return err
	}
	queue, err := walk(*datadir)
	if err != nil {
		return err
	}
	if *rate < 1 {
		*rate = 1
	}
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Kill, os.Interrupt)

	n := time.Now()
	var (
		count uint64
		bytes uint64
	)
	defer func() {
		tick.Stop()
		c.Close()
		log.Printf("%d packets (%.2fKB) processed in %s", count, float64(bytes)/1024, time.Since(n))
	}()
	if *gap > 0 {
		rand.Seed(time.Now().Unix())
	}
	skip := true
	for i, j := 0, 0; ; i++ {
		select {
		case <-tick.C:
			if *gap > 0 && i == j {
				j = i + rand.Intn(*gap)
				skip = !skip
			}
			bs, ok := <-queue
			if !ok {
				return nil
			}
			if *gap > 0 && skip {
				continue
			}
			if _, err := c.Write(bs); err != nil {
				log.Println(err)
				if err, ok := err.(net.Error); ok && !err.Temporary() {
					return nil
				}
			}
			count, bytes = count+1, bytes+uint64(len(bs))
		case <-sig:
			return nil
		}
	}
}

func walk(d string) (<-chan []byte, error) {
	i, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	if !i.IsDir() {
		return nil, fmt.Errorf("%s not a directory", d)
	}
	q := make(chan []byte)
	go func() {
		defer close(q)

		buf := make([]byte, 4*1024*1024)
		err := filepath.Walk(d, func(p string, i os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if i.IsDir() {
				return nil
			}
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()

			s := bufio.NewScanner(f)
			s.Buffer(buf, len(buf))
			s.Split(scanVMUPackets)
			for s.Scan() {
				q <- s.Bytes()
			}
			return s.Err()
		})
		if err != nil {
			log.Println(err)
		}
	}()
	return q, nil
}

func scanVMUPackets(bs []byte, ateof bool) (int, []byte, error) {
	if len(bs) < 4 {
		return 0, nil, nil
	}
	s := int(binary.LittleEndian.Uint32(bs[:4]) + 4)
	if s >= len(bs) {
		return 0, nil, nil
	}
	vs := make([]byte, s-mud.HRDPHeaderLength-mud.HRDLSyncLength-4)
	copy(vs, bs[4+mud.HRDPHeaderLength+mud.HRDLSyncLength:])
	return s, vs, nil
}

type replay struct {
	net.Conn
	size       int
	counter    uint16
	version    uint16
	compressed bool
}

func Replay(a string, s, t, m int, z bool) (net.Conn, error) {
	c, err := net.Dial("tcp", a)
	if err != nil {
		return nil, err
	}
	p := hadock.HadockVersion2
	if s <= 0 {
		p = hadock.HadockVersion1
	}
	if z {
		switch s {
		case gzip.NoCompression, gzip.BestSpeed, gzip.BestCompression, gzip.HuffmanOnly:
		default:
			s = gzip.DefaultCompression
		}
	}
	v := uint16(p)<<12 | uint16(t)<<8 | uint16(m)
	return &replay{Conn: c, size: s, version: v, compressed: z}, nil
}

func (r *replay) Write(bs []byte) (int, error) {
	defer func() {
		r.counter++
	}()
	if r.compressed {
		return r.writeCompressed(bs)
	}
	return r.writePacket(bs)
}

func (r *replay) writeCompressed(bs []byte) (int, error) {
	g, err := gzip.NewWriterLevel(r.Conn, r.size)
	if err != nil {
		return 0, err
	}
	defer g.Close()

	g.Header.Extra = make([]byte, 2)
	binary.BigEndian.PutUint16(g.Header.Extra, r.version)
	n, err := g.Write(bs)
	if err != nil {
		return n, err
	}
	if err := g.Flush(); err != nil {
		return 0, err
	}
	return n, nil
}

func (r *replay) writePacket(bs []byte) (int, error) {
	if r.size <= 0 {
		_, err := io.Copy(r.Conn, r.preparePacketV1(bs))
		return len(bs), err
	}
	for _, w := range r.preparePacketV2(bs) {
		var total int
		if c, err := io.Copy(r.Conn, w); err != nil {
			return total + int(c), err
		} else {
			total += int(c)
		}
	}
	return len(bs), nil
}

func (r *replay) preparePacketV1(bs []byte) io.Reader {
	w := new(bytes.Buffer)
	binary.Write(w, binary.BigEndian, hadock.Preamble)
	binary.Write(w, binary.BigEndian, r.version)
	binary.Write(w, binary.BigEndian, r.counter)
	binary.Write(w, binary.BigEndian, uint32(len(bs)))
	w.Write(bs)
	binary.Write(w, binary.BigEndian, sum.Sum1071(bs))

	return w
}

func (r *replay) preparePacketV2(bs []byte) []io.Reader {
	re := bytes.NewBuffer(bs)
	c := re.Len() / r.size
	rs := make([]io.Reader, 0, c)
	for i := 0; re.Len() > 0; i++ {
		vs := re.Next(r.size)
		s := sum.Sum1071(vs)

		w := new(bytes.Buffer)
		binary.Write(w, binary.BigEndian, hadock.Preamble)
		binary.Write(w, binary.BigEndian, r.version)
		binary.Write(w, binary.BigEndian, uint16(i))
		binary.Write(w, binary.BigEndian, uint16(c))
		binary.Write(w, binary.BigEndian, r.counter)
		binary.Write(w, binary.BigEndian, uint32(len(vs)))
		w.Write(vs)
		binary.Write(w, binary.BigEndian, s)

		rs = append(rs, w)
	}
	return rs
}
