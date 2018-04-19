package distrib

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/busoc/panda"
	"github.com/busoc/panda/hadock"
	"github.com/midbel/uuid"
)

type monkey struct {
	Origin   string
	Realtime bool
	Instance int32
}

func (m monkey) String() string {
	mode := "realtime"
	if !m.Realtime {
		mode = "playback"
	}
	return fmt.Sprintf("%s-%s-%03d", m.Origin, mode, m.Instance)
}

type pool struct {
	when time.Time
	ms   []*message
}

type message struct {
	Uid   string `json:"uid"`
	Count uint32 `json:"num"`
	*hadock.Message
}

type monitor struct {
	count    uint32
	mu       sync.RWMutex
	messages map[monkey]*pool
}

func Monitor(gs []string) (http.Handler, error) {
	m := &monitor{messages: make(map[monkey]*pool)}
	for _, g := range gs {
		a, err := net.ResolveUDPAddr("udp", g)
		if err != nil {
			return nil, err
		}
		r, err := net.ListenMulticastUDP("udp", nil, a)
		if err != nil {
			return nil, err
		}
		go m.Monitor(r, 300)
	}
	go m.Clean()
	return m, nil
}

func (m *monitor) Clean() {
	t := time.NewTicker(time.Minute * 5)
	for t := range t.C {
		m.mu.Lock()
		for n, w := range m.messages {
			if !w.when.IsZero() && t.Sub(w.when) > time.Minute*15 {
				delete(m.messages, n)
			}
		}
		m.mu.Unlock()
	}
}

func (m *monitor) Monitor(r io.ReadCloser, n int) {
	defer r.Close()

	for i, rs := 0, bufio.NewReader(r); ; i++ {
		g, err := decodeMessage(rs)
		if err != nil {
			continue
		}
		g.Generated = mud.GenerationTimeFromEpoch(g.Generated) / 1000
		k := monkey{
			Origin:   g.Origin,
			Realtime: g.Realtime,
			Instance: g.Instance,
		}
		m.mu.Lock()
		w, ok := m.messages[k]
		if !ok {
			w = new(pool)
			w.ms = make([]*message, n)
		}
		u, _ := uuid.UUID4()

		w.when = time.Now()
		w.ms[i%n] = &message{
			Message: g,
			Uid:     u.String(),
			Count:   atomic.AddUint32(&m.count, 1),
		}

		m.messages[k] = w
		m.mu.Unlock()
	}
}

func (m *monitor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.messages) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	ws := new(bytes.Buffer)
	var err error
	switch a := r.Header.Get("accept"); {
	default:
		w.WriteHeader(http.StatusNotAcceptable)
		return
	case isAcceptable(a, "application/json"):
		data := make(map[string][]*message)
		for k, w := range m.messages {
			s := k.String()
			for _, v := range w.ms {
				if v == nil {
					continue
				}
				data[s] = append(data[k.String()], v)
			}
			sort.Slice(data[s], func(i, j int) bool {
				return data[s][i].Generated >= data[s][j].Generated
			})
		}
		err = json.NewEncoder(ws).Encode(data)
	case isAcceptable(a, "application/xml"):
		err = xml.NewEncoder(ws).Encode(m.messages)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		//w.WriteHeader(http.StatusInternalServerError)
		return
	}
	io.Copy(w, ws)
}

func decodeMessage(r io.Reader) (*hadock.Message, error) {
	var m hadock.Message

	m.Origin, _ = readString(r)
	binary.Read(r, binary.BigEndian, &m.Sequence)
	binary.Read(r, binary.BigEndian, &m.Instance)
	binary.Read(r, binary.BigEndian, &m.Channel)
	binary.Read(r, binary.BigEndian, &m.Realtime)
	binary.Read(r, binary.BigEndian, &m.Count)
	binary.Read(r, binary.BigEndian, &m.Elapsed)
	binary.Read(r, binary.BigEndian, &m.Generated)
	binary.Read(r, binary.BigEndian, &m.Acquired)
	m.Reference, _ = readString(r)
	m.UPI, _ = readString(r)

	return &m, nil
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

// func joinPath(base string, v mud.HRPacket, i uint8, g int) (string, error) {
// 	switch i {
// 	case hadock.TEST:
// 		base = path.Join(base, "TEST")
// 	case hadock.SIM1, hadock.SIM2:
// 		base = path.Join(base, "SIM"+fmt.Sprint(i))
// 	case hadock.OPS:
// 		base = path.Join(base, "OPS")
// 	default:
// 		return "", fmt.Errorf("unknown instance %d", i)
// 	}
// 	switch v := v.(type) {
// 	default:
// 		return "", fmt.Errorf("unknown packet type %T", v)
// 	case *mud.Table:
// 		base = path.Join(base, "sciences")
// 	case *mud.Image:
// 		base = path.Join(base, "images")
// 	}
// 	if v.IsRealtime() {
// 		base = path.Join(base, "realtime", v.Origin())
// 	} else {
// 		base = path.Join(base, "playback", v.Origin())
// 	}
// 	t := v.Timestamp()
// 	year, doy, hour := fmt.Sprintf("%04d", t.Year()), fmt.Sprintf("%03d", t.YearDay()), fmt.Sprintf("%02d", t.Hour())
// 	base = path.Join(base, year, doy, hour)
// 	if m := t.Truncate(time.Second * time.Duration(g)); g > 0 {
// 		base = path.Join(base, fmt.Sprintf("%02d", m.Minute()))
// 	}
// 	return base, nil
// }
