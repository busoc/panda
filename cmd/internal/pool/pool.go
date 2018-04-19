package pool

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	ErrAlreadyRunning = errors.New("worker already running")
	ErrNotYetRunning  = errors.New("worker not yet running")
)

type Worker interface {
	Run(string, string, bool) error
	Status() State
	io.Closer
	fmt.Stringer
}

type State struct {
	Id      string    `json:"worker"`
	Count   int       `json:"count"`
	Size    int       `json:"size"`
	Running bool      `json:"running"`
	Last    time.Time `json:"last"`
}

type worker struct {
	Worker
	Auto bool
}

type Pool struct {
	Addr    string
	Datadir string
	Compat  bool

	workers map[string]Worker

	enter    chan worker
	leave    chan string
	start    chan string
	stop     chan string
	failures chan error
}

func New(a, d string, ws []Worker) (*Pool, error) {
	if err := os.MkdirAll(d, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	workers := make(map[string]Worker)
	for _, w := range ws {
		workers[w.String()] = w
	}
	return &Pool{
		Addr:     a,
		Datadir:  d,
		Compat:   false,
		enter:    make(chan worker),
		leave:    make(chan string),
		start:    make(chan string),
		stop:     make(chan string),
		failures: make(chan error),
		workers:  workers,
	}, nil
}

func (p *Pool) Status() []State {
	var vs []State
	for _, w := range p.workers {
		vs = append(vs, w.Status())
	}
	return vs
}

func (p *Pool) Start(n string) error {
	p.start <- n
	return p.check()
}

func (p *Pool) Stop(n string) error {
	p.stop <- n
	return p.check()
}

func (p *Pool) Register(w Worker, s bool) error {
	p.enter <- worker{w, s}
	return p.check()
}

func (p *Pool) Unregister(n string) error {
	p.leave <- n
	return p.check()
}

func (p *Pool) check() error {
	select {
	case e, ok := <-p.failures:
		if !ok {
			return fmt.Errorf("done")
		}
		return e
	default:
		return nil
	}
}

func (p *Pool) Run(a bool) error {
	run := func(w Worker, wg *sync.WaitGroup) {
		if err := w.Run(p.Addr, p.Datadir, p.Compat); err != nil {
			log.Printf("%s: %s", w.String(), err)
		}
		wg.Done()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Kill, os.Interrupt)

	var wg sync.WaitGroup
	if a {
		wg.Add(len(p.workers))
		for _, w := range p.workers {
			go func(w Worker) {
				run(w, &wg)
			}(w)
		}
	}
	for {
		select {
		case w := <-p.enter:
			if _, ok := p.workers[w.String()]; ok {
				p.failures <- fmt.Errorf("%s: already registered", w.String())
				break
			}
			p.workers[w.String()] = w.Worker
			if w.Auto {
				p.start <- w.String()
			}
		case n := <-p.leave:
			if w, ok := p.workers[n]; !ok {
				p.failures <- fmt.Errorf("%s: not registered", n)
			} else {
				w.Close()
				delete(p.workers, n)
			}
		case n := <-p.start:
			if w, ok := p.workers[n]; !ok {
				p.failures <- fmt.Errorf("%s: not registered", n)
			} else {
				wg.Add(1)
				go run(w, &wg)
			}
		case n := <-p.stop:
			if w, ok := p.workers[n]; !ok {
				p.failures <- fmt.Errorf("%s: not registered", n)
			} else {
				w.Close()
			}
		case <-sig:
			for _, w := range p.workers {
				w.Close()
			}
			wg.Wait()
			return nil
		}
	}
}
