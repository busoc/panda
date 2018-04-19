package distrib

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

func Limit(h http.Handler, n int, addrs ...string) http.Handler {
	if n <= 0 {
		return h
	}
	lim := &limiter{
		limit:    rate.Limit(n),
		visitors: make(map[string]*visitor),
	}
	go lim.purge()
	f := func(w http.ResponseWriter, r *http.Request) {
		if !lim.Allow(r) {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(f)
}

type limiter struct {
	limit rate.Limit

	mu       sync.RWMutex
	visitors map[string]*visitor
}

type visitor struct {
	when time.Time
	rate *rate.Limiter
}

func (l *limiter) Allow(r *http.Request) bool {
	l.mu.RLock()
	v, ok := l.visitors[r.RemoteAddr]
	l.mu.RUnlock()
	if !ok {
		v = &visitor{
			when: time.Now(),
			rate: rate.NewLimiter(l.limit, 10),
		}
		l.mu.Lock()
		l.visitors[r.RemoteAddr] = v
		l.mu.Unlock()
	}
	return v.Allow()
}

func (l *limiter) purge() {
	t := time.NewTicker(time.Second * 10)
	for t := range t.C {
		l.mu.RLock()
		var ns []string
		for n, v := range l.visitors {
			if t.Sub(v.when) >= time.Second*120 {
				ns = append(ns, n)
			}
		}
		l.mu.RUnlock()
		if len(ns) == 0 {
			continue
		}
		l.mu.Lock()
		for _, n := range ns {
			delete(l.visitors, n)
		}
		l.mu.Unlock()
	}
}

func (v *visitor) Allow() bool {
	v.when = time.Now()
	return v.rate.Allow()
}
