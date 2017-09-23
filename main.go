package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
)

var concurrency = flag.Int("c", 1, "Number of concurrent writers")
var duration = flag.Duration("d", 0, "The duration to run. If 0, run forever.")

var numOps uint64
var numSyncs uint64
var numBytes uint64

const (
	minLatency = 10 * time.Microsecond
	maxLatency = 1 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type pending struct {
	data []byte
	wg   sync.WaitGroup
}

type wal struct {
	f      *os.File
	commit struct {
		sync.Mutex
		cond       sync.Cond
		committing bool
		pending    []*pending
	}
	sync struct {
		sync.Mutex
		cond    sync.Cond
		pending []*pending
	}
}

func newWAL(path string) *wal {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	w := &wal{f: f}
	w.commit.cond.L = &w.commit.Mutex
	w.sync.cond.L = &w.sync.Mutex
	return w
}

func (w *wal) remove() {
	_ = os.Remove(w.f.Name())
}

func (w *wal) Write(d []byte) error {
	p := &pending{data: d}
	p.wg.Add(1)

	c := &w.commit
	c.Lock()
	leader := len(c.pending) == 0
	c.pending = append(c.pending, p)

	if leader {
		// We're the leader. Wait for any running commit to finish.
		for c.committing {
			c.cond.Wait()
		}
		pending := c.pending
		c.pending = nil
		c.committing = true
		c.Unlock()

		// for _, p := range pending[1:] {
		// 	d = append(d, p.data...)
		// }
		// if _, err := w.f.Write(d); err != nil {
		// 	log.Fatal(err)
		// }
		// atomic.AddUint64(&numBytes, uint64(len(d)))

		for _, p := range pending {
			if _, err := w.f.Write(p.data); err != nil {
				log.Fatal(err)
			}
			atomic.AddUint64(&numBytes, uint64(len(p.data)))
		}

		c.Lock()
		c.committing = false
		c.cond.Signal()
		c.Unlock()

		s := &w.sync
		s.Lock()
		if len(s.pending) == 0 {
			s.pending = pending
		} else {
			s.pending = append(s.pending, pending...)
		}
		s.cond.Signal()
		s.Unlock()
	} else {
		c.Unlock()
	}

	p.wg.Wait()
	atomic.AddUint64(&numOps, 1)
	return nil
}

func (w *wal) syncLoop() {
	s := &w.sync
	s.Lock()
	defer s.Unlock()

	for {
		for len(s.pending) == 0 {
			s.cond.Wait()
		}

		pending := s.pending
		s.pending = nil
		s.Unlock()

		if err := fdatasync(w.f); err != nil {
			log.Fatal(err)
		}
		atomic.AddUint64(&numSyncs, 1)

		for _, p := range pending {
			p.wg.Done()
		}

		s.Lock()
	}
}

type worker struct {
	wal     *wal
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newWorker(wal *wal) *worker {
	w := &worker{wal: wal}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

func (w *worker) run(wg *sync.WaitGroup) {
	defer wg.Done()

	rand := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	randBlock := func() []byte {
		data := make([]byte, rand.Intn(100)+300)
		for i := range data {
			data[i] = byte(rand.Int() & 0xff)
		}
		return data
	}

	for {
		start := time.Now()
		block := randBlock()
		if err := w.wal.Write(block); err != nil {
			log.Fatal(err)
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		w.latency.Unlock()
	}
}

func main() {
	flag.Parse()

	path := "testwal"
	if args := flag.Args(); len(args) > 0 {
		path = args[0]
		fmt.Printf("writing to %s\n", path)
	}

	wal := newWAL(path)
	defer wal.remove()
	go wal.syncLoop()
	workers := make([]*worker, *concurrency)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newWorker(wal)
		go workers[i].run(&wg)
	}

	tick := time.Tick(time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	start := time.Now()
	lastNow := start
	var lastOps uint64
	var lastSyncs uint64
	var lastBytes uint64

	for i := 0; ; i++ {
		select {
		case <-tick:
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			syncs := atomic.LoadUint64(&numSyncs)
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec__syncs/sec_____mb/sec____p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			fmt.Printf("%8s %10.1f %10.1f %10.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(syncs-lastSyncs)/elapsed.Seconds(),
				float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000,
			)
			lastNow = now
			lastOps = ops
			lastSyncs = syncs
			lastBytes = bytes

		case <-done:
			return
		}
	}
}
