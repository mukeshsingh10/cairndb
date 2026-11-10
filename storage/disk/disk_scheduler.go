package disk

import (
	"errors"
	"sync"

	"github.com/mukeshsingh10/cairndb/common"
)

var ErrSchedulerClosed = errors.New("DiskScheduler: closed")

// DiskRequest is a unit of work for DiskScheduler: read into
// Data or write data to the page
//
// The Caller owns the Data and must not touch in between Schedule
// and Wait: the background goroutines reads or writes it concurrently.
type DiskRequest struct {
	IsWrite bool
	PageID  common.PageID
	Data    []byte

	// done receives exactly one value (nil on sucess) when the request has
	// been carried out. It is buffered so worker never blocks on it.
	done chan error
}

// NewDiskRequest builds a request ready to be passed to Scheduler.
func NewDiskRequest(isWrite bool, pageID common.PageID, data []byte) *DiskRequest {
	return &DiskRequest{
		IsWrite: isWrite,
		PageID:  pageID,
		Data:    data,
		done:    make(chan error, 1),
	}
}

// Wait blocks until the request has completed and returns its IO error
// Call it only once per request.
func (r *DiskRequest) Wait() error {
	return <-r.done
}

// DiskScheduler serializes disk I/O through one background goroutine.
//
// Callers enqueue requests with Schedule (which returns immediately) and
// later call Wait on the request. Request are executed strictly in the order
// they were scheduled, so a read scheduled after a write to the same page
// always observes that write.
type DiskScheduler struct {
	dm    *DiskManager
	queue chan *DiskRequest

	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

func NewDiskScheduler(dm *DiskManager) *DiskScheduler {
	s := &DiskScheduler{
		dm:    dm,
		queue: make(chan *DiskRequest, 128),
	}
	s.wg.Add(1)
	go s.worker()
	return s
}

func (s *DiskScheduler) Schedule(r *DiskRequest) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrSchedulerClosed
	}
	s.queue <- r
	return nil
}

// Close stops accepting requests, finishes everything already queued, and
// stops the worker. It does not close the DiskManager.
func (s *DiskScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.queue)
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *DiskScheduler) worker() {
	defer s.wg.Done()
	for r := range s.queue {
		var err error
		if r.IsWrite {
			err = s.dm.WritePage(r.PageID, r.Data)
		} else {
			err = s.dm.ReadPage(r.PageID, r.Data)
		}
		r.done <- err
	}
}
