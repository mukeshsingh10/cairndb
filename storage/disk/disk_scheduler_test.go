package disk

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mukeshsingh10/cairndb/common"
)

func newTestScheduler(t *testing.T) (*DiskManager, *DiskScheduler) {
	t.Helper()
	dm, err := NewDiskManager(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	s := NewDiskScheduler(dm)
	t.Cleanup(func() {
		dm.Close()
		s.Close()
	})
	return dm, s
}

func do(t *testing.T, s *DiskScheduler, r *DiskRequest) {
	t.Helper()
	if err := s.Schedule(r); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerWriteThenRead(t *testing.T) {
	_, s := newTestScheduler(t)
	do(t, s, NewDiskRequest(true, 3, pageOf(0xAB)))
	buf := make([]byte, common.PageSize)
	do(t, s, NewDiskRequest(false, 3, buf))
	if !bytes.Equal(buf, pageOf(0xAB)) {
		t.Error("read didn't return what was written")
	}
}

func TestPreserveOrderPerPage(t *testing.T) {
	_, s := newTestScheduler(t)
	buf := make([]byte, common.PageSize)
	reqs := []*DiskRequest{
		NewDiskRequest(true, 0, pageOf(1)),
		NewDiskRequest(true, 0, pageOf(2)),
		NewDiskRequest(false, 0, buf),
	}

	for _, req := range reqs {
		if err := s.Schedule(req); err != nil {
			t.Fatal(err)
		}
	}

	for _, req := range reqs {
		if err := req.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	if buf[0] != 2 {
		t.Errorf("read saw %d, want 2 (the later write)", buf[0])
	}
}

func TestScheduleConcurentProducers(t *testing.T) {
	_, s := newTestScheduler(t)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				id := common.PageID(g*8 + i)
				w := NewDiskRequest(true, id, pageOf(byte(id)))
				if err := s.Schedule(w); err != nil || w.Wait() != nil {
					t.Errorf("write %d failed", id)
					return
				}
				buf := make([]byte, common.PageSize)
				r := NewDiskRequest(false, id, buf)
				if err := s.Schedule(r); err != nil || r.Wait() != nil {
					t.Errorf("read %d failed", id)
					return
				}
				if !bytes.Equal(buf, pageOf(byte(id))) {
					t.Errorf("page %d mismatch", id)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestSchedulerCloseDrainsQueue(t *testing.T) {
	dm, s := newTestScheduler(t)
	for i := 0; i < 50; i++ {
		if err := s.Schedule(NewDiskRequest(true, common.PageID(i), pageOf(7))); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	if got := dm.NumWrites(); got != 50 {
		t.Fatalf("NumWrites = %d, wants 50", got)
	}
	if err := s.Schedule(NewDiskRequest(true, common.PageID(1), pageOf(1))); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("Schedule after Close = %v, want ErrSchedulerClosed", err)
	}
	s.Close()
}

func TestSchedulerReportsError(t *testing.T) {
	_, s := newTestScheduler(t)
	r := NewDiskRequest(true, 0, make([]byte, 10))
	if err := s.Schedule(r); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(); err == nil {
		t.Fatal("expected an error for a short page")
	}
}
