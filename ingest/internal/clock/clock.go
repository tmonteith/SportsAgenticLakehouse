// Package clock abstracts time so the extractor and poller can be tested against a
// virtual clock (recorded game days replay in milliseconds).
package clock

import (
	"context"
	"sync"
	"time"
)

// Clock tells the time and sleeps.
type Clock interface {
	Now() time.Time
	// Sleep waits for d or until ctx is done, returning ctx.Err() in the latter case.
	Sleep(ctx context.Context, d time.Duration) error
}

// Real is the wall clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Fake is a virtual clock: Sleep advances it instantly. When Stop is set and a Sleep
// would end at or after StopAt, Fake calls Stop (typically a context's cancel func) and
// returns the context's error, which ends the loop under test.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	StopAt time.Time
	Stop   func()
	Slept  []time.Duration // every Sleep request, in order
}

// NewFake returns a Fake set to start.
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.Slept = append(f.Slept, d)
	if d > 0 {
		f.now = f.now.Add(d)
	}
	stop := f.Stop != nil && !f.StopAt.IsZero() && !f.now.Before(f.StopAt)
	f.mu.Unlock()
	if stop {
		f.Stop()
		return ctx.Err()
	}
	return nil
}
