package instance

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/samyfodil/wazy/internal/component/abi"
)

// ErrLoopClosed is the error a Loop reports to the calls still running when
// Close stops it.
var ErrLoopClosed = errors.New("component/instance: loop closed")

// Loop drives one Instance from a goroutine of its own, so that the host can
// run many async export calls on the instance at the same time. The component
// model allows it: a callback-lifted task holds the instance only while its
// guest code runs, and parks between events. CallAsync drives one call at a
// time from the caller's goroutine; a Loop serves any number, each started
// with Start.
//
// While a Loop drives an instance, every operation on the instance runs on
// the loop goroutine, through Post: start a call, drive a host stream or
// future end, use the handle table. AsyncCall.Resolve stays callable from any
// goroutine. Call, CallExport and CallAsync must not be used on the instance.
type Loop struct {
	in *Instance

	// tasks are the calls Start started and that did not finish. Loop
	// goroutine only.
	tasks map[*guestTask]func(error)
	// fatal is a trap that poisoned the instance during a posted function.
	fatal error

	closing bool // guarded by in.amu
	done    chan struct{}
	err     error // valid once done is closed
}

// NewLoop starts the goroutine that drives in. in must not have a CallAsync
// outstanding.
func NewLoop(in *Instance) *Loop {
	l := &Loop{
		in:    in,
		tasks: make(map[*guestTask]func(error)),
		done:  make(chan struct{}),
	}

	in.amu.Lock()
	if in.acond == nil {
		in.acond = sync.NewCond(&in.amu)
	}
	in.mailbox = nil
	in.amu.Unlock()

	// AsyncCall.Resolve from another goroutine then queues to the mailbox the
	// loop drains, as it does for CallAsync.
	in.asyncActive.Store(true)

	go l.run()

	return l
}

// Post queues fn to run on the loop goroutine, counted as a host call: fn may
// call Start, drive host stream and future ends and use the handle table. It
// reports false when the loop stopped. A function still queued when the loop
// stops never runs: wait on Done too.
func (l *Loop) Post(fn func()) bool {
	in := l.in

	in.amu.Lock()
	defer in.amu.Unlock()

	if l.closing {
		return false
	}

	in.mailbox = append(in.mailbox, asyncCompletion{fn: fn})
	in.acond.Signal()

	return true
}

// Start starts a call of export, an async (callback) lift. Call it on the loop
// goroutine, from a function Post runs. ctx is the context the call's host
// imports receive. onReturn, when not nil, is called with the result once the
// export calls task.return; the task can run after that, to write a stream it
// returned. onDone is called once the task exits, with the trap that ended it
// if any. Start returns an error only for a call it refused to start; after
// that, every outcome goes to onDone.
func (l *Loop) Start(ctx context.Context, export string, onReturn func(result []abi.Value), onDone func(err error), args ...abi.Value) error {
	in := l.in

	be, ok := in.exports[export]
	if !ok {
		return fmt.Errorf("component/instance: export %q not found", export)
	}
	if !be.asyncCallback {
		return fmt.Errorf("component/instance: Loop.Start %q: only async (callback) lifts are supported", export)
	}
	if in.poisoned {
		return fmt.Errorf("component/instance: export %q: cannot enter component instance", export)
	}
	if len(args) != len(be.fd.Params) {
		return fmt.Errorf("component/instance: export %q takes %d parameter(s), got %d", export, len(be.fd.Params), len(args))
	}

	fr := &callbackFrame{}
	t, gt := &fr.t, &fr.gt
	t.inst, t.be = in, be
	gt.t, gt.in, gt.be, gt.ctx, gt.exportName = t, in, be, ctx, export
	gt.args = args
	t.gt = gt

	if onReturn != nil {
		t.onResolve = func(vals []abi.Value, cancelled bool) {
			t.result, t.cancelled = vals, cancelled
			if !cancelled {
				in.hostCallback(func() { onReturn(vals) })
			}
		}
	}

	finish := func(err error) {
		delete(l.tasks, gt)
		if onDone != nil {
			onDone(err)
		}
	}
	l.tasks[gt] = finish
	gt.finish = finish

	// A task that cannot enter yet parks, and the loop starts it once the
	// instance lets it. A trap reaches onDone through gt.fail.
	if err := gt.start(); err != nil && in.poisoned {
		l.fatal = err
	}

	return nil
}

// Done is closed once the loop stopped: after Close, or after a trap
// poisoned the instance. Err then says why.
func (l *Loop) Done() <-chan struct{} {
	return l.done
}

// Err returns why the loop stopped, once Done is closed.
func (l *Loop) Err() error {
	<-l.done
	return l.err
}

// Close stops the loop and waits for its goroutine to return. The calls still
// running end with ErrLoopClosed. Do not call it from the loop goroutine.
func (l *Loop) Close() {
	in := l.in

	in.amu.Lock()
	l.closing = true
	in.acond.Broadcast()
	in.amu.Unlock()

	<-l.done
}

func (l *Loop) run() {
	in := l.in
	defer close(l.done)

	for {
		in.amu.Lock()
		for len(in.mailbox) == 0 && !l.closing {
			in.acond.Wait()
		}
		if l.closing {
			in.amu.Unlock()
			l.stop(ErrLoopClosed)
			return
		}
		queue := in.mailbox
		in.mailbox = nil
		in.amu.Unlock()

		for _, c := range queue {
			if err := l.apply(c); err != nil {
				l.stop(err)
				return
			}
			if l.fatal != nil {
				l.stop(l.fatal)
				return
			}
		}

		// A failing task reported its trap to its own onDone; only one that
		// poisoned the instance ends the others.
		if err := in.sched.drainReady(); err != nil && in.poisoned {
			l.stop(err)
			return
		}
	}
}

// apply runs one queued item. A host function that panics, as a host stream
// op does on a broken invariant, stops the loop instead of the process: the
// instance state is unknown after it.
func (l *Loop) apply(c asyncCompletion) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("component/instance: loop: %v", r)
		}
	}()

	return l.in.applyQueued(c)
}

// stop ends every call still running with err, and refuses new work.
func (l *Loop) stop(err error) {
	in := l.in

	in.amu.Lock()
	l.closing = true
	in.mailbox = nil
	in.amu.Unlock()

	l.err = err

	finishers := make([]func(error), 0, len(l.tasks))
	for gt, finish := range l.tasks {
		gt.done = true
		finishers = append(finishers, finish)
	}
	for _, finish := range finishers {
		finish(err)
	}

	in.reapParkedGoroutines()
	in.asyncActive.Store(false)
}
