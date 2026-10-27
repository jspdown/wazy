package instance

import (
	"context"
	_ "embed"
	"errors"
	"testing"

	"github.com/samyfodil/wazy"
	"github.com/samyfodil/wazy/internal/component/abi"
)

// await_import_slot.wat: run-async(slot) awaits the "get" () -> u32 import,
// which writes its result at slot*4, and returns it. Built with
// `wasm-tools parse await_import_slot.wat -o await_import_slot.wasm`.
//
//go:embed testdata/async/await_import_slot.wasm
var awaitImportSlotWasm []byte

type loopSlotKey struct{}

type loopPendingGet struct {
	slot uint32
	call *AsyncCall
}

type loopOutcome struct {
	slot, value uint32
	err         error
}

// newLoopSlotInstance instantiates await_import_slot.wasm with a "get" that
// hands each call, with the slot its context carries, to gets.
func newLoopSlotInstance(t *testing.T, gets chan<- loopPendingGet) *Instance {
	t.Helper()

	ctx := context.Background()
	r := wazy.NewRuntime(ctx)
	t.Cleanup(func() { _ = r.Close(ctx) })

	params, results := getImportParamsResults()
	opt := WithAsyncImport("get", "", func(ctx context.Context, _ []abi.Value, call *AsyncCall) error {
		gets <- loopPendingGet{slot: ctx.Value(loopSlotKey{}).(uint32), call: call}
		return nil
	}, params, results)

	inst, err := Instantiate(ctx, r, awaitImportSlotWasm, opt)
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	return inst
}

// startLoopSlot starts run-async(slot) on l, with slot in the context its
// imports receive, and reports the outcome to out.
func startLoopSlot(l *Loop, slot uint32, out chan<- loopOutcome) bool {
	ctx := context.WithValue(context.Background(), loopSlotKey{}, slot)
	return l.Post(func() {
		var value uint32
		err := l.Start(ctx, "run-async",
			func(res []abi.Value) { value = res[0].(uint32) },
			func(err error) { out <- loopOutcome{slot: slot, value: value, err: err} },
			slot)
		if err != nil {
			out <- loopOutcome{slot: slot, err: err}
		}
	})
}

// TestLoopConcurrentCalls: calls of one async export run on one instance at
// the same time. Every call parks on its import before any resolves, and each
// gets its own result when the imports resolve in reverse order, from other
// goroutines.
func TestLoopConcurrentCalls(t *testing.T) {
	const n = 16

	gets := make(chan loopPendingGet, n)
	inst := newLoopSlotInstance(t, gets)
	defer inst.Close(context.Background())

	l := NewLoop(inst)
	defer l.Close()

	out := make(chan loopOutcome, n)
	for slot := range uint32(n) {
		if !startLoopSlot(l, slot, out) {
			t.Fatal("Post refused on a running loop")
		}
	}

	pending := make([]loopPendingGet, 0, n)
	for range n {
		pending = append(pending, <-gets)
	}
	for i := len(pending) - 1; i >= 0; i-- {
		p := pending[i]
		go p.call.Resolve([]abi.Value{p.slot*100 + 1})
	}

	for range n {
		o := <-out
		if o.err != nil {
			t.Fatalf("slot %d: %v", o.slot, o.err)
		}
		if want := o.slot*100 + 1; o.value != want {
			t.Fatalf("slot %d = %d, want %d", o.slot, o.value, want)
		}
	}
}

// TestLoopCloseEndsCalls: Close ends a parked call with ErrLoopClosed, and the
// loop refuses work after it.
func TestLoopCloseEndsCalls(t *testing.T) {
	gets := make(chan loopPendingGet, 1)
	inst := newLoopSlotInstance(t, gets)
	defer inst.Close(context.Background())

	l := NewLoop(inst)

	out := make(chan loopOutcome, 1)
	startLoopSlot(l, 0, out)
	<-gets

	l.Close()

	if o := <-out; !errors.Is(o.err, ErrLoopClosed) {
		t.Fatalf("outcome error = %v, want ErrLoopClosed", o.err)
	}
	if !errors.Is(l.Err(), ErrLoopClosed) {
		t.Fatalf("Err() = %v, want ErrLoopClosed", l.Err())
	}
	if l.Post(func() {}) {
		t.Fatal("Post accepted work after Close")
	}
}

// TestLoopPostedPanic: a posted function that panics stops the loop, not the
// process.
func TestLoopPostedPanic(t *testing.T) {
	inst := newLoopSlotInstance(t, make(chan loopPendingGet, 1))
	defer inst.Close(context.Background())

	l := NewLoop(inst)
	l.Post(func() { panic("boom") })

	<-l.Done()
	if l.Err() == nil {
		t.Fatal("Err() = nil after a posted function panicked")
	}
}
