package component

import "github.com/samyfodil/wazy/internal/component/instance"

// The host ends of streams and futures. Instance.NewStream/NewFuture (and
// NewStreamOf/NewFutureOf for an element type built in a TypeTable) return a
// writer the host keeps and the value to hand the guest; a stream or future a
// host import receives arrives as a reader. Every op runs on the goroutine
// driving the instance: inside a host import, inside another end's completion
// callback, or between calls.
type (
	StreamWriter = instance.StreamWriter
	StreamReader = instance.StreamReader
	FutureWriter = instance.FutureWriter
	FutureReader = instance.FutureReader
)

// CopyResult is how a stream or future op completed.
type CopyResult = instance.CopyResult

// The CopyResult values.
const (
	CopyCompleted = instance.CopyCompleted
	CopyDropped   = instance.CopyDropped
	CopyCancelled = instance.CopyCancelled
)
