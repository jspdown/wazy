package component

import "github.com/samyfodil/wazy/internal/component/instance"

// Loop drives one Instance from a goroutine of its own, so that the host can
// run many async export calls on the instance at the same time. See
// instance.Loop.
type Loop = instance.Loop

// ErrLoopClosed is the error a Loop reports to the calls still running when
// Close stops it.
var ErrLoopClosed = instance.ErrLoopClosed

// NewLoop starts the goroutine that drives inst. From then on, every operation
// on inst runs through Loop.Post.
func NewLoop(inst *Instance) *Loop { return instance.NewLoop(inst) }
