(component
  (core module $memmod
    (memory (export "mem") 1))
  (core instance $mi (instantiate $memmod))
  (alias core export $mi "mem" (core memory $mem))

  (type $gt (func async (result u32)))
  (import "get" (func $get_import (type $gt)))
  (canon lower (func $get_import) async (memory $mem) (core func $lowered_get))

  ;; run(slot) awaits "get", which writes its u32 at slot*4, and returns it.
  ;; Each task keeps its slot in its own context, so several tasks can wait
  ;; on the instance at the same time.
  (core module $m
    (import "canon" "task.return" (func $tr (param i32)))
    (import "canon" "wsnew" (func $wsnew (result i32)))
    (import "canon" "wjoin" (func $wjoin (param i32 i32)))
    (import "canon" "ctxget" (func $ctxget (result i32)))
    (import "canon" "ctxset" (func $ctxset (param i32)))
    (import "canon" "get" (func $get (param i32) (result i32)))
    (import "canon" "mem" (memory 1))
    (func (export "run") (param $slot i32) (result i32)
      (local $packed i32)
      (local $wset i32)
      (call $ctxset (local.get $slot))
      (local.set $packed (call $get (i32.shl (local.get $slot) (i32.const 2))))
      (local.set $wset (call $wsnew))
      (call $wjoin (i32.shr_u (local.get $packed) (i32.const 4)) (local.get $wset))
      (i32.or (i32.const 2) (i32.shl (local.get $wset) (i32.const 4))))
    (func (export "cb") (param i32 i32 i32) (result i32)
      (call $tr (i32.load (i32.shl (call $ctxget) (i32.const 2))))
      (i32.const 0))
    (func (export "ra") (param i32 i32 i32 i32) (result i32) unreachable))

  (canon task.return (result u32) (core func $tr))
  (canon waitable-set.new (core func $wsnew))
  (canon waitable.join (core func $wjoin))
  (canon context.get i32 0 (core func $ctxget))
  (canon context.set i32 0 (core func $ctxset))

  (core instance $i (instantiate $m (with "canon" (instance
    (export "task.return" (func $tr))
    (export "wsnew" (func $wsnew))
    (export "wjoin" (func $wjoin))
    (export "ctxget" (func $ctxget))
    (export "ctxset" (func $ctxset))
    (export "get" (func $lowered_get))
    (export "mem" (memory $mem))))))

  (alias core export $i "run" (core func $run))
  (alias core export $i "cb" (core func $cb))
  (alias core export $i "ra" (core func $ra))
  (type $rt (func async (param "slot" u32) (result u32)))
  (canon lift (core func $run) (memory $mem) (realloc $ra) async (callback $cb) (func $lifted (type $rt)))
  (export "run-async" (func $lifted))
)
