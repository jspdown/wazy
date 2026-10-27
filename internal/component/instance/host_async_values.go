package instance

import (
	"fmt"

	"github.com/samyfodil/wazy/internal/component/abi"
	"github.com/samyfodil/wazy/internal/component/binary"
)

// This file carries streams and futures across a host import's signature at
// any depth: an argument or result may hold one inside an option, a result, a
// variant, a tuple, a record or a list, as wasi:http 0.3 does
// (`consume-body: static func(...) -> tuple<stream<u8>, future<...>>`). The abi
// layer lifts and lowers a stream or future as a bare handle wherever it sits;
// these walks do the table transfer the top-level path does in
// resolveHandleArg and allocHandleResult.
//
// The element types a host declares are relative to the host's own type
// table, so they are never compared with the component's: the component's
// canon types govern every copy, and the host is responsible for the values it
// writes.

// OwnedRep is a host rep for an own<R> or borrow<R> nested inside a host
// import's composite result -- `result<own<R>, E>`, `tuple<own<R>, ...>`: the
// engine mints the guest's handle for it under the type's resource tag, as it
// does for a top-level one. Unlike minting with HandleTable.NewOwn in the host
// func, minting happens when the result is lowered, on the driving goroutine,
// so an async import may Resolve with one from any goroutine. A bare uint32 at
// that position is still taken as a handle the host already minted.
type OwnedRep uint32

// hostAsyncValues reports whether t, a host import param or result type, holds
// a stream or future at any depth. It rejects an error-context nested below the
// top level, which is still lifted as a bare handle with no table transfer.
func hostAsyncValues(t binary.TypeDesc, resolve abi.Resolver) (bool, error) {
	if typeContainsNested(t, resolve, 0, isErrorContextType) {
		return false, fmt.Errorf("error-context nested inside a composite type is not supported by this milestone")
	}
	return isStreamOrFutureType(t) || typeContainsNested(t, resolve, 0, isStreamOrFutureType), nil
}

// isHandleType reports whether t is an own<R> or a borrow<R>.
func isHandleType(t binary.TypeDesc) bool {
	switch t.(type) {
	case binary.OwnDesc, binary.BorrowDesc:
		return true
	}
	return false
}

// liftHostArgAsyncValues replaces every stream/future handle in v, an argument
// a host import receives, with the host end that takes it over: a
// *StreamReader or a *FutureReader. Each handle is removed from the caller's
// table. own/borrow handles nested below the top level stay bare handles, as
// they do without a stream or future around.
func liftHostArgAsyncValues(in *Instance, resources *handleTable, t binary.TypeDesc, resolve abi.Resolver, v abi.Value, depth int) (abi.Value, error) {
	if depth > maxResourceWalkDepth {
		return v, nil
	}
	switch t.(type) {
	case binary.StreamDesc:
		h, ok := v.(uint32)
		if !ok {
			return nil, fmt.Errorf("stream arg: expected a uint32 handle, got %T", v)
		}
		shared, err := takeReadableStreamEndHost(resources, h)
		if err != nil {
			return nil, fmt.Errorf("stream arg: %w", err)
		}
		return &StreamReader{in: in, shared: shared, state: copyIdle}, nil

	case binary.FutureDesc:
		h, ok := v.(uint32)
		if !ok {
			return nil, fmt.Errorf("future arg: expected a uint32 handle, got %T", v)
		}
		shared, err := takeReadableFutureEndHost(resources, h)
		if err != nil {
			return nil, fmt.Errorf("future arg: %w", err)
		}
		return &FutureReader{in: in, shared: shared, state: copyIdle}, nil

	case binary.OwnDesc, binary.BorrowDesc:
		if depth == 0 {
			return resolveHandleArg(in, resources, nil, t, v)
		}
		return v, nil
	}

	return walkValue(t, resolve, v, depth, func(et binary.TypeDesc, ev abi.Value, edepth int) (abi.Value, error) {
		return liftHostArgAsyncValues(in, resources, et, resolve, ev, edepth)
	})
}

// allocHostResultAsyncValues replaces every stream/future in v, a result a
// host import returns, with a fresh readable-end handle in the caller's table:
// the value NewStream/NewFuture returned, or a *StreamReader/*FutureReader the
// host hands over. A top-level own/borrow is minted as allocHandleResult
// mints it; one nested below the top level is minted from an OwnedRep, or
// passes through as a handle the host minted.
func allocHostResultAsyncValues(resources *handleTable, t binary.TypeDesc, resolve abi.Resolver, v abi.Value, depth int) (abi.Value, error) {
	if depth > maxResourceWalkDepth {
		return v, nil
	}
	switch t.(type) {
	case binary.StreamDesc, binary.FutureDesc:
		return allocHandleResult(resources, t, v)

	case binary.OwnDesc, binary.BorrowDesc:
		if r, ok := v.(OwnedRep); ok {
			return allocHandleResult(resources, t, uint32(r))
		}
		if depth == 0 {
			return allocHandleResult(resources, t, v)
		}
		return v, nil
	}

	return walkValue(t, resolve, v, depth, func(et binary.TypeDesc, ev abi.Value, edepth int) (abi.Value, error) {
		return allocHostResultAsyncValues(resources, et, resolve, ev, edepth)
	})
}

// mapOwns rebuilds v, a value of type t, with every own<R> handle or rep x in
// it replaced by fn(R, x).
func mapOwns(t binary.TypeDesc, resolve abi.Resolver, v abi.Value, depth int, fn func(rt, x uint32) (uint32, error)) (abi.Value, error) {
	if depth > maxResourceWalkDepth {
		return v, nil
	}
	if od, ok := t.(binary.OwnDesc); ok {
		x, ok := v.(uint32)
		if !ok {
			return nil, fmt.Errorf("own<%d>: expected a uint32, got %T", od.ResourceType, v)
		}
		return fn(od.ResourceType, x)
	}
	return walkValue(t, resolve, v, depth, func(et binary.TypeDesc, ev abi.Value, edepth int) (abi.Value, error) {
		return mapOwns(et, resolve, ev, edepth, fn)
	})
}

// isOwnType reports whether t is an own<R>.
func isOwnType(t binary.TypeDesc) bool {
	_, ok := t.(binary.OwnDesc)
	return ok
}

// walkValue rebuilds v, a value of composite type t, with visit applied to
// each child value. Leaves pass through unchanged.
func walkValue(t binary.TypeDesc, resolve abi.Resolver, v abi.Value, depth int, visit func(binary.TypeDesc, abi.Value, int) (abi.Value, error)) (abi.Value, error) {
	child := func(ref *binary.TypeRef, cv abi.Value) (abi.Value, error) {
		ct, err := resolveTypeRef(ref, resolve)
		if err != nil {
			return nil, err
		}
		return visit(ct, cv, depth+1)
	}

	switch d := t.(type) {
	case binary.OptionDesc:
		if v == nil {
			return nil, nil
		}
		return child(&d.Element, v)

	case binary.ResultDesc:
		rv, ok := v.(abi.ResultValue)
		if !ok || rv.Payload == nil {
			return v, nil
		}
		arm := d.Ok
		if rv.IsErr {
			arm = d.Err
		}
		if arm == nil {
			return v, nil
		}
		p, err := child(arm, rv.Payload)
		if err != nil {
			return nil, err
		}
		return abi.ResultValue{IsErr: rv.IsErr, Payload: p}, nil

	case binary.VariantDesc:
		vv, ok := v.(abi.VariantValue)
		if !ok || int(vv.Disc) >= len(d.Cases) || d.Cases[vv.Disc].Type == nil || vv.Payload == nil {
			return v, nil
		}
		p, err := child(d.Cases[vv.Disc].Type, vv.Payload)
		if err != nil {
			return nil, err
		}
		return abi.VariantValue{Disc: vv.Disc, Payload: p}, nil

	case binary.TupleDesc:
		elems, ok := v.([]abi.Value)
		if !ok {
			return v, nil
		}
		out := make([]abi.Value, len(elems))
		for i := range elems {
			if i >= len(d.Elements) {
				out[i] = elems[i]
				continue
			}
			e, err := child(&d.Elements[i], elems[i])
			if err != nil {
				return nil, err
			}
			out[i] = e
		}
		return out, nil

	case binary.RecordDesc:
		fields, ok := v.([]abi.Value)
		if !ok {
			return v, nil
		}
		out := make([]abi.Value, len(fields))
		for i := range fields {
			if i >= len(d.Fields) {
				out[i] = fields[i]
				continue
			}
			f, err := child(&d.Fields[i].Type, fields[i])
			if err != nil {
				return nil, err
			}
			out[i] = f
		}
		return out, nil

	case binary.ListDesc:
		// A list of a fixed-width primitive lifts to a typed slice, which
		// cannot hold a stream or future.
		elems, ok := v.([]abi.Value)
		if !ok {
			return v, nil
		}
		out := make([]abi.Value, len(elems))
		for i := range elems {
			e, err := child(&d.Element, elems[i])
			if err != nil {
				return nil, err
			}
			out[i] = e
		}
		return out, nil

	case binary.FixedListDesc:
		return walkValue(binary.ListDesc{Element: d.Element}, resolve, v, depth, visit)

	case binary.MapDesc:
		// A map's value is a list of key/value pairs, as list<tuple<K, V>>'s.
		entries, ok := v.([]abi.Value)
		if !ok {
			return v, nil
		}
		pair := binary.TupleDesc{Elements: []binary.TypeRef{d.Key, d.Value}}
		out := make([]abi.Value, len(entries))
		for i := range entries {
			e, err := visit(pair, entries[i], depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = e
		}
		return out, nil
	}

	return v, nil
}

// takeReadableStreamEndHost is takeReadableStreamEnd without the element type
// check: see this file's doc.
func takeReadableStreamEndHost(t *handleTable, i uint32) (*sharedStream, error) {
	raw, ok := t.getEntry(i)
	if !ok {
		return nil, fmt.Errorf("unknown stream handle %d", i)
	}
	se, ok := raw.(*streamEnd)
	if !ok {
		return nil, fmt.Errorf("handle %d is not a stream end", i)
	}
	if se.side != sideReadable {
		return nil, fmt.Errorf("handle %d is not a READABLE stream end", i)
	}
	if se.state != copyIdle {
		return nil, fmt.Errorf("handle %d: %s", i, liftNotIdleTrapText(false, se.state))
	}
	if se.inWaitableSet() {
		return nil, fmt.Errorf("handle %d: cannot lift stream while it's in a waitable set", i)
	}
	t.removeEntry(i)
	return se.shared, nil
}

// takeReadableFutureEndHost is takeReadableStreamEndHost's future twin.
func takeReadableFutureEndHost(t *handleTable, i uint32) (*sharedFuture, error) {
	raw, ok := t.getEntry(i)
	if !ok {
		return nil, fmt.Errorf("unknown future handle %d", i)
	}
	fe, ok := raw.(*futureEnd)
	if !ok {
		return nil, fmt.Errorf("handle %d is not a future end", i)
	}
	if fe.side != sideReadable {
		return nil, fmt.Errorf("handle %d is not a READABLE future end", i)
	}
	if fe.state != copyIdle {
		return nil, fmt.Errorf("handle %d: %s", i, liftNotIdleTrapText(true, fe.state))
	}
	if fe.inWaitableSet() {
		return nil, fmt.Errorf("handle %d: cannot lift future while it's in a waitable set", i)
	}
	t.removeEntry(i)
	return fe.shared, nil
}
