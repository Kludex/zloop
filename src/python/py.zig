//! Ergonomic helpers over the CPython C-API. This is the only place besides the
//! type definitions that touches Python.h. It centralises refcounting, error
//! raising, attribute/calling helpers and number conversions so the rest of the
//! adapter reads close to Python.

const std = @import("std");

pub const c = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", "1");
    @cInclude("Python.h");
});

pub const Object = [*c]c.PyObject;
pub const ssize = c.Py_ssize_t;

// -- reference counting -------------------------------------------------------

pub inline fn incref(o: anytype) void {
    c.Py_INCREF(@ptrCast(o));
}
pub inline fn xincref(o: anytype) void {
    c.Py_XINCREF(@ptrCast(o));
}
pub inline fn decref(o: anytype) void {
    c.Py_DECREF(@ptrCast(o));
}
pub inline fn xdecref(o: anytype) void {
    c.Py_XDECREF(@ptrCast(o));
}
/// Steal a reference into a temporary and decref it (for "use then drop").
pub inline fn clear(slot: *Object) void {
    const tmp = slot.*;
    slot.* = null;
    if (tmp != null) c.Py_DECREF(tmp);
}

pub inline fn newRef(o: anytype) Object {
    return c.Py_NewRef(@ptrCast(o));
}

// -- singletons ---------------------------------------------------------------

pub inline fn none() Object {
    return c.Py_NewRef(c.Py_None());
}
pub inline fn true_() Object {
    return c.Py_NewRef(c.Py_True());
}
pub inline fn false_() Object {
    return c.Py_NewRef(c.Py_False());
}
pub inline fn boolean(v: bool) Object {
    return if (v) true_() else false_();
}
pub inline fn isNone(o: Object) bool {
    return o == c.Py_None();
}

// -- errors -------------------------------------------------------------------

/// Sentinel returned to CPython on error after PyErr is set.
pub const err: Object = null;

pub fn raise(exc: Object, msg: [*c]const u8) Object {
    c.PyErr_SetString(exc, msg);
    return null;
}
pub fn raiseType(msg: [*c]const u8) Object {
    return raise(c.PyExc_TypeError, msg);
}
pub fn raiseValue(msg: [*c]const u8) Object {
    return raise(c.PyExc_ValueError, msg);
}
pub fn raiseRuntime(msg: [*c]const u8) Object {
    return raise(c.PyExc_RuntimeError, msg);
}
pub fn errOccurred() bool {
    return c.PyErr_Occurred() != null;
}

// -- numbers / strings --------------------------------------------------------

pub fn fromI64(v: i64) Object {
    return c.PyLong_FromLongLong(v);
}
pub fn fromUsize(v: usize) Object {
    return c.PyLong_FromUnsignedLongLong(v);
}
pub fn fromF64(v: f64) Object {
    return c.PyFloat_FromDouble(v);
}
pub fn asI64(o: Object) ?i64 {
    const v = c.PyLong_AsLongLong(o);
    if (v == -1 and errOccurred()) return null;
    return v;
}
pub fn asF64(o: Object) ?f64 {
    const v = c.PyFloat_AsDouble(o);
    if (v == -1.0 and errOccurred()) return null;
    return v;
}
pub fn fromStr(s: []const u8) Object {
    return c.PyUnicode_FromStringAndSize(s.ptr, @intCast(s.len));
}
pub fn fromStrZ(s: [*c]const u8) Object {
    return c.PyUnicode_FromString(s);
}

// -- attribute & call helpers -------------------------------------------------

pub fn getAttr(o: Object, name: [*c]const u8) Object {
    return c.PyObject_GetAttrString(o, name);
}
pub fn hasAttr(o: Object, name: [*c]const u8) bool {
    return c.PyObject_HasAttrString(o, name) == 1;
}
pub fn setAttr(o: Object, name: [*c]const u8, value: Object) bool {
    return c.PyObject_SetAttrString(o, name, value) == 0;
}

pub fn callNoArgs(callable: Object) Object {
    return c.PyObject_CallNoArgs(callable);
}
pub fn callOneArg(callable: Object, arg: Object) Object {
    return c.PyObject_CallOneArg(callable, arg);
}
/// Call callable(*args) where args is an already-built tuple (borrowed).
pub fn callTuple(callable: Object, args: Object) Object {
    return c.PyObject_Call(callable, args, null);
}
pub fn callTupleKw(callable: Object, args: Object, kwargs: Object) Object {
    return c.PyObject_Call(callable, args, kwargs);
}

pub fn isCallable(o: Object) bool {
    return c.PyCallable_Check(o) == 1;
}
pub fn isTrue(o: Object) bool {
    return c.PyObject_IsTrue(o) == 1;
}

// -- imports ------------------------------------------------------------------

/// Import a module, returning a new reference (or null on error).
pub fn import(name: [*c]const u8) Object {
    return c.PyImport_ImportModule(name);
}

/// Import `module` and fetch `attr`, returning a new reference to the attr.
pub fn importFrom(module: [*c]const u8, attr: [*c]const u8) Object {
    const m = import(module);
    if (m == null) return null;
    defer decref(m);
    return getAttr(m, attr);
}

// -- tuples -------------------------------------------------------------------

pub fn tupleNew(len: ssize) Object {
    return c.PyTuple_New(len);
}

/// A new reference to a (shared, immutable) empty tuple - avoids allocating a
/// fresh 0-length tuple on every no-args callback.
var empty_tuple_cache: Object = null;
pub fn emptyTuple() Object {
    if (empty_tuple_cache == null) empty_tuple_cache = c.PyTuple_New(0);
    return newRef(empty_tuple_cache);
}
/// Store `item` at `idx`, stealing its reference (matches PyTuple_SET_ITEM).
pub fn tupleSet(tuple: Object, idx: ssize, item: Object) void {
    _ = c.PyTuple_SetItem(tuple, idx, item);
}
pub fn tupleGet(tuple: Object, idx: ssize) Object {
    return c.PyTuple_GetItem(tuple, idx); // borrowed
}
pub fn tupleSize(tuple: Object) ssize {
    return c.PyTuple_Size(tuple);
}

// -- type construction (heap types via spec/slots) ----------------------------

pub const Slot = c.PyType_Slot;
pub const Spec = c.PyType_Spec;
pub const MethodDef = c.PyMethodDef;

pub fn typeFromSpec(spec: *Spec) Object {
    return c.PyType_FromSpec(spec);
}

/// Create a heap type from `spec` with a single base class `base` (borrowed).
pub fn typeFromSpecWithBase(spec: *Spec, base: Object) Object {
    const bases = tupleNew(1);
    if (bases == null) return null;
    defer decref(bases);
    tupleSet(bases, 0, newRef(base));
    return c.PyType_FromSpecWithBases(spec, bases);
}

/// Allocate a new instance of `tp` (a type object) with its memory zeroed by
/// tp_alloc. Returns the new object or null on error.
pub fn allocInstance(tp: Object) Object {
    const type_obj: [*c]c.PyTypeObject = @ptrCast(tp);
    const alloc = type_obj.*.tp_alloc.?;
    return alloc(type_obj, 0);
}

pub fn freeInstance(self: Object) void {
    const tp: [*c]c.PyTypeObject = c.Py_TYPE(self);
    const free = tp.*.tp_free.?;
    free(@ptrCast(self));
}

/// Release a Handle's callback/args/context references (called on cancel so a
/// cancelled handle does not keep its closure alive). Defined here to avoid a
/// circular dependency; the layout matches handle.HandleObject's first fields.
pub fn clearCallbackRefs(handle: anytype) void {
    clear(&handle.callback);
    clear(&handle.args);
    clear(&handle.context);
}
