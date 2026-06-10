/* Free-threaded critical-section shim for Handle dispatch/cancel.
 *
 * Py_BEGIN_CRITICAL_SECTION / Py_END_CRITICAL_SECTION are brace-scoped macros
 * around a stack-allocated PyCriticalSection whose layout is private and only
 * defined on free-threaded builds - neither the macros nor the struct survive
 * Zig's translate-c. So the two operations that race on a free-threaded build -
 * dispatch reading a Handle's (callback, args, context) and cancel clearing
 * them - are implemented here in real C, each guarded by a critical section
 * keyed on the Handle. Run takes its own references to the callbacks under the
 * lock, so the actual call happens outside it (no user code runs while held).
 *
 * On regular (GIL) builds the macros expand to bare braces: these are then
 * ordinary field reads/clears with zero locking, since the GIL already
 * serializes them.
 *
 * The field offsets must match handle.HandleObject (ob_base, then the three
 * object pointers and the cancelled byte). A static assert guards the layout.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stddef.h>

/* Py_BEGIN/END_CRITICAL_SECTION arrived in 3.13. On 3.12 (always GIL-bound, so
 * no free-threading to guard) fall back to bare braces, matching what the 3.13+
 * headers expand to on a regular build. */
#ifndef Py_BEGIN_CRITICAL_SECTION
#  define Py_BEGIN_CRITICAL_SECTION(op) {
#  define Py_END_CRITICAL_SECTION() }
#endif

typedef struct {
    PyObject ob_base;
    PyObject *callback;
    PyObject *args;
    PyObject *context;
    unsigned char cancelled;
} ZloopHandleHead;

/* Snapshot (callback, args, context) under the Handle's critical section,
 * taking a new reference to each so the caller can use them after the lock is
 * released even if another thread cancels concurrently. Returns 0 and leaves
 * the out-params NULL when the Handle is already cancelled or has no callback. */
int zloop_handle_snapshot(PyObject *op, PyObject **callback, PyObject **args, PyObject **context) {
    ZloopHandleHead *h = (ZloopHandleHead *)op;
    int live = 0;
    Py_BEGIN_CRITICAL_SECTION(op);
    if (h->cancelled == 0 && h->callback != NULL) {
        *callback = Py_NewRef(h->callback);
        *args = Py_XNewRef(h->args);
        *context = Py_XNewRef(h->context);
        live = 1;
    } else {
        *callback = NULL;
        *args = NULL;
        *context = NULL;
    }
    Py_END_CRITICAL_SECTION();
    return live;
}

/* Mark the Handle cancelled and clear its callback references under the same
 * critical section. Returns 1 if this call performed the cancel (was not
 * already cancelled), 0 otherwise. Whether the timer heap is touched is left to
 * the caller, which runs that only on the loop thread. */
int zloop_handle_mark_cancelled(PyObject *op) {
    ZloopHandleHead *h = (ZloopHandleHead *)op;
    int did = 0;
    Py_BEGIN_CRITICAL_SECTION(op);
    if (h->cancelled == 0) {
        h->cancelled = 1;
        did = 1;
    }
    Py_END_CRITICAL_SECTION();
    return did;
}

/* Clear the callback references under the Handle's critical section. Split from
 * mark_cancelled so the caller can run the (loop-thread-only) timer cancel in
 * between without holding the lock across it. */
void zloop_handle_clear_refs(PyObject *op) {
    ZloopHandleHead *h = (ZloopHandleHead *)op;
    PyObject *cb, *ar, *cx;
    Py_BEGIN_CRITICAL_SECTION(op);
    cb = h->callback;
    ar = h->args;
    cx = h->context;
    h->callback = NULL;
    h->args = NULL;
    h->context = NULL;
    Py_END_CRITICAL_SECTION();
    Py_XDECREF(cb);
    Py_XDECREF(ar);
    Py_XDECREF(cx);
}
