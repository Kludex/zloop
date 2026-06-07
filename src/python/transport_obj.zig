//! The socket transport: a Python object implementing asyncio's Transport
//! interface (write/writelines/close/abort/pause_reading/resume_reading/
//! is_closing/get_extra_info/set_protocol/write_eof). The actual byte movement
//! and readiness handling run in Zig against the loop engine's IoCallback API;
//! it bridges readable data to protocol.data_received and EOF/errors to
//! connection_lost, with high/low-watermark write flow control.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const core_root = @import("core");
const core = core_root.loop;
const sys = core_root.sys;

const gpa = std.heap.c_allocator;

const DEFAULT_HIGH_WATER: usize = 64 * 1024;

pub const TransportObject = extern struct {
    ob_base: c.PyObject,
    state: ?*State,
    // Explicit instance __dict__. A member literally named "__dict__" makes
    // PyType_FromSpec wire up tp_dictoffset, which works on every supported
    // CPython; the managed-dict C-API (PyObject_*ManagedDict) only exists on 3.13+.
    dict: py.Object,
};

/// All the non-POD transport state, heap-allocated so the Python object stays a
/// plain header + pointer.
const State = struct {
    fd: sys.fd_t,
    loop_obj: py.Object, // owned - keeps the Loop (and its `engine`) alive
    engine: *core.Loop,
    protocol: py.Object, // owned
    self_obj: py.Object, // borrowed back-pointer for callbacks
    extra: py.Object, // dict of get_extra_info data (owned)
    /// The Python socket object that owns this fd, if any (owned). Closing it
    /// (rather than the raw fd) avoids a double-close when the socket is GC'd.
    sock_obj: py.Object = null,

    write_buf: std.ArrayList(u8) = .empty,
    high_water: usize = DEFAULT_HIGH_WATER,
    low_water: usize = DEFAULT_HIGH_WATER / 4,

    /// Completion-backend send state. An io_uring SEND borrows its buffer until
    /// the CQE, so the in-flight bytes live in `send_buf` (stable, not mutated
    /// while a send is outstanding); newly written bytes accumulate in `write_buf`
    /// and are promoted to `send_buf` once the in-flight send completes.
    send_buf: std.ArrayList(u8) = .empty,
    send_inflight: bool = false,

    reading: bool = true,
    /// Set once the peer half-closed and eof_received() asked to keep the
    /// connection open; resume_reading must not re-arm the reader after this.
    read_eof: bool = false,
    writing_paused: bool = false, // protocol asked to pause? (we set on >high)
    write_registered: bool = false,
    eof_sent: bool = false,
    closing: bool = false,
    conn_lost: bool = false,
    /// Set once connection_made has been delivered; guards early writes.
    started: bool = false,
    /// The protocol is a BufferedProtocol (get_buffer/buffer_updated), e.g.
    /// asyncio.sslproto.SSLProtocol. Affects the read delivery path.
    buffered: bool = false,
    /// contextvars context captured at creation; protocol callbacks run under
    /// it so create_task() inside data_received inherits the connection's
    /// context, matching asyncio's per-connection context semantics. Owned.
    context: py.Object = null,

    fn protocolPaused(self: *State) bool {
        return self.writing_paused;
    }
};

/// Call `method_name(arg)` on the protocol under the connection's captured
/// context, so contextvars set outside propagate into the ASGI task. Returns
/// the call result (new ref) or null with PyErr set.
fn protoCall1(st: *State, method_name: [*c]const u8, arg: py.Object) py.Object {
    const m = py.getAttr(st.protocol, method_name);
    if (m == null) return null;
    defer py.decref(m);
    if (st.context == null or py.isNone(st.context)) return py.callOneArg(m, arg);
    // Run under the connection's context via the raw C-API (no context.run
    // attribute lookup), preserving the callback's exception across the exit.
    if (py.c.PyContext_Enter(st.context) != 0) return null;
    const result = py.callOneArg(m, arg);
    const exc = py.fetchException();
    if (py.c.PyContext_Exit(st.context) != 0) {
        if (exc != null) py.decref(exc);
        py.xdecref(result);
        return null;
    }
    if (exc != null) {
        py.restoreException(exc);
        return null;
    }
    return result;
}

var transport_type: py.Object = null;

pub fn transportType() py.Object {
    return transport_type;
}

// ---------------------------------------------------------------------------
// creation
// ---------------------------------------------------------------------------

/// Build a transport for an accepted/connected `fd`, attach `protocol`, and
/// schedule connection_made + start reading. `extra` is a dict (owned by
/// caller, we incref). Returns a new transport reference, or null on error.
pub fn create(engine: *core.Loop, loop_obj: py.Object, fd: sys.fd_t, protocol: py.Object, extra: py.Object) py.Object {
    sys.setNonBlocking(fd) catch {
        return py.raiseRuntime("zloop: could not set socket non-blocking");
    };

    const obj = py.allocInstance(transport_type);
    if (obj == null) return null;
    const t: *TransportObject = @ptrCast(obj);

    const st = gpa.create(State) catch {
        py.decref(obj);
        return py.raiseRuntime("zloop: transport alloc failed");
    };
    st.* = .{
        .fd = fd,
        // Own a reference to the Loop so its engine (a raw pointer below) cannot
        // be freed while this transport is alive. Visited in GC traverse/clear,
        // so the Loop<->transport cycle stays collectable.
        .loop_obj = py.newRef(loop_obj),
        .engine = engine,
        .protocol = py.newRef(protocol),
        .self_obj = obj,
        .extra = py.newRef(extra),
    };
    // Adopt the owning socket object from extra["socket"], if present, so we can
    // close it (not the bare fd) and keep it alive for its lifetime.
    const sock = py.c.PyDict_GetItemString(extra, "socket"); // borrowed
    if (sock != null) st.sock_obj = py.newRef(sock);
    // A BufferedProtocol (get_buffer/buffer_updated) takes the zero-copy read
    // path; SSLProtocol is one.
    st.buffered = py.hasAttr(protocol, "get_buffer");
    // Capture the current context so protocol callbacks (and the create_task
    // they make) inherit contextvars set by the caller.
    st.context = captureContext();
    t.state = st;
    t.dict = null; // tp_alloc zero-fills, but be explicit about the __dict__ slot.
    // tp_alloc already GC-tracks GC instances, so we must not track again here
    // (double-track is a fatal assertion).

    // connection_made(transport) under the captured context, then begin
    // reading (selector transports deliver connection_made before first read).
    // After GC tracking, failures must release via decref (dealloc cleans up
    // state and the fd) - never destroyState directly, or it double-frees.
    const r = protoCall1(st, "connection_made", obj);
    if (r == null) {
        py.decref(obj);
        return null;
    }
    py.decref(r);
    st.started = true;

    // connection_made may close the transport synchronously (a common pattern,
    // and what some asyncio tests do). The fd is then already closed, so starting
    // to read would register a dead fd - epoll rejects it with EBADF, kqueue
    // silently tolerates it. Skip reading and tracking when that happened.
    if (st.closing or st.conn_lost) return obj;

    startReading(st) catch {
        py.decref(obj);
        return py.raiseRuntime("zloop: could not start reading");
    };
    trackOnLoop(st); // loop keeps the connection alive until connection_lost
    return obj;
}

/// Ask the loop to keep this transport alive for the connection's lifetime
/// (asyncio keeps accepted/connected transports alive until connection_lost).
/// The loop holds them in a set - a GC root reachable from the running loop -
/// so they survive even when the protocol does not retain the transport.
fn trackOnLoop(st: *State) void {
    callLoopHook(st, "_track_transport");
}
fn untrackFromLoop(st: *State) void {
    callLoopHook(st, "_untrack_transport");
}
fn callLoopHook(st: *State, name: [*c]const u8) void {
    if (st.loop_obj == null) return;
    const m = py.getAttr(st.loop_obj, name);
    if (m == null) {
        py.c.PyErr_Clear();
        return;
    }
    defer py.decref(m);
    const r = py.callOneArg(m, st.self_obj);
    if (r == null) py.c.PyErr_Clear() else py.decref(r);
}

fn destroyState(st: *State) void {
    py.xdecref(st.protocol);
    py.xdecref(st.extra);
    py.xdecref(st.sock_obj);
    py.xdecref(st.context);
    py.xdecref(st.loop_obj);
    st.write_buf.deinit(gpa);
    st.send_buf.deinit(gpa);
    gpa.destroy(st);
}

fn captureContext() py.Object {
    const copy = py.importFrom("contextvars", "copy_context");
    if (copy == null) {
        py.c.PyErr_Clear();
        return py.none();
    }
    defer py.decref(copy);
    const ctx = py.callNoArgs(copy);
    if (ctx == null) {
        py.c.PyErr_Clear();
        return py.none();
    }
    return ctx;
}

/// Close the connection's fd. Prefer closing the owning Python socket object so
/// its own GC does not double-close the descriptor.
fn closeFd(st: *State) void {
    if (st.sock_obj != null) {
        const closem = py.getAttr(st.sock_obj, "close");
        if (closem != null) {
            defer py.decref(closem);
            const r = py.callNoArgs(closem);
            if (r == null) py.c.PyErr_Clear() else py.decref(r);
            return;
        }
        py.c.PyErr_Clear();
    }
    sys.close(st.fd);
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

fn readCallback(ctx: *anyopaque, ev: core.IoEvent) void {
    const st: *State = @ptrCast(@alignCast(ctx));
    if (st.conn_lost) return;
    _ = ev;
    if (st.buffered) deliverBuffered(st) else deliverData(st);
}

/// BufferedProtocol path: get_buffer(sizehint) -> read into it ->
/// buffer_updated(nbytes). Used by SSLProtocol.
fn deliverBuffered(st: *State) void {
    const get_buffer = py.getAttr(st.protocol, "get_buffer");
    if (get_buffer == null) {
        py.c.PyErr_Clear();
        return;
    }
    defer py.decref(get_buffer);
    const hint = py.fromI64(-1);
    defer py.decref(hint);
    const buffer_obj = py.callOneArg(get_buffer, hint);
    if (buffer_obj == null) {
        reportProtocolError(st, "get_buffer");
        return;
    }
    defer py.decref(buffer_obj);

    var view: py.c.Py_buffer = undefined;
    if (py.c.PyObject_GetBuffer(buffer_obj, &view, py.c.PyBUF_WRITABLE) != 0) {
        reportProtocolError(st, "get_buffer");
        return;
    }
    const cap: usize = @intCast(view.len);
    // A zero-length (or null) buffer must not be passed to read(): read(fd,_,0)
    // returns 0, which we'd misread as EOF. asyncio treats this as a protocol
    // error from get_buffer().
    if (cap == 0 or view.buf == null) {
        py.c.PyBuffer_Release(&view);
        _ = py.raiseRuntime("get_buffer() returned an empty buffer");
        reportProtocolError(st, "get_buffer");
        return;
    }
    const dst: [*]u8 = @ptrCast(view.buf.?);
    const n = sys.read(st.fd, dst[0..cap]) catch |err| switch (err) {
        error.WouldBlock, error.Interrupted => {
            py.c.PyBuffer_Release(&view);
            return;
        },
        else => {
            py.c.PyBuffer_Release(&view);
            fatalError(st, err);
            return;
        },
    };
    py.c.PyBuffer_Release(&view);

    if (n == 0) {
        handleEof(st);
        return;
    }
    const arg = py.fromI64(@intCast(n));
    defer py.decref(arg);
    const r = protoCall1(st, "buffer_updated", arg);
    if (r == null) reportProtocolError(st, "buffer_updated") else py.decref(r);
}

// Sized so a typical large message arrives in a single recv() (uvloop's
// benchmark uses up to 100 KiB messages). We recv() directly into an
// uninitialised PyBytes and shrink it to the bytes read - no stack buffer and
// no extra copy on the read hot path.
const READ_CHUNK = 256 * 1024;

fn deliverData(st: *State) void {
    // Drain the socket per readiness notification. Level-triggered epoll re-fires
    // while data/EOF remain, but io_uring's multishot poll is edge-triggered: a
    // recv that doesn't reach EAGAIN/EOF may get no further notification (e.g.
    // "data" then FIN arriving together). Reading until WouldBlock or EOF makes
    // the read path correct on both backends. A short read (< READ_CHUNK) means
    // the socket buffer is drained, so stop without a speculative extra recv.
    while (true) {
        if (st.conn_lost or !st.reading) return;
        var data = py.c.PyBytes_FromStringAndSize(null, READ_CHUNK); // uninitialised
        if (data == null) {
            py.c.PyErr_Clear();
            return;
        }
        const dst: [*]u8 = @ptrCast(py.c.PyBytes_AsString(data));
        const n = sys.read(st.fd, dst[0..READ_CHUNK]) catch |err| switch (err) {
            error.WouldBlock, error.Interrupted => {
                py.decref(data);
                return;
            },
            else => {
                py.decref(data);
                fatalError(st, err);
                return;
            },
        };

        if (n == 0) {
            py.decref(data);
            handleEof(st);
            return;
        }

        // Shrink the bytes object to what we actually read.
        if (n != READ_CHUNK) {
            if (py.c._PyBytes_Resize(&data, @intCast(n)) != 0) {
                py.c.PyErr_Clear();
                return; // _PyBytes_Resize already released `data` on failure
            }
        }

        const r = protoCall1(st, "data_received", data);
        py.decref(data);
        if (r == null) {
            reportProtocolError(st, "data_received");
            return;
        }
        py.decref(r);
        // Loop until WouldBlock/EOF: under edge-triggered io_uring a pending FIN
        // after a short read gets no further notification, so we must reach it
        // here (the next recv returns 0). The extra EAGAIN recv is the cost of
        // being backend-agnostic.
    }
}

/// Plain (non-buffered) connections use the completion backend when active;
/// buffered/SSL connections stay on readiness because the kernel can't hold the
/// protocol's get_buffer() buffer across the async gap.
fn usesCompletion(st: *State) bool {
    return st.engine.isCompletion() and !st.buffered;
}

fn startReading(st: *State) !void {
    if (usesCompletion(st)) {
        // The completion callback lives for the transport's whole lifetime (sends
        // route to it too, even while reading is paused); recv activation is
        // separate. registerCompletion is idempotent, so resume_reading is fine.
        try st.engine.registerCompletion(st.fd, .{ .func = ioCompleted, .ctx = st });
        try st.engine.startRecv(st.fd);
    } else {
        try st.engine.addReader(st.fd, .{ .func = readCallback, .ctx = st });
    }
    st.reading = true;
}

/// Stop reading on whichever backend this connection uses. On completion this
/// cancels the recv but keeps the callback registered so in-flight/queued sends
/// still complete; full teardown happens via stopIo in doConnectionLost/dealloc.
fn stopReading(st: *State) void {
    if (usesCompletion(st)) {
        st.engine.stopRecv(st.fd);
    } else {
        _ = st.engine.removeReader(st.fd);
    }
    st.reading = false;
}

/// Completion-backend I/O callback. The loop routes both recv and send CQEs for
/// this fd here; dispatch by op kind. (Sends must be handled even after
/// pause_reading, so the recv-only `reading` guard lives in readCompleted.)
fn ioCompleted(ctx: *anyopaque, res: core.IoResult) void {
    const st: *State = @ptrCast(@alignCast(ctx));
    if (res.kind == .send) sendCompleted(st, res) else readCompleted(st, res);
}

/// Recv branch: the kernel already recv'd into `res.buf`. Build a PyBytes (GIL
/// held) and deliver data_received. The recv is multishot - the kernel keeps it
/// armed and the loop re-arms when F_MORE drops - so this never re-submits.
/// res.bytes==0 is a clean EOF; <0 is -errno.
fn readCompleted(st: *State, res: core.IoResult) void {
    if (st.conn_lost or !st.reading) return;
    if (res.bytes == 0) {
        handleEof(st);
        return;
    }
    if (res.bytes < 0) {
        fatalError(st, error.RecvFailed);
        return;
    }
    const data = py.c.PyBytes_FromStringAndSize(res.buf.ptr, @intCast(res.buf.len));
    if (data == null) {
        py.c.PyErr_Clear();
    } else {
        const r = protoCall1(st, "data_received", data);
        py.decref(data);
        if (r == null) {
            reportProtocolError(st, "data_received");
            return;
        }
        py.decref(r);
    }
}

fn handleEof(st: *State) void {
    const er = py.getAttr(st.protocol, "eof_received");
    var keep_open = false;
    if (er != null) {
        defer py.decref(er);
        const r = py.callNoArgs(er);
        if (r == null) {
            reportProtocolError(st, "eof_received");
        } else {
            keep_open = py.isTrue(r);
            py.decref(r);
        }
    } else {
        py.c.PyErr_Clear();
    }
    if (!keep_open) {
        closeTransport(st, null);
    } else {
        // protocol wants the connection kept half-open: stop reading for good.
        stopReading(st);
        st.read_eof = true;
    }
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

/// Bytes still owed to the peer: queued plus any in-flight send (completion path).
fn pendingWrite(st: *State) usize {
    return st.write_buf.items.len + st.send_buf.items.len;
}

fn writeBytes(st: *State, data: []const u8) void {
    if (st.conn_lost or st.eof_sent) return;
    if (data.len == 0) return;

    if (usesCompletion(st)) {
        writeBytesCompletion(st, data);
        return;
    }

    var consumed: usize = 0;
    // Fast path: nothing buffered, try to send immediately.
    if (st.write_buf.items.len == 0) {
        consumed = sys.write(st.fd, data) catch |err| switch (err) {
            error.WouldBlock, error.Interrupted => 0,
            else => {
                fatalError(st, err);
                return;
            },
        };
        if (consumed == data.len) return; // fully written
    }

    st.write_buf.appendSlice(gpa, data[consumed..]) catch {
        fatalError(st, error.NoBufferSpace);
        return;
    };
    maybeRegisterWrite(st);
    maybePauseProtocol(st);
}

/// Completion-backend write: an io_uring SEND borrows its buffer until the CQE,
/// so new bytes queue in `write_buf` and a single send is kept in flight from the
/// stable `send_buf`. No sync write() - the send batches into the loop's enter.
fn writeBytesCompletion(st: *State, data: []const u8) void {
    st.write_buf.appendSlice(gpa, data) catch {
        fatalError(st, error.NoBufferSpace);
        return;
    };
    kickSend(st);
    maybePauseProtocol(st);
}

/// Promote queued bytes into the in-flight send buffer and submit a SEND, unless
/// one is already outstanding (one send in flight per fd).
fn kickSend(st: *State) void {
    if (st.send_inflight or st.write_buf.items.len == 0 or st.conn_lost) return;
    std.mem.swap(std.ArrayList(u8), &st.send_buf, &st.write_buf);
    st.write_buf.clearRetainingCapacity();
    st.send_inflight = true;
    st.engine.submitSend(st.fd, st.send_buf.items);
}

/// Completion-backend send callback: `res.bytes` were sent from `send_buf`. Drop
/// them; resubmit the remainder on a partial send; otherwise promote any bytes
/// queued meanwhile, and finish close/eof once fully drained.
fn sendCompleted(st: *State, res: core.IoResult) void {
    if (st.conn_lost) return;
    st.send_inflight = false;
    if (res.bytes < 0) {
        fatalError(st, error.SendFailed);
        return;
    }
    const sent: usize = @intCast(res.bytes);
    const remaining = st.send_buf.items.len - sent;
    if (remaining > 0) {
        std.mem.copyForwards(u8, st.send_buf.items[0..remaining], st.send_buf.items[sent..]);
    }
    st.send_buf.shrinkRetainingCapacity(remaining);

    if (st.send_buf.items.len > 0) { // partial send: resubmit the rest
        st.send_inflight = true;
        st.engine.submitSend(st.fd, st.send_buf.items);
        return;
    }

    kickSend(st); // anything queued while this send was in flight
    maybeResumeProtocol(st);

    if (pendingWrite(st) == 0 and !st.send_inflight) {
        if (st.eof_sent) sys.shutdown(st.fd, 1); // SHUT_WR after flush
        if (st.closing) closeTransport(st, null);
    }
}

fn maybeRegisterWrite(st: *State) void {
    if (!st.write_registered and st.write_buf.items.len > 0) {
        st.engine.addWriter(st.fd, .{ .func = writeCallback, .ctx = st }) catch return;
        st.write_registered = true;
    }
}

fn writeCallback(ctx: *anyopaque, ev: core.IoEvent) void {
    const st: *State = @ptrCast(@alignCast(ctx));
    if (st.conn_lost) return;
    _ = ev;

    const buf = st.write_buf.items;
    if (buf.len == 0) {
        stopWriting(st);
        return;
    }
    const n = sys.write(st.fd, buf) catch |err| switch (err) {
        error.WouldBlock, error.Interrupted => return,
        else => {
            fatalError(st, err);
            return;
        },
    };
    // Drop the written prefix.
    std.mem.copyForwards(u8, st.write_buf.items[0 .. buf.len - n], st.write_buf.items[n..]);
    st.write_buf.shrinkRetainingCapacity(buf.len - n);

    maybeResumeProtocol(st);

    if (st.write_buf.items.len == 0) {
        stopWriting(st);
        if (st.eof_sent) sys.shutdown(st.fd, 1); // SHUT_WR after flush
        if (st.closing) closeTransport(st, null);
    }
}

fn stopWriting(st: *State) void {
    if (st.write_registered) {
        _ = st.engine.removeWriter(st.fd);
        st.write_registered = false;
    }
}

// -- flow control ------------------------------------------------------------

fn maybePauseProtocol(st: *State) void {
    if (!st.writing_paused and pendingWrite(st) > st.high_water) {
        st.writing_paused = true;
        callProtocol(st, "pause_writing");
    }
}

fn maybeResumeProtocol(st: *State) void {
    if (st.writing_paused and pendingWrite(st) <= st.low_water) {
        st.writing_paused = false;
        callProtocol(st, "resume_writing");
    }
}

fn callProtocol(st: *State, name: [*c]const u8) void {
    const m = py.getAttr(st.protocol, name);
    if (m == null) {
        py.c.PyErr_Clear();
        return;
    }
    defer py.decref(m);
    const r = py.callNoArgs(m);
    if (r == null) reportProtocolError(st, name) else py.decref(r);
}

// ---------------------------------------------------------------------------
// closing / errors
// ---------------------------------------------------------------------------

fn closeTransport(st: *State, exc: py.Object) void {
    if (st.conn_lost) return;
    st.closing = true;
    // Defer the real close until queued + in-flight bytes flush (the readiness
    // writeCallback or the completion sendCompleted finishes the close).
    if ((pendingWrite(st) > 0 or st.send_inflight) and exc == null) {
        return;
    }
    doConnectionLost(st, exc);
}

fn fatalError(st: *State, err: anyerror) void {
    if (st.conn_lost) return;
    var namebuf: [64]u8 = undefined;
    const name = std.fmt.bufPrintZ(&namebuf, "{s}", .{@errorName(err)}) catch "error";
    const exc = py.c.PyObject_CallFunction(py.c.PyExc_OSError, "s", name.ptr);
    closeTransport(st, exc);
    if (exc != null) py.decref(exc);
}

/// Tear down the connection: close the fd and stop I/O synchronously, but
/// defer the protocol's connection_lost(exc) to the next loop iteration via
/// call_soon - matching asyncio, so a set_protocol() that runs *after* close()
/// (the WebSocket-upgrade handoff does exactly this) is observed by the
/// connection_lost dispatch.
fn doConnectionLost(st: *State, exc: py.Object) void {
    if (st.conn_lost) return;
    st.conn_lost = true;

    stopWriting(st);
    if (st.reading) {
        stopReading(st);
    }
    if (usesCompletion(st)) st.engine.stopIo(st.fd); // drop callback + cancel send/recv
    closeFd(st);

    scheduleConnectionLost(st, exc);
}

/// Schedule transport._deliver_connection_lost(exc) via the loop's call_soon.
/// The Handle's args hold a ref to the transport, keeping it (and st) alive
/// until the callback runs.
fn scheduleConnectionLost(st: *State, exc: py.Object) void {
    const call_soon = py.getAttr(st.loop_obj, "call_soon");
    if (call_soon == null) {
        py.c.PyErr_Clear();
        return;
    }
    defer py.decref(call_soon);
    const method = py.getAttr(st.self_obj, "_deliver_connection_lost");
    if (method == null) {
        py.c.PyErr_Clear();
        return;
    }
    defer py.decref(method);
    const arg: py.Object = if (exc != null) exc else py.c.Py_None();
    const r = py.c.PyObject_CallFunctionObjArgs(call_soon, method, arg, @as(py.Object, null));
    if (r == null) {
        // Scheduling failed (e.g. the loop is already closed). Deliver
        // connection_lost synchronously instead, so the loop still untracks the
        // transport rather than leaking it in loop._transports.
        py.c.PyErr_Clear();
        const dr = t_deliver_connection_lost(st.self_obj, arg);
        if (dr == null) py.c.PyErr_Clear() else py.decref(dr);
    } else {
        py.decref(r);
    }
}

fn t_deliver_connection_lost(self_obj: ?*c.PyObject, exc: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.protocol != null) {
        const cl = py.getAttr(st.protocol, "connection_lost");
        if (cl != null) {
            defer py.decref(cl);
            const r = py.callOneArg(cl, exc.?);
            if (r == null) py.c.PyErr_Clear() else py.decref(r);
        } else {
            py.c.PyErr_Clear();
        }
        py.clear(&st.protocol);
    }
    // The loop no longer needs to keep this connection alive. Do this last: it
    // may drop the final reference and trigger dealloc.
    untrackFromLoop(st);
    return py.none();
}

fn reportProtocolError(st: *State, where: [*c]const u8) void {
    const exc = py.fetchException();
    if (exc == null) return;
    // Route to loop.call_exception_handler then drop the connection.
    const handler = py.getAttr(st.loop_obj, "call_exception_handler");
    if (handler != null) {
        defer py.decref(handler);
        const ctx = py.c.PyDict_New();
        if (ctx != null) {
            defer py.decref(ctx);
            var msgbuf: [128]u8 = undefined;
            const msg = std.fmt.bufPrintZ(&msgbuf, "Error in protocol.{s}()", .{std.mem.span(where)}) catch "protocol error";
            const message = py.fromStrZ(msg.ptr);
            _ = py.c.PyDict_SetItemString(ctx, "message", message);
            py.xdecref(message);
            _ = py.c.PyDict_SetItemString(ctx, "exception", exc);
            _ = py.c.PyDict_SetItemString(ctx, "transport", st.self_obj);
            const r = py.callOneArg(handler, ctx);
            if (r == null) py.c.PyErr_Clear() else py.decref(r);
        }
    } else {
        py.c.PyErr_Clear();
    }
    closeTransport(st, exc);
    py.decref(exc);
}

// ---------------------------------------------------------------------------
// Python methods
// ---------------------------------------------------------------------------

fn t_dealloc(self_obj: ?*c.PyObject) callconv(.c) void {
    const t: *TransportObject = @ptrCast(self_obj.?);
    py.c.PyObject_GC_UnTrack(@ptrCast(t));
    py.clear(&t.dict);
    if (t.state) |st| {
        if (!st.conn_lost) {
            if (st.reading) stopReading(st);
            stopWriting(st);
            if (usesCompletion(st)) st.engine.stopIo(st.fd);
            closeFd(st);
        }
        destroyState(st);
        t.state = null;
    }
    const t_obj: py.Object = @ptrCast(t);
    const tp: [*c]c.PyTypeObject = c.Py_TYPE(t_obj);
    const free = tp.*.tp_free.?;
    free(t_obj);
}

fn t_traverse(self_obj: ?*c.PyObject, visit: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const t: *TransportObject = @ptrCast(self_obj.?);
    if (t.dict != null) if (visit.?(t.dict, arg) != 0) return -1;
    if (t.state) |st| {
        if (st.protocol != null) if (visit.?(st.protocol, arg) != 0) return -1;
        if (st.extra != null) if (visit.?(st.extra, arg) != 0) return -1;
        if (st.sock_obj != null) if (visit.?(st.sock_obj, arg) != 0) return -1;
        if (st.context != null) if (visit.?(st.context, arg) != 0) return -1;
        if (st.loop_obj != null) if (visit.?(st.loop_obj, arg) != 0) return -1;
    }
    return 0;
}

fn t_clear(self_obj: ?*c.PyObject) callconv(.c) c_int {
    const t: *TransportObject = @ptrCast(self_obj.?);
    py.clear(&t.dict);
    if (t.state) |st| {
        // Detach from the engine before dropping the Loop reference, so a later
        // dealloc cannot touch a freed engine through st.engine.
        if (!st.conn_lost) {
            if (st.reading) stopReading(st);
            stopWriting(st);
            if (usesCompletion(st)) st.engine.stopIo(st.fd);
            closeFd(st);
            st.conn_lost = true;
        }
        py.clear(&st.protocol);
        py.clear(&st.extra);
        py.clear(&st.sock_obj);
        py.clear(&st.context);
        py.clear(&st.loop_obj);
    }
    return 0;
}

fn stateOf(self_obj: ?*c.PyObject) ?*State {
    const t: *TransportObject = @ptrCast(self_obj.?);
    return t.state;
}

fn t_write(self_obj: ?*c.PyObject, data: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.eof_sent) return py.raiseRuntime("Cannot call write() after write_eof()");
    var view: c.Py_buffer = undefined;
    if (c.PyObject_GetBuffer(data.?, &view, c.PyBUF_SIMPLE) != 0) return null;
    defer c.PyBuffer_Release(&view);
    const bytes: [*]const u8 = @ptrCast(view.buf.?);
    writeBytes(st, bytes[0..@intCast(view.len)]);
    return py.none();
}

fn t_writelines(self_obj: ?*c.PyObject, seq: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.eof_sent) return py.raiseRuntime("Cannot call writelines() after write_eof()");
    const iter = c.PyObject_GetIter(seq.?);
    if (iter == null) return null;
    defer py.decref(iter);
    while (true) {
        const item = c.PyIter_Next(iter);
        if (item == null) break;
        defer py.decref(item);
        var view: c.Py_buffer = undefined;
        if (c.PyObject_GetBuffer(item, &view, c.PyBUF_SIMPLE) != 0) return null;
        const bytes: [*]const u8 = @ptrCast(view.buf.?);
        writeBytes(st, bytes[0..@intCast(view.len)]);
        c.PyBuffer_Release(&view);
    }
    if (py.errOccurred()) return null;
    return py.none();
}

fn t_close(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (!st.closing and !st.conn_lost) {
        if (st.reading) {
            stopReading(st);
        }
        closeTransport(st, null);
    }
    return py.none();
}

fn t_abort(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    st.write_buf.clearRetainingCapacity();
    doConnectionLost(st, null);
    return py.none();
}

/// _force_close(exc): drop the connection immediately, reporting `exc` to
/// connection_lost. Used by asyncio.sslproto on handshake/IO failure.
fn t_force_close(self_obj: ?*c.PyObject, exc: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.conn_lost) return py.none();
    st.write_buf.clearRetainingCapacity();
    const e: py.Object = if (exc != null and !py.isNone(exc.?)) exc.? else null;
    doConnectionLost(st, e);
    return py.none();
}

fn t_is_closing(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.true_();
    return py.boolean(st.closing or st.conn_lost);
}

fn t_pause_reading(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.reading and !st.conn_lost) {
        stopReading(st);
    }
    return py.none();
}

fn t_resume_reading(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (!st.reading and !st.conn_lost and !st.read_eof) {
        startReading(st) catch return py.raiseRuntime("zloop: resume_reading failed");
    }
    return py.none();
}

fn t_write_eof(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.eof_sent or st.conn_lost) return py.none();
    st.eof_sent = true;
    // SHUT_WR now if nothing is pending; otherwise the flush path (writeCallback
    // or sendCompleted) shuts down once queued + in-flight bytes drain.
    if (pendingWrite(st) == 0 and !st.send_inflight) sys.shutdown(st.fd, 1);
    return py.none();
}

fn t_can_write_eof(_: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    return py.true_();
}

fn t_get_extra_info(self_obj: ?*c.PyObject, args: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    var name: ?*c.PyObject = null;
    var default: ?*c.PyObject = null;
    if (c.PyArg_UnpackTuple(args, "get_extra_info", 1, 2, &name, &default) == 0) return null;
    const val = c.PyDict_GetItemWithError(st.extra, name); // borrowed
    if (val != null) return py.newRef(val);
    if (py.errOccurred()) return null;
    if (default != null) return py.newRef(default);
    return py.none();
}

fn t_set_protocol(self_obj: ?*c.PyObject, proto: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    py.clear(&st.protocol);
    st.protocol = py.newRef(proto.?);
    return py.none();
}

fn t_get_protocol(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    if (st.protocol == null) return py.none(); // cleared after connection_lost
    return py.newRef(st.protocol);
}

fn t_get_write_buffer_size(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.fromI64(0);
    return py.fromUsize(st.write_buf.items.len);
}

fn t_set_write_buffer_limits(self_obj: ?*c.PyObject, args: ?*c.PyObject, kwargs: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    var high: ?*c.PyObject = null;
    var low: ?*c.PyObject = null;
    var kwlist = [_][*c]u8{ @constCast("high"), @constCast("low"), null };
    if (c.PyArg_ParseTupleAndKeywords(args, kwargs, "|OO", &kwlist, &high, &low) == 0) return null;

    // Mirror asyncio's derivation and validation exactly.
    var h: ?i64 = null;
    if (high != null and !py.isNone(high.?)) h = py.asI64(high.?) orelse return null;
    var l: ?i64 = null;
    if (low != null and !py.isNone(low.?)) l = py.asI64(low.?) orelse return null;

    if (h == null) {
        h = if (l == null) DEFAULT_HIGH_WATER else 4 * l.?;
    }
    if (l == null) {
        l = @divTrunc(h.?, 4);
    }
    if (!(h.? >= l.? and l.? >= 0)) {
        var buf: [96]u8 = undefined;
        const msg = std.fmt.bufPrintZ(&buf, "high ({d}) must be >= low ({d}) must be >= 0", .{ h.?, l.? }) catch "invalid water marks";
        return py.raiseValue(msg.ptr);
    }
    st.high_water = @intCast(h.?);
    st.low_water = @intCast(l.?);
    return py.none();
}

fn t_get_write_buffer_limits(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const st = stateOf(self_obj) orelse return py.none();
    const tup = py.tupleNew(2);
    if (tup == null) return null;
    py.tupleSet(tup, 0, py.fromUsize(st.low_water));
    py.tupleSet(tup, 1, py.fromUsize(st.high_water));
    return tup;
}

const NOARGS = c.METH_NOARGS;
const O = c.METH_O;
const VARARGS = c.METH_VARARGS;
const KW = c.METH_VARARGS | c.METH_KEYWORDS;

var methods = [_]py.MethodDef{
    .{ .ml_name = "write", .ml_meth = t_write, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "writelines", .ml_meth = t_writelines, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "close", .ml_meth = t_close, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "abort", .ml_meth = t_abort, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "_force_close", .ml_meth = t_force_close, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "is_closing", .ml_meth = t_is_closing, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "pause_reading", .ml_meth = t_pause_reading, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "resume_reading", .ml_meth = t_resume_reading, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "write_eof", .ml_meth = t_write_eof, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "can_write_eof", .ml_meth = t_can_write_eof, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "get_extra_info", .ml_meth = t_get_extra_info, .ml_flags = VARARGS, .ml_doc = null },
    .{ .ml_name = "set_protocol", .ml_meth = t_set_protocol, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "get_protocol", .ml_meth = t_get_protocol, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "get_write_buffer_size", .ml_meth = t_get_write_buffer_size, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "set_write_buffer_limits", .ml_meth = @ptrCast(&t_set_write_buffer_limits), .ml_flags = KW, .ml_doc = null },
    .{ .ml_name = "get_write_buffer_limits", .ml_meth = t_get_write_buffer_limits, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "_deliver_connection_lost", .ml_meth = t_deliver_connection_lost, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = null, .ml_meth = null, .ml_flags = 0, .ml_doc = null },
};

// A member named "__dictoffset__" makes PyType_FromSpec point tp_dictoffset at
// our `dict` field, so generic attribute get/set materialize an instance dict on
// every supported CPython (the managed-dict C-API is 3.13+ only).
var members = [_]c.PyMemberDef{
    .{ .name = "__dictoffset__", .type = c.Py_T_PYSSIZET, .offset = @offsetOf(TransportObject, "dict"), .flags = c.Py_READONLY, .doc = null },
    .{ .name = null, .type = 0, .offset = 0, .flags = 0, .doc = null },
};

// Expose `__dict__` itself (tp_dictoffset alone makes attributes work but adds no
// __dict__ descriptor), matching asyncio's pure-Python transports.
var getset = [_]c.PyGetSetDef{
    .{ .name = "__dict__", .get = c.PyObject_GenericGetDict, .set = c.PyObject_GenericSetDict, .doc = null, .closure = null },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

var slots = [_]py.Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&t_dealloc)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_members, .pfunc = @ptrCast(&members) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getset) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&t_traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&t_clear)) },
    .{ .slot = 0, .pfunc = null },
};

// The explicit __dict__ lets callers (and tests) set instance attributes, e.g.
// override transport.write, matching asyncio's pure-Python transports. The dict
// participates in GC via traverse/clear above.
var spec = py.Spec{
    .name = "zloop.Transport",
    .basicsize = @sizeOf(TransportObject),
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC,
    .slots = &slots,
};

pub fn registerType(module: py.Object) bool {
    transport_type = py.typeFromSpec(&spec);
    if (transport_type == null) return false;
    _ = module;
    return true;
}
