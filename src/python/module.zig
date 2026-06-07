const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const loop_obj = @import("loop_obj.zig");
const handle = @import("handle.zig");
const transport = @import("transport_obj.zig");

comptime {
    @export(&PyInit__zloop, .{ .name = "PyInit__zloop", .linkage = .strong });
}

fn new_event_loop(_: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const tp = loop_obj.loopType();
    if (tp == null) return py.raiseRuntime("zloop: Loop type not initialised");
    return py.callNoArgs(tp);
}

var module_methods = [_]py.MethodDef{
    .{ .ml_name = "new_event_loop", .ml_meth = new_event_loop, .ml_flags = c.METH_NOARGS, .ml_doc = "Create a new zloop event loop." },
    .{ .ml_name = null, .ml_meth = null, .ml_flags = 0, .ml_doc = null },
};

var module_def = c.PyModuleDef{
    .m_name = "_zloop",
    .m_doc = "zloop: an asyncio event loop with a Zig core.",
    .m_size = -1,
    .m_methods = &module_methods,
};

fn PyInit__zloop() callconv(.c) ?*c.PyObject {
    const m = c.PyModule_Create(&module_def);
    if (m == null) return null;

    // Declare free-threading support so importing on a free-threaded build does
    // not force the GIL back on. Available only on free-threaded CPython; the
    // @hasDecl guard makes this a no-op on regular (GIL) builds.
    if (@hasDecl(c, "PyUnstable_Module_SetGIL")) {
        _ = c.PyUnstable_Module_SetGIL(m, c.Py_MOD_GIL_NOT_USED);
    }

    if (!handle.registerTypes(m)) {
        py.decref(m);
        return null;
    }
    if (!transport.registerType(m)) {
        py.decref(m);
        return null;
    }
    if (!loop_obj.registerType(m)) {
        py.decref(m);
        return null;
    }

    _ = c.PyModule_AddObjectRef(m, "Loop", loop_obj.loopType());
    _ = c.PyModule_AddObjectRef(m, "Handle", handle.handleType());
    _ = c.PyModule_AddObjectRef(m, "TimerHandle", handle.timerType());
    return m;
}
