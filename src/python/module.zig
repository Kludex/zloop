const std = @import("std");
const c = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", "1");
    @cInclude("Python.h");
});

comptime {
    @export(&PyInit__zloop, .{ .name = "PyInit__zloop", .linkage = .strong });
}

var module_def = c.PyModuleDef{
    .m_name = "_zloop",
    .m_doc = "zloop core extension (toolchain probe)",
    .m_size = -1,
};

fn PyInit__zloop() callconv(.c) ?*c.PyObject {
    const m = c.PyModule_Create(&module_def);
    if (m == null) return null;
    _ = c.PyModule_AddIntConstant(m, "answer", 42);
    return m;
}
