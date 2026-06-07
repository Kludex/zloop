/* Free-threaded atomic shim.
 *
 * CPython's free-threaded headers implement Py_INCREF/refcount helpers with
 * inline atomics. Zig's translate-c (@cImport) can't translate the GCC atomic
 * builtins (`__atomic_load_n`), so it emits the atomic helpers as unresolved
 * `extern` declarations instead of inlining them - and the extension fails to
 * dlopen with "symbol not found: _Py_atomic_load_uint64_relaxed".
 *
 * We never call these from Zig (refcounting goes through Py_IncRef/Py_DecRef),
 * but the dead extern decls still need to resolve. Provide them here in real C,
 * where the builtins compile correctly. Add new symbols if a future header pulls
 * in more (check `nm -u` on the built .so). No effect on non-free-threaded
 * builds, where these symbols aren't referenced.
 */

#include <stdint.h>

uint64_t _Py_atomic_load_uint64_relaxed(const uint64_t *obj) {
    return __atomic_load_n(obj, __ATOMIC_RELAXED);
}
