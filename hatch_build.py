from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

ROOT = Path(__file__).parent


def _zig_command() -> list[str]:
    """Resolve how to invoke Zig: a `zig` on PATH, else the `ziglang` pip package.

    The pip fallback (`python -m ziglang`) works identically on the host and inside
    cibuildwheel's manylinux containers, where a host-installed `zig` isn't visible.
    """
    if shutil.which("zig"):
        return ["zig"]
    try:
        import ziglang  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "Zig toolchain not found: install Zig and put it on PATH, or `pip install ziglang`."
        ) from None
    return [sys.executable, "-m", "ziglang"]


class ZigBuildHook(BuildHookInterface):
    """Compile the Zig extension against the building interpreter during the wheel build.

    This makes `uv build` / `pip wheel` / cibuildwheel produce a correct, platform-tagged
    wheel with no out-of-band step: the `.so` is built here, against `sys.executable`, and
    `build.zig` installs it into the `zloop/` package as `_zloop<EXT_SUFFIX>`.
    """

    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if self.target_name != "wheel":
            return

        include = sysconfig.get_path("platinclude")
        ext_suffix = sysconfig.get_config_var("EXT_SUFFIX")
        if not include or not ext_suffix:
            raise RuntimeError("could not resolve platinclude / EXT_SUFFIX from the building interpreter")

        mode = os.environ.get("ZLOOP_BUILD_MODE", "ReleaseFast")
        env = {**os.environ, "ZLOOP_PYTHON_INCLUDE": include, "ZLOOP_EXT_SUFFIX": ext_suffix}
        subprocess.run(
            [*_zig_command(), "build", f"-Doptimize={mode}"],
            cwd=ROOT,
            env=env,
            check=True,
        )

        artifact = f"zloop/_zloop{ext_suffix}"
        if not (ROOT / artifact).exists():
            raise RuntimeError(f"zig build did not produce {artifact}")

        # Tag the wheel for this interpreter + platform rather than py3-none-any.
        build_data["pure_python"] = False
        build_data["infer_tag"] = True
        build_data["artifacts"].append(artifact)

    def clean(self, versions: list[str]) -> None:
        for path in ROOT.glob("zloop/_zloop*.so"):
            path.unlink()
        for path in ROOT.glob("zloop/_zloop*.pyd"):
            path.unlink()
        print(f"removed compiled extensions; building Zig core via {sys.executable}", file=sys.stderr)
