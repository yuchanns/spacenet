#!/usr/bin/env python3
"""Sync curated upstream Skynet Lua files from third_party/skynet into Core & Contrib embed trees.

Run with --check in CI to guarantee zero byte drift from third_party/skynet.
"""
import argparse
import hashlib
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKYNET = ROOT / "third_party" / "skynet"

MANIFEST = {
    "assets/core/lua": [
        "lualib/loader.lua",
        "lualib/skynet.lua",
        "lualib/skynet/require.lua",
        "lualib/skynet/manager.lua",
        "lualib/skynet/coroutine.lua",
        "lualib/skynet/queue.lua",
        "lualib/skynet/mqueue.lua",
        "lualib/skynet/datacenter.lua",
        "lualib/skynet/service.lua",
        "lualib/skynet/snax.lua",
        "lualib/skynet/debug.lua",
        "lualib/skynet/harbor.lua",
        "lualib/snax/interface.lua",
        "lualib/snax/hotfix.lua",
        "service/bootstrap.lua",
        "service/launcher.lua",
        "service/cdummy.lua",
        "service/service_mgr.lua",
        "service/datacenterd.lua",
        "service/service_provider.lua",
        "service/service_cell.lua",
        "service/snaxd.lua",
    ],
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Verify embedded Lua files match third_party/skynet")
    args = parser.parse_args()

    if not SKYNET.exists():
        print(f"ERROR: upstream skynet submodule missing at {SKYNET}", file=sys.stderr)
        return 1

    mismatches = 0
    total = 0
    for target_rel, files in MANIFEST.items():
        target_root = ROOT / target_rel
        for rel in files:
            total += 1
            src = SKYNET / rel
            dst = target_root / rel
            if not src.exists():
                print(f"MISSING UPSTREAM: {src}", file=sys.stderr)
                mismatches += 1
                continue
            if args.check:
                if not dst.exists():
                    print(f"MISSING EMBED: {dst}", file=sys.stderr)
                    mismatches += 1
                elif sha256(src) != sha256(dst):
                    print(f"DRIFT: {dst} != {src}", file=sys.stderr)
                    mismatches += 1
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dst)

    if args.check:
        if mismatches:
            print(f"FAIL: {mismatches}/{total} embedded Lua files out of sync", file=sys.stderr)
            return 1
        print(f"OK: all {total} embedded Lua files match third_party/skynet byte-for-byte")
    else:
        print(f"Synced {total} Lua files across {len(MANIFEST)} core/contrib packages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
