#!/usr/bin/env python3
"""Sync curated upstream Skynet Lua files from third_party/skynet into Core & Contrib embed trees,
mapping module paths and require targets from skynet / skynet.* to spacenet / spacenet.*.

Run with --check in CI to guarantee zero drift from transformed third_party/skynet sources.
"""
import argparse
import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKYNET = ROOT / "third_party" / "skynet"

REQUIRE_RE = re.compile(
    r"""(\brequire\s*(?:\(\s*)?)(["'])skynet((?:\.[A-Za-z0-9_]+)*)\2"""
)

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
    "contrib/crypt/lua": [
        "lualib/md5.lua",
    ],
    "contrib/socket/lua": [
        "lualib/skynet/socket.lua",
        "lualib/skynet/socketchannel.lua",
        "lualib/skynet/dns.lua",
        "lualib/snax/gateserver.lua",
        "lualib/snax/msgserver.lua",
        "lualib/snax/loginserver.lua",
        "service/gate.lua",
    ],
    "contrib/http/lua": [
        "lualib/http/httpd.lua",
        "lualib/http/httpc.lua",
        "lualib/http/websocket.lua",
        "lualib/http/url.lua",
        "lualib/http/sockethelper.lua",
        "lualib/http/internal.lua",
        "lualib/http/tlshelper.lua",
    ],
    "contrib/sproto/lua": [
        "lualib/sproto.lua",
        "lualib/sprotoloader.lua",
    ],
    "contrib/share/lua": [
        "lualib/skynet/sharetable.lua",
        "lualib/skynet/datasheet/init.lua",
        "lualib/skynet/datasheet/builder.lua",
        "lualib/skynet/datasheet/dump.lua",
        "lualib/skynet/sharedata.lua",
        "lualib/skynet/sharedata/corelib.lua",
        "lualib/skynet/sharemap.lua",
        "lualib/skynet/multicast.lua",
        "service/sharedatad.lua",
        "service/multicastd.lua",
    ],
    "contrib/cluster/lua": [
        "lualib/skynet/cluster.lua",
        "service/clusterd.lua",
        "service/clusteragent.lua",
        "service/clustersender.lua",
        "service/clusterproxy.lua",
    ],
    "contrib/debug/lua": [
        "lualib/skynet/inject.lua",
        "lualib/skynet/injectcode.lua",
        "lualib/skynet/remotedebug.lua",
        "service/cmemory.lua",
        "service/console.lua",
        "service/dbg.lua",
        "service/debug_agent.lua",
        "service/debug_console.lua",
    ],
}


def map_rel_path(rel: str) -> str:
    if rel == "lualib/skynet.lua":
        return "lualib/spacenet.lua"
    if rel.startswith("lualib/skynet/"):
        return "lualib/spacenet/" + rel[len("lualib/skynet/") :]
    return rel


def transform_lua(data: bytes) -> bytes:
    text = data.decode("utf-8")
    transformed = REQUIRE_RE.sub(r"\1\2spacenet\3\2", text)
    return transformed.encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def prune_empty_dirs(root: Path) -> None:
    for d in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify embedded Lua files match transformed third_party/skynet sources",
    )
    args = parser.parse_args()

    if not SKYNET.exists():
        print(f"ERROR: upstream skynet submodule missing at {SKYNET}", file=sys.stderr)
        return 1

    mismatches = 0
    total = 0
    for target_rel, files in MANIFEST.items():
        target_root = ROOT / target_rel
        expected: set[Path] = set()
        for rel in files:
            total += 1
            src = SKYNET / rel
            dst = target_root / map_rel_path(rel)
            expected.add(dst)
            if not src.exists():
                print(f"MISSING UPSTREAM: {src}", file=sys.stderr)
                mismatches += 1
                continue
            want_bytes = transform_lua(src.read_bytes())
            if args.check:
                if not dst.exists():
                    print(f"MISSING EMBED: {dst}", file=sys.stderr)
                    mismatches += 1
                elif sha256_bytes(dst.read_bytes()) != sha256_bytes(want_bytes):
                    print(f"DRIFT: {dst} != transformed {src}", file=sys.stderr)
                    mismatches += 1
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(want_bytes)

        if target_root.exists():
            for existing in sorted(target_root.rglob("*.lua")):
                if existing not in expected:
                    if args.check:
                        print(f"UNEXPECTED EMBED: {existing}", file=sys.stderr)
                        mismatches += 1
                    else:
                        existing.unlink()
            if not args.check:
                prune_empty_dirs(target_root)

    if args.check:
        if mismatches:
            print(f"FAIL: {mismatches} issue(s) across {total} embedded Lua files", file=sys.stderr)
            return 1
        print(f"OK: all {total} embedded Lua files match transformed third_party/skynet")
    else:
        print(f"Synced {total} Lua files across {len(MANIFEST)} core/contrib packages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
