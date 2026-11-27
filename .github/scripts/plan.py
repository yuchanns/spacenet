#!/usr/bin/env python3
"""OpenDAL-style CI planner for spacenet multi-module workspace.

Rules:
- On push to main / workflow_dispatch: run full matrix (core + all contribs + cmd + sync).
- On pull_request:
  - Changes to core runtime / root module / CI / third_party trigger full matrix.
  - Changes isolated to specific contrib/<name> modules trigger only those contribs
    and their transitive reverse dependents (computed from contrib/*/go.mod).
  - Changes isolated to cmd/spacenet trigger only cmd.
  - Pure Markdown/doc edits skip Go test matrices cleanly.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE_PREFIX = "github.com/yuchanns/spacenet/contrib/"


def discover_contribs() -> list[str]:
    contrib_dir = ROOT / "contrib"
    if not contrib_dir.is_dir():
        return []
    return sorted(
        p.name for p in contrib_dir.iterdir() if p.is_dir() and (p / "go.mod").is_file()
    )


def build_reverse_deps(contribs: list[str]) -> dict[str, set[str]]:
    rdeps: dict[str, set[str]] = {c: set() for c in contribs}
    pattern = re.compile(re.escape(MODULE_PREFIX) + r"([A-Za-z0-9_-]+)")
    for c in contribs:
        gomod = (ROOT / "contrib" / c / "go.mod").read_text(encoding="utf-8")
        for dep in pattern.findall(gomod):
            if dep != c and dep in rdeps:
                rdeps[dep].add(c)
    return rdeps


def expand_rdeps(seeds: set[str], rdeps: dict[str, set[str]]) -> list[str]:
    visited: set[str] = set()
    stack = list(seeds)
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        for nxt in rdeps.get(cur, ()):
            if nxt not in visited:
                stack.append(nxt)
    return sorted(visited)


def changed_files(base: str, head: str) -> list[str]:
    out = subprocess.check_output(
        ["git", "diff", "--name-only", f"{base}...{head}"],
        cwd=ROOT,
        text=True,
    )
    return [line.strip() for line in out.splitlines() if line.strip()]


def inspect_gowork_diff(base: str, head: str) -> tuple[bool, set[str], bool]:
    """Return (core_changed, affected_contribs, cmd_changed) for go.work diff."""
    diff = subprocess.check_output(
        ["git", "diff", "-U0", f"{base}...{head}", "--", "go.work"],
        cwd=ROOT,
        text=True,
    )
    affected_contribs: set[str] = set()
    cmd_changed = False
    for line in diff.splitlines():
        if not line or line.startswith(("+++", "---", "@@", "diff ", "index ")):
            continue
        if line[0] not in ("+", "-"):
            continue
        body = line[1:].strip()
        if not body or body in ("replace (", "use (", ")"):
            continue
        m_use = re.match(r"^\./contrib/([A-Za-z0-9_-]+)$", body)
        if m_use:
            affected_contribs.add(m_use.group(1))
            continue
        m_rep = re.match(
            r"^github\.com/yuchanns/spacenet/contrib/([A-Za-z0-9_-]+)\s+v\S+\s+=>\s+\./contrib/\1$",
            body,
        )
        if m_rep:
            affected_contribs.add(m_rep.group(1))
            continue
        if body == "./cmd/spacenet":
            cmd_changed = True
            continue
        # Any other change in go.work (e.g., Go version or root module replace) affects core.
        return True, affected_contribs, cmd_changed
    return False, affected_contribs, cmd_changed


def is_doc_only(path: str) -> bool:
    name = Path(path).name
    return name.endswith(".md") or name in ("LICENSE", "NOTICE", ".gitignore")


def compute_plan(event: str, base: str, head: str) -> dict:
    all_contribs = discover_contribs()
    has_cmd = (ROOT / "cmd" / "spacenet" / "go.mod").is_file()
    rdeps = build_reverse_deps(all_contribs)

    if event != "pull_request" or not base or not head:
        return {
            "reason": f"full run ({event or 'default'})",
            "run_sync": True,
            "run_core": True,
            "run_cmd": has_cmd,
            "contribs": all_contribs,
        }

    files = changed_files(base, head)
    if not files:
        return {
            "reason": "no changed files",
            "run_sync": False,
            "run_core": False,
            "run_cmd": False,
            "contribs": [],
        }

    core_changed = False
    run_sync = False
    run_cmd = False
    changed_contribs: set[str] = set()

    for f in files:
        if is_doc_only(f):
            continue
        if f == "scripts/sync_skynet_lua.py":
            run_sync = True
            continue
        if f == "go.work":
            gw_core, gw_contribs, gw_cmd = inspect_gowork_diff(base, head)
            if gw_core:
                core_changed = True
            changed_contribs.update(c for c in gw_contribs if c in all_contribs)
            if gw_cmd:
                run_cmd = True
            continue
        if f.startswith("contrib/"):
            parts = f.split("/")
            if len(parts) >= 2 and parts[1] in all_contribs:
                changed_contribs.add(parts[1])
                if len(parts) >= 3 and parts[2] == "lua":
                    run_sync = True
                continue
        if f.startswith("cmd/spacenet/"):
            run_cmd = True
            continue
        # Root Go code, runtime/, assets/, go.mod, go.sum, third_party/, .github/ -> full run.
        core_changed = True
        run_sync = True

    if core_changed:
        return {
            "reason": "core/workspace/workflow changed",
            "run_sync": True,
            "run_core": True,
            "run_cmd": has_cmd,
            "contribs": all_contribs,
        }

    selected = expand_rdeps(changed_contribs, rdeps)
    return {
        "reason": f"scoped PR diff ({len(files)} files)",
        "run_sync": run_sync,
        "run_core": False,
        "run_cmd": run_cmd and has_cmd,
        "contribs": selected,
    }


def main() -> int:
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    base = os.environ.get("PR_BASE_SHA", "")
    head = os.environ.get("PR_HEAD_SHA", "")

    plan = compute_plan(event, base, head)
    contribs_json = json.dumps(plan["contribs"])
    has_contribs = len(plan["contribs"]) > 0

    print(f"CI Plan ({plan['reason']}):")
    print(f"  run_sync     = {plan['run_sync']}")
    print(f"  run_core     = {plan['run_core']}")
    print(f"  run_cmd      = {plan['run_cmd']}")
    print(f"  has_contribs = {has_contribs}")
    print(f"  contribs     = {contribs_json}")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as fh:
            fh.write(f"run_sync={'true' if plan['run_sync'] else 'false'}\n")
            fh.write(f"run_core={'true' if plan['run_core'] else 'false'}\n")
            fh.write(f"run_cmd={'true' if plan['run_cmd'] else 'false'}\n")
            fh.write(f"has_contribs={'true' if has_contribs else 'false'}\n")
            fh.write(f"contribs={contribs_json}\n")

    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a", encoding="utf-8") as fh:
            fh.write(f"### CI Plan ({plan['reason']})\n\n")
            fh.write(f"- **verify-sync**: `{plan['run_sync']}`\n")
            fh.write(f"- **core**: `{plan['run_core']}`\n")
            fh.write(f"- **contribs**: `{contribs_json}`\n")
            fh.write(f"- **cmd**: `{plan['run_cmd']}`\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
