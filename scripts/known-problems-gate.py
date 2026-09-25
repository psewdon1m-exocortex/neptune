#!/usr/bin/env python3
"""Revision-bound, fail-closed Part 12 release evidence gate for Neptune."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / ".release/known-problems-policy.json"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key: " + key)
        result[key] = value
    return result


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def load_catalog(policy, directory):
    catalog = directory / "catalog.md"
    if not catalog.exists():
        url = (
            "https://raw.githubusercontent.com/psewdon1m-exocortex/general/"
            + policy["catalog_revision"] + "/" + policy["remote_path"]
        )
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read(2 * 1024 * 1024 + 1)
        if len(data) > 2 * 1024 * 1024 or digest(data) != policy["catalog_sha256"]:
            raise ValueError("Pinned policy checksum mismatch")
        catalog.write_bytes(data)
    data = catalog.read_bytes()
    if digest(data) != policy["catalog_sha256"]:
        raise ValueError("Stale or altered policy catalog")
    text = data.decode("utf-8")
    rows = []
    for line in text.splitlines():
        match = re.match(r"\| \*\*([A-Z]+-\d{2})\*\* \| (.+) \| (.+) \|$", line)
        if match:
            rows.append({"id": match[1], "problem": match[2].strip(), "solution": match[3].strip()})
    ids = [row["id"] for row in rows]
    if not ids or len(ids) != len(set(ids)) or len(ids) != policy["active_ids"]:
        raise ValueError("Invalid catalog ID inventory")
    if any(not row["problem"] or not row["solution"] for row in rows):
        raise ValueError("Empty problem/solution cell")
    for link in sorted(set(re.findall(r"\]\(([^)]+)\)", text))):
        if link.startswith("./") and not re.fullmatch(r"\./PART_\d{2}_[A-Z_]+\.md(?:#[^ ]+)?", link):
            raise ValueError("Unexpected local policy link")
        if link.startswith("./"):
            filename = link[2:].split("#", 1)[0]
            cached = directory / filename
            if not cached.exists():
                url = (
                    "https://raw.githubusercontent.com/psewdon1m-exocortex/general/"
                    + policy["catalog_revision"] + "/" + filename
                )
                with urllib.request.urlopen(url, timeout=30) as response:
                    linked = response.read(2 * 1024 * 1024 + 1)
                if len(linked) > 2 * 1024 * 1024 or not linked.startswith(b"# "):
                    raise ValueError("Invalid linked policy document")
                cached.write_bytes(linked)
    return rows


def expand_plan(policy, rows):
    ids = {row["id"] for row in rows}
    overrides = policy.get("case_overrides", {})
    final = set(policy.get("final_checks", []))
    not_applicable = policy.get("not_applicable", {})
    configured = set(overrides) | final | set(not_applicable)
    if not configured.issubset(ids):
        raise ValueError("Applicability policy names unknown catalog IDs")
    plan = {}
    for row in rows:
        problem_id = row["id"]
        if problem_id in not_applicable:
            entry = {**not_applicable[problem_id], "not_applicable": True}
        else:
            prefix = problem_id.split("-", 1)[0]
            cases = overrides.get(problem_id, policy.get("case_defaults", {}).get(prefix))
            if not cases:
                raise ValueError("Missing executable applicability plan: " + problem_id)
            entry = {"cases": ["published-assets"] if problem_id in final else cases}
            if problem_id in final:
                entry["phase"] = "final"
        entry["scope"] = row["solution"]
        plan[problem_id] = entry
    if len(plan) != len(rows):
        raise ValueError("Applicability plan must contain every active ID exactly once")
    return plan


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["structure", "pre-signing", "final"], default="structure")
    parser.add_argument("--evidence-dir", default=".release-evidence")
    parser.add_argument("--report", default="known-problems-report.json")
    parser.add_argument("--record")
    parser.add_argument("--command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    policy = read_json(POLICY)
    directory = (ROOT / args.evidence_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_relative_to(ROOT):
        raise ValueError("Evidence must remain inside the project")
    if not re.fullmatch(r"[a-f0-9]{40}", policy["catalog_revision"]):
        raise ValueError("Policy must pin a full commit SHA")
    rows = load_catalog(policy, directory)
    plan = expand_plan(policy, rows)
    revision = git("rev-parse", "HEAD")
    run_id = os.getenv("GITHUB_RUN_ID", "local") + ":" + os.getenv("GITHUB_RUN_ATTEMPT", "1")
    identity = {"revision": revision, "catalog_sha256": policy["catalog_sha256"], "run_id": run_id}
    if args.record:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,40}", args.record or "") or not args.command:
            raise ValueError("A receipt needs a named executable command")
        result = subprocess.run(args.command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        log = directory / (args.record + ".log")
        log.write_bytes(result.stdout)
        receipt = {
            **identity,
            "case": args.record,
            "command": args.command,
            "exit_code": result.returncode,
            "log": log.name,
            "log_sha256": digest(result.stdout),
        }
        (directory / (args.record + ".json")).write_text(
            json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
        )
        sys.stdout.buffer.write(result.stdout)
        return result.returncode
    checks = []
    for row in rows:
        problem_id = row["id"]
        entry = plan[problem_id]
        status = "UNKNOWN"
        reason = None
        evidence = []
        if entry.get("not_applicable"):
            reason = entry["reason"]
            paths = entry.get("paths", [])
            if len(reason) < 50 or not paths or any(not (ROOT / item).exists() for item in paths):
                raise ValueError("Unexplained N/A: " + problem_id)
            status = "N/A"
        else:
            good = True
            for case in entry["cases"]:
                if not re.fullmatch(r"[a-z][a-z0-9-]{0,40}", case):
                    raise ValueError("Invalid executable case: " + problem_id)
                receipt_path = directory / (case + ".json")
                if not receipt_path.exists():
                    good = False
                    continue
                receipt = read_json(receipt_path)
                log = directory / receipt.get("log", "")
                if log.parent != directory or not log.is_file() or any(receipt.get(key) != value for key, value in identity.items()):
                    good = False
                    continue
                if receipt.get("case") != case or not receipt.get("command") or digest(log.read_bytes()) != receipt.get("log_sha256"):
                    good = False
                    continue
                if receipt.get("exit_code") != 0:
                    status = "FAIL"
                    good = False
                evidence.append({
                    "receipt": str(receipt_path.relative_to(ROOT)),
                    "command": receipt["command"],
                    "log_sha256": receipt["log_sha256"],
                })
            if good:
                status = "PASS"
        deferred = entry.get("phase") == "final" and args.phase != "final"
        checks.append({
            "id": problem_id,
            "status": status,
            "phase": entry.get("phase", "pre-signing"),
            "deferred": deferred,
            "evidence": evidence,
            "reason": reason,
            "scope": entry["scope"],
        })
    unresolved = [
        item["id"] for item in checks
        if item["status"] in ["FAIL", "UNKNOWN"] and not item["deferred"]
    ]
    tag = os.getenv("GITHUB_REF_NAME", "")
    if args.phase != "structure":
        expected = re.escape(policy["service"]) + r"-v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?"
        if not re.fullmatch(expected, tag):
            raise ValueError("Release qualification requires an exact service-qualified tag")
        if git("rev-parse", tag + "^{commit}") != revision:
            raise ValueError("Tag/revision mismatch")
        if git("status", "--porcelain", "--untracked-files=no"):
            raise ValueError("Dirty source cannot qualify a release")
    report = {
        "schema_version": 1,
        "service": policy["service"],
        **identity,
        "release_tag": tag or None,
        "phase": args.phase,
        "catalog_repository": "https://github.com/psewdon1m-exocortex/general",
        "catalog_revision": policy["catalog_revision"],
        "catalog_path": policy["remote_path"],
        "checks": checks,
        "release_qualification": args.phase == "final" and not unresolved,
        "deployment_activation": {
            "status": "NOT_RUN",
            "reason": "Production DNS/TLS, enrolled credentials, external reachability and installed agent versions require operator activation checks on the target hosts.",
        },
    }
    (ROOT / args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Part 12 {args.phase}: {len(checks)} IDs, {len(unresolved)} unresolved, qualification={report['release_qualification']}")
    return 1 if unresolved and args.phase != "structure" else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print("Part 12 gate:", error, file=sys.stderr)
        sys.exit(1)
