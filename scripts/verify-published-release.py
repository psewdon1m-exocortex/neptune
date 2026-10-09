#!/usr/bin/env python3
"""Qualify anonymous prerelease downloads against the locally verified candidate."""
import argparse
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import urllib.request


METADATA_LIMIT = 2 * 1024 * 1024


def identity(environment=os.environ, require_revision=True):
    tag = environment["GITHUB_REF_NAME"]
    repository = environment["GITHUB_REPOSITORY"]
    revision = environment.get("GITHUB_SHA", "")
    if not re.fullmatch(r"neptune-v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", tag):
        raise ValueError("Invalid Neptune release tag")
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
        raise ValueError("Invalid Neptune repository")
    if require_revision and not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Invalid Neptune source revision")
    return tag, repository, revision


def asset_names(tag):
    version = tag.removeprefix("neptune-v")
    names = ["neptune.pem", "bootstrap.sh"]
    for product, runtime, extension, manifest in [
        ("neptune-linux", "linux-x64", "tar.gz", "neptune-linux-release-linux-x64.json"),
        ("neptune-linux", "linux-arm64", "tar.gz", "neptune-linux-release-linux-arm64.json"),
        ("neptune-windows", "win-x64", "zip", "neptune-windows-release.json"),
    ]:
        artifact = f"{product}-{version}-{runtime}.{extension}"
        names.extend([artifact, artifact + ".sha256", manifest, manifest + ".sig.json"])
    return names


def download(url, output, maximum):
    # Deliberately no credential lookup: visibility must work for an anonymous client.
    if not url.startswith("https://"):
        raise ValueError("Anonymous download requires HTTPS")
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json" if url.startswith("https://api.github.com/") else "application/octet-stream",
        "User-Agent": "neptune-release-verifier",
    })
    deadline = time.monotonic() + 120
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200 or not response.url.startswith("https://"):
            raise ValueError("Anonymous release download failed")
        length = response.headers.get("Content-Length")
        if length is not None and (not length.isdecimal() or int(length) > maximum):
            raise ValueError("Release download exceeds limit")
        size = 0
        while chunk := response.read(64 * 1024):
            size += len(chunk)
            if size > maximum or time.monotonic() > deadline:
                raise ValueError("Release download exceeds limit or deadline")
            output.write(chunk)


def digest(file):
    with file.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def compare_reports(expected, actual):
    def stable(report):
        if not isinstance(report, dict) or report.get("phase") != "final" or report.get("release_qualification") is not True:
            raise ValueError("Report is not a final qualification")
        checks = report.get("checks")
        if not isinstance(checks, list) or not checks or any(
            not isinstance(item, dict) or item.get("status") not in ("PASS", "N/A") or item.get("deferred") is not False for item in checks
        ):
            raise ValueError("Report has unresolved or deferred checks")
        if len({item.get("id") for item in checks}) != len(checks):
            raise ValueError("Report has duplicate checks")
        return {
            **{key: value for key, value in report.items() if key not in ("run_id", "checks")},
            "checks": [{key: value for key, value in item.items() if key != "evidence"} for item in checks],
        }
    if stable(expected) != stable(actual):
        raise ValueError("Published qualification report identity/results differ")


def verify_report(report_path, fetch=download):
    tag, repository, revision = identity()
    expected = json.loads(report_path.read_bytes())
    if expected.get("service") != "neptune" or expected.get("revision") != revision or expected.get("release_tag") != tag:
        raise ValueError("Local qualification report/source identity mismatch")
    output = io.BytesIO()
    fetch(f"https://github.com/{repository}/releases/download/{tag}/known-problems-report.json", output, METADATA_LIMIT)
    compare_reports(expected, json.loads(output.getvalue()))
    print("PASS anonymous final report; current run's gate remains mandatory")


def verify(directory, fetch=download):
    tag, repository, revision = identity()
    base = f"https://github.com/{repository}/releases/download/{tag}/"
    api = f"https://api.github.com/repos/{repository}"

    def read_json(url):
        output = io.BytesIO()
        fetch(url, output, METADATA_LIMIT)
        value = json.loads(output.getvalue())
        if not isinstance(value, dict):
            raise ValueError("Invalid GitHub metadata")
        return value

    release = read_json(f"{api}/releases/tags/{tag}")
    if release.get("tag_name") != tag or release.get("draft") is not False or release.get("prerelease") is not True:
        raise ValueError("Release must be the visible prerelease candidate")
    target = read_json(f"{api}/git/ref/tags/{tag}").get("object", {})
    for _ in range(4):
        if target.get("type") != "tag":
            break
        sha = target.get("sha", "")
        if not re.fullmatch(r"[a-f0-9]{40}", sha):
            raise ValueError("Invalid annotated tag")
        target = read_json(f"{api}/git/tags/{sha}").get("object", {})
    if target.get("type") != "commit" or target.get("sha") != revision:
        raise ValueError("Published tag/source revision mismatch")
    names = asset_names(tag)
    assets = release.get("assets", [])
    if not isinstance(assets, list) or not all(isinstance(item, dict) for item in assets):
        raise ValueError("Invalid release inventory")
    by_name = {item.get("name"): item for item in assets}
    # A report may be present after a failed promotion; executable assets stay immutable.
    if len(by_name) != len(assets) or not set(names).issubset(by_name) or set(by_name) - set(names) - {"known-problems-report.json"}:
        raise ValueError("Published release inventory mismatch")
    spec = importlib.util.spec_from_file_location("unified_release", Path(__file__).with_name("verify-unified-release.py"))
    unified = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(unified)
    unified.verify(directory)
    with tempfile.TemporaryDirectory(prefix="neptune-anonymous-release-") as temporary:
        published = Path(temporary)
        for name in names:
            candidate = directory / name
            size = candidate.stat().st_size
            item = by_name[name]
            if item.get("state") != "uploaded" or item.get("size") != size or item.get("browser_download_url") != base + name:
                raise ValueError(f"Published asset metadata mismatch: {name}")
            destination = published / name
            with destination.open("wb") as output:
                fetch(base + name, output, size)
            if destination.stat().st_size != size or digest(destination) != digest(candidate):
                raise ValueError(f"Published bytes differ from candidate: {name}")
        # Includes Windows's embedded trust root, checksums and three real signatures.
        unified.verify(published)
    print(f"PASS anonymous Neptune candidate: {tag}, revision {revision}, {len(names)} exact assets")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", default="release", type=Path)
    parser.add_argument("--asset-names", action="store_true")
    parser.add_argument("--verify-report", type=Path)
    args = parser.parse_args()
    try:
        if args.verify_report:
            verify_report(args.verify_report)
        elif args.asset_names:
            print("\n".join(asset_names(identity(require_revision=False)[0])))
        else:
            verify(args.directory)
    except Exception as error:
        print(f"Neptune published release verification: {error}", file=sys.stderr)
        sys.exit(1)
