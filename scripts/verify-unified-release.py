#!/usr/bin/env python3
"""Verify every release artifact and trust link before public publication."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile


def verify(directory):
    tag = os.environ["GITHUB_REF_NAME"]
    repository = os.environ["GITHUB_REPOSITORY"]
    if not re.fullmatch(r"neptune-v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", tag):
        raise ValueError("Invalid Neptune release tag")
    version = tag.removeprefix("neptune-v")
    artifacts = [
        (f"neptune-linux-{version}-linux-x64.tar.gz", "neptune-linux", "linux-x64", "neptune-linux-release-linux-x64.json"),
        (f"neptune-linux-{version}-linux-arm64.tar.gz", "neptune-linux", "linux-arm64", "neptune-linux-release-linux-arm64.json"),
        (f"neptune-windows-{version}-win-x64.zip", "neptune-windows", "win-x64", "neptune-windows-release.json"),
    ]
    key = (directory / "neptune.pem").read_bytes()
    if not key:
        raise ValueError("Missing Neptune trust key")
    with zipfile.ZipFile(directory / artifacts[2][0]) as archive:
        if archive.read("release-trust/neptune.pem") != key:
            raise ValueError("Windows bundle trust key differs from release trust key")
    for name, product, runtime, manifest_name in artifacts:
        artifact = directory / name
        if not artifact.is_file() or artifact.stat().st_size == 0:
            raise ValueError(f"Missing release artifact: {name}")
        checksum_lines = (directory / f"{name}.sha256").read_text(encoding="utf-8").strip().splitlines()
        if len(checksum_lines) != 1:
            raise ValueError(f"Invalid checksum file: {name}")
        match = re.fullmatch(r"([a-fA-F0-9]{64})\s+([^\\/]+)", checksum_lines[0])
        if not match or match[2] != name:
            raise ValueError(f"Checksum identity mismatch: {name}")
        actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if actual != match[1].lower():
            raise ValueError(f"Checksum mismatch: {name}")
        manifest_path = directory / manifest_name
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("schema") != "exocortex.neptune.release.v1"
            or manifest.get("product") != product
            or manifest.get("runtime") != runtime
            or manifest.get("version") != version
            or manifest.get("artifact") != name
            or manifest.get("sha256") != actual
            or manifest.get("size") != artifact.stat().st_size
            or manifest.get("url") != f"https://github.com/{repository}/releases/download/{tag}/{name}"
        ):
            raise ValueError(f"Manifest contract mismatch: {manifest_name}")
        subprocess.run(
            [sys.executable, "scripts/verify-release-manifest.py", str(manifest_path), str(directory / f"{manifest_name}.sig.json"), str(directory / "neptune.pem")],
            check=True,
        )
    print("PASS unified Neptune release artifacts, checksums, manifests and trust key")


if __name__ == "__main__":
    try:
        verify(Path(sys.argv[1] if len(sys.argv) > 1 else "release"))
    except (OSError, ValueError, KeyError, zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        print(f"Neptune release verification: {error}", file=sys.stderr)
        sys.exit(1)
