#!/usr/bin/env python3
"""Render the exact helper bootstrap with pinned release and Updater trust."""
import base64
from pathlib import Path
import re
import sys

helper, version, public_key, output = sys.argv[1:]
root = Path(__file__).resolve().parents[1]
assert helper in ("neptune", "gryphon")
assert re.fullmatch(r"\d+\.\d+\.\d+", version)
updater_version = (root / ".release/updater.version").read_text().strip()
updater_sha = (root / ".release/updater-bootstrap.sha256").read_text().strip()
assert re.fullmatch(r"\d+\.\d+\.\d+", updater_version)
assert re.fullmatch(r"[a-f0-9]{64}", updater_sha)
source = (root / "bootstrap.sh").read_text()
for before, after in {
    "__HOST_HELPER__": helper,
    "__HOST_HELPER_VERSION__": version,
    "__HOST_HELPER_PUBLIC_KEY_BASE64__": base64.b64encode(Path(public_key).read_bytes()).decode(),
    "__UPDATER_VERSION__": updater_version,
    "__UPDATER_BOOTSTRAP_SHA256__": updater_sha,
}.items():
    assert before in source
    source = source.replace(before, after)
assert "__HOST_HELPER_" not in source and "__UPDATER_" not in source
Path(output).write_text(source, encoding="utf-8", newline="\n")
