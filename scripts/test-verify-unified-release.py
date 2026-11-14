"""Exercise the published release contract with a disposable signing key."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
TAG = "neptune-v0.1.9"
REPOSITORY = "psewdon1m-exocortex/neptune"
REVISION = "a" * 40


def windows_module():
    spec = importlib.util.spec_from_file_location("windows_release", ROOT / "packaging/windows/windows_release.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_signed_fixture(directory):
    subprocess.run(["node", "scripts/create-release-key.mjs", "neptune", str(directory)], cwd=ROOT, check=True, capture_output=True)
    manifests = []
    payload = directory / ".fixture-payload"
    (payload / "release-trust").mkdir(parents=True)
    (payload / "Neptune.Windows.exe").write_bytes(b"synthetic Windows payload; not executable")
    (payload / "release-trust/neptune.pem").write_bytes((directory / "neptune.pem").read_bytes())
    for product, runtime, suffix, manifest_name in [
        ("neptune-linux", "linux-x64", "tar.gz", "neptune-linux-release-linux-x64.json"),
        ("neptune-linux", "linux-arm64", "tar.gz", "neptune-linux-release-linux-arm64.json"),
        ("neptune-windows", "win-x64", "zip", "neptune-windows-release.json"),
    ]:
        name = f"{product}-0.1.9-{runtime}.{suffix}"
        artifact = directory / name
        if runtime == "win-x64":
            with zipfile.ZipFile(artifact, "w") as bundle:
                bundle.write(directory / "neptune.pem", "release-trust/neptune.pem")
                bundle.write(payload / "Neptune.Windows.exe", "Neptune.Windows.exe")
        else:
            artifact.write_bytes(f"synthetic {runtime} package".encode("ascii"))
        checksum = hashlib.sha256(artifact.read_bytes()).hexdigest()
        (directory / f"{name}.sha256").write_text(f"{checksum}  {name}\n", encoding="utf-8")
        manifest = directory / manifest_name
        manifest.write_text(json.dumps({
            "schema": "exocortex.neptune.release.v1", "product": product,
            "version": "0.1.9", "runtime": runtime, "artifact": name,
            "sha256": checksum, "size": artifact.stat().st_size,
            "url": f"https://github.com/{REPOSITORY}/releases/download/{TAG}/{name}",
        }), encoding="utf-8")
        manifests.append(manifest)
    windows = windows_module()
    inventory = windows.make_inventory(payload, "0.1.9", REVISION, 1)
    windows.write_json(directory / windows.PAYLOAD_MANIFEST, inventory)
    # Contract fixture only: the native packaging test compiles and extracts a real MSI.
    (directory / "neptune-windows-0.1.9-win-x64.msi").write_bytes(bytes.fromhex("d0cf11e0a1b11ae1") + b"synthetic MSI contract fixture")
    decompiled = directory / ".fixture-decompiled.wxs"
    wix = ET.Element(f"{{{windows.WIX}}}Wix")
    for item in inventory["files"]:
        ET.SubElement(wix, f"{{{windows.WIX}}}File", {"Id": windows.file_id(item["path"]), "Source": str(payload / item["path"])})
    ET.ElementTree(wix).write(decompiled)
    windows.finalize(directory, inventory, REPOSITORY, TAG, decompiled)
    manifests.append(directory / windows.BUNDLE_MANIFEST)
    environment = {**os.environ, "RELEASE_SIGNING_KEY_FILE": str(directory / "neptune.private.pem"),
                   "GITHUB_REF_NAME": TAG, "GITHUB_REPOSITORY": REPOSITORY, "GITHUB_SHA": REVISION}
    subprocess.run(["node", "scripts/sign-release.mjs", *map(str, manifests)], cwd=ROOT, env=environment, check=True, capture_output=True)
    return environment


class UnifiedReleaseTests(unittest.TestCase):
    def test_signed_triplet_and_tamper_rejection(self):
        with tempfile.TemporaryDirectory(prefix="neptune-release-contract-") as temporary:
            directory = Path(temporary)
            environment = create_signed_fixture(directory)
            command = [sys.executable, "scripts/verify-unified-release.py", str(directory)]
            good = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, text=True)
            self.assertEqual(good.returncode, 0, good.stdout + good.stderr)
            with (directory / "neptune-linux-0.1.9-linux-x64.tar.gz").open("ab") as artifact:
                artifact.write(b"tampered")
            bad = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, text=True)
            self.assertNotEqual(bad.returncode, 0)
            self.assertIn("Checksum mismatch", bad.stderr)


if __name__ == "__main__":
    unittest.main()
