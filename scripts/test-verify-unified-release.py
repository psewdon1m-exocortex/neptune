"""Exercise the published release contract with a disposable signing key."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
TAG = "neptune-v0.1.9"
REPOSITORY = "psewdon1m-exocortex/neptune"


class UnifiedReleaseTests(unittest.TestCase):
    def test_signed_triplet_and_tamper_rejection(self):
        with tempfile.TemporaryDirectory(prefix="neptune-release-contract-") as temporary:
            directory = Path(temporary)
            subprocess.run(["node", "scripts/create-release-key.mjs", "neptune", str(directory)], cwd=ROOT, check=True, capture_output=True)
            manifests = []
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
            environment = {**os.environ, "RELEASE_SIGNING_KEY_FILE": str(directory / "neptune.private.pem"),
                           "GITHUB_REF_NAME": TAG, "GITHUB_REPOSITORY": REPOSITORY}
            subprocess.run(["node", "scripts/sign-release.mjs", *map(str, manifests)], cwd=ROOT, env=environment, check=True, capture_output=True)
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
