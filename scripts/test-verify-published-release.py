"""Anonymous publication must reject incomplete, replaced and misidentified assets."""
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


published = load("verify-published-release")
fixture = load("test-verify-unified-release")
REVISION = "a" * 40


class PublishedReleaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="neptune-publication-test-")
        cls.directory = Path(cls.temporary.name)
        cls.environment = fixture.create_signed_fixture(cls.directory)
        cls.environment["GITHUB_SHA"] = REVISION
        (cls.directory / "bootstrap.sh").write_text("#!/bin/sh\n# signed candidate bootstrap\n")
        cls.base = f"https://github.com/{fixture.REPOSITORY}/releases/download/{fixture.TAG}/"
        cls.api = f"https://api.github.com/repos/{fixture.REPOSITORY}"
        cls.original = {cls.base + name: (cls.directory / name).read_bytes() for name in published.asset_names(fixture.TAG)}
        cls.metadata = {
            "tag_name": fixture.TAG, "draft": False, "prerelease": True,
            "assets": [{"name": url.removeprefix(cls.base), "size": len(raw), "state": "uploaded", "browser_download_url": url}
                       for url, raw in cls.original.items()],
        }

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def setUp(self):
        self.release = copy.deepcopy(self.metadata)
        self.objects = {self.api + "/git/ref/tags/" + fixture.TAG: {"object": {"type": "commit", "sha": REVISION}}}
        self.assets = self.original.copy()
        self.requests = []

    def fetch(self, url, output, maximum):
        self.requests.append(url)
        raw = (json.dumps(self.release).encode() if url == self.api + "/releases/tags/" + fixture.TAG
               else json.dumps(self.objects[url]).encode() if url in self.objects else self.assets[url])
        if len(raw) > maximum:
            raise ValueError("Release download exceeds limit")
        output.write(raw)

    def verify(self):
        with patch.dict(os.environ, self.environment), contextlib.redirect_stdout(io.StringIO()):
            published.verify(self.directory, self.fetch)

    def test_exact_signed_candidate_and_annotated_tag(self):
        annotation = "b" * 40
        self.objects[self.api + "/git/ref/tags/" + fixture.TAG] = {"object": {"type": "tag", "sha": annotation}}
        self.objects[self.api + "/git/tags/" + annotation] = {"object": {"type": "commit", "sha": REVISION}}
        self.verify()
        self.assertEqual(len([url for url in self.requests if url.startswith(self.base)]), 14)
        self.assertNotIn(self.base + "neptune.private.pem", self.requests)

    def test_wrong_release_state_or_tag_is_rejected(self):
        for changes in [{"draft": True}, {"prerelease": False}, {"tag_name": "neptune-v0.1.8"}]:
            with self.subTest(changes=changes):
                self.release = {**self.metadata, **changes}
                with self.assertRaisesRegex(ValueError, "visible prerelease"):
                    self.verify()

    def test_wrong_commit_and_cyclic_annotation_are_rejected(self):
        ref = self.api + "/git/ref/tags/" + fixture.TAG
        self.objects[ref] = {"object": {"type": "commit", "sha": "b" * 40}}
        with self.assertRaisesRegex(ValueError, "revision mismatch"):
            self.verify()
        self.objects[ref] = {"object": {"type": "tag", "sha": "b" * 40}}
        self.objects[self.api + "/git/tags/" + "b" * 40] = self.objects[ref]
        with self.assertRaisesRegex(ValueError, "revision mismatch"):
            self.verify()

    def test_incomplete_duplicate_and_extra_assets_are_rejected(self):
        assets = self.metadata["assets"]
        for inventory in [assets[:-1], assets + [assets[0]], assets + [{"name": "neptune.private.pem"}]]:
            with self.subTest(inventory=len(inventory)):
                self.release["assets"] = inventory
                with self.assertRaisesRegex(ValueError, "inventory mismatch"):
                    self.verify()

    def test_wrong_asset_url_size_and_upload_state_are_rejected(self):
        for changes in [{"browser_download_url": "https://elsewhere.invalid/key"}, {"size": 1}, {"state": "new"}]:
            with self.subTest(changes=changes):
                self.release = copy.deepcopy(self.metadata)
                self.release["assets"][0].update(changes)
                with self.assertRaisesRegex(ValueError, "metadata mismatch"):
                    self.verify()

    def test_same_size_tamper_truncation_and_overflow_are_rejected(self):
        # Keys, bootstrap, archives, checksums, manifests and signature envelopes all bind to the candidate.
        for url, raw in self.original.items():
            with self.subTest(asset=url):
                self.assets = self.original.copy()
                self.assets[url] = bytes([raw[0] ^ 1]) + raw[1:]
                with self.assertRaisesRegex(ValueError, "bytes differ"):
                    self.verify()
        url = self.base + "neptune.pem"
        for raw, error in [(self.original[url][:-1], "bytes differ"), (self.original[url] + b"x", "exceeds limit")]:
            self.assets = {**self.original, url: raw}
            with self.assertRaisesRegex(ValueError, error):
                self.verify()

    def test_download_is_anonymous_and_bounded_without_content_length(self):
        class Response(io.BytesIO):
            status = 200
            url = "https://release-assets.githubusercontent.com/fixture"
            headers = {}
        requests = []

        def opener(request, timeout):
            requests.append((request, timeout))
            return Response(b"0123456789")

        with patch.dict(os.environ, {"GH_TOKEN": "must-not-be-used", "GITHUB_TOKEN": "must-not-be-used"}), patch.object(published.urllib.request, "urlopen", opener):
            output = io.BytesIO()
            published.download(self.base + "bootstrap.sh", output, 10)
            self.assertEqual(output.getvalue(), b"0123456789")
            self.assertNotIn("Authorization", dict(requests[0][0].header_items()))
            self.assertEqual(requests[0][1], 30)
            with self.assertRaisesRegex(ValueError, "exceeds limit"):
                published.download(self.base + "bootstrap.sh", io.BytesIO(), 9)
            with self.assertRaisesRegex(ValueError, "HTTPS"):
                published.download("http://github.com/file", io.BytesIO(), 10)

    def test_report_retry_preserves_only_same_qualified_tuple_and_results(self):
        expected = {
            "service": "neptune", "revision": REVISION, "release_tag": fixture.TAG,
            "catalog_sha256": "c" * 64, "supplemental_sha256": "d" * 64,
            "phase": "final", "release_qualification": True, "run_id": "2:1",
            "checks": [{"id": "REL-01", "status": "PASS", "deferred": False, "evidence": [{"log_sha256": "a" * 64}]}],
        }
        previous = copy.deepcopy(expected)
        previous["run_id"] = "1:1"
        previous["checks"][0]["evidence"] = [{"log_sha256": "b" * 64}]
        published.compare_reports(expected, previous)
        report_path = self.directory / ".qualification.json"
        report_path.write_text(json.dumps(expected), encoding="utf-8")
        urls = []
        def fetch_report(url, output, maximum):
            urls.append(url)
            output.write(json.dumps(previous).encode())
        with patch.dict(os.environ, self.environment), contextlib.redirect_stdout(io.StringIO()):
            published.verify_report(report_path, fetch_report)
        self.assertEqual(urls, [self.base + "known-problems-report.json"])
        for changes in [{"revision": "b" * 40}, {"catalog_sha256": "e" * 64}, {"supplemental_sha256": "e" * 64}, {"release_tag": "neptune-v0.1.8"}, {"release_qualification": False}, {"phase": "pre-signing"}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                published.compare_reports(expected, {**previous, **changes})
        for status, deferred in [("UNKNOWN", False), ("FAIL", False), ("PASS", True)]:
            bad = copy.deepcopy(previous)
            bad["checks"][0].update(status=status, deferred=deferred)
            with self.assertRaises(ValueError):
                published.compare_reports(expected, bad)


if __name__ == "__main__":
    unittest.main()
