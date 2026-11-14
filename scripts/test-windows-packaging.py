"""Payload isolation, per-user MSI authoring, partial/tampered packages and release gates."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("windows_release", ROOT / "packaging/windows/windows_release.py")
windows = importlib.util.module_from_spec(spec)
spec.loader.exec_module(windows)


class WindowsPackagingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="neptune-windows-payload-")
        self.directory = Path(self.temporary.name)
        self.payload = self.directory / "payload"
        (self.payload / "release-trust").mkdir(parents=True)
        (self.payload / "Neptune.Windows.exe").write_bytes(b"synthetic payload")
        (self.payload / "release-trust/neptune.pem").write_bytes(b"test trust fixture")
        self.inventory = windows.make_inventory(self.payload, "0.1.18", "a" * 40, 1)

    def tearDown(self):
        self.temporary.cleanup()

    def test_per_user_package_owns_application_and_no_profile_or_autostart(self):
        authoring = self.directory / "Neptune.wxs"
        windows.author_msi(self.payload, self.inventory, authoring)
        tree = ET.parse(authoring)
        package = tree.find(f"{{{windows.WIX}}}Package")
        self.assertEqual(package.attrib["Scope"], "perUser")
        self.assertEqual(package.attrib["UpgradeCode"], windows.UPGRADE_CODE)
        self.assertFalse(list(tree.iter(f"{{{windows.WIX}}}CustomAction")))
        self.assertTrue(list(tree.iter(f"{{{windows.WIX}}}MajorUpgrade")))
        files = list(tree.iter(f"{{{windows.WIX}}}File"))
        self.assertEqual({item.attrib["Id"] for item in files}, {windows.file_id(item["path"]) for item in self.inventory["files"]})
        registrations = list(tree.iter(f"{{{windows.WIX}}}RegistryValue"))
        self.assertTrue(all(item.attrib["Root"] == "HKCU" for item in registrations))
        self.assertTrue(any(item.attrib.get("Name") == "InstallDirectory" for item in registrations))
        self.assertFalse(any("Run" in item.attrib["Key"] or "profiles" in item.attrib["Key"].lower() for item in registrations))

    def test_upgrade_recovers_registered_install_directory(self):
        authoring = self.directory / "Neptune.wxs"
        windows.author_msi(self.payload, self.inventory, authoring)
        tree = ET.parse(authoring)
        location = tree.find(f".//{{{windows.WIX}}}Property[@Id='INSTALLDIR']")
        self.assertIsNotNone(location)
        self.assertEqual(location.attrib.get("Secure"), "yes")
        search = location.find(f"{{{windows.WIX}}}RegistrySearch")
        self.assertEqual(search.attrib["Root"], "HKCU")
        self.assertEqual(search.attrib["Key"], r"Software\Exocortex\Neptune\Installer")
        self.assertEqual(search.attrib["Name"], "InstallDirectory")
        self.assertEqual(search.attrib["Bitness"], "always64")
        reinstall = tree.find(f".//{{{windows.WIX}}}Property[@Id='REINSTALLMODE']")
        self.assertEqual(reinstall.attrib["Value"], "amus")

    def test_payload_changes_cannot_keep_previous_identity(self):
        tampered = json.loads(json.dumps(self.inventory))
        tampered["files"][0]["sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            windows.validate_inventory(tampered)

    def test_portable_extra_or_replaced_file_is_rejected(self):
        archive = self.directory / "portable.zip"
        windows.zip_payload(self.payload, self.inventory, archive)
        windows.verify_zip(archive, self.inventory)
        with zipfile.ZipFile(archive, "a") as bundle:
            bundle.writestr("profiles/default/neptune.db", b"must not be shipped")
        with self.assertRaisesRegex(ValueError, "inventory differs"):
            windows.verify_zip(archive, self.inventory)
        (self.payload / "Neptune.Windows.exe").write_bytes(b"replaced payload!")
        windows.zip_payload(self.payload, self.inventory, archive)
        with self.assertRaisesRegex(ValueError, "payload (hash|size) mismatch"):
            windows.verify_zip(archive, self.inventory)

    def test_profile_state_and_case_collision_are_rejected(self):
        state = self.payload / "neptune.db"
        state.write_bytes(b"state")
        with self.assertRaisesRegex(ValueError, "Profile state"):
            windows.make_inventory(self.payload, "0.1.18", "a" * 40, 1)
        state.unlink()
        # Explicit records exercise the rule on both case-sensitive and insensitive filesystems.
        tampered = {**self.inventory, "files": [*self.inventory["files"], {**self.inventory["files"][0], "path": self.inventory["files"][0]["path"].upper()}]}
        tampered["payload_id"] = windows.hashlib.sha256(windows.canonical({key: value for key, value in tampered.items() if key != "payload_id"})).hexdigest()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            windows.validate_inventory(tampered)

    def test_unsafe_windows_names_and_prerelease_aliases_are_rejected(self):
        for name in ["../x", "/absolute", "a\\b", "file:stream", "CON.txt", "file.", "dir//file"]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                windows.safe_path(name)
        for version in ["0.1.18-preview", "0.1.18.1", "256.1.0", "1.0.65536"]:
            with self.subTest(version=version), self.assertRaises(ValueError):
                windows.validate_version(version)

    def test_extracted_installer_cannot_replace_a_file(self):
        extracted = self.directory / "extracted.wxs"
        wix = ET.Element(f"{{{windows.WIX}}}Wix")
        for item in self.inventory["files"]:
            ET.SubElement(wix, f"{{{windows.WIX}}}File", {"Id": windows.file_id(item["path"]), "Source": str(self.payload / item["path"])})
        ET.ElementTree(wix).write(extracted)
        windows.verify_extracted_msi(extracted, self.inventory)
        (self.payload / "Neptune.Windows.exe").write_bytes(b"substituted installer content")
        with self.assertRaisesRegex(ValueError, "MSI payload hash/size mismatch"):
            windows.verify_extracted_msi(extracted, self.inventory)

    def test_release_does_not_accept_packaging_as_update_or_install_acceptance(self):
        readiness = self.directory / "readiness.json"
        gates = {"schema": "exocortex.neptune.windows-release-readiness.v1",
                 "installed_update_accepted": True, "profile_migration_accepted": True,
                 "native_install_accepted": True, "production_signing_accepted": True}
        readiness.write_text(json.dumps(gates), encoding="utf-8")
        windows.require_release_readiness(readiness)
        gates["installed_update_accepted"] = False
        readiness.write_text(json.dumps(gates), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "blocked"):
            windows.require_release_readiness(readiness)


if __name__ == "__main__":
    unittest.main()
