"""One immutable Windows payload, two packages, and a legacy-compatible manifest."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import uuid
import xml.etree.ElementTree as ET
import zipfile


WIX = "http://wixtoolset.org/schemas/v4/wxs"
NAMESPACE = uuid.UUID("1c2854bb-7d79-4fda-aa49-275bb5bf8a50")
UPGRADE_CODE = "{D75C847C-C5AA-5E77-BC1F-24495C372245}"
PAYLOAD_MANIFEST = "neptune-windows-payload.json"
BUNDLE_MANIFEST = "neptune-windows-bundle.json"
PACKAGE_EVIDENCE = "neptune-windows-package-evidence.json"
HEX = re.compile(r"[a-f0-9]{64}")


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def write_json(path, value):
    Path(path).write_bytes(canonical(value) + b"\n")


def safe_path(name):
    if not isinstance(name, str) or not name or "\\" in name:
        raise ValueError("Unsafe payload path")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in ("", ".", "..") for part in name.split("/")):
        raise ValueError("Unsafe payload path")
    for part in path.parts:
        if re.search(r'[<>:"|?*\x00-\x1f]', part) or part.endswith((".", " ")):
            raise ValueError("Unsafe Windows payload name")
        if re.fullmatch(r"(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?", part, re.I):
            raise ValueError("Reserved Windows payload name")
    return name


def validate_version(version):
    # MSI compares only three numeric fields. Do not silently map prereleases to
    # an identical final version or create multiple upgrade identities.
    if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", version):
        raise ValueError("MSI packaging requires a stable three-part version")
    major, minor, patch = map(int, version.split("."))
    if major > 255 or minor > 255 or patch > 65535:
        raise ValueError("Version exceeds Windows Installer version bounds")


def file_id(name):
    return "F" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:30]


def wix_id(kind, name):
    return kind + hashlib.sha256(name.encode("utf-8")).hexdigest()[:30]


def make_inventory(payload, version, revision, protocol):
    validate_version(version)
    if not re.fullmatch(r"[a-f0-9]{40}", revision) or protocol != 1:
        raise ValueError("Invalid source revision or unsupported protocol")
    records = []
    names = set()
    for path in sorted(payload.rglob("*")):
        if path.is_symlink():
            raise ValueError("Windows payload cannot contain symbolic links")
        if not path.is_file():
            continue
        name = safe_path(path.relative_to(payload).as_posix())
        if name.casefold() in names:
            raise ValueError("Case-colliding Windows payload paths")
        names.add(name.casefold())
        if any(part.casefold() == "profiles" for part in PurePosixPath(name).parts) or name.lower().endswith((".db", ".db-wal", ".db-shm")):
            raise ValueError("Profile state must not be packaged with the application")
        records.append({"path": name, "sha256": digest(path), "size": path.stat().st_size})
    if "Neptune.Windows.exe" not in [item["path"] for item in records] or "release-trust/neptune.pem" not in [item["path"] for item in records]:
        raise ValueError("Payload must contain Neptune.Windows.exe and the release trust key")
    inventory = {"schema": "exocortex.neptune.windows-payload.v1", "version": version,
                 "source_revision": revision, "protocol_version": protocol,
                 "runtime": "win-x64", "files": records}
    return {**inventory, "payload_id": hashlib.sha256(canonical(inventory)).hexdigest()}


def validate_inventory(inventory):
    if inventory.get("schema") != "exocortex.neptune.windows-payload.v1" or inventory.get("runtime") != "win-x64":
        raise ValueError("Invalid Windows payload schema")
    validate_version(inventory.get("version", ""))
    if not re.fullmatch(r"[a-f0-9]{40}", inventory.get("source_revision", "")) or inventory.get("protocol_version") != 1:
        raise ValueError("Invalid Windows payload identity")
    expected_id = hashlib.sha256(canonical({key: value for key, value in inventory.items() if key != "payload_id"})).hexdigest()
    if inventory.get("payload_id") != expected_id:
        raise ValueError("Windows payload identity mismatch")
    files = inventory.get("files")
    if not isinstance(files, list) or not files or len(files) > 10000:
        raise ValueError("Invalid payload inventory")
    names = set()
    for item in files:
        name = safe_path(item.get("path"))
        if name.casefold() in names or not HEX.fullmatch(item.get("sha256", "")) or type(item.get("size")) is not int or item["size"] < 0:
            raise ValueError("Invalid or duplicate payload record")
        names.add(name.casefold())
    if not {"neptune.windows.exe", "release-trust/neptune.pem"}.issubset(names):
        raise ValueError("Required Windows payload files are absent")


def author_msi(payload, inventory, destination):
    validate_inventory(inventory)
    ET.register_namespace("", WIX)
    def element(parent, tag, **attributes):
        return ET.SubElement(parent, f"{{{WIX}}}{tag}", attributes)
    root = ET.Element(f"{{{WIX}}}Wix")
    product_code = "{" + str(uuid.uuid5(NAMESPACE, "product:" + inventory["version"] + ":" + inventory["payload_id"])).upper() + "}"
    package = element(root, "Package", Name="Neptune Windows", Manufacturer="Exocortex",
                      Version=inventory["version"], Language="1033", UpgradeCode=UPGRADE_CODE,
                      ProductCode=product_code, Scope="perUser", InstallerVersion="500", Compressed="yes")
    element(package, "MajorUpgrade", DowngradeErrorMessage="A newer Neptune version is already installed.",
            AllowSameVersionUpgrades="yes", Schedule="afterInstallInitialize")
    element(package, "MediaTemplate", EmbedCab="yes", CompressionLevel="high")
    element(package, "Property", Id="ARPNOMODIFY", Value="1")
    element(package, "Property", Id="MSIDISABLERMRESTART", Value="1")
    # Same-version upgrades are allowed and product identity includes payload
    # hashes. MSI's default 'omus' can retain an equal-version DLL with different
    # bytes, so every installed application file must come from this package.
    element(package, "Property", Id="REINSTALLMODE", Value="amus")
    # A major upgrade has a new ProductCode and otherwise resolves INSTALLDIR
    # to the default again. Preserve the registered per-user location; an
    # explicit command-line INSTALLDIR still takes precedence over AppSearch.
    location = element(package, "Property", Id="INSTALLDIR", Secure="yes")
    element(location, "RegistrySearch", Id="PreviousInstallDirectory", Root="HKCU",
            Key=r"Software\Exocortex\Neptune\Installer", Name="InstallDirectory", Type="raw", Bitness="always64")
    build = element(package, "Property", Id="WINDOWSBUILD")
    element(build, "RegistrySearch", Id="WindowsBuildSearch", Root="HKLM",
            Key=r"SOFTWARE\Microsoft\Windows NT\CurrentVersion", Name="CurrentBuildNumber", Type="raw", Bitness="always64")
    element(package, "Launch", Condition="Installed OR (VersionNT64 AND WINDOWSBUILD >= 10240)",
            Message="Neptune requires Windows 10 or later on an x64 system.")
    icon_source = Path(__file__).resolve().parents[2] / "assets/windows/neptune.ico"
    icon = element(package, "Icon", Id="NeptuneIcon", SourceFile=str(icon_source))
    element(package, "Property", Id="ARPPRODUCTICON", Value=icon.attrib["Id"])
    local = element(package, "StandardDirectory", Id="LocalAppDataFolder")
    programs = element(local, "Directory", Id="UserPrograms", Name="Programs")
    vendor = element(programs, "Directory", Id="ExocortexPrograms", Name="Exocortex")
    install = element(vendor, "Directory", Id="INSTALLDIR", Name="Neptune")
    directory_map = {"": install}
    for item in inventory["files"]:
        parent_path = PurePosixPath(item["path"]).parent
        parent = install
        partial = ""
        for part in parent_path.parts:
            partial = part if not partial else partial + "/" + part
            if partial not in directory_map:
                directory_map[partial] = element(parent, "Directory", Id=wix_id("D", partial), Name=part)
            parent = directory_map[partial]
    group = element(package, "ComponentGroup", Id="PayloadComponents")
    for item in inventory["files"]:
        name = item["path"]
        parent_name = PurePosixPath(name).parent.as_posix()
        directory = directory_map["" if parent_name == "." else parent_name].attrib["Id"]
        component = element(group, "Component", Id=wix_id("C", name), Directory=directory,
                            Guid="{" + str(uuid.uuid5(NAMESPACE, "file:" + name)).upper() + "}", Bitness="always64")
        element(component, "File", Id=file_id(name), Source=str(payload / name), Name=PurePosixPath(name).name)
        # Per-user components use HKCU key paths; no profile/state is owned by MSI.
        element(component, "RegistryValue", Root="HKCU", Key=r"Software\Exocortex\Neptune\Installer\Files",
                Name=wix_id("C", name), Value="[#" + file_id(name) + "]", Type="string", KeyPath="yes")
    registration = element(group, "Component", Id="InstallerRegistration", Directory="INSTALLDIR",
                           Guid="{" + str(uuid.uuid5(NAMESPACE, "registration")).upper() + "}", Bitness="always64")
    element(registration, "RegistryValue", Root="HKCU", Key=r"Software\Exocortex\Neptune\Installer",
            Name="InstallDirectory", Value="[INSTALLDIR]", Type="string", KeyPath="yes")
    for directory in (programs, vendor, *directory_map.values()):
        element(registration, "RemoveFolder", Id="Remove" + directory.attrib["Id"], Directory=directory.attrib["Id"], On="uninstall")
    start_menu = element(package, "StandardDirectory", Id="ProgramMenuFolder")
    shortcut_dir = element(start_menu, "Directory", Id="NeptuneStartMenu", Name="Neptune")
    shortcuts = element(shortcut_dir, "Component", Id="NeptuneShortcut", Guid="{" + str(uuid.uuid5(NAMESPACE, "shortcut")).upper() + "}")
    element(shortcuts, "Shortcut", Id="NeptuneStartShortcut", Name="Neptune", Target="[#" + file_id("Neptune.Windows.exe") + "]",
            WorkingDirectory="INSTALLDIR", Icon="NeptuneIcon", Arguments="--profile default")
    element(shortcuts, "RemoveFolder", Id="RemoveNeptuneStartMenu", On="uninstall")
    element(shortcuts, "RegistryValue", Root="HKCU", Key=r"Software\Exocortex\Neptune\Installer", Name="Shortcut", Value="1", Type="integer", KeyPath="yes")
    feature = element(package, "Feature", Id="Application", Title="Neptune Windows", Level="1")
    element(feature, "ComponentGroupRef", Id="PayloadComponents")
    element(feature, "ComponentRef", Id="NeptuneShortcut")
    ET.indent(root)
    ET.ElementTree(root).write(destination, encoding="utf-8", xml_declaration=True)


def zip_payload(payload, inventory, destination):
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for item in inventory["files"]:
            archive.write(payload / item["path"], item["path"])


def verify_zip(path, inventory):
    validate_inventory(inventory)
    expected = {item["path"]: item for item in inventory["files"]}
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != set(expected):
            raise ValueError("Portable ZIP inventory differs from the shared payload")
        for name, item in expected.items():
            safe_path(name)
            info = archive.getinfo(name)
            if info.file_size != item["size"]:
                raise ValueError("Portable ZIP payload size mismatch")
            with archive.open(name) as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]:
                    raise ValueError("Portable ZIP payload hash mismatch")


def verify_extracted_msi(decompiled, inventory):
    validate_inventory(inventory)
    files = {element.attrib["Id"]: element for element in ET.parse(decompiled).iter(f"{{{WIX}}}File")}
    if set(files) != {file_id(item["path"]) for item in inventory["files"]}:
        raise ValueError("MSI file inventory differs from shared payload")
    for item in inventory["files"]:
        file = files[file_id(item["path"])]
        source_text = file.attrib["Source"]
        # WiX's -x directory is independent of the decompiler's logical SourceDir.
        if source_text == "SourceDir\\File\\" + file_id(item["path"]):
            source = decompiled.parent / "extracted" / "File" / file_id(item["path"])
        else:
            source = Path(source_text)
        if not source.is_absolute():
            source = decompiled.parent / source
        if source.stat().st_size != item["size"] or digest(source) != item["sha256"]:
            raise ValueError("Extracted MSI payload hash/size mismatch")


def artifact_descriptor(path, packaging, base_url):
    name = path.name
    sha = digest(path)
    path.with_name(name + ".sha256").write_text(f"{sha}  {name}\n", encoding="utf-8")
    return {"packaging": packaging, "artifact": name, "sha256": sha, "size": path.stat().st_size, "url": base_url + name}


def finalize(directory, inventory, repository, tag, decompiled):
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository) or tag != "neptune-v" + inventory["version"]:
        raise ValueError("Invalid release repository/tag identity")
    version = inventory["version"]
    portable = directory / f"neptune-windows-{version}-win-x64.zip"
    installer = directory / f"neptune-windows-{version}-win-x64.msi"
    verify_zip(portable, inventory)
    verify_extracted_msi(decompiled, inventory)
    base = f"https://github.com/{repository}/releases/download/{tag}/"
    variants = [artifact_descriptor(portable, "portable-zip", base), artifact_descriptor(installer, "per-user-msi", base)]
    inspection_path = decompiled.parent / "msi-inspection.json"
    inspection = json.loads(inspection_path.read_bytes()) if inspection_path.exists() else None
    if inspection is not None and (inspection.get("status") != "PASS" or inspection.get("installer_sha256") != variants[1]["sha256"]):
        raise ValueError("MSI inspection identity mismatch")
    # Keep exactly the contract understood by released portable clients.
    write_json(directory / "neptune-windows-release.json", {
        "schema": "exocortex.neptune.release.v1", "product": "neptune-windows",
        "protocol_version": inventory["protocol_version"], "version": version,
        "runtime": "win-x64", "minimum_os": "Windows 10", **variants[0]})
    write_json(directory / PACKAGE_EVIDENCE, {
        "schema": "exocortex.neptune.windows-package-evidence.v1", "status": "PASS",
        "check": "ZIP and MSI extracted payloads match the same inventory",
        "payload_id": inventory["payload_id"], "variants": variants,
        "builder": "WiX 4.0.6", "installed_runtime_tests": "NOT_RUN",
        "profiles_accessed": False, "install_performed": False, "msi_inspection": inspection})
    write_json(directory / BUNDLE_MANIFEST, {
        "schema": "exocortex.neptune.windows-bundle.v1", "product": "neptune-windows",
        "version": version, "runtime": "win-x64", "protocol_version": inventory["protocol_version"],
        "source_revision": inventory["source_revision"], "payload_id": inventory["payload_id"],
        "payload_manifest": {"artifact": PAYLOAD_MANIFEST, "sha256": digest(directory / PAYLOAD_MANIFEST)},
        "package_evidence": {"artifact": PACKAGE_EVIDENCE, "sha256": digest(directory / PACKAGE_EVIDENCE)},
        "compatibility": {"sync_protocol_min": 1, "sync_protocol_max": 1,
                          "profile_migration": "additive-preserve-originals",
                          "local_cycle_max": 7, "local_source_max": 2,
                          "client_evolution_max": 1,
                          "installed_update": "msi-transaction-health-v1",
                          "portable_update": "staged-swap-health-v1"}, "variants": variants})


def verify_bundle(directory, version, repository, revision=None):
    bundle = json.loads((directory / BUNDLE_MANIFEST).read_bytes())
    if (bundle.get("schema") != "exocortex.neptune.windows-bundle.v1" or bundle.get("product") != "neptune-windows"
            or bundle.get("version") != version or bundle.get("runtime") != "win-x64" or bundle.get("protocol_version") != 1):
        raise ValueError("Windows bundle contract mismatch")
    for key, name in [("payload_manifest", PAYLOAD_MANIFEST), ("package_evidence", PACKAGE_EVIDENCE)]:
        descriptor = bundle.get(key, {})
        if descriptor.get("artifact") != name or descriptor.get("sha256") != digest(directory / name):
            raise ValueError("Windows payload/evidence metadata hash mismatch")
    inventory = json.loads((directory / PAYLOAD_MANIFEST).read_bytes())
    validate_inventory(inventory)
    if inventory["version"] != version or inventory["payload_id"] != bundle.get("payload_id") or inventory["source_revision"] != bundle.get("source_revision"):
        raise ValueError("Windows package source/payload identity mismatch")
    if revision is not None and inventory["source_revision"] != revision:
        raise ValueError("Windows bundle source revision differs from release revision")
    variants = bundle.get("variants")
    if not isinstance(variants, list) or {item.get("packaging") for item in variants} != {"portable-zip", "per-user-msi"} or len(variants) != 2:
        raise ValueError("Both Windows packaging variants are required")
    base = f"https://github.com/{repository}/releases/download/neptune-v{version}/"
    for variant in variants:
        extension = "zip" if variant["packaging"] == "portable-zip" else "msi"
        name = f"neptune-windows-{version}-win-x64.{extension}"
        path = directory / name
        sha = digest(path)
        if variant != {"packaging": variant["packaging"], "artifact": name, "sha256": sha, "size": path.stat().st_size, "url": base + name}:
            raise ValueError("Windows artifact descriptor mismatch")
        if (directory / (name + ".sha256")).read_text(encoding="utf-8").strip() != f"{sha}  {name}":
            raise ValueError("Windows artifact checksum mismatch")
        if extension == "msi":
            with path.open("rb") as stream:
                if stream.read(8) != bytes.fromhex("d0cf11e0a1b11ae1"):
                    raise ValueError("Installer is not a Windows Installer compound file")
        else:
            verify_zip(path, inventory)
    legacy = json.loads((directory / "neptune-windows-release.json").read_bytes())
    portable = next(item for item in variants if item["packaging"] == "portable-zip")
    if any(legacy.get(key) != value for key, value in portable.items()) or legacy.get("schema") != "exocortex.neptune.release.v1":
        raise ValueError("Legacy portable manifest differs from bundle")
    evidence = json.loads((directory / PACKAGE_EVIDENCE).read_bytes())
    if evidence.get("schema") != "exocortex.neptune.windows-package-evidence.v1" or evidence.get("status") != "PASS" or evidence.get("payload_id") != inventory["payload_id"] or evidence.get("variants") != variants:
        raise ValueError("Windows package verification evidence mismatch")
    return bundle


def require_release_readiness(path):
    readiness = json.loads(path.read_bytes())
    gates = ("installed_update_accepted", "profile_migration_accepted", "native_install_accepted", "production_signing_accepted")
    if readiness.get("schema") != "exocortex.neptune.windows-release-readiness.v1" or any(readiness.get(gate) is not True for gate in gates):
        raise ValueError("Windows installed release is blocked: update, migration, native install and signing gates must be accepted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "finalize", "verify", "require-release-readiness"))
    parser.add_argument("--payload", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version")
    parser.add_argument("--revision")
    parser.add_argument("--protocol", type=int, default=1)
    parser.add_argument("--repository")
    parser.add_argument("--tag")
    parser.add_argument("--decompiled", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        inventory = make_inventory(args.payload.resolve(), args.version, args.revision, args.protocol)
        args.output.mkdir(parents=True, exist_ok=True)
        write_json(args.output / PAYLOAD_MANIFEST, inventory)
        author_msi(args.payload.resolve(), inventory, args.output / "Neptune.wxs")
        zip_payload(args.payload.resolve(), inventory, args.output / f"neptune-windows-{args.version}-win-x64.zip")
    elif args.command == "finalize":
        finalize(args.output, json.loads((args.output / PAYLOAD_MANIFEST).read_bytes()), args.repository, args.tag, args.decompiled)
    elif args.command == "verify":
        verify_bundle(args.output, args.version, args.repository, args.revision)
        print("PASS Windows variant manifests, source identity, ZIP payload and MSI extraction evidence")
    else:
        require_release_readiness(args.output)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError, zipfile.BadZipFile) as error:
        print(f"Windows packaging: {error}", file=sys.stderr)
        sys.exit(1)
