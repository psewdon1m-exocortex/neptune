# Windows packaging

This document specializes [Part 05 — CI, releases and local updates](https://github.com/psewdon1m-exocortex/general/blob/main/PART_05_CI_RELEASES_AND_LOCAL_UPDATES.md); that central contract remains authoritative.

Existing public releases contain a self-contained Windows x64 portable ZIP.
The PERF-01 packaging increment builds a per-user MSI and a portable ZIP from
one `Neptune.Windows` publish. Both variants contain identical application,
Core, runtime and release-trust files; there is no separate portable project.
MSI publication remains blocked until native install, installed update,
profile migration and production signing acceptance are recorded. The
checked-in MSIX manifest is retained but is not used by this MSI pipeline.
The executable, Task Manager, taskbar, tray and window use the same
multi-resolution, full-bleed square Neptune portrait icon family without a
decorative outline.

The checked-in icon family is derived from `assets/windows/neptune-master.png`:

```powershell
python .src/generate-windows-icons.py
python scripts/test-windows-icons.py
```

Run independent local clients with separate protected state and identities:

```powershell
Neptune.Windows.exe --profile work
Neptune.Windows.exe --profile personal
```

Only one process may use a given profile at a time.

The **Start with Windows** toggle is stored separately for every profile in the
current user's Windows Run key. It records the current portable executable path,
so enable the toggle again after moving the extracted application directory.
Autostart launches the selected profile minimized and requires no administrator
rights.

Neptune keeps a notification-area icon for its whole lifetime. Closing or
minimizing the window hides it to the tray; use the tray **Exit** command to stop
the client. Portable updates are discovered from Kernel Register and GitHub
Releases, verified against `neptune-windows-release.json` and its SHA-256, then
applied in place by the downloaded Neptune executable after the running process
has exited.

## Common payload and installer

The installer targets `%LOCALAPPDATA%\Programs\Exocortex\Neptune`, registers a
Start menu shortcut and per-user Windows Installer product, and uses a stable
UpgradeCode. Application files and installer registration are its only owned
resources. It does not create, launch, delete or migrate a profile and does not
change any profile's Run-key autostart setting. Existing profiles remain under
the same Neptune state location used by portable clients. Clean install,
uninstall/reinstall, profile selection, tray and actual update behavior still
require the W02–W10 native acceptance tests; package inspection is not that
acceptance.

Released clients and the development candidate have different update paths.
`WindowsPackagingPolicy` rejects portable replacement of an MSI-registered
directory, including direct invocation of the update helper. The development
candidate implements journaled portable replacement and an installer-owned path
that requires trusted current and previous MSI packages. Implementation alone
does not qualify automatic MSI updates for release: the signed native MSI
install/update/rollback gate remains open.

Before replacement, the candidate checks running Neptune executable paths using
Windows limited process-query access. A running copy of the target installation
still blocks the update; an unrelated user's installation does not require
module/VM inspection. Unknown process-inspection failures remain fatal.

The candidate retains the previous payload and checks profile compatibility in
a separate native process before admitting profile writers. New update journals
use profile fingerprint version 2. Only a zero-byte WAL beside a recognized
`neptune.db` or `guarded-sync/<namespace>/<mapping>/cycle.db` with a SQLite header
is equivalent to an absent WAL. Nonempty WAL contents, database bytes and other
profile files remain in the rollback fingerprint. Journals without a version use
the original version-1 fingerprint; unknown versions are refused. The existing
coordination-file exclusions do not change.

Successful rollback persists its terminal receipt before removing its own
login recovery entry. A later terminal recovery retries that cleanup if needed.
An altered profile or previous payload still blocks automatic rollback.

Local acceptance on 22 October 2026 exercised a standard Windows user, the real
published 0.1.18 portable payload, native health processes, preserved DPAPI
credentials and actual directory rollback. The candidate was an unsigned
development build. This evidence does not qualify release discovery, a signed
candidate, MSI, an interrupted-update/logon matrix or live API synchronization.
Evidence and exact candidate manifests are retained under
`.audit/release-acceptance-2026-10-22/solo01` in the workspace; the raw failing
attempts remain alongside their targeted passing repeats.

The build-only WiX Toolset 4.0.6 package is pinned by URL, SHA-256, source revision
and MS-RL license in [packaging-tools.lock.json](packaging-tools.lock.json).
The restore script checks the archive before executing any tool, then extracts
that verified archive into a local build directory. It uses the tool's official
Major runtime roll-forward with the pinned .NET SDK; WiX is not installed
globally. The selected version's [official license](https://raw.githubusercontent.com/wixtoolset/wix/v4.0.6/LICENSE.TXT)
and [release](https://github.com/wixtoolset/wix/releases/tag/v4.0.6) were checked.
Changing this pin requires renewed security/runtime/license qualification;
licenses of newer WiX versions must not be assumed identical. No Burn engine or
WiX custom-action binaries are embedded in these installers.

For local review, publish once, add a disposable public trust key, and package
that same directory. Keep the disposable private key outside the payload:

```powershell
dotnet publish src/Neptune.Windows/Neptune.Windows.csproj -c Release -r win-x64 `
  --self-contained true -p:PublishSingleFile=true -o artifacts/windows-payload
node scripts/create-release-key.mjs neptune artifacts/package-test-key
New-Item -ItemType Directory artifacts/windows-payload/release-trust -Force
Copy-Item artifacts/package-test-key/neptune.pem artifacts/windows-payload/release-trust/neptune.pem
./scripts/build-windows-packages.ps1 -PayloadDirectory artifacts/windows-payload `
  -OutputDirectory artifacts/windows-packages -Version 0.1.18 -SourceRevision (git rev-parse HEAD)
python scripts/test-windows-packaging.py
```

Output must be empty so builds cannot silently mix different payloads. MSI
versions currently require three stable numeric fields; prerelease/four-part
versions are rejected rather than aliased to a final MSI version.

Packaging verification extracts the MSI cabinet with WiX and compares every
file's SHA-256/length to the ZIP and common payload inventory. It never invokes
`msiexec`, registers a product, launches the client or opens its profile.
Installer registrations and upgrade authoring are validated by WiX ICE checks.

Release assets include both packages/checksums, `neptune-windows-payload.json`,
`neptune-windows-package-evidence.json` and the signed
`neptune-windows-bundle.json`. The bundle binds source revision, protocol and
payload identity to both artifact hashes and extraction evidence. The existing
signed `neptune-windows-release.json` remains a portable-v1 manifest understood
by released clients. Exact-asset verifiers require the new complete set and
reject absent variants or mismatched payload/source/artifact identities.

CI builds review packages using a disposable key. Release packaging passes
`-ForRelease` and fails closed while [release-readiness.json](release-readiness.json)
has incomplete gates. That file must only be changed after recording the
corresponding acceptance evidence; a manifest signature alone does not establish
Authenticode trust or native installation/update correctness.
