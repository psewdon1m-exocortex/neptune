# Neptune implementation contract

Status: executable cross-project implementation completed; production deployment
and end-to-end qualification against a real Saturn instance remain.

Neptune is a separate Exocortex repository with two independently versioned
products:

- **Neptune Linux** — one host-wide agent that creates automatic backups for
  multiple registered projects and uploads them to Saturn;
- **Neptune Windows** — a complete desktop application that copies selected
  local directories to Saturn's logical `sync` root. Version 1 is one-way and
  never restores remote changes to the local computer.

Neptune is a client of Kernel and Saturn. It never receives Storage Box/SFTP
credentials and never exposes a web interface.

## Implemented repository layout

- `src/Neptune.Core` contains Kernel Register verification, the SQLite journal,
  exact-byte backup spooling, resumable Saturn upload and one-way WebDAV sync;
- `src/Neptune.Linux` is the host-wide daemon and authenticated Unix-socket API;
- `src/Neptune.Windows` is the 800 x 500 WPF application with isolated profiles,
  native folder selection, DPAPI-protected tokens, watcher hints and periodic
  reconciliation, tray operation, per-profile current-user autostart and a
  checksummed portable updater;
- `packaging/linux` contains the hardened systemd unit and installer;
- `packaging/windows` contains the future MSIX manifest and complete planet icon family;
- `.github/workflows` publishes two independent checksummed release streams.

Build and test with the pinned .NET SDK:

```text
dotnet build Neptune.slnx
dotnet test tests/Neptune.Core.Tests/Neptune.Core.Tests.csproj
```

Multiple clients on one Windows account use separate profiles and therefore
separate protected configuration, SQLite journals and stable client IDs:

```text
Neptune.Windows.exe --profile work
Neptune.Windows.exe --profile personal
```

## 1. Repository and release contract

The repository publishes two separate GitHub release streams:

| Product | Tag | Manifest | Primary artifact |
| --- | --- | --- | --- |
| Linux | `neptune-linux-vMAJOR.MINOR.PATCH` | `neptune-linux-release-linux-x64.json` / `neptune-linux-release-linux-arm64.json` | architecture-specific `.tar.gz` |
| Windows | `neptune-windows-vMAJOR.MINOR.PATCH` | `neptune-windows-release.json` | self-contained portable `.zip` |

Linux and Windows product versions may advance independently. Both manifests
declare the Neptune protocol version, artifact SHA-256, size, architecture,
minimum supported OS and immutable release URL. Release signing and verification
must follow the same trust model as the existing Exocortex Updater releases.

Kernel Register contains `volt://` references for the shared coordinates:

```text
repositories.neptune.url
services.saturn.sni
services.saturn.port
services.saturn.paths.backup_ingest
services.saturn.paths.sync
services.saturn.paths.sync_preferences
services.<project>.backup.saturn_slug
intervals.neptune.register_refresh_sec
```

The referenced Volt values for the paths are normally `/api/v1/backups` and
`/dav/sync`. They are logical
Gateway paths, not physical Storage Box paths. Kernel URL, Kernel service token,
Saturn producer/device tokens, local project endpoints and local control tokens
remain installation secrets/configuration and must not be stored in Register.
Neptune uses conditional Register reads, validates and caches the reference
snapshot, and resolves the required keys through Kernel. Resolved values remain
in memory only, so a fresh process requires available Kernel and Volt. A changed
Register snapshot affects new work; it must not redirect an upload already in
progress.

## 2. Linux topology

Normal operators do not edit the registry or token files. In Saturn
Synchronization they create a 15-minute one-time setup code. Settings → Backup
→ **Initialize Neptune** passes it to Updater, installs a missing daemon or reuses
the existing one, and waits for terminal enrollment status. Updater also installs
required helpers automatically after head registration and Kernel configuration.
On first use it obtains `neptune.pem` from the selected HTTPS release, verifies
the signed manifest and pins the key locally. The service backup command provides
the CLI equivalent. `neptunectl doctor`
remains available for diagnostics.

There is exactly one `neptuned` process per Linux host, even when the host runs
several Exocortex projects. Saturn does not connect back to those hosts: every
agent polls the control plane over outbound HTTPS, so no inbound Neptune port is
required:

```text
Saturn Synchronization -> desired state + command queue in Saturn
                                      ^
                                      | outbound authenticated check-in
                                      |
                                  neptuned
                                      |-> Kernel Register
                                      |-> Saturn Backup API
                                      `-> Saturn WebDAV mirror root

neptuned archive worker -> private backup endpoint -> shared recovery ZIP builder
neptuned mirror worker  -> private mirror endpoint -> raw file or bounded ZIP tree
```

`neptuned` runs as a dedicated unprivileged user. Its daemon-owned project
registry maps a stable project ID to:

- the private local backup-export endpoint;
- a per-project local authentication token;
- the Saturn producer token reference;
- the Register key containing the Saturn service slug;
- the automatic-backup enabled flag and interval in whole hours.

Each project has its own Saturn producer identity and token. One compromised
project must not consume another project's quota or write to its namespace.
The Unix socket authorizes every request by project ID and a distinct control
token. No project may inspect or modify another project's schedule, credentials
or runs.

The installer registers or updates a project without starting a second daemon:

```text
sudo neptunectl register-project <project-id> <project-env-file>
```

The same logical project may run on several servers. Give every deployment its
own Saturn identity and Register slug key, for example
`services.kernel.backup.clients.vps_a.saturn_slug`; its local registration
selects that key. Never copy one producer token to every server merely to share
a slug. Stable client IDs make retries idempotent even when several deployments
start at the same time. Saturn's per-identity `maxConcurrentRuns` still protects
against accidental overlap and may be raised when an identity is intentionally
shared during migration.

Archive and mirror workers have independent schedules, run concurrently and use
separate Saturn credentials. The registry and durable SQLite journal live under `/var/lib/neptune`; secret
references live under `/etc/neptune` with restrictive ownership. Temporary ZIP
files use a bounded private spool. Logs go to journald and must redact tokens,
headers, local sensitive paths and archive content.

## 3. Project integration contract

Every project continues to own its logical backup format and restore semantics.
Neptune does not read project databases or construct project-specific archive
members.

Each project must expose one internal, non-browser backup endpoint reachable
only from the local host and protected by its Neptune token. The endpoint calls
the **same backup builder** as the existing manual action and streams the result
as `application/zip` with a safe filename, source version, schema identifier,
byte length and SHA-256 metadata.

The two flows are therefore:

```text
Manual:    shared backup builder -> unchanged ZIP -> browser download
Automatic: shared backup builder -> unchanged ZIP -> Neptune spool -> Saturn
Restore:   either ZIP -> existing inspect/restore workflow
```

Neptune must not add members, remove members, re-compress, encrypt, rename the
archive format or otherwise transform the ZIP. The SHA-256 stored in Saturn is
calculated over the exact bytes accepted by the existing manual restore path.
Automatic backups downloaded from Saturn must restore through that path without
conversion.

This exact-byte requirement means that current plaintext ZIP profiles use a
Saturn producer identity configured with `requireEncryption=false`. Neptune must
send `encrypted=false`; falsely declaring encryption is forbidden. If encrypted
archives are introduced later, the project must change its shared manual and
automatic builder together and retain versioned restore compatibility.

The automatic run state machine is durable:

```text
scheduled -> exporting -> spooled -> run-created -> uploading
          -> verifying -> complete
          -> retry-wait / failed / cancelled
```

Before creating a Saturn run, Neptune has a complete spool file, its exact size
and SHA-256. It uses a stable idempotency key, resumes from Saturn's reported
offset after restart and removes the spool only after receiving a complete
receipt. Disabling a schedule prevents new runs but does not corrupt or silently
discard an active upload.

### Saturn Synchronization UI

The existing project **Backup** section keeps:

1. **Manual backup** — the existing `Create and download snapshot` behavior,
   unchanged;
2. **Manual restore** — the existing inspect, confirm, restore and rollback
   behavior, unchanged;
3. **Local Neptune status and initialization/repair** — status is read through
   the module adapter and a one-time Saturn code is handed only to Updater.

Automatic pipelines are managed in Saturn's top-level **Synchronization** tab.
It separates recovery ZIP archives, dedicated Volt/Mastermind mirrors and
Windows folder synchronization; it owns scoped identity creation/revocation,
the authoritative schedules, explicit runs and per-agent Neptune release checks.

Saturn's desired state is authoritative. Neptune retains the last applied
revision locally, so a temporary Saturn outage does not stop an already enabled
schedule. Recommended default is disabled with a 24-hour interval; minimum
supported interval is one hour. Changing the interval does not interrupt an
active run. Enabling schedules the next run; it does not silently start one.

The legacy project-local facade may remain for diagnostics and compatibility,
but project Settings must not expose a second automatic schedule editor:

```text
GET  /api/neptune/status
PUT  /api/neptune/schedule       { enabled, interval_hours }
POST /api/neptune/runs           optional explicit automatic run
POST /api/neptune/update/check
POST /api/neptune/update/install
```

The browser never receives a Neptune control token or Saturn producer token.
Saturn queues commands, and the target agent receives them at its next check-in.

Neptune must not replace its root-owned executable from an unprivileged daemon.
The privileged host Updater installs the verified `neptune-linux-*` release
selected from `repositories.neptune.url`. It verifies the per-architecture
manifest and archive SHA-256, performs an atomic replacement, restarts the
daemon, checks its Unix-socket health endpoint and restores the previous binary
if the new daemon does not become healthy. Remote update commands cross a
separate local Unix-socket bridge protected by `/etc/neptune/updater-agent.token`;
that credential authorizes only Neptune Linux replacement and cannot update
arbitrary services.

## 4. Saturn Synchronization

Saturn exposes a top-level **Synchronization** workspace instead of placing
Neptune controls in Settings. It separates Linux recovery archives, Linux
dedicated mirrors and Windows directory synchronization. The workspace lists
identities, state, usage and last successful runs; creates one-time enrollment
codes and Windows passwords; revokes/rotates credentials; controls every remote
Linux schedule; starts explicit runs; and checks/queues verified Neptune updates.

Settings keeps only manual project snapshot download and restore. Current
interchangeable ZIP profiles remain explicitly configured with
`requireEncryption=false`.

Making the card directly visible does not weaken authorization. Creating,
rotating or revoking an identity still requires recent owner proof; list/status
remains available to the authenticated owner. One-time tokens remain visible
only in page memory and are never returned later.

## 5. Windows application

Neptune Windows is a per-user desktop application, not a Windows Service. The
current release is a self-contained portable ZIP and keeps secrets in the
current-user Windows credential protection scope. MSIX packaging and automatic
installation remain deferred until publisher signing is enabled. The user can
enable autostart for each profile independently; Neptune writes the current
portable executable path to the current user's Windows Run key and starts that
profile minimized at sign-in without requiring administrator rights.

While running, Neptune remains available in the Windows notification area. The
window close button hides it instead of stopping synchronization; the tray menu
can reopen the window, synchronize immediately, check for updates or explicitly
exit the process.

The primary window has a tested client size of **800 x 500 logical pixels** and
uses the common Exocortex visual language: black background, white text,
square one-pixel borders, Space Grotesk/monospace typography and Saturn's
current accent obtained from Saturn. `#00A8FF` is only the offline/default
fallback before the first successful preference synchronization.

The first release has one main panel:

```text
+------------------------------------------------------------------+
| Neptune planet   NEPTUNE                         Saturn: ready    |
|------------------------------------------------------------------|
| SYNC WITH SATURN                                                |
| One-way copy of selected local directories to the sync root     |
|                                                                  |
| C:\Work\Project A             Up to date       [Remove]          |
| D:\Photos                     Uploading 42%     [Pause]           |
|                                                                  |
| [Add local directory]                         [Sync now]          |
|------------------------------------------------------------------|
| 3 queued | Last success 14:20 | destination from Kernel Register |
+------------------------------------------------------------------+
```

Directory selection uses the native Windows folder picker. The Saturn origin
and base sync path come only from values resolved through the verified Kernel
Register snapshot and are
not editable text fields. A stable local mapping ID determines the subdirectory
below the registered `/dav/sync` base and prevents name collisions.

Each Windows profile must use its own scoped Saturn device token and a unique,
operator-selected destination such as `User PC`. Remote paths are
`sync/<destination>/<local-directory-name>/<relative-path>`. Neptune atomically
claims the destination with its stable client ID and rejects a folder already
owned by another client. Directories with the same final name cannot be added to
one profile.

Version 1 behavior is deliberately one-way:

- local creates and changes upload to Saturn;
- overwrites use Saturn versions and ETag preconditions;
- local deletion deletes the corresponding object inside the claimed Saturn
  destination, making each configured directory a one-way mirror;
- remote deletion/change never mutates local files;
- filesystem notifications are hints; periodic reconciliation and remote
  enumeration are the source of
  truth;
- locked or changing files retry after a stability check;
- junctions, symlinks and reparse points are not followed by default;
- the local SQLite journal survives restart and records upload offsets,
  checksums, mappings, retries and terminal failures.

The application, Start menu, taskbar, system tray, installer, notifications and
file associations use one Neptune planet icon family rendered at all required
Windows sizes. The taskbar icon also provides progress/error overlays where the
platform supports them. No Saturn artwork is recolored and reused as Neptune;
Neptune receives its own planet silhouette while sharing the accent token.

The accent is not compiled into runtime controls. Neptune reads the current
device-authenticated Saturn preference from the path published in Kernel Register
at connection time and during reconciliation, then updates WPF dynamic resources.

The Windows application reads `repositories.neptune.url` and follows only the
`neptune-windows-v*` release stream. Current portable releases are immutable
GitHub assets verified by the SHA-256 declared in the release manifest; automatic
portable updates can be started from the **Check updates** button or tray menu.
Neptune downloads the Windows release selected from the repository URL in Kernel
Register, verifies its manifest and SHA-256, replaces the application only after
the current process exits and then relaunches the same profile. Other profiles
using the same executable must be closed before replacement.

## 6. Required Saturn protocol work

Linux can use the existing Backup Ingest flow after these contract fixes:

- rename the semantic path parameter from `serviceId` to `serviceSlug`;
- advertise protocol version and maximum chunk size;
- resolve the `backups` role by its stable root ID rather than a literal path;
- allow the operator to configure `requireEncryption=false` for unchanged ZIPs.

Windows version 1 uses scoped, device-authenticated WebDAV over the existing
FileService. A future large-file optimization may add a resumable upload facade:

- capability/version discovery;
- paginated directory listing;
- create, HEAD, PATCH and complete upload operations;
- stable client idempotency key and ETag precondition;
- device-token rotation with bounded overlap;
- no Storage Box path or credential exposure.

All Saturn mutations continue through FileService, audit, versioning and
reconciliation. Neptune never talks directly to SFTP.

## 7. Implementation order and acceptance

1. Freeze Kernel Register keys and versioned Neptune/Saturn/project contracts.
2. Add the top-level Saturn Synchronization workspace and scoped identities for all three pipelines.
3. Add the shared internal backup endpoint and Neptune facade to one pilot
   project without changing its manual backup/restore behavior.
4. Implement the Linux daemon, multi-project registry, scheduler, SQLite journal
   and resumable Saturn client.
5. Extend the host Updater for the Neptune Linux release stream and expose
   version/update state in Saturn Synchronization.
6. Prove that a manually downloaded ZIP and an automatically uploaded/downloaded
   ZIP have the same format and both restore into a clean compatible instance.
7. Roll the integration contract through the remaining projects.
8. Implement and package the 800 x 500 Windows application, planet icon family,
   Register client, directory mappings and one-way sync engine.
9. Add Saturn's resumable device upload API before qualifying large-file Windows
   synchronization for production.

Release acceptance includes multi-project isolation, missed schedules, daemon
and machine restarts at every state, network interruption and resume, Saturn
pause/cancel, quota failures, token rotation/revocation, Register reference-cache
and broker-unavailable behavior, exact archive interchangeability, real clean restore, Windows locked
files and reparse points, long paths/case collisions, installer rollback,
accessibility at 800 x 500 and secret scans of logs, backups and release assets.

The current six-service deployment, trust, recovery and acceptance contract is documented in [Deployment readiness](DEPLOYMENT_READINESS.md).
