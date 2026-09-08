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
  reconciliation;
- `packaging/linux` contains the hardened systemd unit and installer;
- `packaging/windows` contains the MSIX manifest and complete planet icon family;
- `.github/workflows` publishes the two independent signed release streams.

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
| Windows | `neptune-windows-vMAJOR.MINOR.PATCH` | `neptune-windows-release.json` | signed `.msix` |

Linux and Windows product versions may advance independently. Both manifests
declare the Neptune protocol version, artifact SHA-256, size, architecture,
minimum supported OS and immutable release URL. Release signing and verification
must follow the same trust model as the existing Exocortex Updater releases.

Kernel Register contains the shared, non-secret coordinates:

```text
repositories.neptune.url
services.saturn.sni
services.saturn.port
services.saturn.paths.backup_ingest
services.saturn.paths.sync
services.<project>.backup.saturn_slug
intervals.neptune.register_refresh_sec
```

Recommended path values are `/api/v1/backups` and `/dav/sync`. They are logical
Gateway paths, not physical Storage Box paths. Kernel URL, Kernel service token,
Saturn producer/device tokens, local project endpoints and local control tokens
remain installation secrets/configuration and must not be stored in Register.
Neptune uses conditional Register reads, validates the snapshot checksum and
keeps a last-known-good snapshot. A changed Register snapshot affects new work;
it must not redirect an upload already in progress.

## 2. Linux topology

There is exactly one `neptuned` process per Linux host, even when the host runs
several Exocortex projects:

```text
Project Settings -> project backend -> authenticated Unix socket -> neptuned
                                                            |-> Kernel Register
                                                            `-> Saturn Backup API

neptuned -> private loopback project backup endpoint -> project backup builder
```

`neptuned` runs as a dedicated unprivileged user. Its root-owned project
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
neptune register-project <project-id> <project-env-file>
```

The same logical project may run on several servers. Give every deployment its
own Saturn identity and Register slug key, for example
`services.kernel.backup.clients.vps_a.saturn_slug`; its local registration
selects that key. Never copy one producer token to every server merely to share
a slug. Stable client IDs make retries idempotent even when several deployments
start at the same time. Saturn's per-identity `maxConcurrentRuns` still protects
against accidental overlap and may be raised when an identity is intentionally
shared during migration.

The registry and durable SQLite journal live under `/var/lib/neptune`; secret
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

### Required project Settings UI

The existing **Backup** section remains the operator surface. It contains:

1. **Manual backup** — the existing `Create and download snapshot` behavior,
   unchanged;
2. **Manual restore** — the existing inspect, confirm, restore and rollback
   behavior, unchanged;
3. **Automatic backup to Saturn** — Neptune availability, connected state,
   enabled toggle, positive integer interval in hours, last attempt, last
   success, next scheduled run and latest error;
4. **Neptune version** — installed Linux version, update availability, check
   action and update action;
5. optional **Back up to Saturn now** — starts the automatic/Saturn flow and is
   visually distinct from the manual download action.

The schedule is authoritative in Neptune's registry and is read/written through
the project backend. Recommended default is disabled with a 24-hour interval;
minimum supported interval is one hour. Changing the interval does not interrupt
an active run. Enabling schedules the next run; it does not silently start one.

Suggested project-backend facade:

```text
GET  /api/neptune/status
PUT  /api/neptune/schedule       { enabled, interval_hours }
POST /api/neptune/runs           optional explicit automatic run
POST /api/neptune/update/check
POST /api/neptune/update/install
```

Browser calls retain the project's normal operator authentication and CSRF
rules. The backend then calls Neptune over the local Unix socket. A browser
never receives a Neptune control token or Saturn producer token.

Neptune must not replace its root-owned executable from an unprivileged daemon.
The privileged host Updater installs the verified `neptune-linux-*` release
selected from `repositories.neptune.url`. It verifies the per-architecture
manifest and archive SHA-256, performs an atomic replacement, restarts the
daemon, checks its Unix-socket health endpoint and restores the previous binary
if the new daemon does not become healthy.

## 4. Saturn Settings change

Saturn moves **Backup producer identities** out of the collapsed auxiliary area
into a standalone, reorderable Settings card named **Backup connections**.

The card lists project name, immutable slug, enabled/revoked state, freshness,
stored usage, limits and last successful run. It supports create, token rotation
and revoke, and exposes `requireEncryption` explicitly so current interchangeable
ZIP profiles can be configured honestly.

Making the card directly visible does not weaken authorization. Creating,
rotating or revoking an identity still requires recent owner proof; list/status
remains available to the authenticated owner. One-time tokens remain visible
only in page memory and are never returned later.

## 5. Windows application

Neptune Windows is a per-user desktop application, not a Windows Service. It is
packaged as signed MSIX, starts at user logon when enabled and keeps secrets in
the current-user Windows credential protection scope.

The primary window has a tested client size of **800 x 500 logical pixels** and
uses the common Exocortex visual language: black background, white text,
square one-pixel borders, Space Grotesk/monospace typography and Saturn's
canonical accent `#00A8FF`.

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
and base sync path come only from the verified Kernel Register snapshot and are
not editable text fields. A stable local mapping ID determines the subdirectory
below the registered `/dav/sync` base and prevents name collisions.

Each Windows profile must use its own scoped Saturn device token. Remote paths
are always `<client_instance_id>/<mapping_id>/<relative_path>`, so two Windows
accounts, two PCs, or two named profiles on one PC cannot overwrite each other's
namespace unless an operator deliberately reuses their state directory.

Version 1 behavior is deliberately one-way:

- local creates and changes upload to Saturn;
- overwrites use Saturn versions and ETag preconditions;
- local deletion does not delete the Saturn copy;
- remote deletion/change never mutates local files;
- filesystem notifications are hints; periodic reconciliation is the source of
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

The Windows application reads `repositories.neptune.url` and follows only the
`neptune-windows-v*` release stream. Update installation uses signed MSIX update
semantics and never trusts a mutable branch or an unsigned downloaded binary.

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
2. Move Saturn producer identities into the standalone Backup connections card.
3. Add the shared internal backup endpoint and Neptune facade to one pilot
   project without changing its manual backup/restore behavior.
4. Implement the Linux daemon, multi-project registry, scheduler, SQLite journal
   and resumable Saturn client.
5. Extend the host Updater for the Neptune Linux release stream and expose
   version/update state in project Settings.
6. Prove that a manually downloaded ZIP and an automatically uploaded/downloaded
   ZIP have the same format and both restore into a clean compatible instance.
7. Roll the integration contract through the remaining projects.
8. Implement and package the 800 x 500 Windows application, planet icon family,
   Register client, directory mappings and one-way sync engine.
9. Add Saturn's resumable device upload API before qualifying large-file Windows
   synchronization for production.

Release acceptance includes multi-project isolation, missed schedules, daemon
and machine restarts at every state, network interruption and resume, Saturn
pause/cancel, quota failures, token rotation/revocation, Register last-known-good
behavior, exact archive interchangeability, real clean restore, Windows locked
files and reparse points, long paths/case collisions, installer rollback,
accessibility at 800 x 500 and secret scans of logs, backups and release assets.
