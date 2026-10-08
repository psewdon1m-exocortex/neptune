# Service-owned backup policy

The application Settings page is the only editor of its archive and, where enrolled, mirror schedule. Saturn keeps the authoritative revision. Its central inventory remains available for identities, quotas, enrollment and observation; it no longer edits application schedules or issues their manual backup commands.

The authenticated application backend calls the local Unix socket using its own `X-Neptune-Token`:

- `GET /v1/projects/{deployment}/policy`
- `PUT /v1/projects/{deployment}/policy`
- `GET /v1/projects/{deployment}/policy/runs` for run history; the daemon's
  legacy `POST` remains for already integrated producers, while current
  service-facing APIs reject new manual run requests.

Writes carry a stable `requestId` and `expectedRevision`. Neptune resolves Saturn through Kernel and relays the command with the enrolled producer credential. The returned schema is `exocortex.backup.policy.v1`. A successful authoritative write and a locally applied revision are separate observations. Retrying an uncertain response preserves the operation ID; a conflict requires review of the newer policy. Changing an interval does not issue a manual run.

Archive intervals are hours; existing mirror minute intervals are retained exactly. Volt and Mastermind present separate archive and mirror status with one switch and one hourly interval. Their `schedule-all` mutation updates both policies atomically; until the operator changes that control, an imported five-minute mirror is preserved exactly. The Settings field reports an inherited fractional-hour value and allows a new whole-hour value only after explicit editing.

Legacy unversioned schedule/run writers return 426. Upgrade Saturn before Neptune so the new agent can relay policy requests to an implemented upstream endpoint. Upgrade applications with protocol-aware policy UI before exposing the editor. Existing remote desired-state synchronization continues to carry authoritative revisions. The registry retains the existing schedule/next-run metadata and applies updates under its file lock with a durable atomic write. Active accepted transfers keep their execution identity.

Restored policy intent is paused. Resume verifies the enrolled archive source and, for a mirror, its source and destination before applying the new authoritative revision. An authenticated source HEAD must return `X-Neptune-Ready: 1`; HEAD verifies readiness without creating an archive. A failed or uncertain resume leaves the local restore journal intact. Newer restore intent cannot be cleared by an older in-flight request.

The complete grouped-service enrollment profile also requires distinct archive, mirror and resource-reader identities. Reader requests stay scoped to the enrolled root, resolve the current Saturn origin through Kernel, and enforce purpose, range and response-size bounds. Export generations are acknowledged only after remote commitment.

Service Settings can start a scoped Neptune unlink through the local Updater. The daemon first persists `Unlinking`, disables both schedules, and waits for accepted archive and mirror transfers. It abandons unfinished local spool runs for that project so a later enrollment cannot replay an old archive. Saturn then disables desired policy, pending commands and unused setup codes, invalidates the producer credential, and revokes mirror/reader devices while keeping stored archives and the reusable service identity. A hash-only disconnect receipt lets the revoked credential confirm an already completed disconnect after a lost response; it grants no archive access and is cleared at the next enrollment. Finally Neptune removes only that project registration and Updater invalidates its local credential files. A failed remote disconnect leaves the project paused for a retry; the shared daemon and other registrations remain active.

No version number alone proves compatibility. Before deployment, qualify the signed Neptune/Updater/Saturn tuple, including migration of the installed registration and SQLite state. Source tests do not replace Linux/systemd installation and rollback qualification.

## Selected pipeline capabilities

Volt and Mastermind may enroll archive, mirror or both. `NEPTUNE_BACKUP_AVAILABLE=false` persists `ArchiveAvailable=false` for mirror-only profiles: startup recovery, scheduled work and manual commands cannot start an archive, and desired policy cannot enable it. Policy declares `archive.available=false`; its absence means available for old paired profiles. Resume verifies only the enrolled sources and checks that local and Saturn capability profiles agree. Mirror-only Settings use `schedule` with `pipeline=mirror`; paired Settings retain `schedule-all`. Archive-only profiles receive no mirror/reader credentials. Mastermind reader credentials accompany its vault mirror.
